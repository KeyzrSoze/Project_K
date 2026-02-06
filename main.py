import asyncio
import faulthandler
import os
import signal
import sqlite3
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import List, Optional, Tuple

from pydantic import ValidationError

from utils.logger import logger

faulthandler.enable()
try:
    from config import AppConfig
    from services.feed import MarketFeed
    from services.execution import OrderExecutor
    from services.accounting import PortfolioTracker
    from strategies.momentum import MomentumStrategy
    from services.harvester import MarketHarvester
    from services.recorder import AsyncMarketRecorder
    from services.features import FeatureExtractor
    from services.db import DatabaseManager, DB_PATH
except ImportError as e:
    logger.log_error(f"FATAL: A required project file could not be imported: {e}")
    raise


async def amain():
    """The main asynchronous entry point for the application."""
    logger.log_info("--- Starting Project K-Alpha (Phase 4: AI-Data) ---")
    logger.log_info(
        f"[Main] Startup: pid={os.getpid()}, cwd={os.getcwd()}, db={DB_PATH}"
    )

    shutdown_event = asyncio.Event()
    exit_reason = "normal"

    loop = asyncio.get_running_loop()

    def _asyncio_exception_handler(_loop, context):
        msg = context.get("message", "Asyncio exception")
        exc = context.get("exception")
        if exc is not None:
            tb = "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            )
            logger.log_error(f"[Main] Asyncio exception: {msg}\n{tb}")
        else:
            logger.log_error(f"[Main] Asyncio exception: {msg}")

    loop.set_exception_handler(_asyncio_exception_handler)

    def _signal_handler(signum: int):
        nonlocal exit_reason
        exit_reason = f"signal {signum}"
        logger.log_warn(f"[Main] Signal received: {signum}. Initiating shutdown.")
        shutdown_event.set()

    def _dump_stacks_handler(signum: int):
        logger.log_warn(
            f"[Main] Signal received: {signum}. Dumping stack traces (all threads)."
        )
        faulthandler.dump_traceback(all_threads=True)

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _signal_handler, sig)
            except NotImplementedError:
                signal.signal(sig, lambda s, f: _signal_handler(s))

        # On-demand stack dump for diagnosing "hung but alive" states.
        if hasattr(signal, "SIGUSR1"):
            try:
                loop.add_signal_handler(
                    signal.SIGUSR1, _dump_stacks_handler, signal.SIGUSR1
                )
            except NotImplementedError:
                signal.signal(signal.SIGUSR1, lambda s, f: _dump_stacks_handler(s))
    except Exception as e:
        logger.log_warn(f"[Main] Signal handler setup failed: {e}")

    recorder: Optional[AsyncMarketRecorder] = None
    heartbeat_task: Optional[asyncio.Task] = None
    feed_connect_lock = asyncio.Lock()
    api_executor: Optional[ThreadPoolExecutor] = None
    db_write_executor: Optional[ThreadPoolExecutor] = None
    db_read_executor: Optional[ThreadPoolExecutor] = None
    db_health_executor: Optional[ThreadPoolExecutor] = None

    # Harvester supervision
    harvester_generation: int = 0
    current_harvester_task: Optional[asyncio.Task] = None
    current_harvester: Optional[MarketHarvester] = None
    harvester_tasks: List[asyncio.Task] = []
    harvester_restart_lock = asyncio.Lock()
    restart_after_ts: float = 0.0
    restart_reason: str = ""

    # Heartbeat / watchdog tuning
    HEARTBEAT_INTERVAL_S: float = 30.0
    METRICS_STALE_THRESHOLD_S: float = 180.0
    METRICS_STALE_CONSECUTIVE_HEARTBEATS: int = 2
    FORCE_EXIT_STALE_S: float = 600.0
    TIMEOUT_WINDOW_THRESHOLD: int = 10  # timeouts per ~120s window
    CIRCUIT_BREAKER_COOLDOWN_S: float = 60.0
    DB_HEALTH_TIMEOUT_S: float = 5.0
    DB_HEALTH_CONSECUTIVE_FAILURES_THRESHOLD: int = 3
    DB_HEALTH_FORCE_EXIT_UNHEALTHY_S: float = 600.0
    DB_CIRCUIT_BREAKER_COOLDOWN_S: float = 60.0

    try:
        config = AppConfig()
        db = DatabaseManager()

        # Isolate API calls and DB work in separate executors.
        API_EXECUTOR_WORKERS: int = 6
        try:
            DB_READ_EXECUTOR_WORKERS: int = int(
                os.getenv("PROJECT_K_DB_READ_WORKERS") or "4"
            )
        except ValueError:
            logger.log_warn(
                f"[Main] Invalid PROJECT_K_DB_READ_WORKERS={os.getenv('PROJECT_K_DB_READ_WORKERS')!r}; defaulting to 4."
            )
            DB_READ_EXECUTOR_WORKERS = 4
        api_executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=API_EXECUTOR_WORKERS, thread_name_prefix="pk_api"
        )
        db_write_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pk_dbw"
        )
        db_read_executor = ThreadPoolExecutor(
            max_workers=max(1, DB_READ_EXECUTOR_WORKERS), thread_name_prefix="pk_dbr"
        )
        db_health_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pk_dbh"
        )

        class _InflightCounters:
            inflight_reads: int = 0
            inflight_writes: int = 0

        inflight = _InflightCounters()

        # Initialize services
        feed = MarketFeed(
            config,
            logger,
            api_executor=api_executor,
            db_read_executor=db_read_executor,
            inflight_counters=inflight,
        )
        strategy = MomentumStrategy()
        tracker = PortfolioTracker()
        recorder = AsyncMarketRecorder()
        feature_extractor = FeatureExtractor()
        executor: Optional[OrderExecutor] = None

        tickers: List[str] = []
        last_ticker_refresh_time: float = 0.0
        TICKER_REFRESH_INTERVAL: float = 60.0
        ORDERBOOK_POLL_INTERVAL: float = 2.0

        def _is_current(gen: int) -> bool:
            return (not shutdown_event.is_set()) and gen == harvester_generation

        def _harvester_state(task: Optional[asyncio.Task]) -> str:
            if task is None:
                return "none"
            if task.cancelled():
                return "cancelled"
            if task.done():
                return "done"
            return "running"

        def _get_metrics_max_info_sync() -> Tuple[float, str]:
            return db.get_market_metrics_max_info(role="health")

        async def _get_metrics_max_info() -> Tuple[float, str]:
            fut = loop.run_in_executor(db_health_executor, _get_metrics_max_info_sync)
            return await asyncio.wait_for(fut, timeout=DB_HEALTH_TIMEOUT_S)

        def _thread_counts() -> Tuple[int, int, int, int, int]:
            threads = threading.enumerate()
            total = len(threads)
            api_threads = sum(1 for t in threads if t.name.startswith("pk_api"))
            dbr_threads = sum(1 for t in threads if t.name.startswith("pk_dbr"))
            dbw_threads = sum(1 for t in threads if t.name.startswith("pk_dbw"))
            dbh_threads = sum(1 for t in threads if t.name.startswith("pk_dbh"))
            return total, api_threads, dbr_threads, dbw_threads, dbh_threads

        async def _start_or_restart_harvester(reason: str) -> None:
            nonlocal harvester_generation, current_harvester_task, current_harvester

            async with harvester_restart_lock:
                if shutdown_event.is_set():
                    return

                if not feed.is_connected or not feed.api_client:
                    logger.log_warn(
                        f"[Main] Harvester start requested but feed not connected (reason={reason})."
                    )
                    return

                old_task = current_harvester_task

                harvester_generation += 1
                gen = harvester_generation
                logger.log_warn(
                    f"[Main] Starting MarketHarvester gen={gen} (reason={reason})."
                )

                harvester = MarketHarvester(
                    feed.api_client,
                    logger,
                    generation=gen,
                    is_current=_is_current,
                    api_executor=api_executor,
                    db_write_executor=db_write_executor,
                    db_read_executor=db_read_executor,
                    inflight_counters=inflight,
                )
                current_harvester = harvester
                new_task = asyncio.create_task(harvester.run_harvest_loop())
                new_task.add_done_callback(
                    lambda t, g=gen: _on_harvester_done(t, g)
                )

                harvester_tasks.append(new_task)
                current_harvester_task = new_task

                if old_task and not old_task.done():
                    old_task.cancel()
                    try:
                        await asyncio.wait_for(old_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.log_error(
                            f"[Main] Previous MarketHarvester did not cancel within 5s (old_gen<{gen})."
                        )
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logger.log_error(
                            f"[Main] Error awaiting previous MarketHarvester cancellation: {e}"
                        )

        def _on_harvester_done(task: asyncio.Task, gen: int) -> None:
            nonlocal restart_after_ts, restart_reason, current_harvester

            if shutdown_event.is_set():
                return

            # If this isn't the active generation, it's expected to stop.
            if gen != harvester_generation:
                logger.log_info(
                    f"[Main] MarketHarvester gen={gen} ended (stale)."
                )
                return
            current_harvester = None

            if task.cancelled():
                logger.log_warn(
                    f"[Main] MarketHarvester gen={gen} cancelled."
                )
            else:
                exc = task.exception()
                if exc:
                    tb = "".join(
                        traceback.format_exception(type(exc), exc, exc.__traceback__)
                    )
                    logger.log_error(
                        f"[Main] MarketHarvester gen={gen} crashed: {exc}\n{tb}"
                    )
                else:
                    logger.log_warn(
                        f"[Main] MarketHarvester gen={gen} ended unexpectedly."
                    )

            # Schedule a restart shortly (heartbeat loop will execute it).
            if restart_after_ts <= 0.0:
                restart_after_ts = time.time() + 5.0
                restart_reason = "harvester ended"

        circuit_breaker_last_ts: float = 0.0
        circuit_breaker_reinit_count: int = 0
        stale_heartbeat_hits: int = 0
        stale_start_ts: float = 0.0
        db_circuit_breaker_last_ts: float = 0.0
        db_circuit_breaker_reinit_count: int = 0
        db_health_consecutive_failures: int = 0
        last_successful_metrics_read_mono: float = time.monotonic()
        last_db_health_failure_repr: str = ""
        last_db_health_failure_kind: str = ""

        async def _reinit_client_and_executors(reason: str) -> None:
            nonlocal api_executor, executor, circuit_breaker_last_ts, circuit_breaker_reinit_count

            now = time.time()
            if now - circuit_breaker_last_ts < CIRCUIT_BREAKER_COOLDOWN_S:
                logger.log_warn(
                    f"[CircuitBreaker] Suppressing reinit (reason={reason}) due to cooldown "
                    f"({now - circuit_breaker_last_ts:.1f}s since last)."
                )
                return

            circuit_breaker_last_ts = now
            circuit_breaker_reinit_count += 1

            logger.log_error(
                f"[CircuitBreaker] Triggered (reason={reason}). Reinitializing Kalshi client + API executor "
                f"(attempt={circuit_breaker_reinit_count})."
            )

            # Cancel active harvester first (it may be stuck in an executor thread).
            if current_harvester_task and not current_harvester_task.done():
                current_harvester_task.cancel()
                with suppress(asyncio.CancelledError):
                    try:
                        await asyncio.wait_for(current_harvester_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.log_error(
                            "[CircuitBreaker] Harvester did not cancel within 5s; continuing with reinit."
                        )

            # Replace the API executor to avoid starvation from stuck threads.
            old_api_executor = api_executor
            api_executor = ThreadPoolExecutor(
                max_workers=API_EXECUTOR_WORKERS, thread_name_prefix="pk_api"
            )
            try:
                old_api_executor.shutdown(wait=False, cancel_futures=True)
            except Exception as e:
                logger.log_warn(f"[CircuitBreaker] Failed to shutdown old API executor: {e}")

            feed.set_executors(api_executor=api_executor)

            # Reconnect the SDK client.
            async with feed_connect_lock:
                feed.is_connected = False
                ok = await feed.connect()
                if not ok or not feed.api_client:
                    logger.log_error("[CircuitBreaker] Feed reconnect failed; will retry in main loop.")
                    return

                executor = OrderExecutor(feed.api_client, logger, tracker)

            await _start_or_restart_harvester(f"circuit breaker: {reason}")

        async def _reinit_db_write_executor_and_connections(reason: str) -> None:
            nonlocal db_write_executor, db_health_executor, db_circuit_breaker_last_ts, db_circuit_breaker_reinit_count

            now = time.time()
            if now - db_circuit_breaker_last_ts < DB_CIRCUIT_BREAKER_COOLDOWN_S:
                logger.log_warn(
                    f"[DBCircuitBreaker] Suppressing reinit (reason={reason}) due to cooldown "
                    f"({now - db_circuit_breaker_last_ts:.1f}s since last)."
                )
                return

            db_circuit_breaker_last_ts = now
            db_circuit_breaker_reinit_count += 1

            logger.log_error(
                f"[DBCircuitBreaker] Triggered (reason={reason}). Reinitializing DB write executor + connection cache "
                f"(attempt={db_circuit_breaker_reinit_count})."
            )

            if current_harvester_task and not current_harvester_task.done():
                current_harvester_task.cancel()
                with suppress(asyncio.CancelledError):
                    try:
                        await asyncio.wait_for(current_harvester_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.log_error(
                            "[DBCircuitBreaker] Harvester did not cancel within 5s; continuing with DB reinit."
                        )

            old_db_write_executor = db_write_executor
            db_write_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="pk_dbw"
            )
            if old_db_write_executor:
                try:
                    old_db_write_executor.shutdown(wait=False, cancel_futures=True)
                except Exception as e:
                    logger.log_warn(
                        f"[DBCircuitBreaker] Failed to shutdown old DB write executor: {e}"
                    )

            old_db_health_executor = db_health_executor
            db_health_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="pk_dbh"
            )
            if old_db_health_executor:
                try:
                    old_db_health_executor.shutdown(wait=False, cancel_futures=True)
                except Exception as e:
                    logger.log_warn(
                        f"[DBCircuitBreaker] Failed to shutdown old DB health executor: {e}"
                    )

            try:
                DatabaseManager.reset_connection_cache()
            except Exception as e:
                logger.log_warn(
                    f"[DBCircuitBreaker] Failed to reset DB connection cache: {e}"
                )

            await _start_or_restart_harvester(f"db circuit breaker: {reason}")

        async def _heartbeat_loop() -> None:
            nonlocal restart_after_ts, restart_reason, stale_heartbeat_hits, stale_start_ts
            nonlocal db_health_consecutive_failures, last_successful_metrics_read_mono, last_db_health_failure_repr, last_db_health_failure_kind

            while not shutdown_event.is_set():
                now = time.time()

                max_ts = 0.0
                max_local = "n/a"
                metrics_read_start_mono = time.monotonic()
                try:
                    max_ts, max_local = await _get_metrics_max_info()
                    db_health_consecutive_failures = 0
                    last_successful_metrics_read_mono = time.monotonic()
                    last_db_health_failure_repr = ""
                    last_db_health_failure_kind = ""
                except asyncio.TimeoutError as e:
                    elapsed_s = time.monotonic() - metrics_read_start_mono
                    db_health_consecutive_failures += 1
                    last_db_health_failure_kind = "timeout"
                    last_db_health_failure_repr = repr(e)
                    logger.log_warn(
                        f"[Heartbeat] DB health check timed out after {elapsed_s:.2f}s: {repr(e)}"
                    )
                except sqlite3.OperationalError as e:
                    elapsed_s = time.monotonic() - metrics_read_start_mono
                    db_health_consecutive_failures += 1
                    last_db_health_failure_kind = "sqlite3.OperationalError"
                    last_db_health_failure_repr = repr(e)
                    logger.log_warn(
                        f"[Heartbeat] DB health check failed after {elapsed_s:.2f}s (locked/busy): {repr(e)}"
                    )
                except Exception as e:
                    elapsed_s = time.monotonic() - metrics_read_start_mono
                    db_health_consecutive_failures += 1
                    last_db_health_failure_kind = type(e).__name__
                    last_db_health_failure_repr = repr(e)
                    logger.log_warn(
                        f"[Heartbeat] DB health check failed after {elapsed_s:.2f}s: {repr(e)}"
                    )

                stale_s: Optional[float] = None
                if max_ts:
                    stale_s = max(0.0, now - max_ts)

                # Track consecutive stale heartbeats for circuit breaking.
                if stale_s is not None and stale_s > METRICS_STALE_THRESHOLD_S:
                    stale_heartbeat_hits += 1
                    if stale_start_ts <= 0.0:
                        stale_start_ts = now
                else:
                    stale_heartbeat_hits = 0
                    stale_start_ts = 0.0

                h_state = _harvester_state(current_harvester_task)
                stale_str = f"{stale_s:.0f}" if stale_s is not None else "n/a"
                db_health_age_s = time.monotonic() - last_successful_metrics_read_mono
                db_health_last = last_db_health_failure_kind or "ok"

                timeouts_120s = 0
                consecutive_timeouts = 0
                if current_harvester and current_harvester.generation == harvester_generation:
                    try:
                        timeouts_120s, consecutive_timeouts = current_harvester.get_timeout_stats()
                    except Exception:
                        timeouts_120s, consecutive_timeouts = 0, 0

                total_threads, api_threads, dbr_threads, dbw_threads, dbh_threads = _thread_counts()

                logger.log_info(
                    f"[Heartbeat] pid={os.getpid()} db={DB_PATH} "
                    f"max_ts={max_ts:.0f} ({max_local}) stale_s={stale_str} "
                    f"api_timeouts_120s={timeouts_120s} consecutive_timeouts={consecutive_timeouts} "
                    f"db_health_age_s={db_health_age_s:.0f} db_health_failures={db_health_consecutive_failures} db_health_last={db_health_last} "
                    f"inflight_reads={inflight.inflight_reads} inflight_writes={inflight.inflight_writes} "
                    f"threads={total_threads} api_threads={api_threads} "
                    f"dbr_threads={dbr_threads} dbw_threads={dbw_threads} dbh_threads={dbh_threads} "
                    f"active_tickers={len(tickers)} harvester_state={h_state} harvester_gen={harvester_generation}"
                )

                if db_health_consecutive_failures >= DB_HEALTH_CONSECUTIVE_FAILURES_THRESHOLD:
                    await _reinit_db_write_executor_and_connections(
                        f"{db_health_consecutive_failures} consecutive DB health failures "
                        f"({last_db_health_failure_kind}: {last_db_health_failure_repr})"
                    )
                    db_health_consecutive_failures = 0
                    last_db_health_failure_kind = ""
                    last_db_health_failure_repr = ""

                if (
                    db_circuit_breaker_reinit_count > 0
                    and db_health_age_s > DB_HEALTH_FORCE_EXIT_UNHEALTHY_S
                ):
                    logger.log_error(
                        f"[Watchdog] DB health check has not succeeded for >{DB_HEALTH_FORCE_EXIT_UNHEALTHY_S:.0f}s "
                        f"after DB reinit attempts (attempts={db_circuit_breaker_reinit_count}). "
                        "Forcing hard exit for external supervisor restart."
                    )
                    os._exit(2)

                # Restart if the active harvester task is missing/done.
                if feed.is_connected and (
                    current_harvester_task is None or current_harvester_task.done()
                ):
                    if restart_after_ts <= 0.0:
                        restart_after_ts = now + 1.0
                        restart_reason = "harvester missing/done"

                # Circuit breaker: too many API timeouts OR stale metrics for N consecutive heartbeats.
                if feed.is_connected and timeouts_120s >= TIMEOUT_WINDOW_THRESHOLD:
                    await _reinit_client_and_executors(
                        f"api timeouts >= {TIMEOUT_WINDOW_THRESHOLD} in 120s"
                    )
                    restart_after_ts = 0.0
                    restart_reason = ""
                elif (
                    feed.is_connected
                    and stale_heartbeat_hits >= METRICS_STALE_CONSECUTIVE_HEARTBEATS
                ):
                    await _reinit_client_and_executors(
                        f"market_metrics stale for {stale_heartbeat_hits} heartbeats"
                    )
                    restart_after_ts = 0.0
                    restart_reason = ""

                # Hard fail if staleness persists for too long even after reinit attempts.
                if (
                    feed.is_connected
                    and stale_start_ts > 0.0
                    and stale_s is not None
                    and stale_s > METRICS_STALE_THRESHOLD_S
                    and circuit_breaker_reinit_count > 0
                    and (now - stale_start_ts) > FORCE_EXIT_STALE_S
                ):
                    logger.log_error(
                        f"[Watchdog] market_metrics stale for >{FORCE_EXIT_STALE_S:.0f}s after reinit attempts. "
                        "Forcing hard exit for external supervisor restart."
                    )
                    os._exit(2)

                # Execute any scheduled restart.
                if restart_after_ts and now >= restart_after_ts:
                    await _start_or_restart_harvester(restart_reason or "scheduled")
                    restart_after_ts = 0.0
                    restart_reason = ""

                await asyncio.sleep(HEARTBEAT_INTERVAL_S)

        # Start heartbeat/watchdog in the background.
        heartbeat_task = asyncio.create_task(_heartbeat_loop())

        def _on_heartbeat_done(task: asyncio.Task) -> None:
            if shutdown_event.is_set():
                return
            if task.cancelled():
                logger.log_warn("[Main] Heartbeat task cancelled.")
                return
            exc = task.exception()
            if exc:
                tb = "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                )
                logger.log_error(f"[Main] Heartbeat task crashed: {exc}\n{tb}")
                # Recreate heartbeat if it ever dies.
                nonlocal heartbeat_task
                heartbeat_task = asyncio.create_task(_heartbeat_loop())
                heartbeat_task.add_done_callback(_on_heartbeat_done)

        heartbeat_task.add_done_callback(_on_heartbeat_done)

        while True:
            if shutdown_event.is_set():
                logger.log_info("[Main] Shutdown event set. Exiting main loop.")
                break

            try:
                # 1) Connect and initialize services
                if not feed.is_connected:
                    async with feed_connect_lock:
                        ok = await feed.connect()
                    if not ok:
                        logger.log_warn("Connection failed. Retrying in 15 seconds...")
                        await asyncio.sleep(15)
                        continue

                    logger.log_info("Connection successful. Initializing services.")
                    executor = OrderExecutor(feed.api_client, logger, tracker)
                    await _start_or_restart_harvester("feed connected")

                # 2) Refresh tickers
                now = time.time()
                if (not tickers) or (now - last_ticker_refresh_time > TICKER_REFRESH_INTERVAL):
                    logger.log_info("Refreshing ticker list from database...")
                    try:
                        tickers = await feed.get_active_tickers()
                    except Exception as e:
                        logger.log_warn(f"Ticker refresh failed (transient): {e}")
                        await asyncio.sleep(10)
                        continue

                    last_ticker_refresh_time = now
                    if not tickers:
                        logger.log_warn("Could not refresh tickers. Retrying in 60s.")
                        await asyncio.sleep(60)
                        continue
                    logger.log_info(f"Scanner Update: Tracking {len(tickers)} markets.")

                # 3) Poll, Enrich, Record, and Analyze
                for ticker in tickers:
                    if shutdown_event.is_set():
                        break

                    market_data = await feed.poll_orderbook(ticker)
                    if market_data and market_data.get("bid") is not None:
                        enriched_data = feature_extractor.transform(market_data)

                        await recorder.record(
                            enriched_data=enriched_data,
                            category=market_data["category"],
                            series_ticker=market_data["series_ticker"],
                            status=market_data["status"],
                        )

                        tracker.update_valuation(ticker, enriched_data["bid"])

                        if executor:
                            try:
                                signal_out = strategy.analyze_ticker(enriched_data)
                                if signal_out:
                                    logger.log_info(
                                        f"STRATEGY SIGNAL for {ticker}: {signal_out}"
                                    )
                                    await executor.place_limit_order(
                                        ticker=ticker,
                                        side=signal_out["side"],
                                        count=1,
                                        price=signal_out["price"],
                                    )
                            except Exception as e:
                                logger.log_error(
                                    f"Error during strategy/execution for {ticker}: {e}"
                                )

                # 4) Log portfolio summary
                logger.log_info(tracker.get_portfolio_summary())

                # 5) Wait for next cycle
                await asyncio.sleep(ORDERBOOK_POLL_INTERVAL)

            except Exception as e:
                tb = traceback.format_exc()
                logger.log_error(f"[Main] Main loop error: {e}\n{tb}")
                logger.log_info("Resetting connection and restarting loop in 10 seconds...")
                feed.is_connected = False
                executor = None
                await asyncio.sleep(10)

    finally:
        shutdown_event.set()

        if heartbeat_task:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

        for task in list(harvester_tasks):
            task.cancel()
        for task in list(harvester_tasks):
            with suppress(asyncio.CancelledError):
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                except Exception:
                    pass

        # Executors: don't wait for hung threads (we enforce HTTP timeouts + circuit breaker).
        if api_executor:
            with suppress(Exception):
                api_executor.shutdown(wait=False, cancel_futures=True)
        if db_read_executor:
            with suppress(Exception):
                db_read_executor.shutdown(wait=False, cancel_futures=True)
        if db_write_executor:
            with suppress(Exception):
                db_write_executor.shutdown(wait=False, cancel_futures=True)
        if db_health_executor:
            with suppress(Exception):
                db_health_executor.shutdown(wait=False, cancel_futures=True)

        if recorder:
            logger.log_info("Flushing any remaining data before exit...")
            await recorder.close()

        logger.log_info(f"[Main] Exiting (reason: {exit_reason}).")


if __name__ == "__main__":
    logger.log_info("[Main] Process starting.")
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        logger.log_info("[Main] KeyboardInterrupt received. Exiting.")
    except Exception as e:
        tb = traceback.format_exc()
        logger.log_error(f"[Main] Fatal, unhandled exception: {e}\n{tb}")
    finally:
        logger.log_info("[Main] Process exiting.")
