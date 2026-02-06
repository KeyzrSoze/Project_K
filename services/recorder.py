import asyncio
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from collections import defaultdict
from services.db import ensure_training_dir, REPO_ROOT


class AsyncMarketRecorder:
    """
    Asynchronously records enriched market data to a partitioned Parquet dataset.
    Optimized for M4 performance with a 30-second flush interval and stateful time indexing.
    """

    def __init__(self, save_path: Optional[str] = None, flush_interval=30):
        """
        Initializes the recorder.
        Args:
            save_path (str): The root directory for the partitioned Parquet dataset.
            flush_interval (int): The interval in seconds to flush the buffer to disk.
        """
        if save_path is None:
            save_root = ensure_training_dir()
        else:
            save_root = Path(os.path.expanduser(save_path))
            if not save_root.is_absolute():
                # Interpret relative paths as repo-root-relative for portability.
                save_root = Path(REPO_ROOT) / save_root
            save_root = save_root.resolve(strict=False)
            save_root.mkdir(parents=True, exist_ok=True)

        self.save_path = str(save_root)
        self.flush_interval = flush_interval
        self.buffer: List[Dict] = []
        self.last_flush_time = time.time()
        # For stateful time_idx per ticker
        self.time_indexes = defaultdict(int)

    async def record(self, enriched_data: dict, category: str, series_ticker: str, status: str):
        """
        Appends an enriched data snapshot to the buffer with additional metadata.
        """
        ticker = enriched_data.get('ticker')
        if not ticker:
            return  # Skip records without a ticker

        # Increment time index for this specific ticker
        self.time_indexes[ticker] += 1

        # Create the final record to be stored
        record_to_append = {
            'time_idx': self.time_indexes[ticker],
            'category': category,
            'series_ticker': series_ticker,
            'status': status,
            **enriched_data
        }

        self.buffer.append(record_to_append)
        if time.time() - self.last_flush_time > self.flush_interval:
            await self.flush()

    async def flush(self):
        """
        Flushes the in-memory buffer to the Parquet dataset in a non-blocking manner.
        """
        if not self.buffer:
            return

        buffer_copy = self.buffer.copy()
        self.buffer.clear()
        self.last_flush_time = time.time()

        # Run the synchronous disk I/O operation in a separate thread
        await asyncio.to_thread(self._write_to_disk, buffer_copy)

    def _write_to_disk(self, data_batch: List[Dict]):
        """
        Converts the data batch to a pandas DataFrame and writes it to a partitioned
        Parquet dataset using date and category for partitioning.
        """
        if not data_batch:
            return

        df = pd.DataFrame(data_batch)

        # FIXED: Explicitly tell Pandas these are seconds (Unix Epoch), not nanoseconds.
        # This prevents the "Time Machine" bug where dates revert to 1970.
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

        # Create date column for partitioning
        df['date'] = df['timestamp'].dt.date

        try:
            # Convert to PyArrow Table for efficient writing
            table = pa.Table.from_pandas(df, preserve_index=False)

            # Write to partitioned Parquet dataset.
            # This creates a directory structure like:
            # /data/training/date=2026-02-01/category=CRYPTO/some-file.parquet
            pq.write_to_dataset(
                table,
                root_path=self.save_path,
                partition_cols=['date', 'category'],
                existing_data_behavior='overwrite_or_ignore',
                row_group_size=1000,
                compression='snappy'
            )
            print(
                f"Flushed {len(data_batch)} records to partitioned dataset at {self.save_path}")
        except Exception as e:
            print(f"[ERROR] Recorder failed to write partitioned data: {e}")

    async def close(self):
        """
        Ensures any remaining data in the buffer is flushed before shutting down.
        """
        await self.flush()
