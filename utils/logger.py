import datetime
from colorama import Fore, Style, init

# Initialize colorama to automatically reset styles
init(autoreset=True)

class Logger:
    """A simple logging wrapper using colorama for colored output."""

    def _get_timestamp(self) -> str:
        """Returns the current timestamp formatted for logging."""
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def log_info(self, msg: str):
        """Logs an informational message."""
        print(f"{Style.DIM}[{self._get_timestamp()}] INFO: {msg}")

    def log_warn(self, msg: str):
        """Logs a warning message in yellow."""
        print(f"{Fore.YELLOW}[{self._get_timestamp()}] WARN: {msg}")

    def log_error(self, msg: str):
        """Logs an error message in red."""
        print(f"{Fore.RED}[{self._get_timestamp()}] ERROR: {msg}")

    def log_market(self, ticker: str, bid: float, ask: float, spread: int):
        """Logs market data with colored prices for bid/ask."""
        timestamp = self._get_timestamp()
        bid_str = f"${bid:.2f}"
        ask_str = f"${ask:.2f}"
        spread_str = f"{spread}¢"

        print(
            f"[{timestamp}] {Fore.WHITE}{Style.BRIGHT}{ticker}{Style.RESET_ALL} | "
            f"BID: {Fore.CYAN}{bid_str}{Style.RESET_ALL} vs "
            f"ASK: {Fore.MAGENTA}{ask_str}{Style.RESET_ALL} | "
            f"Spread: {spread_str}"
        )

# Singleton instance for easy access across the application
logger = Logger()
