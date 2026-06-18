"""Structured errors for CLI and pipeline."""


class LaomaSignalEngineError(Exception):
    """Base engine error."""


class BinanceRequestError(LaomaSignalEngineError):
    """REST request to Binance failed after retries."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
