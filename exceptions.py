"""Shared crawl exceptions."""


class SessionExpiredError(Exception):
    """Playwright session missing or login wall detected."""

    def __init__(self, source: str, message: str):
        self.source = source
        self.message = message
        super().__init__(message)
