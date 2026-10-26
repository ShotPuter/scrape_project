"""Transport errors, independent of a particular API."""

from __future__ import annotations


class HTTPClientError(Exception):
    """An HTTP, network, or JSON decoding failure."""

    def __init__(
        self, message: str, status_code: int | None = None, retry_after: str | None = None, retryable: bool | None = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after
        self._retryable = retryable

    @property
    def retryable(self) -> bool:
        if self._retryable is not None:
            return self._retryable
        return self.status_code in {403, 407, 429} or (self.status_code is not None and self.status_code >= 500)
