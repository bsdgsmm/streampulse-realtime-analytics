"""Bounded retries with exponential backoff and full jitter."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class TransientError(Exception):
    """A failure that is expected to succeed if the operation is repeated."""


class RetriesExhaustedError(Exception):
    def __init__(self, operation: str, attempts: int, last_error: BaseException) -> None:
        super().__init__(f"{operation} failed after {attempts} attempts: {last_error}")
        self.operation = operation
        self.attempts = attempts
        self.last_error = last_error


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 8.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("delays must not be negative")

    def backoff_ceiling(self, retry_number: int) -> float:
        """Upper bound of the delay before retry ``retry_number`` (1-based)."""
        return min(self.max_delay_seconds, self.base_delay_seconds * 2 ** (retry_number - 1))


def retry_call[T](
    func: Callable[[], T],
    policy: RetryPolicy,
    *,
    operation: str,
    retry_on: tuple[type[BaseException], ...] = (TransientError,),
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    """Call ``func`` until it succeeds, a non-retryable error occurs, or attempts run out."""
    jitter = rng or random.Random()
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return func()
        except retry_on as exc:
            if attempt == policy.max_attempts:
                raise RetriesExhaustedError(operation, attempt, exc) from exc
            delay = jitter.uniform(0, policy.backoff_ceiling(attempt))
            logger.warning(
                "Transient failure, retrying",
                extra={
                    "operation": operation,
                    "attempt": attempt,
                    "max_attempts": policy.max_attempts,
                    "delay_seconds": round(delay, 3),
                    "error": str(exc),
                },
            )
            if on_retry is not None:
                on_retry(attempt, exc, delay)
            sleep(delay)
    raise AssertionError("unreachable")
