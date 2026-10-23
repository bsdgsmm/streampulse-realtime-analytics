from __future__ import annotations

import random

import pytest

from streampulse.retry import RetriesExhaustedError, RetryPolicy, TransientError, retry_call


class Flaky:
    def __init__(self, failures: int, error: Exception | None = None) -> None:
        self.failures = failures
        self.calls = 0
        self.error = error or TransientError("temporary")

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return "ok"


def test_succeeds_after_transient_failures() -> None:
    sleeps: list[float] = []
    retries: list[int] = []
    func = Flaky(failures=2)
    result = retry_call(
        func,
        RetryPolicy(max_attempts=5, base_delay_seconds=1.0, max_delay_seconds=10.0),
        operation="test",
        sleep=sleeps.append,
        rng=random.Random(0),
        on_retry=lambda attempt, _exc, _delay: retries.append(attempt),
    )
    assert result == "ok"
    assert func.calls == 3
    assert retries == [1, 2]
    assert len(sleeps) == 2


def test_gives_up_after_max_attempts() -> None:
    func = Flaky(failures=10)
    with pytest.raises(RetriesExhaustedError) as info:
        retry_call(func, RetryPolicy(max_attempts=3), operation="insert", sleep=lambda _: None)
    assert func.calls == 3
    assert info.value.attempts == 3
    assert isinstance(info.value.last_error, TransientError)


def test_permanent_errors_are_not_retried() -> None:
    func = Flaky(failures=1, error=ValueError("bad data"))
    with pytest.raises(ValueError):
        retry_call(func, RetryPolicy(max_attempts=5), operation="insert", sleep=lambda _: None)
    assert func.calls == 1


def test_backoff_grows_exponentially_and_is_capped() -> None:
    policy = RetryPolicy(max_attempts=10, base_delay_seconds=0.5, max_delay_seconds=3.0)
    assert [policy.backoff_ceiling(n) for n in range(1, 6)] == [0.5, 1.0, 2.0, 3.0, 3.0]


def test_jittered_delays_stay_within_ceiling() -> None:
    sleeps: list[float] = []
    policy = RetryPolicy(max_attempts=6, base_delay_seconds=0.5, max_delay_seconds=2.0)
    with pytest.raises(RetriesExhaustedError):
        retry_call(
            Flaky(failures=100), policy, operation="x", sleep=sleeps.append, rng=random.Random(1)
        )
    assert len(sleeps) == 5
    for retry_number, delay in enumerate(sleeps, start=1):
        assert 0 <= delay <= policy.backoff_ceiling(retry_number)


def test_policy_validation() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(base_delay_seconds=-1)
