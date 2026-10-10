from __future__ import annotations

import pytest

from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ingestion.conflation import LatestByKey


def test_backoff_grows_exponentially_and_caps() -> None:
    policy = BackoffPolicy(initial_s=1.0, multiplier=2.0, max_s=10.0, jitter=0.0)
    assert [policy.delay_s(a, 0.5) for a in range(1, 7)] == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0]


def test_backoff_jitter_spreads_symmetrically() -> None:
    policy = BackoffPolicy(initial_s=10.0, max_s=10.0, jitter=0.2)
    assert policy.delay_s(1, 0.0) == pytest.approx(8.0)
    assert policy.delay_s(1, 0.5) == pytest.approx(10.0)
    assert policy.delay_s(1, 0.999999) == pytest.approx(12.0, abs=1e-4)


def test_backoff_honours_venue_minimum_and_survives_huge_attempts() -> None:
    policy = BackoffPolicy(initial_s=0.5, max_s=30.0, jitter=0.0)
    assert policy.delay_s(1, 0.5, minimum_s=15.0) == 15.0
    assert policy.delay_s(10_000, 0.5) == 30.0


@pytest.mark.parametrize(
    "kwargs",
    [{"initial_s": 0}, {"initial_s": 5, "max_s": 1}, {"multiplier": 0.5}, {"jitter": 1.0}],
)
def test_backoff_rejects_nonsense(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="backoff"):
        BackoffPolicy(**kwargs)


def test_attempts_are_one_based() -> None:
    with pytest.raises(ValueError, match="1-based"):
        BackoffPolicy().delay_s(0, 0.5)


def test_latest_by_key_keeps_only_newest_per_key_in_first_put_order() -> None:
    conflator: LatestByKey[str, int] = LatestByKey()
    for key, value in [("a", 1), ("b", 2), ("a", 3), ("c", 4), ("a", 5)]:
        conflator.put(key, value)
    assert len(conflator) == 3
    assert conflator.drain() == [5, 2, 4]
    assert conflator.drain() == []
