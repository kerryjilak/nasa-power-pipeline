"""Shared test setup."""

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_for_tests():
    """Run every flow in the tests against a throwaway Prefect database."""
    with prefect_test_harness():
        yield

def make_payload(days: int = 3, **changes) -> dict:
    """Build a response shaped like NASA POWER's, with optional changed fields.

    Pass a changed field as a list, one value per day, e.g. T2M=[26.0, 99.0, 27.0].
    """
    dates = [f"202601{day:02d}" for day in range(1, days + 1)]
    values = {
        "T2M": [26.0 + day * 0.5 for day in range(days)],
        "RH2M": [80.0 + day * 0.5 for day in range(days)],
        "PRECTOTCORR": [0.0, 2.5, 0.4][:days] + [0.0] * max(0, days - 3),
        "WS10M": [1.5 + day * 0.5 for day in range(days)],
    }
    values.update(changes)
    parameter = {name: dict(zip(dates, column)) for name, column in values.items()}
    return {"properties": {"parameter": parameter}}
