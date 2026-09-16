import logging

import pytest

from cstar.base.env import ENV_CSTAR_ORCH_MAX_CONC, max_concurrency


def test_max_concurrency_default_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the documented default (10) is returned when the env var is unset."""
    monkeypatch.delenv(ENV_CSTAR_ORCH_MAX_CONC, raising=False)

    assert max_concurrency() == 10


def test_max_concurrency_parses_valid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify a valid integer value is parsed and returned as-is."""
    monkeypatch.setenv(ENV_CSTAR_ORCH_MAX_CONC, "42")

    assert max_concurrency() == 42


def test_max_concurrency_falls_back_on_non_integer(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a non-integer value falls back to the default with a logged warning."""
    monkeypatch.setenv(ENV_CSTAR_ORCH_MAX_CONC, "not-a-number")

    with caplog.at_level(logging.WARNING):
        value = max_concurrency()

    assert value == 10
    assert "Unable to parse" in caplog.text


@pytest.mark.parametrize("bad_value", ["0", "-1", "-100"])
def test_max_concurrency_falls_back_on_non_positive(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    bad_value: str,
) -> None:
    """Verify a zero or negative value falls back to the default with a logged warning.

    Parameters
    ----------
    bad_value : str
        A parseable but invalid (non-positive) configured value.
    """
    monkeypatch.setenv(ENV_CSTAR_ORCH_MAX_CONC, bad_value)

    with caplog.at_level(logging.WARNING):
        value = max_concurrency()

    assert value == 10
    assert "invalid" in caplog.text
