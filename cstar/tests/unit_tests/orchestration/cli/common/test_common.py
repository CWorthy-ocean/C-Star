from unittest import mock

import pytest
import typer

from cstar.cli.cache.promote import (
    key_callback,
    runid_callback,
)


@pytest.mark.parametrize(
    ("input_value", "exp_value"),
    [
        pytest.param("abc", "abc", id="happy path"),
        pytest.param(" 123", "123", id="leading whitespace"),
        pytest.param("xyz ", "xyz", id="trailing whitespace"),
        pytest.param("  pqr   ", "pqr", id="surrounding whitespace"),
        pytest.param(" a\n\t", "a", id="control characters"),
    ],
)
@pytest.mark.asyncio
async def test_cli_cache_common_runid_callback(
    input_value: str, exp_value: str
) -> None:
    """Verify that `runid_callback` strips whitespace from inputs and returns
    the cleaned value.

    Parameters
    ----------
    input_value : str
        Parameterized inputs for the target function.
    exp_value : str
        Parameterized output values expected after executing the target function.
    """
    mock_ctx = mock.MagicMock(spec=typer.Context)
    actual_run_id = runid_callback(mock_ctx, input_value)

    # confirm the run-id is returned with any callback cleaning appleid
    assert actual_run_id == exp_value


@pytest.mark.parametrize(
    ("input_value", "exp_value"),
    [
        pytest.param("abc", "abc", id="happy path"),
        pytest.param(" 123", "123", id="leading whitespace"),
        pytest.param("xyz ", "xyz", id="trailing whitespace"),
        pytest.param("  pqr   ", "pqr", id="surrounding whitespace"),
        pytest.param(" a\n\t", "a", id="control characters"),
    ],
)
@pytest.mark.asyncio
async def test_cli_cache_common_key_callback(input_value: str, exp_value: str) -> None:
    """Verify that `key_callback` strips whitespace from inputs and returns
    the cleaned value.

    Parameters
    ----------
    input_value : str
        Parameterized inputs for the target function.
    exp_value : str
        Parameterized output values expected after executing the target function.
    """
    mock_ctx = mock.MagicMock(spec=typer.Context)
    actual_run_id = key_callback(mock_ctx, input_value)

    # confirm the run-id is returned with any callback cleaning appleid
    assert actual_run_id == exp_value
