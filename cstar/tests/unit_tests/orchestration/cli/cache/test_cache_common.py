from collections.abc import Callable
from pathlib import Path
from unittest import mock

import pytest
import typer

from cstar.cli.cache.common import (
    key_callback,
    list_runs_with_cache,
    print_not_found,
    runid_callback,
)
from cstar.io.utils import get_artifact_cache


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


@pytest.mark.parametrize(
    "callback",
    [runid_callback, key_callback],
    ids=["run-id", "key"],
)
def test_cli_cache_common_callbacks_reject_blank_input(
    callback: Callable[[typer.Context, str], str],
) -> None:
    """A value that is only whitespace is a mistake, not an empty selection.

    Parameters
    ----------
    callback : Callable
        Callback under test.
    """
    with pytest.raises(typer.Exit):
        callback(mock.MagicMock(spec=typer.Context), "   ")


@pytest.mark.parametrize(
    ("run_id", "key", "expected"),
    [
        pytest.param(
            "run-a",
            "k.nc",
            "No cached artifact 'k.nc' found for run 'run-a'",
            id="user tier",
        ),
        pytest.param(
            "",
            "k.nc",
            "No cached artifact 'k.nc' found in shared cache",
            id="shared tier",
        ),
    ],
)
def test_cli_cache_common_not_found_wording(
    run_id: str, key: str, expected: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """The two tiers are described differently, since only one has runs.

    Parameters
    ----------
    run_id : str
        Run identifier, empty for the shared tier.
    key : str
        Artifact key.
    expected : str
        Message the user should see.
    capsys : pytest.CaptureFixture
        Captured standard output.
    """
    print_not_found(run_id, key)

    assert expected in capsys.readouterr().out


@pytest.mark.usefixtures("cache")
def test_cli_cache_common_not_found_says_nothing_without_a_key(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """With no key there is nothing to name, so nothing is printed.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Captured standard output.
    """
    print_not_found("run-a", "")

    assert capsys.readouterr().out == ""


@pytest.mark.usefixtures("cache")
def test_cli_cache_common_lists_runs_for_completion(tmp_path: Path) -> None:
    """Shell completion offers the runs that actually have entries.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    item = tmp_path / "something.txt"
    item.write_text("payload")
    get_artifact_cache().ingest(item, "k.nc", "run-a")

    assert "run-a" in list_runs_with_cache()
