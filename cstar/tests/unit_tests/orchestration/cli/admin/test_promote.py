import os
import typing as t
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.cli.admin.promote import (
    ARG_KEY,
    ARG_RUNID,
    app,
    confirm_overwrite,
    get_artifact_cache,
    key_callback,
    runid_callback,
)


@pytest.fixture
def mock_artifact_cache_env(tmp_path: Path) -> Generator[dict[str, str]]:
    mock_env = {
        "USER": "mockuid",
        "PROJECT": str(tmp_path / "project123"),
        "SCRATCH": str(tmp_path / "scratch"),
    }

    with mock.patch.dict(os.environ, mock_env):
        yield mock_env


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
async def test_cli_admin_promote_runid_callback(
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
async def test_cli_admin_promote_key_callback(input_value: str, exp_value: str) -> None:
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
    ("answer", "exp_value"),
    [
        ("y", True),
        ("n", False),
    ],
)
@pytest.mark.asyncio
async def test_cli_admin_promote_overwrite(answer: str, exp_value: bool) -> None:
    """Verify that `key_callback` strips whitespace from inputs and returns
    the cleaned value.

    Parameters
    ----------
    answer : str
        Parameterized inputs for the target function.
    exp_value : str
        Parameterized output values expected after executing the target function.
    """
    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value=answer)):
        actual_value = confirm_overwrite()

    # confirm the run-id is returned with any callback cleaning appleid
    assert actual_value == exp_value


@pytest.mark.parametrize(
    ("key", "run_id"),
    [
        pytest.param("mock-key", "mock-runidX", id="bad run-id"),
        pytest.param("mock-keyX", "mock-runid", id="bad key"),
        pytest.param("mock-keyX", "mock-runidX", id="bad key and run-id"),
    ],
)
@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_admin_promote_entry_not_found(
    tmp_path: Path, key: str, run_id: str
) -> None:
    """Verify that the the key and run callbacks are wired up.

    Parameters
    ----------
    key : str
        The key to pass to the promote method
    run_id : str
        The run-id to pass to the promote method
    """
    item_path = tmp_path / "something.txt"
    item_path.write_text("Well isn't this something?")

    actual_key: t.Final[str] = "mock-key"
    actual_run_id: t.Final[str] = "mock-runid"

    cache = get_artifact_cache()
    location = cache.ingest(item_path, actual_key, actual_run_id)
    assert location.exists

    runner = CliRunner()
    args = [
        ARG_RUNID,
        run_id,
        ARG_KEY,
        key,
    ]
    result = runner.invoke(app, args, color=False)

    assert "No cache entry found" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_admin_promote_happy_path(tmp_path: Path) -> None:
    """Verify that the CLI command executes ArtifactCache.promote for the
    specified value.

    Parameters
    ----------
    key : str
        The key to pass to the promote method
    run_id : str
        The run-id to pass to the promote method
    """
    item_path = tmp_path / "something.txt"
    item_path.write_text("Well isn't this something?")

    key: t.Final[str] = "mock-key"
    run_id: t.Final[str] = "mock-runid"

    cache = get_artifact_cache()
    location = cache.ingest(item_path, key, run_id)
    assert location.exists
    assert run_id in str(location.path)

    location_check = cache.resolve(key, run_id)
    assert location_check
    assert location_check.path == location.path

    runner = CliRunner()
    args = [
        ARG_RUNID,
        run_id,
        ARG_KEY,
        key,
    ]
    result = runner.invoke(app, args, color=False)

    assert "is promoted to the group cache" in result.stdout

    # confirm the shared cache is prioritized over user cache (even with run-id)
    shared_location = cache.resolve(key, run_id)

    assert shared_location
    assert shared_location.exists
    assert shared_location.path != location.path
    assert run_id not in str(shared_location.path)
