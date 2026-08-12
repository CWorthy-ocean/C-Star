import os
import typing as t
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.cli.cache.common import (
    ARG_KEY,
    ARG_RUNID,
    confirm_overwrite,
    key_callback,
    runid_callback,
)
from cstar.cli.cache.promote import app
from cstar.io.utils import get_artifact_cache
from cstar.orchestration.artifact_cache import (
    ArtifactCache,
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
    tmp_path: Path, key: str, run_id: str, cache: ArtifactCache
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

    assert "No cache artifact found" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_admin_promote_happy_path(tmp_path: Path, cache: ArtifactCache) -> None:
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

    assert "promoted to the group cache" in result.stdout

    # confirm the shared cache is prioritized over user cache (even with run-id)
    shared_location = cache.resolve(key, run_id)

    assert shared_location
    assert shared_location.exists
    assert shared_location.path != location.path
    assert run_id not in str(shared_location.path)


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_admin_promote_conflict_declined_keeps_the_shared_copy(
    tmp_path: Path, cache: ArtifactCache
) -> None:
    """A name already published with different content is not taken silently.

    The shared tier is addressed by name alone, so overwriting would replace
    an artifact other runs are already reading. Declining has to leave it.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    key: t.Final[str] = "mock-key"
    original = tmp_path / "original.txt"
    original.write_text("published content")

    cache.ingest(original, key, "run-a")
    cache.promote(key, "run-a")

    divergent = tmp_path / "divergent.txt"
    divergent.write_text("different content")
    cache.ingest(divergent, key, "run-b")

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="n")):
        result = CliRunner().invoke(
            app, [ARG_RUNID, "run-b", ARG_KEY, key], color=False
        )

    assert "already exists" in result.stdout
    assert "promoted to the group cache" not in result.stdout

    shared = cache.resolve(key)
    assert shared is not None
    assert shared.path.read_text() == "published content"


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_admin_promote_conflict_accepted_replaces_it(
    tmp_path: Path, cache: ArtifactCache
) -> None:
    """Confirming the prompt republishes over the existing name.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    key: t.Final[str] = "mock-key"
    original = tmp_path / "original.txt"
    original.write_text("published content")

    cache.ingest(original, key, "run-a")
    cache.promote(key, "run-a")

    divergent = tmp_path / "divergent.txt"
    divergent.write_text("different content")
    cache.ingest(divergent, key, "run-b")

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="y")):
        result = CliRunner().invoke(
            app, [ARG_RUNID, "run-b", ARG_KEY, key], color=False
        )

    assert "promoted to the group cache" in result.stdout

    shared = cache.resolve(key)
    assert shared is not None
    assert shared.path.read_text() == "different content"


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_admin_promote_yes_replaces_without_asking(
    tmp_path: Path, cache: ArtifactCache
) -> None:
    """``--yes`` is for unattended use, so it must not stop to ask.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    key: t.Final[str] = "mock-key"
    original = tmp_path / "original.txt"
    original.write_text("published content")

    cache.ingest(original, key, "run-a")
    cache.promote(key, "run-a")

    divergent = tmp_path / "divergent.txt"
    divergent.write_text("different content")
    cache.ingest(divergent, key, "run-b")

    with mock.patch("rich.prompt.Prompt.ask") as prompt:
        result = CliRunner().invoke(
            app, [ARG_RUNID, "run-b", ARG_KEY, key, "--yes"], color=False
        )

    prompt.assert_not_called()
    assert "promoted to the group cache" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_admin_promote_verbose_names_the_location(tmp_path: Path) -> None:
    """Verbose output identifies where the artifact was published.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    key: t.Final[str] = "mock-key"
    item = tmp_path / "something.txt"
    item.write_text("payload")

    get_artifact_cache().ingest(item, key, "run-a")

    plain = CliRunner().invoke(app, [ARG_RUNID, "run-a", ARG_KEY, key], color=False)
    assert "Location(" not in plain.stdout

    get_artifact_cache().delete_shared(key, True)
    verbose = CliRunner().invoke(
        app, [ARG_RUNID, "run-a", ARG_KEY, key, "--verbose"], color=False
    )
    assert "Location(" in verbose.stdout
