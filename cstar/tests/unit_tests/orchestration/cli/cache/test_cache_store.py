"""Unit tests for the ``cstar cache store`` command."""

import typing as t
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.cli.cache.common import ARG_KEY, ARG_MOVE, ARG_PATH, ARG_RUNID, ARG_YES
from cstar.cli.cache.store import app
from cstar.orchestration.artifact_cache import ArtifactCache

KEY: t.Final[str] = "mock-key"
RUN_ID: t.Final[str] = "mock-runid"


@pytest.fixture
def source(tmp_path: Path) -> Path:
    """Return a file standing in for something the user wants cached.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Path
        Path to the file.
    """
    item = tmp_path / "something.txt"
    item.write_text("Well isn't this something?")
    return item


def _store(*args: str) -> t.Any:
    """Invoke the command with the given arguments.

    Parameters
    ----------
    *args : str
        Command-line arguments.

    Returns
    -------
    Any
        The runner result.
    """
    return CliRunner().invoke(app, list(args), color=False)


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_store_adds_an_artifact(source: Path, cache: ArtifactCache) -> None:
    """A file named by the caller is copied into the run's cache.

    Parameters
    ----------
    source : Path
        File to store.
    """
    result = _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source))

    assert result.exit_code == 0
    assert "has been added to the cache" in result.stdout

    location = cache.resolve(KEY, RUN_ID)
    assert location is not None
    assert location.path.read_text() == source.read_text()


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_store_leaves_the_source_in_place(source: Path) -> None:
    """Storing copies by default, so the caller keeps its own file.

    Parameters
    ----------
    source : Path
        File to store.
    """
    _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source))

    assert source.is_file()


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_store_refuses_to_replace_without_consent(
    source: Path, cache: ArtifactCache
) -> None:
    """A second store under one key must not silently displace the first.

    The cache does not overwrite unless asked, and declining the prompt has to
    leave the original in place rather than half-replace it.

    Parameters
    ----------
    source : Path
        File to store.
    """
    _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source))
    source.write_text("replacement content")

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="n")):
        result = _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source))

    assert "will be overwritten" in result.stdout

    location = cache.resolve(KEY, RUN_ID)
    assert location is not None
    assert location.path.read_text() == "Well isn't this something?"


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_store_replaces_when_confirmed(
    source: Path, cache: ArtifactCache
) -> None:
    """Answering the prompt affirmatively replaces the stored artifact.

    Parameters
    ----------
    source : Path
        File to store.
    """
    _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source))
    source.write_text("replacement content")

    with mock.patch("typer.confirm", mock.Mock(return_value="y")):
        result = _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source))

    assert "has been updated" in result.stdout

    location = cache.resolve(KEY, RUN_ID)
    assert location is not None
    assert location.path.read_text() == "replacement content"


@pytest.mark.usefixtures("cache")
@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_store_yes_skips_the_prompt(source: Path) -> None:
    """``--yes`` is for unattended use, so it must not stop to ask.

    Parameters
    ----------
    source : Path
        File to store.
    """
    _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source))
    source.write_text("replacement content")

    with mock.patch("typer.confirm") as prompt:
        result = _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source), ARG_YES)

    prompt.assert_not_called()
    assert "has been overwritten" in result.stdout


@pytest.mark.usefixtures("cache")
@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_store_reports_the_path_when_verbose(source: Path) -> None:
    """Verbose output names where the artifact landed.

    Parameters
    ----------
    source : Path
        File to store.
    """
    result = _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source), "--verbose")

    assert KEY in result.stdout
    assert RUN_ID in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_store_move_takes_the_source(
    source: Path, cache: ArtifactCache
) -> None:
    """``--move`` is advertised as relocating rather than copying.

    Parameters
    ----------
    source : Path
        File to store.
    """
    result = _store(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(source), ARG_MOVE)

    assert result.exit_code == 0
    location = cache.resolve(KEY, RUN_ID)
    assert location is not None
    assert location.path.is_file()
    assert not source.exists(), "--move must relocate rather than copy"


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_store_missing_source_is_reported(tmp_path: Path) -> None:
    """Storing a file that is not there fails rather than caching nothing.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    result = _store(
        ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_PATH, str(tmp_path / "absent.txt")
    )

    assert result.exit_code != 0


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_store_two_keys_are_two_artifacts(
    source: Path, cache: ArtifactCache
) -> None:
    """Distinct keys must not collide within one run.

    Parameters
    ----------
    source : Path
        File to store.
    """
    _store(ARG_RUNID, RUN_ID, ARG_KEY, "first", ARG_PATH, str(source))
    source.write_text("second content")
    _store(ARG_RUNID, RUN_ID, ARG_KEY, "second", ARG_PATH, str(source))

    first = cache.resolve("first", RUN_ID)
    second = cache.resolve("second", RUN_ID)

    assert first is not None
    assert second is not None
    assert first.path.read_text() != second.path.read_text()
