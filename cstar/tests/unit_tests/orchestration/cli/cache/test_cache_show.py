"""Unit tests for the ``cstar cache show`` command."""

import typing as t
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.cli.cache.common import ARG_KEY, ARG_RUNID
from cstar.cli.cache.show import app
from cstar.io.utils import get_artifact_cache

KEY: t.Final[str] = "mock-key"
RUN_ID: t.Final[str] = "mock-runid"


@pytest.fixture
def stored(tmp_path: Path) -> Path:
    """Commit one user-tier artifact and return the file it came from.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Path
        The source file.
    """
    item = tmp_path / "something.txt"
    item.write_text("Well isn't this something?")
    get_artifact_cache().ingest(item, KEY, RUN_ID)
    return item


def _show(*args: str) -> t.Any:
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
def test_cli_cache_show_reports_an_empty_cache() -> None:
    """A cold cache says so rather than printing empty headings."""
    result = _show()

    assert result.exit_code == 0
    assert "no cache entries available" in result.stdout.lower()


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_show_lists_runs_and_shared_artifacts() -> None:
    """With no arguments the command surveys what is cached."""
    cache = get_artifact_cache()
    cache.promote(KEY, RUN_ID)

    result = _show()

    assert result.exit_code == 0
    assert RUN_ID in result.stdout
    assert KEY in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_show_lists_one_runs_artifacts() -> None:
    """A run identifier alone narrows the survey to that run."""
    result = _show(ARG_RUNID, RUN_ID)

    assert result.exit_code == 0
    assert KEY in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_show_reports_an_unknown_run() -> None:
    """Naming a run with nothing cached is reported, not an empty listing."""
    result = _show(ARG_RUNID, "no-such-run")

    assert result.exit_code == 0
    assert "no cached entries" in result.stdout.lower()


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_show_locates_one_artifact() -> None:
    """A run and a key together resolve to a path."""
    result = _show(ARG_RUNID, RUN_ID, ARG_KEY, KEY)

    assert result.exit_code == 0
    assert "is cached at" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_show_reports_a_missing_key_for_a_run() -> None:
    """A key absent from a run that does have entries is reported.

    Exercises the user-tier branch of ``print_not_found``, which is worded
    differently from the shared-tier one.
    """
    result = _show(ARG_RUNID, RUN_ID, ARG_KEY, "no-such-key")

    assert result.exit_code == 0
    assert f"No cached artifact 'no-such-key' found for run {RUN_ID!r}" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_show_finds_a_shared_artifact_without_a_run() -> None:
    """A key alone searches the shared tier, which needs no run identity."""
    get_artifact_cache().promote(KEY, RUN_ID)

    result = _show(ARG_KEY, KEY)

    assert result.exit_code == 0
    assert "is cached at" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_show_reports_a_missing_shared_key() -> None:
    """The shared-tier miss is worded for a tier that has no runs."""
    result = _show(ARG_KEY, "no-such-key")

    assert result.exit_code == 0
    assert "No cached artifact 'no-such-key' found in shared cache" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_show_verbose_adds_the_cache_path() -> None:
    """Verbose listings name where each artifact sits on disk."""
    plain = _show(ARG_RUNID, RUN_ID)
    verbose = _show(ARG_RUNID, RUN_ID, "--verbose")

    assert "cache path" not in plain.stdout
    assert "cache path" in verbose.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_show_verbose_describes_a_single_artifact() -> None:
    """Verbose output for one artifact shows the record, not just the path."""
    result = _show(ARG_RUNID, RUN_ID, ARG_KEY, KEY, "--verbose")

    assert result.exit_code == 0
    assert "Location(" in result.stdout
