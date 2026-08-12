"""Unit tests for the ``cstar cache clean`` command."""

import typing as t
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.cli.cache.clean import app
from cstar.cli.cache.common import ARG_KEY, ARG_RUNID, ARG_YES
from cstar.orchestration.artifact_cache import (
    ArtifactCache,
    ArtifactNotFoundError,
    UnsafePathError,
)

KEY: t.Final[str] = "mock-key"
RUN_ID: t.Final[str] = "mock-runid"


@pytest.fixture
def stored(tmp_path: Path, cache: ArtifactCache) -> Path:
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
    cache.ingest(item, KEY, RUN_ID)
    return item


def _clean(*args: str) -> t.Any:
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


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_removes_a_user_artifact(cache: ArtifactCache) -> None:
    """A confirmed deletion removes the artifact from the run."""
    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="y")):
        result = _clean(ARG_RUNID, RUN_ID, ARG_KEY, KEY)

    assert result.exit_code == 0
    assert "has been deleted" in result.stdout
    assert cache.resolve(KEY, RUN_ID) is None


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_declining_keeps_the_artifact(cache: ArtifactCache) -> None:
    """Deletion is irreversible, so a refusal has to leave it alone."""
    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="n")):
        result = _clean(ARG_RUNID, RUN_ID, ARG_KEY, KEY)

    assert result.exit_code == 0
    assert "was not removed" in result.stdout
    assert cache.resolve(KEY, RUN_ID) is not None


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_yes_skips_the_prompt(cache: ArtifactCache) -> None:
    """``--yes`` is for unattended use, so it must not stop to ask."""
    with mock.patch("rich.prompt.Prompt.ask") as prompt:
        result = _clean(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_YES)

    prompt.assert_not_called()
    assert "has been deleted" in result.stdout
    assert cache.resolve(KEY, RUN_ID) is None


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_clean_reports_an_unknown_artifact(cache: ArtifactCache) -> None:
    """Nothing to delete is reported rather than treated as a deletion."""
    result = _clean(ARG_RUNID, RUN_ID, ARG_KEY, "no-such-key")

    assert result.exit_code == 0
    assert "No cached artifact" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_verbose_names_the_tier(cache: ArtifactCache) -> None:
    """Verbose output says which tier the artifact was removed from."""
    result = _clean(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_YES, "--verbose")

    assert "has been deleted" in result.stdout
    assert "cache" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_leaves_the_shared_copy_alone(cache: ArtifactCache) -> None:
    """Cleaning a run must not reach into the tier everyone else reads."""
    cache.promote(KEY, RUN_ID)

    _clean(ARG_RUNID, RUN_ID, ARG_KEY, KEY, ARG_YES)

    assert cache.resolve(KEY) is not None


# ---------------------------------------------------------------------------
# Shared tier
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_removes_a_shared_artifact(cache: ArtifactCache) -> None:
    """A key with no run identifier targets the shared tier.

    That routing is what makes the tier reachable at all: the command decides
    between tiers on whether a run was supplied.
    """
    cache.promote(KEY, RUN_ID)

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(side_effect=["y", "y"])):
        result = _clean(ARG_KEY, KEY)

    assert result.exit_code == 0
    assert "has been deleted" in result.stdout
    assert cache.resolve(KEY) is None


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_shared_removal_needs_both_answers(
    cache: ArtifactCache,
) -> None:
    """A shared deletion affects every user, so one answer is not enough.

    The first refusal has to stick. A second prompt that replaced the answer
    rather than adding to it would delete an artifact the user had already
    declined to remove, making the extra question worse than none at all.
    """
    cache.promote(KEY, RUN_ID)

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(side_effect=["n", "y"])):
        result = _clean(ARG_KEY, KEY)

    assert "was not removed" in result.stdout
    assert cache.resolve(KEY) is not None


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_shared_miss_is_worded_for_the_tier(
    cache: ArtifactCache,
) -> None:
    """A shared miss has no run to mention, so it is described differently."""
    result = _clean(ARG_KEY, "no-such-key")

    assert result.exit_code == 0
    assert "found in shared cache" in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_leaves_the_user_copy_alone(cache: ArtifactCache) -> None:
    """Removing the published copy must not reach into a run's workspace."""
    cache.promote(KEY, RUN_ID)

    _clean(ARG_KEY, KEY, ARG_YES)

    assert cache.resolve(KEY, RUN_ID) is not None


# ---------------------------------------------------------------------------
# Whole-run removal
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_clean_removes_a_whole_run(
    tmp_path: Path, cache: ArtifactCache
) -> None:
    """A run identifier with no key clears everything that run cached.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    item = tmp_path / "something.txt"
    item.write_text("payload")
    cache.ingest(item, "first.nc", RUN_ID)
    cache.ingest(item, "second.nc", RUN_ID)

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="y")):
        result = _clean(ARG_RUNID, RUN_ID)

    assert result.exit_code == 0
    assert "have been deleted" in result.stdout
    assert cache.resolve("first.nc", RUN_ID) is None
    assert cache.resolve("second.nc", RUN_ID) is None


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_clean_declining_keeps_the_whole_run(
    tmp_path: Path, cache: ArtifactCache
) -> None:
    """A refusal has to stop the deletion, not merely be recorded.

    This is the most destructive path the command has — one answer removes
    every artifact a run produced — so a refusal that were computed and then
    ignored would lose the lot without a further word.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    item = tmp_path / "something.txt"
    item.write_text("payload")
    cache.ingest(item, "first.nc", RUN_ID)
    cache.ingest(item, "second.nc", RUN_ID)

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="n")):
        result = _clean(ARG_RUNID, RUN_ID)

    assert result.exit_code == 0
    assert "No artifacts were removed" in result.stdout
    assert cache.resolve("first.nc", RUN_ID) is not None
    assert cache.resolve("second.nc", RUN_ID) is not None


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_clean_run_prompt_names_the_run(
    tmp_path: Path, cache: ArtifactCache
) -> None:
    """The prompt says what is about to go, since a run has no single path.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    item = tmp_path / "something.txt"
    item.write_text("payload")
    cache.ingest(item, "first.nc", RUN_ID)

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="n")):
        result = _clean(ARG_RUNID, RUN_ID)

    assert f"Every artifact cached for run {RUN_ID!r}" in result.stdout
    assert "does not exist" not in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_clean_run_yes_skips_the_prompt(
    tmp_path: Path, cache: ArtifactCache
) -> None:
    """``--yes`` is for unattended use, so it must not stop to ask.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    item = tmp_path / "something.txt"
    item.write_text("payload")
    cache.ingest(item, "first.nc", RUN_ID)

    with mock.patch("rich.prompt.Prompt.ask") as prompt:
        result = _clean(ARG_RUNID, RUN_ID, ARG_YES)

    prompt.assert_not_called()
    assert "have been deleted" in result.stdout
    assert cache.resolve("first.nc", RUN_ID) is None


# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("mock_artifact_cache_env")
def test_cli_cache_clean_requires_a_key_or_a_run() -> None:
    """With neither argument there is nothing to identify, so it says so.

    It must also not reach the prompt: asking "shall I delete?" before knowing
    what would be deleted is how an accident happens.
    """
    with mock.patch("rich.prompt.Prompt.ask") as prompt:
        result = _clean()

    prompt.assert_not_called()
    assert "A key or run-id is required" in result.stdout


# ---------------------------------------------------------------------------
# Failures reported by the cache
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_reports_a_refused_shared_deletion(
    cache: ArtifactCache,
) -> None:
    """The cache guards shared deletion itself, and the refusal is surfaced.

    Confirming at the prompt is not the same as confirming to the cache; if
    the flag fails to reach it the command must say so rather than report a
    deletion that never happened.
    """
    cache.promote(KEY, RUN_ID)

    with (
        mock.patch("rich.prompt.Prompt.ask", mock.Mock(side_effect=["y", "y"])),
        mock.patch.object(
            type(cache), "delete_shared", side_effect=UnsafePathError("guarded")
        ),
    ):
        result = _clean(ARG_KEY, KEY)

    assert "Confirmation required to remove shared artifact" in result.stdout
    assert KEY in result.stdout


@pytest.mark.usefixtures("mock_artifact_cache_env", "stored")
def test_cli_cache_clean_reports_an_artifact_that_vanished(
    cache: ArtifactCache,
) -> None:
    """An artifact removed between the lookup and the delete is reported.

    Two processes cleaning the same run is the ordinary way this happens, and
    it should read as a failed removal rather than a crash.
    """
    with (
        mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="y")),
        mock.patch.object(
            type(cache), "delete_user", side_effect=ArtifactNotFoundError("gone")
        ),
    ):
        result = _clean(ARG_RUNID, RUN_ID, ARG_KEY, KEY)

    assert "could not be removed" in result.stdout
    assert result.exit_code == 0
