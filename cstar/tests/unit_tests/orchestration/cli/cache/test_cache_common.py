from collections.abc import Callable
from pathlib import Path
from unittest import mock

import pytest
import typer

from cstar.cli.cache.common import (
    confirm_overwrite,
    confirm_remove,
    confirm_remove_run,
    key_callback,
    list_runs_with_cache,
    print_not_found,
    runid_callback,
)
from cstar.io.utils import get_artifact_cache
from cstar.orchestration.artifact_cache import Location, Tier


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


def test_cli_cache_common_overwrite_without_a_location(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The prompt still works when the caller has no location to show.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Captured standard output.
    """
    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="y")):
        assert confirm_overwrite(location=None) is True

    assert "already exists" in capsys.readouterr().out


def test_cli_cache_common_overwrite_forced_skips_the_prompt() -> None:
    """``--yes`` is for unattended use, so it must not stop to ask."""
    with mock.patch("rich.prompt.Prompt.ask") as prompt:
        assert confirm_overwrite(force_overwrite=True) is True

    prompt.assert_not_called()


def test_cli_cache_common_remove_refuses_without_a_location(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Nothing to remove is a refusal, not a silent success.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Captured standard output.
    """
    assert confirm_remove(location=None) is False
    assert "does not exist" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("answer", "expected"),
    [("y", True), ("n", False)],
)
def test_cli_cache_common_remove_from_a_run_asks_once(
    answer: str, expected: bool
) -> None:
    """A user-tier deletion affects only its own run, so one answer suffices.

    Parameters
    ----------
    answer : str
        Response to the prompt.
    expected : bool
        Whether the removal should proceed.
    """
    location = Location(
        path=Path("/tmp/x.nc"), tier=Tier.USER, name="x.nc", run_id="run-a"
    )

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value=answer)) as ask:
        assert confirm_remove(False, location) is expected

    assert ask.call_count == 1


@pytest.mark.parametrize(
    ("answers", "expected"),
    [
        pytest.param(["y", "y"], True, id="both yes"),
        pytest.param(["y", "n"], False, id="second no"),
        pytest.param(["n", "y"], False, id="first no"),
    ],
)
def test_cli_cache_common_remove_shared_needs_both_answers(
    answers: list[str], expected: bool
) -> None:
    """A shared deletion affects every user, so one answer is not enough.

    The "first no" case is the one that matters: a second prompt that replaced
    the first answer rather than adding to it would delete an artifact the user
    had already declined to remove, which makes the extra question worse than
    no question at all.

    Parameters
    ----------
    answers : list of str
        Responses to the prompts, in order.
    expected : bool
        Whether the removal should proceed.
    """
    location = Location(
        path=Path("/tmp/x.nc"), tier=Tier.SHARED, name="x.nc", run_id=None
    )

    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(side_effect=answers)):
        assert confirm_remove(False, location) is expected


def test_cli_cache_common_remove_forced_skips_both_prompts() -> None:
    """``--yes`` bypasses the shared double-check as well as the first."""
    location = Location(
        path=Path("/tmp/x.nc"), tier=Tier.SHARED, name="x.nc", run_id=None
    )

    with mock.patch("rich.prompt.Prompt.ask") as prompt:
        assert confirm_remove(True, location) is True

    prompt.assert_not_called()


@pytest.mark.usefixtures("mock_artifact_cache_env")
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


def test_cli_cache_common_remove_run_names_the_run(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A whole-run removal has no single path, so the run is named instead.

    Parameters
    ----------
    capsys : pytest.CaptureFixture
        Captured standard output.
    """
    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value="y")):
        assert confirm_remove_run("run-a") is True

    assert "Every artifact cached for run 'run-a'" in capsys.readouterr().out


@pytest.mark.parametrize(("answer", "expected"), [("y", True), ("n", False)])
def test_cli_cache_common_remove_run_honours_the_answer(
    answer: str, expected: bool
) -> None:
    """One answer decides it, and a refusal must be reported as such.

    Parameters
    ----------
    answer : str
        Response to the prompt.
    expected : bool
        Whether the removal should proceed.
    """
    with mock.patch("rich.prompt.Prompt.ask", mock.Mock(return_value=answer)):
        assert confirm_remove_run("run-a") is expected


def test_cli_cache_common_remove_run_forced_skips_the_prompt() -> None:
    """``--yes`` is for unattended use, so it must not stop to ask."""
    with mock.patch("rich.prompt.Prompt.ask") as prompt:
        assert confirm_remove_run("run-a", True) is True

    prompt.assert_not_called()
