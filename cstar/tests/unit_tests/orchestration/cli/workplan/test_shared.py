import datetime
import io
import logging
import os
import typing as t
from collections import OrderedDict
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
import typer
from rich.console import Console

from cstar.base.env import ENV_CSTAR_RUNID
from cstar.cli.workplan.shared import (
    autocomplete_step_list,
    check_and_capture_kvp,
    check_and_capture_kvps,
    display_summary,
    list_steps,
    refresh_disk_usage,
)
from cstar.execution.file_system import JobFileSystemManager, StateDirectoryManager
from cstar.orchestration.models import Workplan
from cstar.orchestration.tracking import WorkplanRun

SHARED_LOGGER = "cstar.cli.workplan.shared"


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        pytest.param("key=value", ("key", "value"), id="ideal"),
        pytest.param("k=v", ("k", "v"), id="key and value::single character"),
        pytest.param(" k = v ", ("k", "v"), id="key and value::surrounding whitespace"),
        pytest.param(" k=v", ("k", "v"), id="key::leading whitespace"),
        pytest.param("k =v", ("k", "v"), id="key::trailing whitespace"),
        pytest.param(" k =v", ("k", "v"), id="key::surrounding whitespace"),
        pytest.param(" k k =v", ("k k", "v"), id="key::internal whitespace"),
        pytest.param("k= v", ("k", "v"), id="value::leading whitespace"),
        pytest.param("k=v ", ("k", "v"), id="value::trailing whitespace"),
        pytest.param("k= v ", ("k", "v"), id="value::surrounding whitespace"),
        pytest.param("k= v v ", ("k", "v v"), id="value::internal whitespace"),
        pytest.param("k\n\t=v", ("k", "v"), id="key::escape sequences"),
    ],
)
def test_capture_kvp_happy_path(entry: str, expected: tuple[str, str]) -> None:
    """Verify an ideal input for `check_and_capture_kvp` is handled properly.

    Parameters
    ----------
    entry : str
        The entry value to attempt to parse
    expected : tuple[str, str]
        The expected output
    """
    actual = check_and_capture_kvp(entry)

    assert actual == expected


@pytest.mark.parametrize(
    ("entry", "error"),
    [
        pytest.param("=value", "without key", id="missing::key"),
        pytest.param(" =value", "without key", id="whitespace::key"),
        pytest.param("key=", "empty value", id="missing::value"),
        pytest.param("key= ", "empty value", id="whitespace::value"),
        pytest.param("=", "key and value", id="missing::key and value"),
        pytest.param(" = ", "key and value", id="whitespace::key and value"),
        pytest.param("", "expected format", id="format::empty"),
        pytest.param(" ", "expected format", id="format::whitespace"),
        pytest.param("xxx", "expected format", id="format::no kvp delimiter"),
    ],
)
def test_capture_kvp_invalid_inputs(entry: str, error: str) -> None:
    """Verify incorrectly formatted input for `check_and_capture_kvp` result
    in the expected exceptions.

    Parameters
    ----------
    entry : str
        The entry value to attempt to parse
    error : str
        The error string that should be matched if the proper error is raised
    """
    with pytest.raises(typer.BadParameter, match=error):
        _ = check_and_capture_kvp(entry)


@pytest.mark.parametrize(
    ("entries", "expected"),
    [
        pytest.param(
            ["var1=value1", "var2=value2"],
            {"var1": "value1", "var2": "value2"},
            id="ideal",
        ),
        pytest.param(
            [],
            {},
            id="empty list",
        ),
        pytest.param(
            None,
            {},
            id="null",
        ),
    ],
)
def test_capture_kvps_happy_path(
    entries: list[str],
    expected: Mapping[str, str],
) -> None:
    """Verify that valid inputs produce the expected output.

    Parameters
    ----------
    entries : list[str]
        The list of items to parse
    expected : Mapping[str, str]
        The expected output
    """
    actual = check_and_capture_kvps(entries)
    assert expected == actual


@pytest.mark.parametrize(
    "entries",
    [
        pytest.param(
            ["x=X", "x=X"],
            id="sequential::value match",
        ),
        pytest.param(
            ["x=A", "x=B"],
            id="sequential::value mismatch",
        ),
        pytest.param(
            ["x=X", "y=Y", "x=X"],
            id="non-sequential::value match",
        ),
        pytest.param(
            ["x=A", "y=Y", "x=B"],
            id="non-sequential::value mismatch",
        ),
    ],
)
def test_capture_kvps_duplicate(
    entries: list[str],
) -> None:
    """Verify that receiving the same key more than once results in an exception.

    NOTE: the current implementation does not ignore duplicate keys, even
    if the values are matching.

    Parameters
    ----------
    entries : list[str]
        The list of items to parse
    expected : Mapping[str, str]
        The expected output
    """
    with pytest.raises(typer.BadParameter, match="multiple"):
        _ = check_and_capture_kvps(entries)


@pytest.mark.asyncio
async def test_list_steps_empty_run_id() -> None:
    """Verify that listing steps with no filter returns all available steps
    in the results.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    steps = await list_steps(run_id="", incomplete="")

    # confirm that an empty list is returned
    assert not steps


@pytest.mark.asyncio
async def test_list_steps_unfiltered(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that listing steps with no filter returns all available steps
    in the results.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan
    num_steps = len(wp.steps)

    # confirm that a step is discovered when not using a filter
    steps = await list_steps(fake_run_id, incomplete="")
    assert len(steps) == num_steps
    assert set(steps) == {s.name for s in wp.steps}


@pytest.mark.asyncio
async def test_list_steps_filtered(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that listing steps with a filter returns a subset of available results.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    for step in wp.steps:
        # confirm that using a step name as the filter returns that single result
        steps = await list_steps(fake_run_id, step.name)
        assert len(steps) == 1
        assert step.name in steps

        # confirm case-insensitivity
        steps = await list_steps(fake_run_id, step.name.lower())
        assert len(steps) == 1
        assert step.name in steps


@pytest.mark.asyncio
async def test_list_steps_filter_all(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that listing steps with an filter that has no matches results in
    no results.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, _, fake_run_id = executed_workplan

    # confirm a nonsense name is not found
    steps = await list_steps(fake_run_id, incomplete="xyzpqr")
    assert not steps


@pytest.mark.asyncio
async def test_autocomplete_step_list_no_run_id() -> None:
    """Verify that a missing run-id parameter raises a typer exception."""
    mock_typer_ctx = mock.Mock(params={"no-run-id-param-to-locate": 0})

    with (
        mock.patch("typer.Context", mock_typer_ctx),
        pytest.raises(typer.BadParameter, match="run-id is required"),
    ):
        autocomplete_step_list(mock_typer_ctx, incomplete="")


@pytest.mark.asyncio
async def test_autocomplete_step_list_empty_run_id() -> None:
    """Verify that a missing run-id parameter raises a typer exception."""
    mock_typer_ctx = mock.Mock(params={"run-id": ""})

    with (
        mock.patch("typer.Context", mock_typer_ctx),
        pytest.raises(typer.BadParameter, match="run-id is required"),
    ):
        autocomplete_step_list(mock_typer_ctx, incomplete="")


def test_autocomplete_step_list_happy_path(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that a valid run-id locates a run.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    mock_typer_ctx = mock.Mock(params={"run_id": fake_run_id})

    with mock.patch("typer.Context", mock_typer_ctx):
        steps = autocomplete_step_list(mock_typer_ctx, incomplete="")
        assert len(steps) == len(wp.steps)


# ---------------------------------------------------------------------------
# list_steps: fallback to tasks-directory search
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_steps_falls_back_to_tasks_dir_when_no_run_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a missing run record falls through to the tasks-directory scan.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory used to stand in for the state/data home.
    monkeypatch : pytest.MonkeyPatch
        The active monkeypatch fixture.
    """
    fake_run_id = "fake-run-id"

    monkeypatch.setattr(
        "cstar.cli.workplan.shared.TrackingRepository.get_workplan_run",
        mock.AsyncMock(return_value=None),
    )

    with mock.patch.dict(os.environ, {ENV_CSTAR_RUNID: fake_run_id}):
        data_dir = StateDirectoryManager.data_dir()
        tasks_dir = JobFileSystemManager(data_dir).tasks_dir
        for step_name in ("step-a", "step-b"):
            (tasks_dir / step_name).mkdir(parents=True, exist_ok=True)

        steps = await list_steps(fake_run_id, incomplete="")

    assert set(steps) == {"step-a", "step-b"}


# ---------------------------------------------------------------------------
# refresh_disk_usage
# ---------------------------------------------------------------------------


def make_plan(steps: Mapping[str, Path]) -> SimpleNamespace:
    """Build a stand-in workplan exposing only what `refresh_disk_usage` reads.

    Parameters
    ----------
    steps : Mapping[str, Path]
        Step name to working directory.

    Returns
    -------
    SimpleNamespace
        An object with a `.steps` list of name/working_dir pairs.
    """
    return SimpleNamespace(
        steps=[
            SimpleNamespace(name=name, working_dir=path) for name, path in steps.items()
        ]
    )


def make_workplan_run(run_id: str, tmp_path: Path) -> WorkplanRun:
    """Build a bare `WorkplanRun` with dummy paths under `tmp_path`.

    Parameters
    ----------
    run_id : str
        The unique run identifier.
    tmp_path : Path
        The directory the dummy paths are rooted under.

    Returns
    -------
    WorkplanRun
    """
    return WorkplanRun(
        workplan_path=tmp_path / run_id / "workplan.yaml",
        trx_workplan_path=tmp_path / run_id / "trx.yaml",
        output_path=tmp_path / run_id / "out",
        run_id=run_id,
    )


@pytest.mark.asyncio
async def test_refresh_disk_usage_measures_and_persists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify measured runs are passed step directories and then persisted.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for dummy step paths.
    monkeypatch : pytest.MonkeyPatch
        The active monkeypatch fixture.
    """
    run = make_workplan_run("run-1", tmp_path)
    step_dirs = {"step-a": tmp_path / "step-a", "step-b": tmp_path / "step-b"}
    plan = make_plan(step_dirs)

    measure_mock = mock.AsyncMock()
    put_mock = mock.AsyncMock()
    monkeypatch.setattr("cstar.cli.workplan.shared.measure_step_sizes", measure_mock)
    monkeypatch.setattr(
        "cstar.cli.workplan.shared.TrackingRepository.update_workplan_run", put_mock
    )

    plans = t.cast("dict[Path, t.Any]", {run.trx_workplan_path: plan})
    await refresh_disk_usage([run], plans)

    measure_mock.assert_awaited_once()
    assert measure_mock.await_args is not None
    awaited_run, awaited_step_dirs, _sem = measure_mock.await_args.args
    assert awaited_run is run
    assert awaited_step_dirs == step_dirs
    put_mock.assert_awaited_once_with(run.run_id, mock.ANY)


@pytest.mark.asyncio
async def test_refresh_disk_usage_skips_and_warns_for_run_without_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a run with neither a loaded workplan nor a tasks directory is
    skipped, not measured or persisted.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for dummy step paths.
    monkeypatch : pytest.MonkeyPatch
        The active monkeypatch fixture.
    caplog : pytest.LogCaptureFixture
        Captures the collapsed warning naming the skipped run.
    """
    run = make_workplan_run("run-without-plan", tmp_path)

    measure_mock = mock.AsyncMock()
    put_mock = mock.AsyncMock()
    monkeypatch.setattr("cstar.cli.workplan.shared.measure_step_sizes", measure_mock)
    monkeypatch.setattr(
        "cstar.cli.workplan.shared.TrackingRepository.update_workplan_run", put_mock
    )

    with caplog.at_level(logging.WARNING, logger=SHARED_LOGGER):
        await refresh_disk_usage([run], {})

    measure_mock.assert_not_awaited()
    put_mock.assert_not_awaited()
    assert any("run-without-plan" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_refresh_disk_usage_falls_back_to_tasks_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a run without a loadable workplan is measured from the
    subdirectories of its tasks directory.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for dummy step paths.
    monkeypatch : pytest.MonkeyPatch
        The active monkeypatch fixture.
    """
    run = make_workplan_run("run-fallback", tmp_path)
    tasks_dir = run.output_path / "tasks"
    for name in ("step-a", "step-b"):
        (tasks_dir / name).mkdir(parents=True)
    (tasks_dir / "not-a-step.txt").write_text("ignored")

    measure_mock = mock.AsyncMock(return_value={"step-a": 1})
    update_mock = mock.AsyncMock()
    monkeypatch.setattr("cstar.cli.workplan.shared.measure_step_sizes", measure_mock)
    monkeypatch.setattr(
        "cstar.cli.workplan.shared.TrackingRepository.update_workplan_run", update_mock
    )

    await refresh_disk_usage([run], {})

    assert measure_mock.await_args is not None
    _run, awaited_step_dirs, _sem = measure_mock.await_args.args
    assert awaited_step_dirs == {
        "step-a": tasks_dir / "step-a",
        "step-b": tasks_dir / "step-b",
    }
    update_mock.assert_awaited_once_with(run.run_id, mock.ANY)


# ---------------------------------------------------------------------------
# display_summary caption
# ---------------------------------------------------------------------------


def test_display_summary_caption_shows_measured_size_and_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a measured run's caption shows total MB and the measurement time.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for dummy run paths.
    monkeypatch : pytest.MonkeyPatch
        The active monkeypatch fixture.
    """
    measured_at = datetime.datetime(2026, 9, 12, 3, 4, 0, tzinfo=datetime.UTC)
    run = make_workplan_run("run-1", tmp_path)
    run.step_sizes.update({"step-a": 5, "step-b": 3})
    run.size_measured_at = measured_at

    buffer = io.StringIO()
    monkeypatch.setattr(
        "cstar.cli.workplan.shared.console", Console(file=buffer, width=200)
    )

    display_summary(run, OrderedDict())

    rendered = buffer.getvalue()
    assert "8MB of step output" in rendered
    assert measured_at.astimezone().strftime("%Y-%m-%d %H:%M") in rendered


def test_display_summary_caption_shows_hint_when_unmeasured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an unmeasured run's caption points at `cstar workplan status --size`.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for dummy run paths.
    monkeypatch : pytest.MonkeyPatch
        The active monkeypatch fixture.
    """
    run = make_workplan_run("run-1", tmp_path)

    buffer = io.StringIO()
    monkeypatch.setattr(
        "cstar.cli.workplan.shared.console", Console(file=buffer, width=200)
    )

    display_summary(run, OrderedDict())

    rendered = buffer.getvalue()
    assert "Disk usage not measured" in rendered
    assert "cstar workplan status run-1 --size" in rendered
