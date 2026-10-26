"""Tests for the prior-attempt reuse logic shared by every launcher."""

import logging
import typing as t
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.launch.common import (
    build_attempt_log_header,
    cancel_prior_attempt,
    is_foreign_handle,
    prior_of_this_run,
    resolve_prior_attempt,
    waits_on_replaced_dependency,
)
from cstar.orchestration.models import (
    KEY_CLOBBER,
    KEY_PRE_RUN,
    KEY_RESUME,
    Application,
)
from cstar.orchestration.orchestration import (
    Launcher,
    LiveStep,
    ProcessHandle,
    Status,
    Task,
)

T0 = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
"""When the prior attempt of the dependency tests was submitted."""


def _live_step(
    tmp_path: Path,
    *,
    clobber: bool = False,
    resume: bool = False,
    pre_run: bool = False,
) -> LiveStep:
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    overrides: dict[str, bool] = {}
    if clobber:
        overrides[KEY_CLOBBER] = True
    if resume:
        overrides[KEY_RESUME] = True
    if pre_run:
        overrides[KEY_PRE_RUN] = True

    return LiveStep(
        name="test step",
        application=Application.HELLO_WORLD,
        blueprint=bp_path,
        working_dir=tmp_path / "unit-test-work-dir",
        workflow_overrides=overrides,
    )


@pytest.mark.parametrize(
    ("prior_status", "clobber", "resume", "exp_reuse", "exp_clobber_set"),
    [
        pytest.param(
            Status.Failed, False, False, False, True, id="failed-no-resume-clobbers"
        ),
        pytest.param(
            Status.Cancelled,
            False,
            False,
            False,
            True,
            id="cancelled-no-resume-clobbers",
        ),
        pytest.param(
            Status.Failed, False, True, False, False, id="failed-resume-no-clobber"
        ),
        pytest.param(
            Status.Cancelled,
            False,
            True,
            False,
            False,
            id="cancelled-resume-no-clobber",
        ),
        pytest.param(Status.Done, False, False, True, False, id="done-reused"),
        pytest.param(Status.Done, True, False, False, True, id="done-clobbered"),
        pytest.param(
            Status.Submitted, False, False, True, False, id="submitted-reused"
        ),
        pytest.param(
            Status.Submitted, True, False, False, True, id="submitted-clobbered"
        ),
        pytest.param(Status.Running, False, False, True, False, id="running-reused"),
        pytest.param(Status.Unsubmitted, False, False, False, False, id="unsubmitted"),
    ],
)
def test_resolve_prior_attempt(
    tmp_path: Path,
    prior_status: Status,
    clobber: bool,
    resume: bool,
    exp_reuse: bool,
    exp_clobber_set: bool,
) -> None:
    """Verify the reuse decision and clobber side-effect for every prior status,
    with and without a per-step resume/clobber marker.
    """
    step = _live_step(tmp_path, clobber=clobber, resume=resume)

    reuse = resolve_prior_attempt(step, prior_status)

    assert reuse is exp_reuse
    assert step.workflow_overrides.get(KEY_CLOBBER, False) is exp_clobber_set


@pytest.mark.parametrize(
    (
        "prior_status",
        "prior_pre_run",
        "step_pre_run",
        "clobber",
        "exp_reuse",
        "exp_resume_set",
        "exp_clobber_set",
    ),
    [
        pytest.param(
            Status.Done, True, False, False, False, True, False, id="attach-to-pre-run"
        ),
        pytest.param(
            Status.Done, True, True, False, True, False, False, id="pre-run-repeated"
        ),
        pytest.param(
            Status.Done, True, False, True, False, False, True, id="pre-run-clobbered"
        ),
        pytest.param(
            Status.Done, False, False, False, True, False, False, id="not-a-pre-run"
        ),
        pytest.param(
            Status.Failed, True, False, False, False, False, True, id="failed-pre-run"
        ),
        pytest.param(
            Status.Failed,
            True,
            True,
            False,
            False,
            False,
            True,
            id="failed-pre-run-again",
        ),
    ],
)
def test_resolve_prior_attempt_pre_run(
    tmp_path: Path,
    prior_status: Status,
    prior_pre_run: bool,
    step_pre_run: bool,
    clobber: bool,
    exp_reuse: bool,
    exp_resume_set: bool,
    exp_clobber_set: bool,
) -> None:
    """Verify a completed pre-run is attached to (resume) by a real run, reused
    by a repeated pre-run, and that failed or ordinary priors behave as before.
    """
    step = _live_step(tmp_path, clobber=clobber, pre_run=step_pre_run)

    reuse = resolve_prior_attempt(step, prior_status, prior_pre_run=prior_pre_run)

    assert reuse is exp_reuse
    assert step.workflow_overrides.get(KEY_RESUME, False) is exp_resume_set
    assert step.workflow_overrides.get(KEY_CLOBBER, False) is exp_clobber_set


@pytest.mark.parametrize("prior_status", [Status.Submitted, Status.Running])
def test_resolve_prior_attempt_pre_run_in_progress_fails_loudly(
    tmp_path: Path, prior_status: Status
) -> None:
    """A real run must not adopt a pre-run that is still in progress: the step
    would later read Done without the model ever launching.
    """
    step = _live_step(tmp_path)

    with pytest.raises(CstarExpectationFailed, match="still"):
        resolve_prior_attempt(step, prior_status, prior_pre_run=True)

    assert KEY_RESUME not in step.workflow_overrides


def test_resolve_prior_attempt_pre_run_in_progress_clobbered(tmp_path: Path) -> None:
    """With clobber requested, an in-progress pre-run is simply resubmitted."""
    step = _live_step(tmp_path, clobber=True)

    reuse = resolve_prior_attempt(step, Status.Running, prior_pre_run=True)

    assert reuse is False
    assert KEY_RESUME not in step.workflow_overrides


@pytest.mark.parametrize(
    ("handle_launcher", "expected"),
    [
        pytest.param("", False, id="empty-treated-as-own"),
        pytest.param("local", False, id="same"),
        pytest.param("slurm", True, id="different"),
    ],
)
def test_is_foreign_handle(handle_launcher: str, expected: bool) -> None:
    """Only a handle naming a different launcher is foreign; an empty name
    (written before names were recorded) is treated as the caller's own.
    """
    handle = ProcessHandle(
        pid="1", name="step", run_id="run", launcher_name=handle_launcher
    )

    assert is_foreign_handle(handle, "local") is expected


@pytest.mark.parametrize(
    ("handle_uuid", "run_uuid", "kept"),
    [
        pytest.param("a", "a", True, id="same-run"),
        pytest.param("a", "b", False, id="another-run"),
        pytest.param("", "b", True, id="sentinel-before-uuids"),
        pytest.param("a", "", True, id="manifest-not-written"),
    ],
)
def test_prior_of_this_run(
    handle_uuid: str,
    run_uuid: str,
    kept: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Only a handle recording a different manifest uuid than the run's is
    dropped, with one warning; one whose run cannot be told is kept.
    """
    handle = ProcessHandle(pid="1", name="step", run_id="run", run_uuid=handle_uuid)

    assert prior_of_this_run(handle, run_uuid) is (handle if kept else None)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == (0 if kept else 1)


def test_prior_of_this_run_without_a_handle() -> None:
    """No sentinel stays no handle."""
    assert prior_of_this_run(None, "a") is None


@pytest.mark.parametrize("prior_status", [Status.Submitted, Status.Running])
@pytest.mark.parametrize("step_pre_run", [False, True])
def test_resolve_prior_attempt_foreign_in_progress_fails_loudly(
    tmp_path: Path, prior_status: Status, step_pre_run: bool
) -> None:
    """An in-progress attempt under another launcher cannot be tracked here, so
    it is neither adopted nor silently duplicated.
    """
    step = _live_step(tmp_path, pre_run=step_pre_run)

    with pytest.raises(CstarExpectationFailed, match="another launcher"):
        resolve_prior_attempt(step, prior_status, prior_foreign=True)

    assert KEY_CLOBBER not in step.workflow_overrides


def test_resolve_prior_attempt_foreign_in_progress_clobbered(tmp_path: Path) -> None:
    """With clobber requested, an in-progress foreign attempt is resubmitted."""
    step = _live_step(tmp_path, clobber=True)

    reuse = resolve_prior_attempt(step, Status.Running, prior_foreign=True)

    assert reuse is False


def test_resolve_prior_attempt_foreign_done_reused(tmp_path: Path) -> None:
    """A terminal foreign attempt is adopted as-is: nothing will query it."""
    step = _live_step(tmp_path)

    assert resolve_prior_attempt(step, Status.Done, prior_foreign=True) is True


def test_resolve_prior_attempt_foreign_failed_clobbers(tmp_path: Path) -> None:
    """A failed foreign attempt leads to a clobbered re-run, like a local one."""
    step = _live_step(tmp_path, pre_run=True)

    reuse = resolve_prior_attempt(step, Status.Failed, prior_foreign=True)

    assert reuse is False
    assert step.workflow_overrides[KEY_CLOBBER] is True


@pytest.mark.parametrize(
    ("rotated_log", "resume", "after_pre_run", "expected"),
    [
        pytest.param(
            None,
            False,
            False,
            "ready for run 'my-run' step 'my-step'!\n",
            id="no-rotation",
        ),
        pytest.param(
            Path("my-step.out.1"),
            False,
            False,
            "re-running step 'my-step' for run 'my-run'; prior log: my-step.out.1\n",
            id="re-run",
        ),
        pytest.param(
            Path("my-step.out.1"),
            True,
            False,
            "resuming step 'my-step' for run 'my-run' after a failed prior "
            "attempt; prior log: my-step.out.1\n",
            id="resume",
        ),
        pytest.param(
            Path("my-step.out.1"),
            False,
            True,
            "launching step 'my-step' for run 'my-run' from its pre-run; "
            "prior log: my-step.out.1\n",
            id="after-pre-run",
        ),
        pytest.param(
            Path("my-step.out.1"),
            True,
            True,
            "launching step 'my-step' for run 'my-run' from its pre-run; "
            "prior log: my-step.out.1\n",
            id="after-pre-run-wins-over-resume",
        ),
    ],
)
def test_build_attempt_log_header(
    rotated_log: Path | None,
    resume: bool,
    after_pre_run: bool,
    expected: str,
) -> None:
    """Verify the log header wording for each combination of rotation, resume
    and a preceding pre-run.
    """
    header = build_attempt_log_header(
        "my-step", "my-run", rotated_log, resume=resume, after_pre_run=after_pre_run
    )

    assert header == expected


def test_build_attempt_log_header_after_pre_run_without_rotation() -> None:
    """With no prior log to rotate, the pre-run wording does not apply."""
    header = build_attempt_log_header(
        "my-step", "my-run", None, resume=True, after_pre_run=True
    )

    assert header == "ready for run 'my-run' step 'my-step'!\n"


def _handle(
    submitted_at: datetime | None = None,
    *,
    pid: str = "12345",
    launcher_name: str = "",
    status: Status = Status.Submitted,
) -> ProcessHandle:
    return ProcessHandle(
        pid=pid,
        name="step",
        run_id="run",
        launcher_name=launcher_name,
        status=status,
        submitted_at=submitted_at,
    )


@pytest.mark.parametrize(
    ("prior_at", "dependency_ats", "expected"),
    [
        pytest.param(T0, [], False, id="no-dependencies"),
        pytest.param(T0, [None], False, id="dependency-without-submission-time"),
        pytest.param(
            T0, [T0 - timedelta(hours=1)], False, id="dependency-older-than-prior"
        ),
        pytest.param(T0, [T0], False, id="dependency-submitted-with-prior"),
        pytest.param(
            T0, [T0 + timedelta(hours=1)], True, id="dependency-newer-than-prior"
        ),
        pytest.param(
            None, [T0], True, id="prior-without-submission-time-predates-dependency"
        ),
        pytest.param(None, [None], False, id="neither-has-a-submission-time"),
        pytest.param(None, [], False, id="prior-without-time-no-dependencies"),
        pytest.param(
            T0,
            [None, T0 - timedelta(hours=1), T0 + timedelta(seconds=1)],
            True,
            id="one-newer-among-several",
        ),
        pytest.param(
            T0,
            [None, T0 - timedelta(hours=1), T0],
            False,
            id="none-newer-among-several",
        ),
    ],
)
def test_waits_on_replaced_dependency(
    prior_at: datetime | None,
    dependency_ats: list[datetime | None],
    expected: bool,
) -> None:
    """Verify a prior attempt waits on a replaced dependency only when some
    dependency was submitted after it; a prior with no recorded time predates
    every dependency that has one, and a dependency with none is unchanged.
    """
    prior = _handle(prior_at, pid="1")
    dependencies = [_handle(at, pid=str(2 + i)) for i, at in enumerate(dependency_ats)]

    assert waits_on_replaced_dependency(prior, dependencies) is expected


@pytest.mark.parametrize(
    ("prior_status", "replaced", "exp_reuse"),
    [
        pytest.param(Status.Submitted, True, False, id="queued-behind-replaced"),
        pytest.param(Status.Submitted, False, True, id="queued-unchanged-adopted"),
        pytest.param(Status.Running, True, True, id="running-adopted"),
        pytest.param(Status.Ending, True, True, id="ending-adopted"),
        pytest.param(Status.Done, True, True, id="done-adopted"),
    ],
)
def test_resolve_prior_attempt_replaced_dependency(
    tmp_path: Path, prior_status: Status, replaced: bool, exp_reuse: bool
) -> None:
    """Verify only a prior attempt still queued is replaced for a dependency
    submitted since, and without a clobber: one running or done has passed its
    dependencies, and an unchanged queue is adopted.
    """
    step = _live_step(tmp_path)

    reuse = resolve_prior_attempt(step, prior_status, replaced_dependency=replaced)

    assert reuse is exp_reuse
    assert KEY_CLOBBER not in step.workflow_overrides


def test_resolve_prior_attempt_replaced_dependency_failed_still_clobbers(
    tmp_path: Path,
) -> None:
    """A failed prior attempt is clobbered as before, replaced dependency or not."""
    step = _live_step(tmp_path)

    reuse = resolve_prior_attempt(step, Status.Failed, replaced_dependency=True)

    assert reuse is False
    assert step.workflow_overrides[KEY_CLOBBER] is True


def test_resolve_prior_attempt_replaced_dependency_foreign_fails_loudly(
    tmp_path: Path,
) -> None:
    """A queued attempt under another launcher is refused, not quietly replaced:
    this launcher cannot cancel it.
    """
    step = _live_step(tmp_path)

    with pytest.raises(CstarExpectationFailed, match="another launcher"):
        resolve_prior_attempt(
            step, Status.Submitted, prior_foreign=True, replaced_dependency=True
        )


class _FakeLauncher(Launcher[ProcessHandle]):
    """A launcher that records the cancellations and status queries made of it."""

    name = "fake"

    cancel_works: t.ClassVar[bool]
    """Whether `cancel` marks the task `Cancelled`."""
    statuses: t.ClassVar[list[Status]]
    """What `query_status` reports in turn; the last is reported from then on."""
    cancelled: t.ClassVar[list[Task[ProcessHandle]]]
    """The tasks `cancel` was awaited with."""
    queried: t.ClassVar[list[ProcessHandle]]
    """The handles `query_status` was awaited with."""

    @classmethod
    async def cancel(cls, item: Task[ProcessHandle]) -> Task[ProcessHandle]:
        cls.cancelled.append(item)
        if cls.cancel_works:
            item.status = Status.Cancelled
        return item

    @classmethod
    async def query_status(cls, item: Task[ProcessHandle] | ProcessHandle) -> Status:
        cls.queried.append(item.handle if isinstance(item, Task) else item)
        return cls.statuses.pop(0) if len(cls.statuses) > 1 else cls.statuses[0]


def _fake_launcher(
    *, cancel_works: bool, statuses: list[Status]
) -> type[_FakeLauncher]:
    """Build a fake launcher with its own record of the calls made to it."""

    class _Launcher(_FakeLauncher):
        pass

    _Launcher.cancel_works = cancel_works
    _Launcher.statuses = list(statuses)
    _Launcher.cancelled = []
    _Launcher.queried = []
    return _Launcher


async def test_cancel_prior_attempt_cancels_the_handle(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify the prior attempt is cancelled once, through a task of the step and
    handle, and the cancellation is logged.
    """
    step = _live_step(tmp_path)
    handle = _handle(status=Status.Running)
    launcher = _fake_launcher(cancel_works=True, statuses=[Status.Cancelled])

    with caplog.at_level(logging.INFO):
        await cancel_prior_attempt(launcher, step, handle)

    assert len(launcher.cancelled) == 1
    assert launcher.cancelled[0].handle is handle
    assert launcher.cancelled[0].step is step
    infos = [r.getMessage() for r in caplog.records if r.levelname == "INFO"]
    assert any("Cancelled job 12345 of step 'test step'" in m for m in infos)


async def test_cancel_prior_attempt_refuses_a_job_it_cannot_cancel(
    tmp_path: Path,
) -> None:
    """Verify a job that is still in progress after a failed cancel is refused at
    once, naming the job and the step, without waiting for it.
    """
    step = _live_step(tmp_path)
    launcher = _fake_launcher(cancel_works=False, statuses=[Status.Running])

    with pytest.raises(CstarExpectationFailed) as exc_info:
        await cancel_prior_attempt(launcher, step, _handle(status=Status.Running))

    assert "12345" in str(exc_info.value)
    assert "'test step'" in str(exc_info.value)
    assert len(launcher.cancelled) == 1
    assert len(launcher.queried) == 1


@pytest.mark.parametrize("ended", [Status.Done, Status.Failed])
async def test_cancel_prior_attempt_accepts_a_job_that_ended_by_itself(
    tmp_path: Path, ended: Status, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify a failed cancel is no obstacle when the job has ended on its own,
    and is not logged as a cancellation.
    """
    step = _live_step(tmp_path)
    launcher = _fake_launcher(cancel_works=False, statuses=[ended])

    with caplog.at_level(logging.INFO):
        await cancel_prior_attempt(launcher, step, _handle(status=Status.Running))

    assert len(launcher.cancelled) == 1
    assert not any("Cancelled job" in r.getMessage() for r in caplog.records)


@pytest.mark.usefixtures("instant_cancel")
async def test_cancel_prior_attempt_refuses_a_cancelled_job_that_never_ends(
    tmp_path: Path,
) -> None:
    """Verify a job still in progress when the time for it to end is up is refused."""
    step = _live_step(tmp_path)
    launcher = _fake_launcher(cancel_works=True, statuses=[Status.Running])

    with pytest.raises(CstarExpectationFailed, match="still Running") as exc_info:
        await cancel_prior_attempt(launcher, step, _handle(status=Status.Running))

    assert "12345" in str(exc_info.value)
    assert "'test step'" in str(exc_info.value)


async def test_cancel_prior_attempt_waits_for_the_job_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the status is polled until a cancelled job has ended."""
    monkeypatch.setattr("cstar.orchestration.launch.common.CANCEL_POLL_DELAY", 0)
    step = _live_step(tmp_path)
    launcher = _fake_launcher(
        cancel_works=True, statuses=[Status.Running, Status.Cancelled]
    )

    await cancel_prior_attempt(launcher, step, _handle(status=Status.Running))

    assert len(launcher.cancelled) == 1
    assert len(launcher.queried) == 2


async def test_cancel_prior_attempt_leaves_a_foreign_job_alone(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify a job of another launcher is neither cancelled nor queried, and is
    named in one warning.
    """
    step = _live_step(tmp_path)
    launcher = _fake_launcher(cancel_works=True, statuses=[Status.Running])
    foreign = _handle(pid="777", launcher_name="slurm", status=Status.Running)

    await cancel_prior_attempt(launcher, step, foreign)

    assert launcher.cancelled == []
    assert launcher.queried == []
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "777" in warnings[0]
    assert "slurm" in warnings[0]
    assert "'test step'" in warnings[0]
