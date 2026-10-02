"""Tests for the prior-attempt reuse logic shared by every launcher."""

from pathlib import Path

import pytest

from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.launch.common import (
    build_attempt_log_header,
    is_foreign_handle,
    resolve_prior_attempt,
)
from cstar.orchestration.models import (
    KEY_CLOBBER,
    KEY_PRE_RUN,
    KEY_RESUME,
    Application,
)
from cstar.orchestration.orchestration import LiveStep, ProcessHandle, Status


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
