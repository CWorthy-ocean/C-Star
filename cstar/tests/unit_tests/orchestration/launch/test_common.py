"""Tests for the prior-attempt reuse logic shared by every launcher."""

from pathlib import Path

import pytest

from cstar.orchestration.launch.common import (
    build_attempt_log_header,
    resolve_prior_attempt,
)
from cstar.orchestration.models import KEY_CLOBBER, KEY_RESUME, Application
from cstar.orchestration.orchestration import LiveStep, Status


def _live_step(
    tmp_path: Path,
    *,
    clobber: bool = False,
    resume: bool = False,
) -> LiveStep:
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    overrides: dict[str, bool] = {}
    if clobber:
        overrides[KEY_CLOBBER] = True
    if resume:
        overrides[KEY_RESUME] = True

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
    ("rotated_log", "resume", "expected"),
    [
        pytest.param(
            None,
            False,
            "ready for run 'my-run' step 'my-step'!\n",
            id="no-rotation",
        ),
        pytest.param(
            Path("my-step.out.1"),
            False,
            "re-running step 'my-step' for run 'my-run'; prior log: my-step.out.1\n",
            id="re-run",
        ),
        pytest.param(
            Path("my-step.out.1"),
            True,
            "resuming step 'my-step' for run 'my-run' after a failed prior "
            "attempt; prior log: my-step.out.1\n",
            id="resume",
        ),
    ],
)
def test_build_attempt_log_header(
    rotated_log: Path | None,
    resume: bool,
    expected: str,
) -> None:
    """Verify the log header wording for each combination of rotation and resume."""
    header = build_attempt_log_header("my-step", "my-run", rotated_log, resume=resume)

    assert header == expected
