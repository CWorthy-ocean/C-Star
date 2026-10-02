"""Prior-attempt reuse logic shared by every launcher.

Both `SlurmLauncher` and `LocalLauncher` reload a step's persisted sentinel
handle when re-entering a run (`workplan run --run-id`), decide whether to
reuse it or resubmit, and, on resubmission, rotate the prior log out of the
way and annotate the fresh one. Centralizing that decision and the log
wording here keeps the two launchers in lockstep and gives the logic a
single owner and test surface.
"""

import typing as t
from pathlib import Path

from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.orchestration.models import KEY_CLOBBER, KEY_RESUME
from cstar.orchestration.orchestration import Status

if t.TYPE_CHECKING:
    from cstar.orchestration.orchestration import LiveStep

log = get_logger(__name__)


def resolve_prior_attempt(
    step: "LiveStep", prior_status: Status, *, prior_pre_run: bool = False
) -> bool:
    """Decide whether a step's persisted prior handle should be reused.

    Parameters
    ----------
    step : LiveStep
        The step being (re-)launched. When the prior attempt failed and the
        step is not marked for resume, this mutates `step.workflow_overrides`
        to force a clobbered re-run; when the prior attempt was a completed
        pre-run and this launch is not, it marks the step for resume so the
        step attaches to the prepared working directory.
    prior_status : Status
        The freshly-queried status of the step's persisted prior handle.
    prior_pre_run : bool
        Whether the prior attempt ran in pre-run mode (`ProcessHandle.pre_run`).

    Returns
    -------
    bool
        `True` when the prior handle should be adopted in place of
        submitting a new one.

    Raises
    ------
    CstarExpectationFailed
        If the prior attempt is a pre-run that is still in progress and this
        launch is a real run: adopting it would report the step done without
        the model ever launching.
    """
    reuse: bool

    if prior_pre_run and not step.pre_run and Status.is_in_progress(prior_status):
        if not step.clobber:
            msg = (
                f"The pre-run of step {step.name!r} is still {prior_status.name}; "
                "wait for it to finish, or re-run with --clobber to start over."
            )
            raise CstarExpectationFailed(msg)
        reuse = False
        log.debug("Prior pre-run of %r in progress; clobbering.", step.name)
    elif (
        prior_status is Status.Done
        and prior_pre_run
        and not step.pre_run
        and not step.clobber
    ):
        log.debug(
            "Prior run of %r was a pre-run; attaching to its prepared working "
            "directory (--resume).",
            step.name,
        )
        step.workflow_overrides[KEY_RESUME] = True
        reuse = False
    elif Status.is_failure(prior_status):
        if step.resume:
            log.debug(
                "Prior run of %r in %r state; resuming in place (--resume).",
                step.name,
                prior_status.name,
            )
        else:
            log.debug("Prior run of %r in fail state. Re-running.", step.name)
            step.workflow_overrides[KEY_CLOBBER] = True
        reuse = False
    elif Status.is_terminal(prior_status) or Status.is_in_progress(prior_status):
        # re-use the result from a run that terminated successfully, or adopt
        # a job that is still queued/running instead of submitting a
        # duplicate, unless the step is configured to be clobbered
        reuse = not step.clobber
        log.debug(
            "Prior run of %r in %r state. Re-use: %s",
            step.name,
            prior_status.name,
            reuse,
        )
    else:
        reuse = False
        log.debug(
            "Prior run of %r in %r state; treating as unsubmitted.",
            step.name,
            prior_status.name,
        )

    return reuse


def build_attempt_log_header(
    step_name: str,
    run_id: str,
    rotated_log: Path | None,
    *,
    resume: bool,
) -> str:
    """Compose the first line written to a step's log for this attempt.

    Parameters
    ----------
    step_name : str
        The name of the step being (re-)launched.
    run_id : str
        The run-id the step is executing under.
    rotated_log : Path | None
        The path a prior log was rotated to, or `None` when no prior log
        existed to rotate.
    resume : bool
        Whether this attempt resumes a failed prior attempt in place.

    Returns
    -------
    str
        A single line, including a trailing newline, to write as the first
        line of the fresh log.
    """
    if rotated_log is None:
        return f"ready for run {run_id!r} step {step_name!r}!\n"

    if resume:
        return (
            f"resuming step {step_name!r} for run {run_id!r} after a failed "
            f"prior attempt; prior log: {rotated_log.name}\n"
        )

    return (
        f"re-running step {step_name!r} for run {run_id!r}; "
        f"prior log: {rotated_log.name}\n"
    )
