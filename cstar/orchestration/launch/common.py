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
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_RESUME
from cstar.orchestration.models import KEY_CLOBBER, KEY_RESUME
from cstar.orchestration.orchestration import Status

if t.TYPE_CHECKING:
    from cstar.orchestration.orchestration import LiveStep, ProcessHandle

log = get_logger(__name__)


def is_foreign_handle(handle: "ProcessHandle", launcher_name: str) -> bool:
    """Report whether a handle was created by a launcher other than `launcher_name`.

    A foreign handle cannot be queried by the calling launcher (a local pid is
    no SLURM job id, and vice versa), so only its persisted status is usable.
    An empty `launcher_name` on the handle means it was written before names
    were recorded; it is treated as the caller's own for backward compatibility.

    Parameters
    ----------
    handle : ProcessHandle
        The handle read from a persisted sentinel.
    launcher_name : str
        The name of the launcher asking.

    Returns
    -------
    bool
    """
    return bool(handle.launcher_name) and handle.launcher_name != launcher_name


def resolve_prior_attempt(
    step: "LiveStep",
    prior_status: Status,
    *,
    prior_pre_run: bool = False,
    prior_foreign: bool = False,
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
    prior_foreign : bool
        Whether the prior handle was created by another launcher (see
        `is_foreign_handle`), so its `prior_status` is only the persisted one.

    Returns
    -------
    bool
        `True` when the prior handle should be adopted in place of
        submitting a new one.

    Raises
    ------
    CstarExpectationFailed
        If the prior attempt is still in progress and this launch cannot
        track it, unless the step is clobbered. That is the case when it was
        created by another launcher, or when it is a pre-run and this launch
        is a real run (adopting it would report the step done without the
        model ever launching).
    """
    reuse: bool

    untrackable_pre_run = prior_pre_run and not step.pre_run
    if Status.is_in_progress(prior_status) and (prior_foreign or untrackable_pre_run):
        if not step.clobber:
            if prior_foreign:
                msg = (
                    f"Step {step.name!r} has an attempt still {prior_status.name} "
                    f"under another launcher; wait for it to finish, or re-run "
                    f"with {ARG_CLOBBER} to start over."
                )
            else:
                msg = (
                    f"The pre-run of step {step.name!r} is still {prior_status.name}; "
                    f"wait for it to finish, or re-run with {ARG_CLOBBER} to start over."
                )
            raise CstarExpectationFailed(msg)
        reuse = False
        log.debug("Prior attempt of %r in progress; clobbering.", step.name)
    elif (
        prior_status is Status.Done
        and prior_pre_run
        and not step.pre_run
        and not step.clobber
    ):
        log.debug(
            "Prior run of %r was a pre-run; attaching to its prepared working "
            "directory (%s).",
            step.name,
            ARG_RESUME,
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
    after_pre_run: bool = False,
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
    after_pre_run : bool
        Whether this attempt launches a real run from a completed pre-run.

    Returns
    -------
    str
        A single line, including a trailing newline, to write as the first
        line of the fresh log.
    """
    if rotated_log is None:
        return f"ready for run {run_id!r} step {step_name!r}!\n"

    if after_pre_run:
        return (
            f"launching step {step_name!r} for run {run_id!r} from its pre-run; "
            f"prior log: {rotated_log.name}\n"
        )

    if resume:
        return (
            f"resuming step {step_name!r} for run {run_id!r} after a failed "
            f"prior attempt; prior log: {rotated_log.name}\n"
        )

    return (
        f"re-running step {step_name!r} for run {run_id!r}; "
        f"prior log: {rotated_log.name}\n"
    )
