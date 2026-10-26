"""Prior-attempt reuse logic shared by every launcher.

Both `SlurmLauncher` and `LocalLauncher` reload a step's persisted sentinel
handle when re-entering a run (`workplan submit --run-id`), decide whether to
reuse it or resubmit, cancel it when it is replaced while still in progress,
and, on resubmission, rotate the prior log out of the way and annotate the
fresh one. Centralizing that decision and the log wording here keeps the two
launchers in lockstep and gives the logic a single owner and test surface.
"""

import asyncio
import time
import typing as t
from pathlib import Path

from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_RESUME
from cstar.orchestration.models import KEY_CLOBBER, KEY_RESUME
from cstar.orchestration.orchestration import Status, Task

if t.TYPE_CHECKING:
    from collections.abc import Sequence

    from cstar.orchestration.orchestration import Launcher, LiveStep, ProcessHandle

log = get_logger(__name__)

_THandle = t.TypeVar("_THandle", bound="ProcessHandle")

CANCEL_TIMEOUT: t.Final[float] = 90.0
"""Seconds a cancelled prior attempt is given to end before its step is refused."""
CANCEL_POLL_DELAY: t.Final[float] = 2.0
"""Seconds between the status checks made while a cancelled prior attempt ends."""


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


def prior_of_this_run(
    handle: "_THandle | None",
    run_uuid: str,
) -> "_THandle | None":
    """Drop a persisted prior handle that belongs to another run of the same run-id.

    Sentinels are kept by run-id, so a run started under the id of one whose
    directory was removed or moved finds that run's handles. Adopting one
    would report the step done without running it. A handle is another run's
    when it records a manifest uuid other than `run_uuid`; one recording none
    (written before the uuid was recorded, or when the manifest could not be
    written) cannot be told apart and is kept.

    Parameters
    ----------
    handle : ProcessHandle | None
        The handle read from the step's sentinel, if any.
    run_uuid : str
        The uuid of the current run's manifest; empty when it could not be written.

    Returns
    -------
    ProcessHandle | None
        `handle`, or `None` when it belongs to another run.
    """
    if handle is None or not handle.run_uuid or not run_uuid:
        return handle
    if handle.run_uuid == run_uuid:
        return handle

    log.warning(
        "Step %r has a sentinel from another run of %r (%s, not %s); "
        "launching the step again.",
        handle.name,
        handle.run_id,
        handle.run_uuid,
        run_uuid,
    )
    return None


def waits_on_replaced_dependency(
    prior: "ProcessHandle",
    dependencies: "Sequence[ProcessHandle]",
) -> bool:
    """Report whether a dependency was submitted after a step's prior attempt.

    A queued attempt waits on the jobs its dependencies had when it was
    submitted (a SLURM `afterok`, the pids a local proxy polls). A dependency
    submitted since then replaced the job it waits on, which will not succeed
    for it. A prior attempt with no recorded submission time predates every
    one that has a record; a dependency with none cannot be told and is taken
    as unchanged.

    Parameters
    ----------
    prior : ProcessHandle
        The handle read from the step's sentinel.
    dependencies : Sequence[ProcessHandle]
        The handles of the step's dependencies in this launch.

    Returns
    -------
    bool
    """
    return any(
        dep.submitted_at is not None
        and (prior.submitted_at is None or dep.submitted_at > prior.submitted_at)
        for dep in dependencies
    )


def resolve_prior_attempt(
    step: "LiveStep",
    prior_status: Status,
    *,
    prior_pre_run: bool = False,
    prior_foreign: bool = False,
    replaced_dependency: bool = False,
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
    replaced_dependency : bool
        Whether a dependency was submitted after the prior attempt (see
        `waits_on_replaced_dependency`). A prior attempt still queued then
        waits on a job that no longer stands for the dependency, and is
        replaced; one already running has passed its dependencies and is not.

    Returns
    -------
    bool
        `True` when the prior handle should be adopted in place of
        submitting a new one. A prior attempt that is in progress and not
        adopted must be cancelled before the step is submitted again (see
        `cancel_prior_attempt`).

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
    elif prior_status is Status.Submitted and replaced_dependency:
        log.debug(
            "Prior run of %r is queued behind a dependency submitted since; "
            "replacing it.",
            step.name,
        )
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


async def cancel_prior_attempt(
    launcher: "type[Launcher[_THandle]]",
    step: "LiveStep",
    handle: _THandle,
) -> None:
    """Cancel a step's prior attempt that is still in progress, before it is replaced.

    Submitting the step again without this leaves two jobs sharing its working
    directory, or a queued job that runs the step's old definition. The step is
    submitted only once the attempt has ended, so the replacement cannot clear
    the directory under it. An attempt created by another launcher cannot be
    cancelled by `launcher` (a local pid is no SLURM job id, and vice versa); it
    is named in a warning and left alone.

    Parameters
    ----------
    launcher : type[Launcher]
        The launcher replacing the attempt.
    step : LiveStep
        The step whose attempt is replaced.
    handle : ProcessHandle
        The handle of the prior attempt, read from the step's sentinel.

    Raises
    ------
    CstarExpectationFailed
        If the attempt could not be cancelled, or did not end within
        `CANCEL_TIMEOUT` seconds of being cancelled.
    """
    if is_foreign_handle(handle, launcher.name):
        log.warning(
            "Step %r has an attempt still %s under the %s launcher (job %s), "
            "which the %s launcher cannot cancel; stop it yourself, as the step "
            "is submitted again beside it.",
            step.name,
            handle.status.name,
            handle.launcher_name,
            handle.pid,
            launcher.name,
        )
        return

    cancelled = await launcher.cancel(Task(step=step, handle=handle))
    deadline = time.monotonic() + CANCEL_TIMEOUT
    while Status.is_in_progress(status := await launcher.query_status(handle)):
        if cancelled.status is not Status.Cancelled:
            msg = (
                f"Could not cancel job {handle.pid} of step {step.name!r}, which "
                f"is still {status.name}; cancel it yourself, then submit again"
            )
            raise CstarExpectationFailed(msg)
        if time.monotonic() > deadline:
            msg = (
                f"Job {handle.pid} of step {step.name!r} was cancelled but is "
                f"still {status.name} after {CANCEL_TIMEOUT:g}s; submit again "
                "once it has ended"
            )
            raise CstarExpectationFailed(msg)
        await asyncio.sleep(CANCEL_POLL_DELAY)

    if cancelled.status is Status.Cancelled:
        log.info(
            "Cancelled job %s of step %r to submit it again.", handle.pid, step.name
        )


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
