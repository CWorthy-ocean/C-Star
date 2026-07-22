import asyncio
import datetime
import os
import subprocess
import textwrap
import typing as t
from pathlib import Path
from subprocess import run as sprun

from psutil import NoSuchProcess
from psutil import Process as PsProcess
from pydantic import PrivateAttr

from cstar.base.env import ENV_CSTAR_ORCH_LOCAL_DELAY, ENV_CSTAR_RUNID, get_env_item
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.orchestration.adapter import StepToRunRequestAdapter
from cstar.orchestration.formatting import RunRequestCommandFormatter
from cstar.orchestration.orchestration import (
    Launcher,
    LiveStep,
    ProcessHandle,
    Status,
    Task,
)
from cstar.orchestration.state import StateRepository

if t.TYPE_CHECKING:
    from cstar.orchestration.models import Step


log = get_logger(__name__)


def run_as_process(step: "Step", cmd: list[str], log_file: Path) -> dict[str, int]:
    with log_file.open("w+") as log:
        p = sprun(args=cmd, text=True, check=True, stdout=log, stderr=log)
    return {step.name: p.returncode}


class LocalHandle(ProcessHandle):
    """Handle enabling reference to a task running in local processes."""

    start_at: datetime.datetime | float
    """The process creation time as a posix timestamp (in seconds)."""

    _process: subprocess.Popen[bytes] = PrivateAttr()
    """The process handle (used only for simulating local processes)."""

    status: Status = Status.Unsubmitted
    """The current status of the task."""

    launcher_name: str = "local"
    """The launcher used to launch the process."""

    @property
    def start_ts(self) -> float:
        if isinstance(self.start_at, datetime.datetime):
            self.start_at = self.start_at.timestamp()
        return self.start_at

    @property
    def elapsed(self) -> float:
        """The number of seconds passed since the task was started.

        Returns
        -------
        float
        """
        now = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        return now - self.start_ts

    @property
    def process(self) -> subprocess.Popen[bytes]:
        return self._process

    @process.setter
    def process(self, value: subprocess.Popen[bytes]) -> None:
        self.status = Status.Submitted
        self._process = value

    @property
    def is_expired(self) -> bool:
        return not hasattr(self, "_process")


class LocalLauncher(Launcher[LocalHandle]):
    """A launcher that executes steps in a local process."""

    tasks: t.ClassVar[dict[str, str]] = {}
    use_proxy: t.ClassVar[bool] = False

    @classmethod
    def check_preconditions(cls) -> None:
        """Perform launcher-specific startup validation."""

    @staticmethod
    def _create_dep_aware_script(
        step: "LiveStep", dependencies: list[LocalHandle]
    ) -> str:
        """Create a script that will execute the desired command for a
        `Step` while also waiting for any dependencies to complete.

        Returns
        -------
        str
        """
        blueprint_path = str(step.blueprint_path)

        adapter = StepToRunRequestAdapter(step)
        command = RunRequestCommandFormatter().format(adapter.adapt())
        command = command.replace(blueprint_path, '"$BLUEPRINT_PATH"')

        pids = " ".join([f'"{h.pid}"' for h in dependencies])
        local_dep_delay = get_env_item(ENV_CSTAR_ORCH_LOCAL_DELAY).value

        return textwrap.dedent(f"""\
            #!/bin/bash
            SENTINEL_PATH="{StateRepository.sentinel_path(step.name)}"
            BLUEPRINT_PATH="{step.blueprint_path}"
            DEP_PIDS=({pids})

            # values from `Status` enum
            RUNNING={Status.Running.value}
            DONE={Status.Done.value}
            FAILED={Status.Failed.value}

            update_status() {{
                local status=$1
                if [ "$(uname)" = "Darwin" ]; then
                    sed -i '' "s/^status:.*$/status: $status/" "$2"
                else
                    sed -i "s/^status:.*$/status: $status/" "$2"
                fi
            }}

            # wait for dependencies to complete.
            for DEP_PID in "${{DEP_PIDS[@]}}"; do
                while kill -0 "$DEP_PID" 2>/dev/null; do
                    echo "Awaiting process $DEP_PID"
                    sleep {local_dep_delay}
                done
            done

            # update status to running
            update_status $RUNNING $SENTINEL_PATH

            # run the target command
            {command}

            # update the status to `Done` if target command is successful, otherwise `Failed`
            RC=$?
            STATUS=$FAILED
            if [ $RC -eq 0 ]; then
                STATUS=$DONE
            fi
            update_status $STATUS $SENTINEL_PATH
            exit $RC
        """)

    @staticmethod
    async def _submit(step: "LiveStep", dependencies: list[LocalHandle]) -> LocalHandle:
        """Submit a step to a local process.

        Parameters
        ----------
        step : LiveStep
            The step to execute in a local process.
        dependencies : list[LocalHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        LocalHandle | None
            A ProcessHandle identifying the newly submitted job.
        """
        if LocalLauncher.use_proxy:
            script = LocalLauncher._create_dep_aware_script(step, dependencies)
        else:
            adapter = StepToRunRequestAdapter(step)
            script = RunRequestCommandFormatter().format(adapter.adapt())

        step.fsm.prepare()
        step.script_path.write_text(script)
        log.debug(f"Created run script at path: {step.script_path}")
        log_file = step.log_path

        try:
            if not step.fsm.root_dir.exists():
                step.fsm.prepare()

            cmd = ["sh", str(step.script_path)]

            local_process = subprocess.Popen(
                cmd,
                cwd=step.fsm.run_dir,
                stdin=subprocess.PIPE,
                stdout=step.log_path.open("w"),
                stderr=subprocess.STDOUT,
            )

            create_time = datetime.datetime.now(tz=datetime.timezone.utc)

            if pid := local_process.pid:
                msg = f"Local run of {step.application!r} created pid: {pid}"
                log.debug(msg)
                msg = f"Logs for step {step.safe_name!r} can be found at: {log_file}"
                log.info(msg)
                LocalLauncher.tasks[step.name] = str(pid)

                try:
                    ps_process = PsProcess(pid)
                    create_timestamp = ps_process.create_time()
                    create_time = datetime.datetime.fromtimestamp(
                        create_timestamp, tz=datetime.timezone.utc
                    )
                except NoSuchProcess:
                    msg = f"Unable to retrieve exact start time for pid: {pid}"
                    log.debug(msg)

                handle = LocalHandle(
                    pid=str(pid),
                    name=step.name,
                    run_id=str(os.getenv(ENV_CSTAR_RUNID, "")),
                    start_at=create_time,
                    status=Status.Submitted,
                )

                handle.process = local_process
                return handle

        finally:
            ...

        msg = "Unable to retrieve process ID for local process."
        raise RuntimeError(msg)

    @staticmethod
    async def _status(handle: LocalHandle) -> str:
        """Retrieve the status of a step running in local process.

        Parameters
        ----------
        handle : LocalHandle
            A handle object for a process-based task.

        Returns
        -------
        str
            The current status of the step.
        """
        if handle.is_expired:
            if not Status.is_terminal(handle.status):
                return "RUNNING"
            return "COMPLETED"

        rc = handle.process.returncode

        if rc is None:
            status = "RUNNING"
        elif rc == 0:
            status = "COMPLETED"
            msg = f"Return code for handle {handle!r} is `{rc}`."
            log.debug(msg)
        else:
            status = "FAILED"
            msg = f"Failure code for handle {handle!r} is `{rc}`."
            log.warning(msg)

        return status

    @classmethod
    async def launch(
        cls,
        step: "LiveStep",
        dependencies: list[LocalHandle],
    ) -> Task[LocalHandle]:
        """Launch a step in local process.

        Parameters
        ----------
        step : LiveStep
            The step to run in a local process.
        dependencies : list[LocalHandle]
            The list of tasks that must complete prior to execution of the submitted Step.

        Returns
        -------
        Task[LocalHandle]
            A Task containing information about the newly submitted job.
        """
        tasks = [asyncio.Task(cls.query_status(h)) for h in dependencies]
        statuses = await asyncio.gather(*tasks)

        failure_found = any(map(Status.is_failure, statuses))

        if not LocalLauncher.use_proxy:
            active_found = any(map(Status.is_in_progress, statuses))

            # wait for the dependencies to complete before launching
            while active_found and not failure_found:
                await asyncio.sleep(1)

                tasks = [asyncio.Task(cls.query_status(h)) for h in dependencies]
                statuses = await asyncio.gather(*tasks)
                active_found = any(map(Status.is_in_progress, statuses))
                failure_found = any(map(Status.is_failure, statuses))

        if failure_found:
            msg = f"Dependency of step {step.name} failed. Unable to continue."
            raise CstarExpectationFailed(msg)

        live_step = LiveStep.from_step(step)
        handle = await LocalLauncher._submit(live_step, dependencies)
        return Task[LocalHandle](
            step=live_step,
            handle=handle,
        )

    @classmethod
    async def query_status(cls, item: Task[LocalHandle] | LocalHandle) -> Status:
        """Retrieve the status of an item.

        Parameters
        ----------
        item : Task[LocalHandle] | LocalHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Status
            The current status of the item.
        """
        handle = item.handle if isinstance(item, Task) else item
        raw_status = await LocalLauncher._status(handle)

        match raw_status:
            case "PENDING":
                return Status.Submitted
            case "RUNNING" | "ENDING":
                return Status.Running
            case "COMPLETED":
                return Status.Done
            case "CANCELLED":
                return Status.Cancelled
            case "FAILED":
                return Status.Failed
            case _:
                return Status.Unsubmitted

    @classmethod
    async def update_status(
        cls,
        item: Task[LocalHandle] | LocalHandle,
    ) -> tuple[bool, LocalHandle]:
        """Query and update the status for a running task.

        Parameters
        ----------
        item : Task[LocalHandle] | LocalHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Task[LocalHandle] | LocalHandle
        """
        handle = item.handle if isinstance(item, Task) else item
        prior = handle.status
        current = await LocalLauncher.query_status(item)

        if changed := prior != current:
            handle.status = current

        return changed, handle

    @classmethod
    async def cancel(cls, item: Task[LocalHandle]) -> Task[LocalHandle]:
        """Cancel a task, if possible.

        Parameters
        ----------
        item : Task or ProcessHandle
            A task or process handle to cancel.

        Returns
        -------
        Task[LocalHandle]
            The task after the cancellation attempt has completed.
        """
        process = item.handle.process

        if not item.handle.is_expired:  # wonky is-null check...
            if process.returncode is not None:
                msg = f"Unable to cancel a completed task `{process.pid}"
                log.debug(msg)
            else:
                process.kill()
                item.status = Status.Cancelled

        return item

    @classmethod
    def handle_klass(cls) -> type[LocalHandle]:
        return LocalHandle
