import asyncio
import datetime
import shlex
import subprocess
from pathlib import Path

from psutil import NoSuchProcess
from psutil import Process as PsProcess

from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.converter.converter import get_command_mapping
from cstar.orchestration.models import Application, Step
from cstar.orchestration.orchestration import (
    Launcher,
    ProcessHandle,
    Status,
    Task,
)


class LocalHandle(ProcessHandle):
    """Handle enabling reference to a task running in local processes."""

    status_file: Path
    """Path to a file containing the return code of the process."""
    create_time: float
    """The process creation time as a posix timestamp (in seconds)."""
    output_file: Path
    """Path to the file where the process's standard output and error are written."""

    def __init__(
        self,
        pid: str,
        create_time: float,
        status_file: Path,
        output_file: Path,
    ) -> None:
        """Initialize the local handle.

        Parameters
        ----------
        pid : str
            The process ID.
        create_time : float
            The process start time (posix timestamp).
        status_file : Path
            Path to the file where exit status is persisted.
        output_file : Path
            Path to the stdout/stderr log file.
        """
        super().__init__(pid=pid)
        self.create_time = create_time
        self.status_file = status_file
        self.output_file = output_file


class LocalLauncher(Launcher[LocalHandle]):
    """A launcher that executes steps in a local process with persistent status."""

    @staticmethod
    async def _submit(step: Step) -> LocalHandle:
        """Submit a step to run as a local process.

        Parameters
        ----------
        step : Step
            The step to execute.

        Returns
        -------
        LocalHandle
            A ProcessHandle identifying the newly submitted job.
        """
        # We reuse the conversion logic from SlurmLauncher to get the real command

        step_converter = get_command_mapping(
            Application(step.application), LocalLauncher
        )
        if step_converter is None:
            msg = f"No command converter found for application: {step.application}"
            raise ValueError(msg)

        cmd_str = step_converter(step)

        # Determine paths for logs and status tracking
        # We assume the orchestrator has set up the environment/directories
        # Using a pattern similar to SlurmLauncher
        from cstar.orchestration.models import RomsMarblBlueprint
        from cstar.orchestration.serialization import deserialize

        bp = deserialize(Path(step.blueprint_path), RomsMarblBlueprint)
        step_fs = step.file_system(bp)

        output_file = step_fs.logs_dir / f"{step.safe_name}.out"
        status_file = step_fs.logs_dir / f"{step.safe_name}.status"

        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Wrap the command to capture the exit code even if we aren't watching
        # (cmd) > output 2>&1 ; echo $? > status_file
        wrapped_cmd = f"({cmd_str}) > {shlex.quote(str(output_file))} 2>&1; echo $? > {shlex.quote(str(status_file))}"

        proc = subprocess.Popen(
            wrapped_cmd,
            shell=True,
            start_new_session=True,  # run in its own session so it survives parent exit
            cwd=step_fs.work_dir,
        )

        pid = proc.pid
        try:
            ps_proc = PsProcess(pid)
            create_time = ps_proc.create_time()
        except NoSuchProcess:
            # Fallback if it finished instantly
            create_time = datetime.datetime.now().timestamp()

        return LocalHandle(
            pid=str(pid),
            create_time=create_time,
            status_file=status_file,
            output_file=output_file,
        )

    @classmethod
    async def launch(cls, step: Step, dependencies: list[LocalHandle]) -> Task:
        """Launch a step in local process, honoring dependencies.

        Parameters
        ----------
        step : Step
            The step to run.
        dependencies : list[LocalHandle]
            The list of tasks that must complete prior to execution.

        Returns
        -------
        Task[LocalHandle]
            A Task containing information about the newly submitted job.
        """
        # Wait for all dependencies to reach a terminal state
        for dep in dependencies:
            while True:
                status = await cls.query_status(
                    None, dep
                )  # step is unused in our query_status
                if status == Status.Done:
                    break
                if status in {Status.Failed, Status.Cancelled}:
                    raise CstarExpectationFailed(
                        f"Dependency {dep.pid} failed with status {status}. "
                        f"Unable to launch {step.name}."
                    )
                await asyncio.sleep(2)

        handle = await cls._submit(step)
        return Task(
            status=Status.Submitted,
            step=step,
            handle=handle,
        )

    @classmethod
    async def query_status(
        cls, step: Step | None, item: Task[LocalHandle] | LocalHandle
    ) -> Status:
        """Retrieve the status of an item by checking disk and process table.

        Parameters
        ----------
        step : Step, optional
            The step that will be queried for (unused in this implementation).
        item : Task[LocalHandle] | LocalHandle
            An item with a handle to be used to execute a status query.

        Returns
        -------
        Status
            The current status of the item.
        """
        handle = item.handle if isinstance(item, Task) else item

        # 1. Check if the status file exists (terminal state reached)
        if handle.status_file.exists():
            try:
                exit_code = int(handle.status_file.read_text().strip())
            except (ValueError, OSError):
                # File might be incomplete or locked; fall through to process check
                pass
            else:
                return Status.Done if exit_code == 0 else Status.Failed

        # 2. Check if the process is still running
        try:
            pid = int(handle.pid)
            ps_proc = PsProcess(pid)
            # Verify it's the SAME process by checking creation time
            if abs(ps_proc.create_time() - handle.create_time) < 1.0:
                return Status.Running
        except (NoSuchProcess, ValueError):
            pass

        # 3. If no process and no status file, it might have been killed or vanished
        if handle.status_file.exists():
            # Re-check status file in case it appeared between steps
            try:
                exit_code = int(handle.status_file.read_text().strip())
                return Status.Done if exit_code == 0 else Status.Failed
            except (ValueError, OSError):
                pass

        return Status.Failed

    @classmethod
    async def cancel(cls, item: Task[LocalHandle]) -> Task[LocalHandle]:
        """Cancel a task by terminating its process.

        Parameters
        ----------
        item : Task[LocalHandle]
            The task to cancel.

        Returns
        -------
        Task[LocalHandle]
            The task after the cancellation attempt.
        """
        handle = item.handle
        try:
            pid = int(handle.pid)
            ps_proc = PsProcess(pid)
            if abs(ps_proc.create_time() - handle.create_time) < 1.0:
                ps_proc.terminate()
                try:
                    ps_proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    ps_proc.kill()
                item.status = Status.Cancelled
        except (NoSuchProcess, ValueError):
            pass

        return item
