import subprocess
from datetime import datetime
from pathlib import Path

from cstar.execution.handler import ExecutionHandler, ExecutionStatus


class LocalProcess(ExecutionHandler):
    """Execution handler for managing and monitoring local subprocesses.

    This class handles the execution of commands as local subprocesses,
    allowing users to monitor their status, stream output, and manage
    lifecycle events such as cancellation.

    Attributes
    ----------
    commands : str
        The shell command(s) to be executed as a subprocess.
    run_path : Path
        The directory from which the subprocess will be executed.
    output_file : Path
        The file where the subprocess's standard output and error will
        be written.
    status : ExecutionStatus
        The current status of the subprocess, represented as an
        `ExecutionStatus` enum value.

    Methods
    -------
    start()
        Start the subprocess using the specified command.
    cancel()
        Cancel the running subprocess.
    updates(seconds=10)
        Stream live updates from the subprocess's output file for a
        specified duration.
    """

    def __init__(
        self,
        commands: str,
        output_file: str | Path | None = None,
        run_path: str | Path | None = None,
    ):
        """Initialize a `LocalProcess` instance.

        Parameters
        ----------
        commands : str
            The shell command(s) to execute as a subprocess.
        output_file : str or Path, optional
            The file path where the subprocess's standard output and error
            will be written. Defaults to an auto-generated file in the
            `run_path` directory.
        run_path : str or Path, optional
            The directory from which the subprocess will be executed.
            Defaults to the current working directory.
        """
        self.commands = commands
        self.run_path = Path(run_path) if run_path is not None else Path.cwd()
        self._default_name = (
            f"cstar_process_{datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')}"
        )
        self._output_file = (
            Path(output_file) if output_file is not None else output_file
        )

        self._output_file_handle = None
        self._process = None
        self._returncode = None
        self._cancelled = False

    def __str__(self) -> str:
        base_str = self.__class__.__name__ + "\n"
        base_str += "-" * (len(base_str) - 1)
        base_str += f"\nCommands: {self.commands}"
        base_str += f"\nRun path: {self.run_path}"
        base_str += f"\nOutput file: {self.output_file}"
        base_str += f"\nStatus: {self.status}"

        return base_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\ncommands = {self.commands!r},"
        repr_str += f"\noutput_file = {self.output_file!r},"
        repr_str += f"\nrun_path = {self.run_path!r}"
        repr_str += "\n)"

        repr_str += f"\nState: <status = {self.status!r}>"

        return repr_str

    def start(self):
        """Start the local process.

        This method initiates the execution of the command specified during
        initialization. The command runs in a subprocess, with its standard
        output and error directed to the output file.

        Notes
        -----
        - The output file is opened and actively written to during execution.
        - Use the `status` property to monitor the current state of the
          subprocess.

        See Also
        --------
        cancel : Terminates the running subprocess.
        """
        # Open the output file to write to
        self._output_file_handle = open(self.output_file, "w")
        local_process = subprocess.Popen(
            self.commands.split(),
            cwd=self.run_path,
            stdin=subprocess.PIPE,
            stdout=self._output_file_handle,
            stderr=subprocess.STDOUT,
        )
        self._process = local_process

    @property
    def output_file(self) -> Path:
        """The file in which to write this task's STDOUT and STDERR."""
        return (
            self.run_path / f"{self._default_name}.out"
            if self._output_file is None
            else self._output_file
        )

    @property
    def status(self) -> ExecutionStatus:
        """Retrieve the current status of the local process.

        This property determines the status of the process based on its
        lifecycle and return code. The possible statuses are represented by
        the `ExecutionStatus` enum.

        Returns
        -------
        ExecutionStatus
            The current status of the process. Possible values include:
            - `ExecutionStatus.UNSUBMITTED`: The task has not been started.
            - `ExecutionStatus.RUNNING`: The task is currently executing.
            - `ExecutionStatus.COMPLETED`: The task finished successfully.
            - `ExecutionStatus.FAILED`: The task finished unsuccessfully.
            - `ExecutionStatus.CANCELLED`: The task was cancelled using LocalProcess.cancel()
        """
        if self._process is not None:
            if self._process.poll() is None:
                return ExecutionStatus.RUNNING
            else:
                self._drop_process()

        if self._cancelled:
            return ExecutionStatus.CANCELLED

        match self._returncode:
            case None:
                return ExecutionStatus.UNSUBMITTED
            case 0:
                return ExecutionStatus.COMPLETED
            case _:
                return ExecutionStatus.FAILED

    def _drop_process(self) -> None:
        """Un-sets private attributes associated with a completed subprocess.

        This method:
        - Sets LocalProcess._returncode to LocalProcess._process.returncode
        - Sets LocalProcess._process to None
        - Closes output_file
        - Sets LocalProcess._output_file_handle to None

        If the _process attribute is not set, no action is taken.
        If it is set to a running process, a RuntimeError is raised.
        """
        if self._process is None:
            return
        elif self._process.poll() is not None:
            self._returncode = self._process.returncode
            self._process = None
        else:
            raise RuntimeError(
                "LocalProcess._drop_process() called on still-active process. Await completion or use LocalProcess.cancel()"
            )

        if self._output_file_handle:
            self._output_file_handle.close()
            self._output_file_handle = None

    def cancel(self):
        """Cancel the local process.

        This method terminates the subprocess if it is currently running.
        A graceful shutdown is attempted using `terminate` (SIGTERM). If the
        subprocess does not terminate within a timeout period, it is forcefully
        killed using `kill` (SIGKILL).

        Notes
        -----
        - The status of the subprocess is updated to `ExecutionStatus.CANCELLED`
          after termination or killing.
        - The method ensures that the output file handle is closed after
          cancelling the process.

        See Also
        --------
        wait : Wait for the local process to finish
        """
        if self._process and self.status == ExecutionStatus.RUNNING:
            try:
                self._process.terminate()  # Send SIGTERM to allow graceful shutdown
                self._process.wait(timeout=5)  # Wait for it to terminate
            except subprocess.TimeoutExpired:
                self._process.kill()  # Forcefully kill if it doesn't terminate
            finally:
                if self._output_file_handle:
                    self._output_file_handle.close()
                    self._output_file_handle = None
                self._cancelled = True
                self._drop_process()
        else:
            self.log.info(f"Cannot cancel job with status '{self.status}'")
            return

    def wait(self):
        """Wait for the local process to finish.

        See Also
        --------
        cancel : end the current process
        """
        if self.status == ExecutionStatus.RUNNING:
            self._process.wait()
        else:
            self.log.info(
                f"Cannot wait for process with execution status '{self.status}'"
            )
