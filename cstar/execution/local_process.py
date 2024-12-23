import subprocess
from pathlib import Path
from typing import Optional
from datetime import datetime
from cstar.execution.handler import ExecutionHandler, ExecutionStatus


class LocalProcess(ExecutionHandler):
    def __init__(
        self,
        commands: str,
        output_file: Optional[str | Path] = None,
        run_path: Optional[str | Path] = None,
    ):
        self.commands = commands

        default_name = (
            f"cstar_process_{datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')}"
        )
        self.run_path = Path(run_path) if run_path is not None else Path.cwd()
        self.output_file = (
            self.run_path / f"{default_name}.out"
            if output_file is None
            else output_file
        )

        self._process = None
        self._output_file_handle = None
        self._cancelled = False

    def start(self):
        # Open the output file to write to
        self._output_file_handle = open(self.output_file, "w")
        local_process = subprocess.Popen(
            self.commands.split(),
            # shell=True,
            cwd=self.run_path,
            stdin=subprocess.PIPE,
            stdout=self._output_file_handle,
            stderr=subprocess.STDOUT,
        )
        self._process = local_process

    @property
    def status(self):
        """Return the current status of the process."""
        if self._cancelled:
            return ExecutionStatus.CANCELLED
        if self._process is None:
            return ExecutionStatus.UNSUBMITTED
        if self._process.poll() is None:
            return ExecutionStatus.RUNNING
        if self._process.returncode == 0:
            if self._output_file_handle:
                self._output_file_handle.close()
                self._output_file_handle = None
            return ExecutionStatus.COMPLETED
        elif self._process.returncode is not None:
            if self._output_file_handle:
                self._output_file_handle.close()
                self._output_file_handle = None
            return ExecutionStatus.FAILED
        return ExecutionStatus.UNKNOWN

    def cancel(self):
        """Cancel the running process."""
        if self._process and self.status == ExecutionStatus.RUNNING:
            self._process.terminate()  # Send SIGTERM to allow graceful shutdown
            try:
                self._process.wait(timeout=5)  # Wait for it to terminate
            except subprocess.TimeoutExpired:
                self.process.kill()  # Forcefully kill if it doesn't terminate
            finally:
                if self._output_file_handle:
                    self._output_file_handle.close()
                    self._output_file_handle = None
                self._cancelled = True
