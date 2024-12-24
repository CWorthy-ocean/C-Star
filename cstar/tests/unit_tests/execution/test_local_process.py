from unittest.mock import MagicMock, patch
from pathlib import Path
from cstar.execution.local_process import LocalProcess, ExecutionStatus
import subprocess


class TestLocalProcess:
    """Tests for the `LocalProcess` class.

    This test class covers the functionality of the `LocalProcess` class, which is a subclass
    of `ExecutionHandler` designed to manage task execution in a local subprocess. It validates the
    class's behavior across its lifecycle, including initialization, starting processes,
    monitoring status, and cancellation.

    Tests
    -----
    test_initialization_defaults
        Ensures that default attributes are correctly assigned during initialization.
    test_start_success
        Verifies that the subprocess starts successfully with valid commands and updates the status.
    test_status
        Confirms the `status` property reflects the process's lifecycle states (e.g., RUNNING, COMPLETED, FAILED).
    test_status_failed_closes_file_handle
        Ensures that a failed process closes the output file handle and updates the status to FAILED.
    test_status_unknown_no_returncode
        Validates that the status is `UNKNOWN` when the subprocess state cannot be determined.
    test_cancel_graceful_termination
        Ensures that `cancel` gracefully terminates a running process using `terminate` without requiring `kill`.
    test_cancel_forceful_termination
        Confirms that `cancel` forcefully terminates a process using `kill` after `terminate` times out.
    test_cancel_non_running_process
        Validates that `cancel` does not attempt to terminate or kill a non-running process and provides feedback.
    """

    def setup_method(self, method):
        """Sets up reusable parameters and paths for tests in `TestLocalProcess`.

        This method initializes common attributes for creating a `LocalProcess`
        instance, including the commands to execute, the output file path, and
        the run directory.

        Parameters
        ----------
        method : Callable
            The test method being executed. This parameter is part of the pytest
            `setup_method` signature but is not used in this setup.

        Attributes
        ----------
        - commands : str
            The shell command(s) to execute.
        """
        self.commands = "echo Hello, World"

    def test_initialization_defaults(self, tmp_path):
        """Ensures that default attributes are correctly applied during initialization.

        This test verifies that if `run_path` and `output_file` are not explicitly
        provided, `LocalProcess` assigns default values based on the current
        working directory.

        Asserts
        -------
        - That `run_path` defaults to `Path.cwd()`.
        - That `output_file` is auto-generated with a timestamped name.
        """
        process = LocalProcess(commands=self.commands, run_path=None, output_file=None)
        assert process.run_path == Path.cwd()
        assert process.output_file.name.startswith("cstar_process_")
        assert process.status == ExecutionStatus.UNSUBMITTED

    @patch("subprocess.Popen")
    def test_start_success(self, mock_popen, tmp_path):
        """Verifies that the subprocess starts successfully with valid commands.

        This test ensures that the `start` method:
        - Initializes a subprocess with the correct parameters (commands, working directory).
        - Redirects the standard output and error to the specified output file.
        - Updates the `status` property to `RUNNING`.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate a successful subprocess startup.

        Asserts
        -------
        - That the subprocess is called with the correct arguments.
        - That the `status` property reflects the `RUNNING` state after startup.
        """
        mock_popen.return_value.poll.return_value = None  # Simulate running process
        process = LocalProcess(
            commands=self.commands,
            run_path=tmp_path,
            output_file=tmp_path / "output.log",
        )

        with patch("builtins.open", MagicMock()) as mock_open:
            process.start()
            mock_open.assert_called_once_with(process.output_file, "w")

        mock_popen.assert_called_once_with(
            self.commands.split(),
            cwd=tmp_path,
            stdin=subprocess.PIPE,
            stdout=mock_open.return_value,
            stderr=subprocess.STDOUT,
        )
        assert process.status == ExecutionStatus.RUNNING

    @patch("subprocess.Popen")
    def test_status(self, mock_popen, tmp_path):
        """Tests that the `status` property reflects the subprocess lifecycle.

        This test covers the following states:
        - `UNSUBMITTED`: Before `start()` is called.
        - `RUNNING`: After `start()` is called and the process is active.
        - `COMPLETED`: After the process terminates successfully.
        - `FAILED`: After the process terminates with a non-zero return code.
        - `UNKNOWN`: When the process state is indeterminate.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate the subprocess lifecycle.

        Asserts
        -------
        - That the `status` property reflects the correct `ExecutionStatus` for
          each stage of the process lifecycle.
        """
        # Mock subprocess behavior
        mock_process = MagicMock()
        mock_popen.return_value = mock_process

        # Initial state: UNSUBMITTED
        process = LocalProcess(
            commands=self.commands,
            run_path=tmp_path,
            output_file=tmp_path / "output.log",
        )
        assert process.status == ExecutionStatus.UNSUBMITTED

        # After starting: RUNNING
        mock_process.poll.return_value = None  # Simulate running process
        process.start()
        assert process.status == ExecutionStatus.RUNNING

        # After completion: COMPLETED
        mock_process.poll.return_value = 0  # Simulate successful termination
        mock_process.returncode = 0
        assert process.status == ExecutionStatus.COMPLETED

        # After failure: FAILED
        mock_process.poll.return_value = 1  # Simulate unsuccessful termination
        mock_process.returncode = 1
        assert process.status == ExecutionStatus.FAILED

        # Indeterminate state: UNKNOWN
        mock_process.returncode = None  # Indeterminate state
        process._process = mock_process  # Ensure process is assigned
        assert process.status == ExecutionStatus.UNKNOWN

    @patch("subprocess.Popen")
    def test_status_failed_closes_file_handle(self, mock_popen, tmp_path):
        """Verifies that a failed process closes the output file handle.

        This test ensures:
        - The `_output_file_handle` is closed and set to `None` after failure.
        - The status is set to `ExecutionStatus.FAILED`.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate a failed subprocess.

        Asserts
        -------
        - That `_output_file_handle.close()` is called.
        - That `_output_file_handle` is set to `None`.
        - That the status is `FAILED`.
        """
        # Mock subprocess behavior
        mock_process = MagicMock()
        mock_popen.return_value = mock_process
        mock_process.returncode = 1  # Process failed

        # Create the LocalProcess instance
        process = LocalProcess(
            commands=self.commands,
            run_path=tmp_path,
            output_file=tmp_path / "output.log",
        )
        process._process = mock_process

        # Mock the output file handle
        mock_file_handle = MagicMock()
        process._output_file_handle = mock_file_handle

        # Check the status
        assert process.status == ExecutionStatus.FAILED
        mock_file_handle.close.assert_called_once()  # Ensure the file handle is closed
        assert process._output_file_handle is None  # Ensure the file handle is cleared

    @patch("subprocess.Popen")
    def test_cancel_graceful_termination(self, mock_popen, tmp_path):
        """Ensures that the `cancel` method gracefully terminates a running process.

        This test verifies:
        - `terminate()` is called to stop the process.
        - `kill()` is not called when `terminate()` succeeds.
        - The `status` property is updated to `CANCELLED`.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate the subprocess lifecycle.

        Asserts
        -------
        - That `terminate()` is called to stop the process.
        - That `kill()` is not called if `terminate()` succeeds.
        - That the `status` property reflects the `CANCELLED` state after cancellation.
        """
        # Mock subprocess behavior
        mock_process = MagicMock()
        mock_popen.return_value = mock_process

        # Simulate a running process
        mock_process.poll.return_value = None  # Process is active
        process = LocalProcess(
            commands=self.commands,
            run_path=tmp_path,
            output_file=tmp_path / "output.log",
        )
        process.start()

        # Verify the process has started
        assert process.status == ExecutionStatus.RUNNING
        assert process._process is mock_process  # Ensure the process was assigned

        # Test graceful termination
        process.cancel()
        mock_process.terminate.assert_called_once()  # Ensure terminate() was called
        mock_process.kill.assert_not_called()  # kill() should not be called if terminate succeeds
        assert process.status == ExecutionStatus.CANCELLED

    @patch("subprocess.Popen")
    def test_cancel_forceful_termination(self, mock_popen, tmp_path):
        """Ensures that the `cancel` method forcefully terminates a process if needed.

        This test verifies:
        - `terminate()` is called but fails with a `TimeoutExpired` exception.
        - `kill()` is called as a fallback to stop the process.
        - The `status` property is updated to `CANCELLED`.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate the subprocess lifecycle.

        Asserts
        -------
        - That `terminate()` is called to stop the process.
        - That `kill()` is called if `terminate()` fails.
        - That the `status` property reflects the `CANCELLED` state after cancellation.
        """
        # Mock subprocess behavior
        mock_process = MagicMock()
        mock_popen.return_value = mock_process

        # Simulate a running process
        mock_process.poll.return_value = None  # Process is active
        mock_process.terminate.side_effect = subprocess.TimeoutExpired(
            cmd="mock", timeout=5
        )  # Simulate timeout
        process = LocalProcess(
            commands=self.commands,
            run_path=tmp_path,
            output_file=tmp_path / "output.log",
        )
        process.start()

        # Verify the process has started
        assert process.status == ExecutionStatus.RUNNING
        assert process._process is mock_process  # Ensure the process was assigned

        # Test forceful termination
        process.cancel()
        mock_process.terminate.assert_called_once()  # Ensure terminate() was called
        mock_process.kill.assert_called_once()  # Ensure kill() was called after terminate timeout
        assert process.status == ExecutionStatus.CANCELLED

    @patch("subprocess.Popen")
    @patch("builtins.print")
    def test_cancel_non_running_process(self, mock_print, mock_popen, tmp_path):
        """Ensures that `cancel` does not attempt to terminate a non-running process.

        This test verifies:
        - The `print` statement is invoked with the correct message.
        - Neither `terminate` nor `kill` is called.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate the subprocess lifecycle.
        builtins.print
            Mocked to capture printed messages for validation.

        Asserts
        -------
        - That the `print` statement outputs the correct message.
        - That neither `terminate` nor `kill` is called.
        """
        # Mock subprocess behavior
        mock_process = MagicMock()
        mock_popen.return_value = mock_process

        # Simulate a completed process
        mock_process.poll.return_value = 0  # Process has completed
        process = LocalProcess(
            commands=self.commands,
            run_path=tmp_path,
            output_file=tmp_path / "output.log",
        )
        process._process = mock_process  # Directly assign the mock process

        # Test cancel on a completed process
        process.cancel()
        mock_print.assert_called_once_with(
            f"Cannot cancel job with status {process.status}"
        )
        mock_process.terminate.assert_not_called()
        mock_process.kill.assert_not_called()
