import logging
import subprocess
from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest

from cstar.execution.local_process import ExecutionStatus, LocalProcess


@pytest.fixture
def mock_local_process(tmp_path):
    return LocalProcess(
        commands="echo 'Hello, World'",
        run_path=tmp_path,
        output_file=tmp_path / "output.log",
    )


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
    test_drop_process
        Tests the behavior of the private _drop_process method.
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
        # Mock subprocess behavior
        self.mock_subprocess = MagicMock()
        self.patcher = patch("subprocess.Popen")
        self.mock_popen = self.patcher.start()
        self.mock_popen.return_value = self.mock_subprocess

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
        mock_local_process = LocalProcess(
            commands='echo "Hello, World"', run_path=None, output_file=None
        )
        assert mock_local_process.run_path == Path.cwd()
        assert mock_local_process.output_file.name.startswith("cstar_process_")
        assert mock_local_process.status == ExecutionStatus.UNSUBMITTED

    def test_str(self):
        mock_local_process = LocalProcess(
            commands='echo "Hello, World"',
            run_path="test/path",
            output_file="outfile.out",
        )
        test_str = dedent(
            """\
        LocalProcess
        ------------
        Commands: echo "Hello, World"
        Run path: test/path
        Output file: outfile.out
        Status: unsubmitted"""
        )

        assert mock_local_process.__str__() == test_str

    def test_repr(self):
        mock_local_process = LocalProcess(
            commands='echo "Hello, World"',
            run_path="test/path",
            output_file="outfile.out",
        )
        test_repr = dedent(
            """\
        LocalProcess(
        commands = 'echo "Hello, World"',
        output_file = PosixPath('outfile.out'),
        run_path = PosixPath('test/path')
        )
        State: <status = <ExecutionStatus.UNSUBMITTED: 1>>"""
        )

        assert mock_local_process.__repr__() == test_repr, (
            f"expected \n{test_repr}\n, got \n{mock_local_process.__repr__()}\n"
        )

    def test_start_success(self, tmp_path, mock_local_process):
        """Verifies that the subprocess starts successfully with valid commands.

        This test ensures that the `start` method:
        - Initializes a subprocess with the correct parameters (commands, working directory).
        - Redirects the standard output and error to the specified output file.
        - Updates the `status` property to `RUNNING`.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate a successful subprocess startup.

        Fixtures
        --------
        mock_local_process
            Creates a simple local process
        tmp_path
            Builtin fixture to create and use a temporary path for filesystem interaction

        Asserts
        -------
        - That the subprocess is called with the correct arguments.
        - That the `status` property reflects the `RUNNING` state after startup.
        """
        self.mock_subprocess.poll.return_value = None  # Simulate running process

        with patch("builtins.open", MagicMock()) as mock_open:
            mock_local_process.start()
            mock_open.assert_called_once_with(mock_local_process.output_file, "w")

        self.mock_popen.assert_called_once_with(
            ["echo", "'Hello,", "World'"],
            cwd=tmp_path,
            stdin=subprocess.PIPE,
            stdout=mock_open.return_value,
            stderr=subprocess.STDOUT,
        )
        assert mock_local_process.status == ExecutionStatus.RUNNING

    def test_status(self, tmp_path, mock_local_process):
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

        Fixtures
        --------
        mock_local_process
            Creates a simple local process
        tmp_path
            Builtin fixture to create and use a temporary path for filesystem interaction

        Asserts
        -------
        - That the `status` property reflects the correct `ExecutionStatus` for
          each stage of the process lifecycle.
        """
        assert mock_local_process.status == ExecutionStatus.UNSUBMITTED

        # After starting: RUNNING
        self.mock_subprocess.poll.return_value = None  # Simulate running process
        mock_local_process.start()
        assert mock_local_process.status == ExecutionStatus.RUNNING
        assert mock_local_process._process is not None

        # After completion: COMPLETED
        self.mock_subprocess.poll.return_value = 0  # Simulate successful termination
        self.mock_subprocess.returncode = 0

        mock_local_process.start()
        assert mock_local_process.status == ExecutionStatus.COMPLETED
        assert mock_local_process._process is None

        # After failure: FAILED
        self.mock_subprocess.poll.return_value = 1  # Simulate unsuccessful termination
        self.mock_subprocess.returncode = 1

        mock_local_process.start()
        assert mock_local_process.status == ExecutionStatus.FAILED

    def test_drop_process(self, tmp_path, mock_local_process):
        """Tests the behavior of the private _drop_process method.

        This test checks behavior in three situations:
        - _process un-set
        - _process set to a completed subprocess.Popen instance
        - _process set to a running subprocess.Popen instance

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate a failed subprocess.

        Fixtures
        --------
        tmp_path
            Builtin fixture to create and use a temporary path for filesystem interaction
        mock_local_process
            Creates a simple local process

        Asserts
        -------
        - no action is taken if the _process attribute is not set
        - _returncode is set if _process is set to a complete subprocess
        - _process is un-set if _process is set to a complete subprocess
        - RuntimeError raised if _process is set to a running subprocess
        """
        # no process (return early):
        mock_local_process._process = None
        mock_local_process._drop_process()
        assert mock_local_process._returncode is None
        self.mock_subprocess.poll.assert_not_called()

        # valid process (set _returncode, clear _process):
        self.mock_subprocess.returncode = 0
        mock_local_process._process = self.mock_subprocess
        mock_local_process._drop_process()
        assert mock_local_process._returncode == 0
        assert mock_local_process._process is None

        # running process (raise)
        self.mock_subprocess.poll.return_value = None
        mock_local_process._process = self.mock_subprocess
        with pytest.raises(RuntimeError) as err_msg:
            mock_local_process._drop_process()
            assert str(err_msg.value) == (
                "LocalProcess._drop_process() called on still-active process. "
                "Await completion or use LocalProcess.cancel()"
            )

    def test_cancel_graceful_termination(self, tmp_path, mock_local_process):
        """Ensures that the `cancel` method gracefully terminates a running process.

        This test verifies:
        - `terminate()` is called to stop the process.
        - `kill()` is not called when `terminate()` succeeds.
        - The `status` property is updated to `CANCELLED`.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate the subprocess lifecycle.

        Fixtures
        --------
        tmp_path
            Builtin fixture to create and use a temporary path for filesystem interaction
        mock_local_process
            Creates a simple local process

        Asserts
        -------
        - That `terminate()` is called to stop the process.
        - That `kill()` is not called if `terminate()` succeeds.
        - That the `status` property reflects the `CANCELLED` state after cancellation.
        """
        # Simulate a running process
        self.mock_subprocess.poll.return_value = None  # Process is active
        mock_local_process.start()

        # Verify the process has started
        assert mock_local_process.status == ExecutionStatus.RUNNING
        assert (
            mock_local_process._process is self.mock_subprocess
        )  # Ensure the process was assigned

        # cancel() call will call process.poll() internally twice;
        # once while still running [None], once in _drop_process() [termination code]:
        self.mock_subprocess.poll.side_effect = [None, -15]
        # Test graceful termination
        mock_local_process.cancel()
        self.mock_subprocess.terminate.assert_called_once()  # Ensure terminate() was called
        self.mock_subprocess.kill.assert_not_called()  # kill() should not be called if terminate succeeds
        assert mock_local_process.status == ExecutionStatus.CANCELLED

    def test_cancel_forceful_termination(self, tmp_path, mock_local_process):
        """Ensures that the `cancel` method forcefully terminates a process if needed.

        This test verifies:
        - `terminate()` is called but fails with a `TimeoutExpired` exception.
        - `kill()` is called as a fallback to stop the process.
        - The `status` property is updated to `CANCELLED`.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate the subprocess lifecycle.

        Fixtures
        --------
        tmp_path
            Builtin fixture to create and use a temporary path for filesystem interaction
        mock_local_process
            Creates a simple local process

        Asserts
        -------
        - That `terminate()` is called to stop the process.
        - That `kill()` is called if `terminate()` fails.
        - That the `status` property reflects the `CANCELLED` state after cancellation.
        """
        # Simulate a running process
        self.mock_subprocess.poll.return_value = None  # Process is active
        self.mock_subprocess.terminate.side_effect = subprocess.TimeoutExpired(
            cmd="mock", timeout=5
        )  # Simulate timeout

        mock_local_process.start()

        # Verify the process has started
        assert mock_local_process.status == ExecutionStatus.RUNNING
        assert (
            mock_local_process._process is self.mock_subprocess
        )  # Ensure the process was assigned

        # cancel() call will call process.poll() internally twice;
        # once while still running [None], once in _drop_process() [termination code]:
        self.mock_subprocess.poll.side_effect = [None, -15]

        # Test forceful termination
        mock_local_process.cancel()

        self.mock_subprocess.terminate.assert_called_once()  # Ensure terminate() was called
        self.mock_subprocess.kill.assert_called_once()  # Ensure kill() was called after terminate timeout

        assert mock_local_process.status == ExecutionStatus.CANCELLED

    def test_cancel_non_running_process(
        self, tmp_path, mock_local_process, caplog: pytest.LogCaptureFixture
    ):
        """Ensures that `cancel` does not attempt to terminate a non-running process.

        This test verifies:
        - The `print` statement is invoked with the correct message.
        - Neither `terminate` nor `kill` is called.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate the subprocess lifecycle.

        Fixtures
        --------
        tmp_path (pathlib.Path)
            Builtin fixture to create and use a temporary path for filesystem interaction
        mock_local_process (cstar.execution.LocalProcess)
            Creates a simple local process
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to captures log outputs

        Asserts
        -------
        - That the correct message is logged
        - That neither `terminate` nor `kill` is called.
        """
        # Simulate a completed process
        self.mock_subprocess.poll.return_value = 0  # Process has completed

        mock_local_process._process = (
            self.mock_subprocess
        )  # Directly assign the mock process

        caplog.set_level(logging.INFO, logger=mock_local_process.log.name)

        # Test cancel on a completed process
        mock_local_process.cancel()

        captured = caplog.text
        assert (
            f"Cannot cancel job with status '{mock_local_process.status}'" in captured
        )
        self.mock_subprocess.terminate.assert_not_called()
        self.mock_subprocess.kill.assert_not_called()

    def test_wait_running_process(self, tmp_path, mock_local_process):
        """Ensures that `wait` correctly waits for a running process to complete.

        This test verifies:
        - `wait()` is called on the process when it is running.
        - The process's final status is updated correctly.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate the subprocess lifecycle.

        Fixtures
        --------
        tmp_path
            Builtin fixture to create and use a temporary path for filesystem interaction
        mock_local_process
            Creates a simple local process

        Asserts
        -------
        - subprocess.Popen.wait() is called once
        - Information message is not printed
        """
        # Simulate a running process
        self.mock_subprocess.poll.return_value = None  # Process is active
        mock_local_process._process = self.mock_subprocess  # Assign mock process
        mock_local_process._cancelled = False

        # Test wait on a running process
        mock_local_process.wait()
        self.mock_subprocess.wait.assert_called_once()  # Ensure `wait` was called on the process

    def test_wait_non_running_process(
        self, tmp_path, mock_local_process, caplog: pytest.LogCaptureFixture
    ):
        """Ensures that `wait` does not perform actions for non-running processes.

        This test verifies:
        - `wait()` prints an appropriate message for non-running processes.
        - `wait()` does not attempt to wait on processes that are not running.

        Mocks
        -----
        subprocess.Popen
            Mocked to simulate the subprocess lifecycle.

        Fixtures
        --------
        tmp_path (pathlib.Path)
            Builtin fixture to create and use a temporary path for filesystem interaction
        mock_local_process (cstar.execution.LocalProcess)
            Creates a simple local process
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log outputs

        Asserts
        -------
        - An information message is logged
        - subprocess.Popen.wait is not called
        """
        mock_local_process._process = None  # Assign mock process
        mock_local_process._cancelled = True
        caplog.set_level(logging.INFO, logger=mock_local_process.log.name)

        mock_local_process.wait()
        captured = caplog.text
        assert (
            f"Cannot wait for process with execution status '{ExecutionStatus.CANCELLED}'"
            in captured
        )
        self.mock_subprocess.wait.assert_not_called()

    def teardown_method(self):
        patch.stopall()
