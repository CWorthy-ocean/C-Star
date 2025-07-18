import logging
import os
import threading
import time
from pathlib import Path
from unittest import mock
from unittest.mock import PropertyMock, patch

import pytest

from cstar.execution.handler import ExecutionHandler, ExecutionStatus


class MockExecutionHandler(ExecutionHandler):
    """Mock implementation of `ExecutionHandler` for testing purposes."""

    def __init__(self, status, output_file):
        self._status = status
        self._output_file = Path(output_file)

    @property
    def status(self):
        return self._status

    @property
    def output_file(self):
        return self._output_file


class TestExecutionHandlerUpdates:
    """Tests for the `updates` method in the `ExecutionHandler` class.

    Tests
    -----
    test_updates_non_running_job
        Validates that `updates()` provides appropriate feedback when the job is not running.
    test_updates_running_job_with_tmp_file
        Verifies that `updates()` streams live updates from the output file when the job is running.
    test_updates_indefinite_with_seconds_param_0
        Confirms that `updates()` runs indefinitely when `seconds=0` and allows termination via user interruption.
    """

    def test_updates_non_running_job(self, tmp_path, caplog: pytest.LogCaptureFixture):
        """Validates that `updates()` provides appropriate feedback when the job is not
        running.

        This test ensures that if the job status is not `RUNNING`, the method informs
        the user and provides instructions for viewing the job output if applicable.

        Mocks
        -----
        MockExecutionHandler.status
            Mocked to return `ExecutionStatus.COMPLETED`, simulating a non-running job.

        Fixtures
        --------
        LogCaptureFixture
            Captures log outputs to verify output messages.

        Asserts
        -------
        - That the user is informed the job is not running.
        - That instructions for viewing the job output are provided if the job status
          is `COMPLETED` or similar.
        """
        caplog.set_level(logging.WARNING)
        handler = MockExecutionHandler(
            ExecutionStatus.COMPLETED, tmp_path / "mock_output.log"
        )

        handler.updates(seconds=10)

        captured = caplog.text
        assert (
            "This job is currently not running (completed). Live updates cannot be provided."
            in captured
        )
        assert f"See {handler.output_file.resolve()} for job output" in captured

    def test_updates_running_job_with_tmp_file(
        self, tmp_path, caplog: pytest.LogCaptureFixture
    ):
        """Verifies that `updates()` streams live updates from the job's output file
        when the job is running.

        This test creates a temporary output file and pre-populates it with initial
        content. Live updates are then appended to the file in real-time using a
        background thread, simulating a running job. The `updates()` method is tested
        to ensure it reads and prints both the pre-existing content and new updates.

        Mocks
        -----
        MockExecutionHandler.status
            Mocked to return `ExecutionStatus.RUNNING`, simulating a running job.

        Fixtures
        --------
        tmp_path (pathlib.Path): builtin fixture creating a temporary pathlib.Path object
        caplog (pytest.LogCaptureFixture): Builtin fixture to capture log outputs

        Asserts
        -------
        - That all live updates appended to the output file are logged correctly.
        - That previously existing content in the file is also logged.
        - That the method properly interacts with the output file in real-time.
        """
        # Create a temporary output file
        output_file = tmp_path / "output.log"
        initial_content = ["First line\n"]
        live_updates = ["Second line\n", "Third line\n", "Fourth line\n"]

        # Write initial content to the file
        with output_file.open("w") as f:
            f.writelines(initial_content)

        handler = MockExecutionHandler(ExecutionStatus.RUNNING, output_file)

        # Get the logger from the ExecutionHandler instance:
        caplog.set_level(logging.INFO, logger=handler.log.name)

        # Function to simulate appending live updates to the file
        def append_live_updates():
            with output_file.open("a") as f:
                for line in live_updates:
                    time.sleep(0.01)  # Ensure `updates()` is actively reading
                    f.write(line)
                    f.flush()  # Immediately write to disk

        # Start the live update simulation in a background thread
        updater_thread = threading.Thread(target=append_live_updates, daemon=True)
        updater_thread.start()

        # Run the `updates` method
        handler.updates(seconds=0.25)

        # Ensure both initial and live update lines are printed
        captured = caplog.text
        for line in live_updates:
            assert line in captured

        # Ensure the thread finishes before the test ends
        updater_thread.join()

    def test_updates_indefinite_with_seconds_param_0(
        self, tmp_path, caplog: pytest.LogCaptureFixture
    ):
        """Confirms that `updates()` runs indefinitely when `seconds=0` and allows
        termination via user interruption.

        This test creates a temporary output file and pre-populates it with content.
        Unlike `test_updates_running_job_with_tmp_file`, this test does not continue
        to update the temporary file, so no updates are actually provided. The
        stream is then killed via a simulated `KeyboardInterrupt` and the test
        ensures a graceful exit.

        Mocks
        -----
        MockExecutionHandler.status
            Mocked to return `ExecutionStatus.RUNNING`, simulating a running job.
        builtins.input
            Mocked to simulate user responses to the confirmation prompt.
        time.sleep
            Mocked to simulate a `KeyboardInterrupt` during indefinite updates.

        Fixtures
        --------
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to captures log outputs
        tmp_path (pathlib.Path)
            Builtin fixture to provide a temporary file path

        Asserts
        -------
        - That the user is prompted to confirm running the updates indefinitely.
        - That the method handles user interruptions and prints the appropriate message.
        - That no additional file operations occur if the user denies the confirmation.
        """
        # Create a temporary output file
        output_file = tmp_path / "output.log"
        content = ["Line 1\n", "Line 2\n", "Line 3\n"]

        # Write initial content to the file
        with output_file.open("w") as f:
            f.writelines(content)

        handler = MockExecutionHandler(ExecutionStatus.RUNNING, output_file)
        # Get logger to capture from ExecutionHandler instance:
        caplog.set_level(logging.INFO, logger=handler.log.name)

        # Mock the `status` property to return "running"
        with (
            patch.object(
                MockExecutionHandler, "status", new_callable=PropertyMock
            ) as mock_status,
            caplog.at_level(logging.INFO),
        ):
            mock_status.return_value = ExecutionStatus.RUNNING

            # Patch input to simulate the confirmation prompt
            with patch("builtins.input", side_effect=["y"]) as mock_input:
                # Replace `time.sleep` with a side effect that raises KeyboardInterrupt
                # Simulate a KeyboardInterrupt during the updates call
                with patch("time.sleep", side_effect=KeyboardInterrupt):
                    handler.updates(seconds=0)  # Run updates indefinitely

                    # Assert that the "stopped by user" message was printed
                    assert "Live status updates stopped by user." in caplog.text

                # Verify that the prompt was displayed to the user
                mock_input.assert_called_once_with(
                    "This will provide indefinite updates to your job. You can stop it anytime using Ctrl+C. "
                    "Do you want to continue? (y/n): "
                )
            # Patch input to simulate the confirmation prompt
            with patch("builtins.input", side_effect=["n"]) as mock_input:
                with patch("builtins.open", create=True) as mock_open:
                    handler.updates(seconds=0)
                    mock_open.assert_not_called()

    def test_updates_stops_when_status_changes(
        self, tmp_path, caplog: pytest.LogCaptureFixture
    ):
        """Verifies that `updates()` stops execution when `status` changes to non-
        RUNNING.

        This test ensures:
        - The conditional block exits `updates` when `status` is not `RUNNING`.
        - Only lines added while the job is `RUNNING` are streamed.

        Fixtures
        --------
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log outputs
        """
        # Create a temporary output file
        output_file = tmp_path / "output.log"
        initial_content = ["First line\n"]
        running_updates = ["Second line\n", "Third line\n"]
        completed_updates = ["Fourth line\n", "Fifth line\n"]
        # Write initial content to the file
        with output_file.open("w") as f:
            f.writelines(initial_content)

        # Initialize the handler with status `RUNNING`
        handler = MockExecutionHandler(ExecutionStatus.RUNNING, output_file)
        caplog.set_level(logging.INFO, logger=handler.log.name)

        # Function to simulate appending live updates and changing status
        def append_updates_and_change_status():
            with output_file.open("a") as f:
                for line in running_updates:
                    time.sleep(0.1)  # Ensure updates() is actively reading
                    f.write(line)
                    f.flush()

                # Change the status to `COMPLETED` after writing running updates
                time.sleep(0.2)
                handler._status = ExecutionStatus.COMPLETED
                for line in completed_updates:
                    time.sleep(0.1)
                    f.write(line)
                    f.flush()

        # Start the background thread to append updates and change status
        updater_thread = threading.Thread(
            target=append_updates_and_change_status, daemon=True
        )
        updater_thread.start()

        # Run the `updates` method
        with mock.patch.dict(os.environ, {"CSTAR_INTERACTIVE": "0"}):
            handler.updates(seconds=0)

        # Verify that only lines from `running_updates` were printed
        printed_calls = caplog.text

        for line in running_updates:
            assert line in printed_calls

        # Verify that lines from `completed_updates` were not printed
        for line in completed_updates:
            assert line not in printed_calls

        # Ensure the thread finishes before the test ends
        updater_thread.join()
