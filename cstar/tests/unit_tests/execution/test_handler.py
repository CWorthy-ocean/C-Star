import logging
import threading
import time
from pathlib import Path
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

    @pytest.mark.asyncio
    async def test_updates_non_running_job(
        self, tmp_path, caplog: pytest.LogCaptureFixture
    ):
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

        await handler.updates(seconds=10)

        captured = caplog.text
        assert (
            "This job is currently not running (completed). Live updates cannot be provided."
            in captured
        )
        assert f"See {handler.output_file.resolve()} for job output" in captured

    @pytest.mark.asyncio
    async def test_updates_running_job_with_tmp_file(
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
        await handler.updates(seconds=0.25)

        # Ensure both initial and live update lines are printed
        captured = caplog.text
        for line in live_updates:
            assert line in captured

        # Ensure the thread finishes before the test ends
        updater_thread.join()

    @pytest.mark.asyncio
    async def test_updates_indefinite_with_seconds_param_0(
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

            # Simulate a KeyboardInterrupt during the updates call
            with patch("asyncio.sleep", side_effect=KeyboardInterrupt):
                await handler.updates(seconds=0)  # Run updates indefinitely

                # Assert that the "stopped by user" message was printed
                assert "Live status updates stopped by user." in caplog.text

    @pytest.mark.asyncio
    async def test_updates_forwards_tail_when_status_becomes_terminal(
        self, tmp_path, caplog: pytest.LogCaptureFixture
    ):
        """Regression: when the job reaches a terminal state mid-stream, `updates()`
        must forward the remaining output (the tail) before stopping, and must return
        rather than loop forever.

        Previously the loop checked the status *before* logging the line it had just
        read and returned immediately on a terminal status, dropping the final lines
        of the ROMS log on exit.

        Fixtures
        --------
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log outputs
        """
        output_file = tmp_path / "output.log"
        # The process writes all of its output (including the final tail), then dies.
        all_lines = [
            "First line\n",
            "Second line\n",
            "Third line\n",
            "Final crash message\n",
        ]
        with output_file.open("w") as f:
            f.writelines(all_lines)

        handler = MockExecutionHandler(ExecutionStatus.RUNNING, output_file)
        caplog.set_level(logging.INFO, logger=handler.log.name)

        # Flip to a terminal status shortly after `updates()` starts so the loop
        # observes the transition. With seconds=0 the call would loop forever if the
        # terminal transition were ignored, so returning on its own is part of the test.
        def finish_job():
            time.sleep(0.2)
            handler._status = ExecutionStatus.FAILED

        updater_thread = threading.Thread(target=finish_job, daemon=True)
        updater_thread.start()

        await handler.updates(seconds=0)

        captured = caplog.text
        for line in all_lines:
            assert line.rstrip() in captured

        updater_thread.join()

    @pytest.mark.asyncio
    async def test_updates_forwards_output_when_already_terminal(
        self, tmp_path, caplog: pytest.LogCaptureFixture
    ):
        """Regression: if the job finished between polls (already terminal when
        `updates()` is called), output not yet forwarded must still be logged rather
        than replaced by only a 'see the file' message.

        Fixtures
        --------
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log outputs
        """
        output_file = tmp_path / "output.log"
        lines = ["line A\n", "line B\n", "final line\n"]
        with output_file.open("w") as f:
            f.writelines(lines)

        handler = MockExecutionHandler(ExecutionStatus.FAILED, output_file)
        caplog.set_level(logging.INFO, logger=handler.log.name)

        await handler.updates(seconds=5)

        captured = caplog.text
        # the remaining output (including the tail) is forwarded to the main log
        for line in lines:
            assert line.rstrip() in captured
        # and the user is still pointed at the full output file
        assert f"See {output_file.resolve()} for job output" in captured

    @pytest.mark.asyncio
    async def test_updates_does_not_duplicate_across_calls(
        self, tmp_path, caplog: pytest.LogCaptureFixture
    ):
        """Verify that the tracked read position prevents the same lines from being
        forwarded twice across successive `updates()` calls.
        """
        output_file = tmp_path / "output.log"
        with output_file.open("w") as f:
            f.writelines(["alpha\n", "beta\n"])

        handler = MockExecutionHandler(ExecutionStatus.RUNNING, output_file)
        caplog.set_level(logging.INFO, logger=handler.log.name)

        # First poll forwards the two existing lines.
        await handler.updates(seconds=0.2)
        # Append one new line, then poll again.
        with output_file.open("a") as f:
            f.write("gamma\n")
        await handler.updates(seconds=0.2)

        assert caplog.text.count("alpha") == 1
        assert caplog.text.count("beta") == 1
        assert caplog.text.count("gamma") == 1
