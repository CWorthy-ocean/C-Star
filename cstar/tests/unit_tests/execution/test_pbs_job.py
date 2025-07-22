import json
import logging
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from cstar.execution.scheduler_job import (
    ExecutionStatus,
    PBSJob,
)
from cstar.system.scheduler import PBSQueue, PBSScheduler


class TestPBSJob:
    """Tests for the `PBSJob` class.

    These tests cover the behavior and functionality of the `PBSJob` class,
    including job script generation, job submission, status retrieval, and cancellation.

    Tests
    -----
    test_script
        Verifies that the PBS job script is correctly generated with all directives.
    test_submit
        Ensures that the `submit` method properly submits a PBS job and extracts the job ID.
    test_submit_raises
        Confirms that `submit` raises a `RuntimeError` for invalid `qsub` responses or errors.
    test_cancel_running_job
        Tests that the `cancel` method successfully cancels a running job.
    test_cancel_completed_job
        Verifies that `cancel` does not proceed if the job is already completed.
    test_cancel_failure
        Ensures that a `RuntimeError` is raised if the `qdel` command fails.
    test_status
        Validates the status retrieval logic for various job states and qstat outputs.
    test_status_json_decode_error
        Confirms that a `RuntimeError` is raised when the `qstat` output cannot be parsed as JSON.
    """

    def setup_method(self, method):
        """Sets up the common parameters and objects needed for tests in `TestPBSJob`.

        This method initializes a mocked PBS queue and scheduler, along with a set of
        common job parameters to be used across multiple test cases.

        Parameters
        ----------
        method : Callable
            The test method being executed. This parameter is part of the pytest
            `setup_method` signature but is not used in this setup.

        Attributes
        ----------
        - mock_queu`: A `PBSQueue` object with a maximum walltime of "02:00:00".
        - scheduler: A `PBSScheduler` object initialized with the mocked queue.
        - common_job_params: A dictionary containing standard job parameters,
          including `scheduler`, `commands`, `account_key`, `cpus`, `nodes`, `walltime`,
          `job_name`, `output_file`, and `queue_name`.
        """
        # Create PBSQueue with specified max_walltime
        self.mock_queue = PBSQueue(name="test_queue", max_walltime="02:00:00")

        # Create the PBSScheduler with the mock queue
        self.scheduler = PBSScheduler(
            queues=[self.mock_queue],
            primary_queue_name="test_queue",
            other_scheduler_directives={"mock_directive": "mock_value"},
        )

        # Define common job parameters
        self.common_job_params = {
            "scheduler": self.scheduler,
            "commands": "echo Hello, World",
            "account_key": "test_account",
            "cpus": 4,
            "nodes": 2,
            "walltime": "02:00:00",
            "job_name": "test_pbs_job",
            "output_file": "/test/pbs_output.log",
            "queue_name": "test_queue",
        }

    def test_script(self):  # , mock_scheduler):
        """Verifies that the PBS job script is correctly generated with all directives.

        This test checks that the `script` property of `PBSJob` produces the expected
        job script, including user-provided parameters and any scheduler directives.

        Asserts
        -------
        - That the generated job script matches the expected content, including:
          - PBS directives (`#PBS ...`) such as queue, walltime, and nodes.
          - Custom scheduler directives provided in `other_scheduler_directives`.
          - Commands to execute in the job script (`echo Hello, World`).
        """
        # Initialize a PBSJob
        job = PBSJob(**self.common_job_params)

        expected_script = (
            "#PBS -S /bin/bash\n"
            "#PBS -N test_pbs_job\n"
            "#PBS -o /test/pbs_output.log\n"
            "#PBS -A test_account\n"
            "#PBS -l select=2:ncpus=2,walltime=02:00:00\n"
            "#PBS -q test_queue\n"
            "#PBS -j oe\n"
            "#PBS -k eod\n"
            "#PBS -V\n"
            "#PBS mock_directive mock_value"
            "\ncd ${PBS_O_WORKDIR}"
            "\n\necho Hello, World"
        )

        # Validate the script content
        assert job.script.strip() == expected_script.strip(), (
            f"Script mismatch!\nExpected:\n{expected_script}\n\nGot:\n{job.script}"
        )

    @patch("subprocess.run")
    def test_submit(self, mock_subprocess, tmp_path):
        """Ensures that the `submit` method properly submits a PBS job and extracts the
        job ID.

        This test validates that the `PBSJob.submit` method:
        - Saves the job script to the specified path.
        - Executes the `qsub` command to submit the job.
        - Extracts the job ID from the `qsub` output.

        Mocks
        -----
        subprocess.run
            Mocked to simulate the execution of the `qsub` command and its output.

        Asserts
        -------
        - That the `submit` method successfully parses and assigns the job ID.
        - That the script file is created in the specified path.
        - That the `qsub` command is executed with the correct arguments.
        """
        # Mock subprocess.run for qsub
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="12345.mockserver\n", stderr=""
        )

        # Create temporary paths
        script_path = tmp_path / "test_pbs_job.sh"
        run_path = tmp_path

        # Initialize a PBSJob
        job = PBSJob(
            **self.common_job_params,
            script_path=script_path,
            run_path=run_path,
        )

        # Submit the job
        job.submit()

    @pytest.mark.parametrize(
        "returncode, stdout, stderr, match_message",
        [
            (1, "", "Error submitting job", "Non-zero exit code when submitting job"),
            (0, "InvalidJobIDFormat", "", "Unexpected job ID format from qsub"),
        ],
    )
    @patch("subprocess.run")
    def test_submit_raises(
        self,
        mock_subprocess,
        returncode,
        stdout,
        stderr,
        match_message,
        tmp_path,
    ):
        """Confirms that `submit` raises a `RuntimeError` for invalid `qsub` responses
        or errors.

        This test validates that the `PBSJob.submit` method handles submission errors gracefully
        by raising appropriate exceptions when:
        - The `qsub` command exits with a non-zero return code.
        - The job ID extracted from `qsub` output is invalid.

        The test uses parameterization to check multiple failure scenarios, including
        different combinations of return codes, standard output, and standard error.

        Mocks
        -----
        subprocess.run
            Mocked to simulate the execution of the `qsub` command.

        Asserts
        -------
        - That a `RuntimeError` is raised with the expected error message when submission fails.
        """
        # Mock subprocess.run for qsub
        mock_subprocess.return_value = MagicMock(
            returncode=returncode, stdout=stdout, stderr=stderr
        )

        # Create temporary paths
        script_path = tmp_path / "test_pbs_job.sh"
        run_path = tmp_path

        # Initialize a PBSJob
        job = PBSJob(
            **self.common_job_params,
            script_path=script_path,
            run_path=run_path,
        )

        # Check for expected exceptions
        with pytest.raises(RuntimeError, match=match_message):
            job.submit()

    @patch("subprocess.run")
    @patch("cstar.execution.scheduler_job.PBSJob.status", new_callable=PropertyMock)
    def test_cancel_running_job(self, mock_status, mock_subprocess, tmp_path):
        """Tests that the `cancel` method successfully cancels a running PBS job.

        This test ensures that when the job status is `RUNNING`, the `cancel` method
        executes the `qdel` command and cancels the job without errors.

        Mocks
        -----
        PBSJob.status
            Mocked to return `ExecutionStatus.RUNNING`, simulating a running job.
        subprocess.run
            Mocked to simulate successful execution of the `qdel` command.

        Asserts
        -------
        - That the `qdel` command is called with the correct job ID and parameters.
        - That no exceptions are raised during the cancellation process.
        """
        # Mock the status to "running"
        mock_status.return_value = ExecutionStatus.RUNNING

        # Mock subprocess.run for successful cancellation
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="", stderr="")

        # Create a PBSJob with a set job ID
        job = PBSJob(
            **self.common_job_params,
            run_path=tmp_path,
        )
        job._id = 12345  # Manually set the job ID

        # Cancel the job
        job.cancel()

        # Verify qdel was called correctly
        mock_subprocess.assert_called_once_with(
            "qdel 12345",
            shell=True,
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )

    @patch("subprocess.run")
    @patch("cstar.execution.scheduler_job.PBSJob.status", new_callable=PropertyMock)
    def test_cancel_completed_job(
        self,
        mock_status,
        mock_subprocess,
        tmp_path,
        caplog: pytest.LogCaptureFixture,
    ):
        """Verifies that the `cancel` method does not proceed if the job is already
        completed.

        This test ensures that calling `cancel` on a job with a `COMPLETED` status does not
        invoke the `qdel` command and instead informs the user.

        Mocks
        -----
        mock_status (PBSJob.status)
            Mocked to return "completed", simulating a completed job.
        mock_subprocess (subprocess.run)
            Mocked to ensure that the `qdel` command is not executed.
        tmp_path (pathlib.Path)
            Builtin fixture to create a temporary filepath
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log outputs

        Asserts
        -------
        - That the user is informed that the job cannot be canceled due to its completed status.
        - That the `qdel` command is not called for a completed job.
        """
        # Mock the status to "completed"
        mock_status.return_value = "completed"

        # Create a PBSJob with a set job ID
        job = PBSJob(
            **self.common_job_params,
            run_path=tmp_path,
        )
        job._id = 12345  # Manually set the job ID

        # Get the logger from the job instance
        caplog.set_level(logging.INFO, logger=job.log.name)

        # Attempt to cancel the job
        job.cancel()

        # Verify the message was printed
        captured = caplog.text
        assert "Cannot cancel job with status completed" in captured

        # Verify qdel was not called
        mock_subprocess.assert_not_called()

    ##
    @patch("subprocess.run")
    @patch("cstar.execution.scheduler_job.PBSJob.status", new_callable=PropertyMock)
    def test_cancel_failure(self, mock_status, mock_subprocess, tmp_path):
        """Ensures that a `RuntimeError` is raised if the `qdel` command fails to cancel
        a job.

        This test verifies that the `cancel` method handles errors from the `qdel` command
        gracefully by raising an appropriate exception when the command returns a non-zero
        exit code.

        Mocks
        -----
        PBSJob.status
            Mocked to return `ExecutionStatus.RUNNING`, simulating a running job.
        subprocess.run
            Mocked to simulate a failed execution of the `qdel` command, returning a non-zero exit code.

        Asserts
        -------
        - That a `RuntimeError` is raised with an appropriate error message when `qdel` fails.
        - That the `qdel` command is called with the correct job ID and parameters.
        """
        # Mock the status to "running"
        mock_status.return_value = ExecutionStatus.RUNNING

        # Mock subprocess.run for qdel failure
        mock_subprocess.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error cancelling job"
        )

        # Create a PBSJob with a set job ID
        job = PBSJob(
            **self.common_job_params,
            run_path=tmp_path,
        )
        job._id = 12345  # Manually set the job ID

        # Expect an error when qdel fails
        with pytest.raises(
            RuntimeError, match="Non-zero exit code when cancelling job"
        ):
            job.cancel()

    ##
    @pytest.mark.parametrize(
        "qstat_output, exit_status, expected_status, should_raise, expected_exception, expected_message",
        [
            (
                {"Jobs": {"12345": {"job_state": "Q"}}},
                None,
                ExecutionStatus.PENDING,
                False,
                None,
                None,
            ),  # Pending job
            (
                {"Jobs": {"12345": {"job_state": "R"}}},
                None,
                ExecutionStatus.RUNNING,
                False,
                None,
                None,
            ),  # Running job
            (
                {"Jobs": {"12345": {"job_state": "C"}}},
                None,
                ExecutionStatus.COMPLETED,
                False,
                None,
                None,
            ),  # Completed job
            (
                {"Jobs": {"12345": {"job_state": "H"}}},
                None,
                ExecutionStatus.HELD,
                False,
                None,
                None,
            ),  # Held job
            (
                {"Jobs": {"12345": {"job_state": "F", "Exit_status": 1}}},
                None,
                ExecutionStatus.FAILED,
                False,
                None,
                None,
            ),  # Failed job
            (
                {"Jobs": {"12345": {"job_state": "F", "Exit_status": 0}}},
                None,
                ExecutionStatus.COMPLETED,
                False,
                None,
                None,
            ),  # Completed with Exit_status 0
            (
                None,
                1,
                None,
                True,
                RuntimeError,
                "Failed to retrieve job status using qstat",
            ),  # qstat command failure
            (
                {"Jobs": {}},
                None,
                None,
                True,
                RuntimeError,
                "Job ID 12345 not found in qstat output.",
            ),  # Missing job info
            (
                {"Jobs": {"12345": {"job_state": "E"}}},
                None,
                ExecutionStatus.ENDING,
                False,
                None,
                None,
            ),  # Ending job
            (
                {"Jobs": {"12345": {"job_state": "X"}}},
                None,
                ExecutionStatus.UNKNOWN,
                False,
                None,
                None,
            ),  # Unknown job state
            (
                "invalid_json",
                None,
                None,
                True,
                RuntimeError,
                "Failed to parse JSON from qstat output",
            ),  # JSONDecodeError
        ],
    )
    @patch("subprocess.run")
    def test_status(
        self,
        mock_subprocess,
        qstat_output,
        exit_status,
        expected_status,
        should_raise,
        expected_exception,
        expected_message,
    ):
        """Validates the job status retrieval logic for various job states and `qstat`
        outputs.

        This test verifies that the `status` property of `PBSJob` correctly parses the
        output of the `qstat` command to determine the current job state. It also checks
        that appropriate exceptions are raised in error scenarios, such as invalid `qstat`
        outputs or missing job information.

        This test uses parameterization to evaluate:
        - Standard job states such as `PENDING`, `RUNNING`, `COMPLETED`, and `FAILED`.
        - Edge cases like unknown states, empty job data, and invalid JSON output from qstat.
        - Error conditions such as command failures or JSON decoding errors.

        Mocks
        -----
        subprocess.run
            Mocked to simulate the execution of the `qstat` command and its output.

        Asserts
        -------
        - That the job status is correctly determined for valid `qstat` outputs.
        - That appropriate exceptions are raised for invalid or error scenarios.
        """
        # Mock qstat command output
        if qstat_output is not None:
            if qstat_output == "invalid_json":
                mock_subprocess.return_value = MagicMock(
                    returncode=0, stdout="Invalid JSON", stderr=""
                )
            else:
                mock_subprocess.return_value = MagicMock(
                    returncode=0, stdout=json.dumps(qstat_output), stderr=""
                )
        else:
            mock_subprocess.return_value = MagicMock(
                returncode=1, stdout="", stderr="Error: qstat command failed"
            )

        # Create a PBSJob with a set job ID
        job = PBSJob(**self.common_job_params)
        job._id = 12345  # Manually set the job ID

        # Check the expected outcome
        if should_raise:
            with pytest.raises(expected_exception, match=expected_message):
                job.status
        else:
            assert job.status == expected_status, (
                f"Expected status '{expected_status}' but got '{job.status}'"
            )

    @patch("json.loads", side_effect=json.JSONDecodeError("Expecting value", "", 0))
    @patch("subprocess.run")
    def test_status_json_decode_error(self, mock_subprocess, mock_json_loads):
        """Confirms that a `RuntimeError` is raised when the `qstat` output cannot be
        parsed as JSON.

        This test ensures that the `status` property of `PBSJob` handles JSON decoding
        errors gracefully by raising a `RuntimeError` when the output from the `qstat`
        command is invalid.

        Mocks
        -----
        subprocess.run
            Mocked to simulate a successful `qstat` command execution with invalid JSON output.
        json.loads
            Mocked to raise a `JSONDecodeError` when attempting to parse the `qstat` output.

        Asserts
        -------
        - That a `RuntimeError` is raised with an appropriate error message indicating the JSON
          parsing failure.
        """
        # Mock qstat command output
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="Invalid JSON", stderr=""
        )

        # Create a PBSJob with a set job ID
        job = PBSJob(**self.common_job_params)
        job._id = 12345  # Manually set the job ID

        # Check for JSONDecodeError handling
        with pytest.raises(
            RuntimeError, match="Failed to parse JSON from qstat output"
        ):
            job.status
