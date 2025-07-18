import logging
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from cstar.execution.scheduler_job import ExecutionStatus, SlurmJob
from cstar.system.scheduler import SlurmPartition, SlurmQOS, SlurmScheduler


class TestSlurmJob:
    """Tests for the `SlurmJob` class.

    These tests cover the functionality of the `SlurmJob` class, including job script
    generation, submission, cancellation, status retrieval, and script saving.

    Tests
    -----
    test_script_task_distribution_not_required
        Verifies the job script is correctly generated when the system does not explicitly require
        the user to specify requested nodes and cpus
    test_script_task_distribution_required
        Verifies the job script is correctly generated when the system explicitly requires
        the user to specify requested nodes and cpus
    test_submit
        Ensures that the job is properly submitted and the job ID is extracted.
    test_submit_raises
        Confirms that a `RuntimeError` is raised for invalid submission scenarios.
    test_cancel
        Tests that the `cancel` method works correctly, raising an exception if cancellation fails.
    test_save_script
        Ensures that the job script is saved to the specified path and matches the expected content.
    test_status
        Verifies the status retrieval logic for various job states and scenarios.
    """

    def setup_method(self, method):
        """Sets up the common parameters and objects needed for tests in `TestSlurmJob`.

        This method initializes a mocked SLURM queue and scheduler, along with a set of
        common job parameters and mock environment variables, to be used across multiple tests.

        Parameters
        ----------
        method : Callable
            The test method being executed. This parameter is part of the pytest
            `setup_method` signature but is not used in this setup.

        Attributes
        ----------
        - mock_qos: A mocked `SlurmQOS` object with a maximum walltime of "02:00:00".
        - mock_partition: A mocked `SlurmPartition` object with a maximum walltime of "02:00:00".
        - scheduler: A `SlurmScheduler` object initialized with the mocked queue and
          task distribution set to `False`.
        - common_job_params: A dictionary containing common job parameters, such as
          `scheduler`, `commands`, `account_key`, `cpus`, `walltime`, and more.
        - mock_env_vars: A dictionary simulating environment variables.
        """
        # Create a SlurmQOS and patch its max_walltime property
        self.mock_qos = SlurmQOS(name="test_queue", query_name="test_queue")
        self.mock_partition = SlurmPartition(name="test_queue")
        self.patch_qos_properties = patch.object(
            type(self.mock_qos),
            "max_walltime",
            new_callable=PropertyMock,
            return_value="02:00:00",
        )
        self.mock_max_qos_walltime = self.patch_qos_properties.start()

        self.patch_partition_properties = patch.object(
            type(self.mock_partition),
            "max_walltime",
            new_callable=PropertyMock,
            return_value="02:00:00",
        )
        self.mock_max_partition_walltime = self.patch_partition_properties.start()

        # Create the SlurmScheduler with the patched queue
        self.scheduler = SlurmScheduler(
            queues=[self.mock_qos],
            primary_queue_name="test_queue",
            other_scheduler_directives={"-mock_directive": "mock_value"},
            requires_task_distribution=False,
        )

        # Define common job parameters
        self.common_job_params = {
            "scheduler": self.scheduler,
            "commands": "echo Hello, World",
            "account_key": "test_account",
            "cpus": 4,
            "walltime": "01:00:00",
            "job_name": "test_job",
            "output_file": "/test/output.log",
            "queue_name": "test_queue",
        }

        self.mock_env_vars = {
            "MPIHOME": "/mock/mpi",
            "NETCDFHOME": "/mock/netcdf",
            "LD_LIBRARY_PATH": "/mock/lib",
        }

    def teardown_method(self, method):
        self.patch_qos_properties.stop()  # Stop the patch_queue_properties to restore the original behavior
        self.patch_partition_properties.stop()

    def test_script_task_distribution_not_required(self):
        """Verifies that the correct script is generated when the node x cpu breakdown
        is not required.
        """
        # Initialize the job
        job = SlurmJob(
            **self.common_job_params,
        )

        expected_script = (
            "#!/bin/bash\n"
            "#SBATCH --job-name=test_job\n"
            "#SBATCH --output=/test/output.log\n"
            "#SBATCH --qos=test_queue\n"
            "#SBATCH --ntasks=4\n"
            "#SBATCH --account=test_account\n"
            "#SBATCH --export=ALL\n"
            "#SBATCH --mail-type=ALL\n"
            "#SBATCH --time=01:00:00\n"
            "#SBATCH -mock_directive mock_value\n"
            "\necho Hello, World"
        )

        assert job.script.strip() == expected_script.strip(), (
            f"Expected:\n{expected_script}\n\nGot:\n{job.script}"
        )

    @pytest.mark.filterwarnings(
        r"ignore:.* Attempting to create scheduler job.*:UserWarning"
    )
    @patch(
        "cstar.system.scheduler.SlurmScheduler.global_max_cpus_per_node",
        new_callable=PropertyMock,
    )
    def test_script_task_distribution_required(
        self,
        mock_global_max_cpus,
    ):
        """Verifies the job script is correctly generated when the nodes x cpu
        distribution is required by the system.

        This test ensures that the `SlurmJob.script` property includes the appropriate SLURM
        directives for a job where task distribution is required by the scheduler. It validates
        that nodes and tasks-per-node directives are correctly calculated and included.

        Mocks
        -----
        SlurmScheduler.global_max_cpus_per_node
            Mocked to return the system's maximum CPUs per node (64).

        Asserts
        -------
        - That the generated job script matches a pre-specified expected script
        """
        mock_global_max_cpus.return_value = 64

        params = self.common_job_params
        new_scheduler = self.scheduler
        new_scheduler.queues = [
            self.mock_partition,
        ]
        new_scheduler.requires_task_distribution = True
        params.update({"scheduler": new_scheduler})

        # Initialize the job
        job = SlurmJob(
            **params,
        )

        expected_script = (
            "#!/bin/bash\n"
            "#SBATCH --job-name=test_job\n"
            "#SBATCH --output=/test/output.log\n"
            "#SBATCH --partition=test_queue\n"
            "#SBATCH --nodes=1\n"
            "#SBATCH --ntasks-per-node=4\n"
            "#SBATCH --account=test_account\n"
            "#SBATCH --export=ALL\n"
            "#SBATCH --mail-type=ALL\n"
            "#SBATCH --time=01:00:00\n"
            "#SBATCH -mock_directive mock_value\n"
            "\necho Hello, World"
        )

        assert job.script.strip() == expected_script.strip(), (
            f"Expected:\n{expected_script}\n\nGot:\n{job.script}"
        )

    @patch(
        "cstar.system.manager.CStarSystemManager.environment", new_callable=PropertyMock
    )
    @patch.dict(
        "os.environ",
        {"SLURM_JOB_ID": "123", "SLURM_NODELIST": "mock_node", "SOME_ENV_VAR": "value"},
        clear=True,
    )
    @patch("subprocess.run")
    def test_submit(self, mock_subprocess, mock_environment, tmp_path):
        """Ensures that the `submit` method properly submits a SLURM job and sets the
        job ID.

        This test validates that the `SlurmJob.submit` method:
        - Saves the job script to the specified path.
        - Executes the `sbatch` command to submit the job.
        - Extracts and sets the job ID from the `sbatch` output.

        Mocks
        -----
        subprocess.run
            Mocked to simulate the `sbatch` command's execution and output.
        CStarSystemManager.environment
            Mocked to provide environment variables and Lmod settings.
        os.environ
            Mocked to include specific SLURM-related environment variables.

        Asserts
        -------
        - That the job script file is created and its content matches the expected script.
        - That the `sbatch` command is executed with the correct arguments.
        - That the job ID is correctly extracted and assigned.
        """
        # Mock environment
        mock_environment.return_value.environment_variables = self.mock_env_vars
        mock_environment.return_value.uses_lmod = False

        # Create temporary directory for the job script
        script_path = tmp_path / "test_job.sh"
        run_path = tmp_path

        # Mock the subprocess.run behavior for sbatch
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="Submitted batch job 12345\n", stderr=""
        )

        # Initialize the job
        job = SlurmJob(
            **self.common_job_params,
            script_path=script_path,
            run_path=run_path,
        )

        job.submit()

        # Check that the script file is saved and matches job.script
        assert script_path.exists()
        with script_path.open() as f:
            script_content = f.read()
        assert script_content.strip() == job.script.strip(), (
            f"Script content mismatch!\nExpected:\n{job.script}\nGot:\n{script_content}"
        )

        # Hardcoded expected environment (excluding SLURM_ variables)
        expected_env = {"SOME_ENV_VAR": "value"}

        # Check that sbatch was called correctly
        mock_subprocess.assert_called_once_with(
            f"sbatch {script_path}",
            shell=True,
            cwd=run_path,
            env=expected_env,
            capture_output=True,
            text=True,
        )

        # Check that the job ID was set
        assert job.id == 12345

    @pytest.mark.parametrize(
        "subprocess_stdout, subprocess_returncode, expected_exception_message",
        [
            # Case 1: Non-zero return code from subprocess.run
            (
                "",
                1,
                "Non-zero exit code when submitting job. Return Code: `1`. STDERR:",
            ),
            # Case 2: Missing job ID in subprocess stdout
            (
                "Submitted job without ID",
                0,
                "Failed to parse job ID from sbatch output",
            ),
        ],
    )
    @patch("subprocess.run")
    def test_submit_raises(
        self,
        mock_subprocess,
        tmp_path,
        subprocess_stdout,
        subprocess_returncode,
        expected_exception_message,
    ):
        """Ensures that `submit` raises a `RuntimeError` for invalid `sbatch` responses
        or errors.

        This test validates that the `SlurmJob.submit` method handles errors correctly
        by raising a `RuntimeError` in the following scenarios:
        - The `sbatch` command exits with a non-zero return code.
        - The job ID is missing or malformed in the `sbatch` output.

        The test uses parameterization to evaluate multiple failure scenarios.

        Mocks
        -----
        subprocess.run
            Mocked to simulate various `sbatch` command responses, including failures
            and invalid output.

        Asserts
        -------
        - That a `RuntimeError` is raised with the expected message for each failure scenario.
        """
        # Mock the subprocess.run behavior
        mock_subprocess.return_value = MagicMock(
            returncode=subprocess_returncode,
            stdout=subprocess_stdout,
            stderr="Error" if subprocess_returncode != 0 else "",
        )

        # Create temporary directory for the job script
        script_path = tmp_path / "test_job.sh"
        run_path = tmp_path

        # Initialize the job
        job = SlurmJob(
            **self.common_job_params,
            script_path=script_path,
            run_path=run_path,
        )

        # Assert that the correct exception is raised with the expected message
        with pytest.raises(RuntimeError, match=expected_exception_message):
            job.submit()

    @patch("subprocess.run")
    @patch("cstar.execution.scheduler_job.SlurmJob.status", new_callable=PropertyMock)
    def test_cancel(self, mock_status, mock_subprocess, tmp_path, log: logging.Logger):
        """Verifies that the `cancel` method cancels a SLURM job and raises an exception
        if it fails.

        This test ensures that the `SlurmJob.cancel` method:
        - Executes the `scancel` command with the correct job ID and parameters.
        - Successfully cancels the job when `scancel` returns a zero exit code.
        - Raises a `RuntimeError` when `scancel` fails (non-zero exit code).

        Mocks
        -----
        subprocess.run
            Mocked to simulate the `scancel` command, including both successful and
            failed executions.
        SlurmJob.status
            Mocked to simulate the job status as RUNNING

        Asserts
        -------
        - That `scancel` is called with the correct job ID and working directory.
        - That the job is successfully canceled when `scancel` succeeds.
        - That a `RuntimeError` is raised with an appropriate error message when `scancel` fails.
        """
        # Create a temporary directory for the job run path
        run_path = tmp_path

        # Mock the status to simulate a running job
        mock_status.return_value = ExecutionStatus.RUNNING

        # Mock the subprocess.run behavior for successful cancellation
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="", stderr="")

        # Initialize the job with a set job ID
        job = SlurmJob(
            **self.common_job_params,
            run_path=run_path,
        )
        job._id = 12345  # Manually set the job ID

        # Call cancel
        job.cancel()

        # Log mock call arguments
        log.info(f"Mock call args after success: {mock_subprocess.call_args_list}")

        # Check that scancel was called correctly
        mock_subprocess.assert_called_once_with(
            "scancel 12345",
            shell=True,
            cwd=run_path,
            capture_output=True,
            text=True,
        )

        # Reset the mock for the next scenario
        mock_subprocess.reset_mock()

        # Mock the subprocess.run behavior for failed cancellation
        mock_subprocess.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error: Job not found"
        )

        # Call cancel and expect a RuntimeError
        with pytest.raises(
            RuntimeError, match="Non-zero exit code when cancelling job."
        ):
            job.cancel()

        # Verify that scancel was still called
        mock_subprocess.assert_called_once_with(
            "scancel 12345",
            shell=True,
            cwd=run_path,
            capture_output=True,
            text=True,
        )

    def test_save_script(self, tmp_path):
        """Tests the `save_script` method, creating a temporary job script file and
        checking its content.

        This test validates that the `SlurmJob.save_script` method:
        - Saves the job script to the specified file path.
        - Writes the content of the `script` property to the file.

        Asserts
        -------
        - That the job script file is created at the specified path.
        - That the content of the created file matches the expected script.
        """
        # Define paths for the script file
        script_path = tmp_path / "test_job.sh"

        # Initialize the job
        job = SlurmJob(
            **self.common_job_params,
            script_path=script_path,
        )

        # Call save_script
        job.save_script()

        # Check that the file was created
        assert script_path.exists(), "The script file was not created."

        # Verify the file content matches job.script
        with script_path.open() as f:
            file_content = f.read()

        assert file_content.strip() == job.script.strip(), (
            f"Script content mismatch!\nExpected:\n{job.script}\nGot:\n{file_content}"
        )

    @patch("subprocess.run")
    @pytest.mark.parametrize(
        "job_id, sacct_output, return_code, expected_status, should_raise",
        [
            (None, "", 0, ExecutionStatus.UNSUBMITTED, False),  # Unsubmitted job
            (12345, "PENDING\n", 0, ExecutionStatus.PENDING, False),  # Pending job
            (12345, "RUNNING\n", 0, ExecutionStatus.RUNNING, False),  # Running job
            (
                12345,
                "COMPLETED\n",
                0,
                ExecutionStatus.COMPLETED,
                False,
            ),  # Completed job
            (
                12345,
                "CANCELLED\n",
                0,
                ExecutionStatus.CANCELLED,
                False,
            ),  # Cancelled job
            (12345, "FAILED\n", 0, ExecutionStatus.FAILED, False),  # Failed job
            (12345, "", 1, None, True),  # sacct command failure
        ],
    )
    def test_status(
        self,
        mock_subprocess,
        job_id,
        sacct_output,
        return_code,
        expected_status,
        should_raise,
    ):
        """Verifies the status retrieval logic for a SLURM job using the `sacct`
        command.

        This test ensures that the `SlurmJob.status` property:
        - Retrieves the correct job status from `sacct` output for various job states.
        - Raises an exception when `sacct` fails or produces invalid output.

        The test uses parameterization to evaluate:
        - Valid job states like `PENDING`, `RUNNING`, `COMPLETED`, etc.
        - Error scenarios such as `sacct` command failures.

        Mocks
        -----
        subprocess.run
            Mocked to simulate the `sacct` command, returning various outputs and return codes.

        Asserts
        -------
        - That the job status matches the expected state for valid `sacct` outputs.
        - That a `RuntimeError` is raised when `sacct` fails or returns invalid data.
        """
        # Initialize the job
        job = SlurmJob(**self.common_job_params)

        # Set the job ID if provided
        if job_id is not None:
            job._id = job_id

        # Mock subprocess.run behavior
        mock_subprocess.return_value = MagicMock(
            returncode=return_code,
            stdout=sacct_output,
            stderr="Error: sacct command failed" if return_code != 0 else "",
        )

        # Check the expected outcome
        if should_raise:
            with pytest.raises(
                RuntimeError, match="Failed to retrieve job status using sacct"
            ):
                job.status
        else:
            assert job.status == expected_status, (
                f"Expected status '{expected_status}' but got '{job.status}'"
            )
