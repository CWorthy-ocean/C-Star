import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from cstar.system.scheduler import (
    SlurmScheduler,
    SlurmQueue,
)
from cstar.system.scheduler_job import SlurmJob, JobStatus


class TestSlurmJob:
    """Tests for the SlurmJob class."""

    def setup_method(self, method):
        # Create a SlurmQueue and patch its max_walltime property
        self.mock_queue = SlurmQueue(name="test_queue", query_name="test_queue")
        self.patch_queue_properties = patch.object(
            type(self.mock_queue),
            "max_walltime",
            new_callable=PropertyMock,
            return_value="02:00:00",
        )
        self.mock_max_walltime = self.patch_queue_properties.start()

        # Create the SlurmScheduler with the patched queue
        self.scheduler = SlurmScheduler(
            queues=[self.mock_queue],
            primary_queue_name="test_queue",
            queue_flag="mock_flag",
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
        self.patch_queue_properties.stop()  # Stop the patch_queue_properties to restore the original behavior

    def test_script_task_distribution_not_required(self):
        # Initialize the job
        job = SlurmJob(
            **self.common_job_params,
        )

        expected_script = (
            "#!/bin/bash\n"
            "#SBATCH --job-name=test_job\n"
            "#SBATCH --output=/test/output.log\n"
            "#SBATCH --mock_flag=test_queue\n"
            "#SBATCH --ntasks=4\n"
            "#SBATCH --account=test_account\n"
            "#SBATCH --export=ALL\n"
            "#SBATCH --mail-type=ALL\n"
            "#SBATCH --time=01:00:00\n"
            "#SBATCH -mock_directive mock_value\n"
            "\necho Hello, World"
        )

        assert (
            job.script.strip() == expected_script.strip()
        ), f"Expected:\n{expected_script}\n\nGot:\n{job.script}"

    @pytest.mark.filterwarnings(
        r"ignore:.* Attempting to create scheduler job.*:UserWarning"
    )
    @patch(
        "cstar.system.scheduler.SlurmScheduler.global_max_cpus_per_node",
        new_callable=PropertyMock,
    )
    def test_script_task_distribution_required(self, mock_global_max_cpus):
        mock_global_max_cpus.return_value = 64

        new_scheduler = self.scheduler
        new_scheduler.requires_task_distribution = True

        params = self.common_job_params
        params.update({"scheduler": new_scheduler})

        # Initialize the job
        job = SlurmJob(
            **self.common_job_params,
        )

        expected_script = (
            "#!/bin/bash\n"
            "#SBATCH --job-name=test_job\n"
            "#SBATCH --output=/test/output.log\n"
            "#SBATCH --mock_flag=test_queue\n"
            "#SBATCH --nodes=1\n"
            "#SBATCH --ntasks-per-node=4\n"
            "#SBATCH --account=test_account\n"
            "#SBATCH --export=ALL\n"
            "#SBATCH --mail-type=ALL\n"
            "#SBATCH --time=01:00:00\n"
            "#SBATCH -mock_directive mock_value\n"
            "\necho Hello, World"
        )

        assert (
            job.script.strip() == expected_script.strip()
        ), f"Expected:\n{expected_script}\n\nGot:\n{job.script}"

    @patch("subprocess.run")
    @patch(
        "cstar.system.manager.CStarSystemManager.environment", new_callable=PropertyMock
    )
    @patch.dict(
        "os.environ",
        {"SLURM_JOB_ID": "123", "SLURM_NODELIST": "mock_node", "SOME_ENV_VAR": "value"},
        clear=True,
    )
    def test_submit(self, mock_environment, mock_subprocess, tmp_path):
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
        assert (
            script_content.strip() == job.script.strip()
        ), f"Script content mismatch!\nExpected:\n{job.script}\nGot:\n{script_content}"

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
            ("", 1, "Non-zero exit code when submitting job. STDERR:"),
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
    def test_cancel(self, mock_subprocess, tmp_path):
        # Create a temporary directory for the job run path
        run_path = tmp_path

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

        assert (
            file_content.strip() == job.script.strip()
        ), f"Script content mismatch!\nExpected:\n{job.script}\nGot:\n{file_content}"

    @patch("subprocess.run")
    @pytest.mark.parametrize(
        "job_id, sacct_output, return_code, expected_status, should_raise",
        [
            (None, "", 0, JobStatus.UNSUBMITTED, False),  # Unsubmitted job
            (12345, "PENDING\n", 0, JobStatus.PENDING, False),  # Pending job
            (12345, "RUNNING\n", 0, JobStatus.RUNNING, False),  # Running job
            (12345, "COMPLETED\n", 0, JobStatus.COMPLETED, False),  # Completed job
            (12345, "CANCELLED\n", 0, JobStatus.CANCELLED, False),  # Cancelled job
            (12345, "FAILED\n", 0, JobStatus.FAILED, False),  # Failed job
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
            assert (
                job.status == expected_status
            ), f"Expected status '{expected_status}' but got '{job.status}'"
