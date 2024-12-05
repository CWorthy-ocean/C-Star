import time
import pytest
import threading
from unittest.mock import MagicMock, patch, PropertyMock
from cstar.base.scheduler import Scheduler
from cstar.base.scheduler_job import SchedulerJob, SlurmJob, PBSJob


class MockSchedulerJob(SchedulerJob):
    @property
    def status(self):
        return "mock_status"

    def submit(self):
        return "mock_submit"


class MockScheduler(Scheduler):
    def __init__(self):
        # Initialize with mock values
        super().__init__(
            queue_flag="mock_flag",
            queues=[MagicMock(name="default_queue", max_walltime="02:00:00")],
            primary_queue_name="default_queue",
            other_scheduler_directives={"-mock_directive": "mock_value"},
        )

    def get_queue(self, name):
        # Return a mocked queue with default properties
        return MagicMock(name=name, max_walltime="02:00:00")

    @property
    def global_max_cpus_per_node(self):
        return 64  # Mocked maximum CPUs per node

    @property
    def global_max_mem_per_node_gb(self):
        return 128  # Mocked maximum memory in GB


class TestSchedulerJobBase:
    @pytest.mark.filterwarnings("ignore:Walltime parameter unspecified")
    def test_initialization_defaults(self):
        job = MockSchedulerJob(
            scheduler=MockScheduler(),
            commands="echo Hello, World",
            account_key="test_account",
            cpus=4,
        )

        assert job.job_name.startswith("cstar_job_")
        assert job.script_path.name.endswith(".sh")
        assert job.queue_name == "default_queue"
        assert job.walltime == "02:00:00"
        assert job.cpus == 4

    def test_walltime_exceeds_max_walltime(self):
        with pytest.raises(
            ValueError, match="Selected walltime 04:00:00 exceeds maximum"
        ):
            MockSchedulerJob(
                scheduler=MockScheduler(),
                commands="echo Hello, World",
                account_key="test_account",
                cpus=4,
                walltime="04:00:00",  # Requesting 2 hours
            )

    def test_updates_non_running_job(self):
        job = MockSchedulerJob(
            scheduler=MockScheduler(),
            commands="echo Hello, World",
            account_key="test_account",
            walltime="00:20:00",
            cpus=4,
        )

        with patch.object(
            MockSchedulerJob, "status", new_callable=PropertyMock
        ) as mock_status:
            mock_status.return_value = "completed"
            with patch("builtins.print") as mock_print:
                job.updates(seconds=10)

                mock_print.assert_any_call(
                    "This job is currently not running (completed). Live updates cannot be provided."
                )
                mock_print.assert_any_call(
                    f"See {job.output_file.resolve()} for job output"
                )

    def test_updates_running_job_with_tmp_file(self, tmp_path):
        # Create a temporary output file
        output_file = tmp_path / "output.log"
        initial_content = ["First line\n"]
        live_updates = ["Second line\n", "Third line\n", "Fourth line\n"]
        print(output_file)
        # Write initial content to the file
        with output_file.open("w") as f:
            f.writelines(initial_content)

        # Create a MockSchedulerJob instance and assign the output file
        job = MockSchedulerJob(
            scheduler=MockScheduler(),
            commands="echo Hello, World",
            account_key="test_account",
            walltime="00:20:00",
            output_file=output_file,
            cpus=4,
        )

        # Mock the `status` property to return "running"
        with patch.object(
            MockSchedulerJob, "status", new_callable=PropertyMock
        ) as mock_status:
            mock_status.return_value = "running"

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
            with patch("builtins.print") as mock_print:
                job.updates(seconds=0.25)

                # Ensure both initial and live update lines are printed
                for line in live_updates:
                    mock_print.assert_any_call(line, end="")

            # Ensure the thread finishes before the test ends
            updater_thread.join()


##


class TestCalculateNodeDistribution:
    """Tests for `_calculate_node_distribution`, ensuring correct calculation of nodes
    and cores per node for various input scenarios."""

    def setup_method(self):
        # Use the MockScheduler for testing
        self.mock_job = MockSchedulerJob(
            scheduler=MockScheduler(),
            commands="echo Test",
            account_key="test_account",
            walltime="00:20:00",
            cpus=1,
        )

    def test_exact_division(self):
        """Test when `n_cores_required` is an exact multiple of `tot_cores_per_node`."""
        n_cores_required = 256
        tot_cores_per_node = 64

        result = self.mock_job._calculate_node_distribution(
            n_cores_required, tot_cores_per_node
        )
        assert result == (4, 64), f"Expected (4, 64), got {result}"

    def test_partial_division(self):
        """Test when `n_cores_required` is not an exact multiple of
        `tot_cores_per_node`."""
        n_cores_required = 300
        tot_cores_per_node = 64

        result = self.mock_job._calculate_node_distribution(
            n_cores_required, tot_cores_per_node
        )
        assert result == (5, 60), f"Expected (5, 60), got {result}"

    def test_single_node(self):
        """Test when `n_cores_required` is less than or equal to
        `tot_cores_per_node`."""
        n_cores_required = 50
        tot_cores_per_node = 64

        result = self.mock_job._calculate_node_distribution(
            n_cores_required, tot_cores_per_node
        )
        assert result == (1, 50), f"Expected (1, 50), got {result}"

    def test_minimum_cores(self):
        """Test the edge case where `n_cores_required` is very low, such as 1."""
        n_cores_required = 1
        tot_cores_per_node = 64

        result = self.mock_job._calculate_node_distribution(
            n_cores_required, tot_cores_per_node
        )
        assert result == (1, 1), f"Expected (1, 1), got {result}"


##


class TestSlurmJob:
    """Tests for the SlurmJob class."""

    def setup_method(self, method):
        # Mock the environment variables
        self.mock_env_vars = {
            "MPIHOME": "/mock/mpi",
            "NETCDFHOME": "/mock/netcdf",
            "LD_LIBRARY_PATH": "/mock/lib",
        }

        # Define common job parameters
        self.common_job_params = {
            "scheduler": MockScheduler(),
            "commands": "echo Hello, World",
            "account_key": "test_account",
            "cpus": 4,
            "walltime": "01:00:00",
            "job_name": "test_job",
        }

    @patch("cstar.base.system.CStarSystem.name", new_callable=PropertyMock)
    @patch("cstar.base.system.CStarSystem.environment", new_callable=PropertyMock)
    def test_script(self, mock_environment, mock_sysname, tmp_path):
        # Mock environment
        mock_environment.return_value.environment_variables = self.mock_env_vars
        mock_environment.return_value.uses_lmod = True
        mock_sysname.return_value = "mock_system"

        # Mock package_root to point to tmp_path
        mock_environment.return_value.package_root = tmp_path

        # Create the necessary mock .lmod file in the mocked package_root
        lmod_dir = tmp_path / "additional_files/lmod_lists"
        lmod_dir.mkdir(parents=True, exist_ok=True)
        lmod_file = lmod_dir / "mock_system.lmod"
        lmod_modules = ["moduleA", "moduleB"]
        lmod_file.write_text("\n".join(lmod_modules))

        # Initialize the job
        job = SlurmJob(
            **self.common_job_params,
            output_file="/test/output.log",
            queue_name="test_queue",
        )

        expected_script = (
            "#!/bin/bash\n"
            "#SBATCH --job-name=test_job\n"
            "#SBATCH --output=/test/output.log\n"
            "#SBATCH --mock_flag=test_queue\n"
            "#SBATCH --ntasks=4\n"
            "#SBATCH --account=test_account\n"
            "#SBATCH --export=NONE\n"
            "#SBATCH --mail-type=ALL\n"
            "#SBATCH --time=01:00:00\n"
            "#SBATCH -mock_directive mock_value"
            "\nmodule reset\n"
            "module load moduleA\n\n"
            "module load moduleB"
            "\nprintenv\n"
            'export MPIHOME="/mock/mpi"\n'
            'export NETCDFHOME="/mock/netcdf"\n'
            'export LD_LIBRARY_PATH="/mock/lib"\n'
            "\necho Hello, World"
        )

        assert (
            job.script.strip() == expected_script.strip()
        ), f"Expected:\n{expected_script}\n\nGot:\n{job.script}"

    @patch("subprocess.run")
    @patch("cstar.base.system.CStarSystem.environment", new_callable=PropertyMock)
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

    @patch("subprocess.run")
    @pytest.mark.parametrize(
        "job_id, sacct_output, return_code, expected_status, should_raise",
        [
            (None, "", 0, "unsubmitted", False),  # Unsubmitted job
            (12345, "PENDING\n", 0, "pending", False),  # Pending job
            (12345, "RUNNING\n", 0, "running", False),  # Running job
            (12345, "COMPLETED\n", 0, "completed", False),  # Completed job
            (12345, "CANCELLED\n", 0, "cancelled", False),  # Cancelled job
            (12345, "FAILED\n", 0, "failed", False),  # Failed job
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


class TestPBSJob:
    """Tests for the PBSJob class."""

    def setup_method(self, method):
        # Shared setup for PBSJob tests
        self.common_job_params = {
            "scheduler": MockScheduler(),
            "commands": "echo Hello, World",
            "account_key": "test_account",
            "cpus": 4,
            "nodes": 2,
            "cpus_per_node": 2,
            "walltime": "02:00:00",
            "job_name": "test_pbs_job",
            "output_file": "/test/pbs_output.log",
            "queue_name": "test_queue",
        }

    @patch("cstar.base.system.CStarSystem.scheduler", new_callable=PropertyMock)
    def test_script(self, mock_scheduler):
        # Mock the scheduler and its attributes
        mock_scheduler.return_value = MagicMock()
        mock_scheduler.return_value.queue_flag = "q"
        mock_scheduler.return_value.other_scheduler_directives = {
            "mock_directive": "mock_value"
        }

        # Initialize a PBSJob
        job = PBSJob(**self.common_job_params)

        expected_script = (
            "#PBS -S /bin/bash\n"
            "#PBS -N test_pbs_job\n"
            "#PBS -o /test/pbs_output.log\n"
            "#PBS -A test_account\n"
            "#PBS -l select=2:ncpus=2,walltime=02:00:00\n"
            "#PBS -mock_flag test_queue\n"
            "#PBS -j oe\n"
            "#PBS -k eod\n"
            "#PBS -V\n"
            "#PBS mock_directive mock_value"
            "\ncd ${PBS_O_WORKDIR}"
            "\n\necho Hello, World"
        )

        # Validate the script content
        assert (
            job.script.strip() == expected_script.strip()
        ), f"Script mismatch!\nExpected:\n{expected_script}\n\nGot:\n{job.script}"
