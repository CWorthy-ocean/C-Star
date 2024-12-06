# 148, 154, 158-174, 194, 198, 219-228, 241-242, 358, 369, 416, 434, 451-456, 478, 498
# 195, 199, 220-229, 242-243, 359, 370, 417, 435, 452-457, 479, 499
# 359, 370, 417, 435, 452-457, 479, 499
# TestSchedulerJobBase
# - Test of warning when queue.max_walltime is missing [111]
# - Test of different pathways to nodes and cpus_per_node [entire block, 158]
# - Test of confirmation that user wants to continue with updates(seconds==0) [219]
# - Test of catching KeyboardInterrupt in updates() [241]

# TestSlurmJob
# - Test of raise when sbatch return code is nonzero [358]
# - Test of raise when no matches for JobID regex [369]

# TestPBSJob
# - case of "unsubmitted" in status property when id is None [416]
# - Test of raise in status property on inability to find JobID in qstat/JSON fields [434]
# - Check for "E" in status property [451]
# - Check of unknown job_state in status property [454]
# - Test of raise in status property for JSONDecodeError [456]
# - Test of raise in submit method when regex can't find JobID [478]
# - Test of raise in cancel method when returncode is 0


import json
import time
import pytest
import threading
from unittest.mock import MagicMock, patch, PropertyMock
from cstar.base.scheduler import (
    Scheduler,
    SlurmScheduler,
    PBSScheduler,
    SlurmQueue,
    PBSQueue,
)
from cstar.base.scheduler_job import (
    SchedulerJob,
    SlurmJob,
    PBSJob,
    create_scheduler_job,
)


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
    def setup_method(self, method):
        # Define common job parameters
        self.common_job_params = {
            "scheduler": MockScheduler(),
            "commands": "echo Hello, World",
            "account_key": "test_account",
            "cpus": 4,
            "walltime": "01:00:00",
        }

    @pytest.mark.filterwarnings("ignore:Walltime parameter unspecified")
    def test_initialization_defaults(self):
        params = {
            key: value
            for key, value in self.common_job_params.items()
            if key != "walltime"
        }
        job = MockSchedulerJob(**params)

        assert job.job_name.startswith("cstar_job_")
        assert job.script_path.name.endswith(".sh")
        assert job.queue_name == "default_queue"
        assert job.walltime == "02:00:00"
        assert job.cpus == 4

    def test_init_no_walltime_and_no_queue_max_walltime(self):
        with patch.object(
            MockScheduler, "get_queue", return_value=MagicMock(max_walltime=None)
        ):
            params = {
                key: value
                for key, value in self.common_job_params.items()
                if key != "walltime"
            }
            with pytest.raises(
                ValueError,
                match="Cannot create scheduler job: walltime parameter not provided",
            ):
                MockSchedulerJob(**params)

    def test_init_walltime_provided_but_no_queue_max_walltime(self):
        with patch.object(
            MockScheduler, "get_queue", return_value=MagicMock(max_walltime=None)
        ):
            with pytest.warns(
                UserWarning,
                match="Unable to determine the maximum allowed walltime for chosen queue",
            ):
                job = MockSchedulerJob(**self.common_job_params)
                assert job.walltime == "01:00:00"

    def test_init_no_walltime_but_queue_max_walltime_provided(self):
        with patch.object(
            MockScheduler, "get_queue", return_value=MagicMock(max_walltime="02:00:00")
        ):
            params = {
                key: value
                for key, value in self.common_job_params.items()
                if key != "walltime"
            }
            with pytest.warns(UserWarning, match="Walltime parameter unspecified"):
                job = MockSchedulerJob(**params)
                assert job.walltime == "02:00:00"

    def test_init_walltime_exceeds_max_walltime(self):
        with pytest.raises(
            ValueError, match="Selected walltime 04:00:00 exceeds maximum"
        ):
            params = {
                key: value
                for key, value in self.common_job_params.items()
                if key != "walltime"
            }
            MockSchedulerJob(**params, walltime="04:00:00")

    def test_init_pbs_without_nodes_but_with_cpus_per_node(self):
        mock_queue = PBSQueue(name="main", max_walltime="02:00:00")
        scheduler = PBSScheduler(
            queue_flag="q", queues=[mock_queue], primary_queue_name="main"
        )

        params = self.common_job_params.copy()
        params.update(
            {
                "scheduler": scheduler,
                "nodes": None,
                "cpus_per_node": 16,  # Explicit cpus_per_node
                "queue_name": "main",
            }
        )

        job = MockSchedulerJob(**params)

        # Nodes should be calculated as ceil(cpus / cpus_per_node)
        assert job.nodes == 1  # cpus=4, cpus_per_node=16
        assert job.cpus_per_node == 16

    def test_init_pbs__with_nodes_but_without_cpus_per_node(self):
        mock_queue = PBSQueue(name="main", max_walltime="02:00:00")
        scheduler = PBSScheduler(
            queue_flag="q", queues=[mock_queue], primary_queue_name="main"
        )

        params = self.common_job_params.copy()
        params.update(
            {
                "scheduler": scheduler,
                "nodes": 2,  # Explicit nodes
                "cpus_per_node": None,  # cpus_per_node not specified
                "queue_name": "main",
            }
        )

        job = PBSJob(**params)

        # cpus_per_node should be calculated as cpus / nodes
        assert job.nodes == 2
        assert job.cpus_per_node == 2  # cpus=4, nodes=2

    @patch.object(PBSScheduler, "global_max_cpus_per_node", new_callable=PropertyMock)
    def test_init_pbs_without_nodes_or_cpus_per_node(self, mock_max_cpus_per_node):
        mock_queue = PBSQueue(name="main", max_walltime="02:00:00")
        scheduler = PBSScheduler(
            queue_flag="q", queues=[mock_queue], primary_queue_name="main"
        )
        mock_max_cpus_per_node.return_value = 128  # Mock the maximum CPUs per node

        params = self.common_job_params.copy()
        params.update(
            {
                "scheduler": scheduler,
                "nodes": None,
                "cpus_per_node": None,  # Both nodes and cpus_per_node are missing
                "queue_name": "main",
            }
        )
        with pytest.warns(
            UserWarning,
            match="Attempting to create scheduler job without 'nodes' and 'cpus_per_node'",
        ):
            job = PBSJob(**params)
            # Check the calculated values from _calculate_node_distribution
            assert job.nodes == 1
            assert job.cpus_per_node == 4  # cpus=4, max_cpus_per_node=128

    @pytest.mark.parametrize(
        "nodes, cpus_per_node, expected_nodes, expected_cpus_per_node",
        [
            (3, 8, 3, 8),  # Both provided as integers
            (None, 8, None, 8),  # Only cpus_per_node provided
            (3, None, 3, None),  # Only nodes provided
            (None, None, None, None),  # Neither provided
        ],
    )
    def test_init_cpus_without_pbs(
        self, nodes, cpus_per_node, expected_nodes, expected_cpus_per_node
    ):
        params = self.common_job_params.copy()
        params.update(
            {
                "scheduler": MockScheduler(),
                "nodes": nodes,
                "cpus_per_node": cpus_per_node,
            }
        )

        job = MockSchedulerJob(**params)

        # Ensure nodes and cpus_per_node are correctly assigned
        assert job.nodes == expected_nodes
        assert job.cpus_per_node == expected_cpus_per_node

    def test_updates_non_running_job(self):
        job = MockSchedulerJob(**self.common_job_params)

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

        job = MockSchedulerJob(**self.common_job_params, output_file=output_file)

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

    def test_updates_indefinite_with_seconds_param_0(self, tmp_path):
        # Create a temporary output file
        output_file = tmp_path / "output.log"
        content = ["Line 1\n", "Line 2\n", "Line 3\n"]

        # Write initial content to the file
        with output_file.open("w") as f:
            f.writelines(content)

        job = MockSchedulerJob(**self.common_job_params, output_file=output_file)

        # Mock the `status` property to return "running"
        with patch.object(
            MockSchedulerJob, "status", new_callable=PropertyMock
        ) as mock_status:
            mock_status.return_value = "running"

            # Patch input to simulate the confirmation prompt
            with patch("builtins.input", side_effect=["y"]) as mock_input:
                # Replace `time.sleep` with a side effect that raises KeyboardInterrupt
                with patch("builtins.print") as mock_print:
                    # Simulate a KeyboardInterrupt during the updates call
                    with patch("time.sleep", side_effect=KeyboardInterrupt):
                        job.updates(seconds=0)  # Run updates indefinitely

                        # Assert that the "stopped by user" message was printed
                        mock_print.assert_any_call(
                            "\nLive status updates stopped by user."
                        )
                # Verify that the prompt was displayed to the user
                mock_input.assert_called_once_with(
                    "This will provide indefinite updates to your job. You can stop it anytime using Ctrl+C. "
                    "Do you want to continue? (y/n): "
                )
            # Patch input to simulate the confirmation prompt
            with patch("builtins.input", side_effect=["n"]) as mock_input:
                with patch("builtins.open", create=True) as mock_open:
                    job.updates(seconds=0)
                    mock_open.assert_not_called()


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

    @patch("subprocess.run")
    @patch("cstar.base.system.CStarSystem.scheduler", new_callable=PropertyMock)
    def test_submit(self, mock_scheduler, mock_subprocess, tmp_path):
        # Mock scheduler attributes
        mock_scheduler.return_value = MagicMock()
        mock_scheduler.return_value.queue_flag = "q"
        mock_scheduler.return_value.other_scheduler_directives = {}

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
    @patch("cstar.base.system.CStarSystem.scheduler", new_callable=PropertyMock)
    def test_submit_raises(
        self,
        mock_scheduler,
        mock_subprocess,
        returncode,
        stdout,
        stderr,
        match_message,
        tmp_path,
    ):
        # Mock scheduler attributes
        mock_scheduler.return_value = MagicMock()
        mock_scheduler.return_value.queue_flag = "q"
        mock_scheduler.return_value.other_scheduler_directives = {}

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
    @patch("cstar.base.scheduler_job.PBSJob.status", new_callable=PropertyMock)
    def test_cancel_running_job(self, mock_status, mock_subprocess, tmp_path):
        # Mock the status to "running"
        mock_status.return_value = "running"

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
    @patch("cstar.base.scheduler_job.PBSJob.status", new_callable=PropertyMock)
    @patch("builtins.print")
    def test_cancel_completed_job(
        self, mock_print, mock_status, mock_subprocess, tmp_path
    ):
        # Mock the status to "completed"
        mock_status.return_value = "completed"

        # Create a PBSJob with a set job ID
        job = PBSJob(
            **self.common_job_params,
            run_path=tmp_path,
        )
        job._id = 12345  # Manually set the job ID

        # Attempt to cancel the job
        job.cancel()

        # Verify the message was printed
        mock_print.assert_called_with("Cannot cancel job with status completed")

        # Verify qdel was not called
        mock_subprocess.assert_not_called()

    ##
    @patch("subprocess.run")
    @patch("cstar.base.scheduler_job.PBSJob.status", new_callable=PropertyMock)
    def test_cancel_failure(self, mock_status, mock_subprocess, tmp_path):
        # Mock the status to "running"
        mock_status.return_value = "running"

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
                "pending",
                False,
                None,
                None,
            ),  # Pending job
            (
                {"Jobs": {"12345": {"job_state": "R"}}},
                None,
                "running",
                False,
                None,
                None,
            ),  # Running job
            (
                {"Jobs": {"12345": {"job_state": "C"}}},
                None,
                "completed",
                False,
                None,
                None,
            ),  # Completed job
            (
                {"Jobs": {"12345": {"job_state": "H"}}},
                None,
                "held",
                False,
                None,
                None,
            ),  # Held job
            (
                {"Jobs": {"12345": {"job_state": "F", "Exit_status": 1}}},
                None,
                "failed",
                False,
                None,
                None,
            ),  # Failed job
            (
                {"Jobs": {"12345": {"job_state": "F", "Exit_status": 0}}},
                None,
                "completed",
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
                "ending",
                False,
                None,
                None,
            ),  # Ending job
            (
                {"Jobs": {"12345": {"job_state": "X"}}},
                None,
                "unknown (X)",
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
            assert (
                job.status == expected_status
            ), f"Expected status '{expected_status}' but got '{job.status}'"

    def test_status_unsubmitted(self):
        # Create a PBSJob without setting a job ID
        job = PBSJob(**self.common_job_params)

        # Assert the status is "unsubmitted"
        assert (
            job.status == "unsubmitted"
        ), "Expected 'unsubmitted' status when job ID is None"

    @patch("json.loads", side_effect=json.JSONDecodeError("Expecting value", "", 0))
    @patch("subprocess.run")
    def test_status_json_decode_error(self, mock_subprocess, mock_json_loads):
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


class TestCreateSchedulerJob:
    @patch("cstar.base.system.CStarSystem.scheduler", new_callable=PropertyMock)
    @patch("cstar.base.scheduler.SlurmQueue.max_walltime", new_callable=PropertyMock)
    def test_create_slurm_job(self, mock_max_walltime, mock_scheduler):
        # Mock max_walltime for the queue
        mock_max_walltime.return_value = "02:00:00"

        # Mock the scheduler to be a SlurmScheduler with a valid queue
        mock_queue = SlurmQueue(name="test_queue", query_name="test_queue")
        mock_scheduler.return_value = SlurmScheduler(
            queues=[mock_queue],
            primary_queue_name="test_queue",
            queue_flag="mock_flag",
        )

        # Explicitly provide `queue_name`
        job = create_scheduler_job(
            commands="echo Hello, World",
            cpus=4,
            account_key="test_account",
            walltime="01:00:00",
            queue_name="test_queue",  # Explicitly specify queue_name
        )

        # Ensure the returned job is a SlurmJob instance
        assert isinstance(job, SlurmJob), f"Expected SlurmJob, got {type(job).__name__}"
        assert job.commands == "echo Hello, World"
        assert job.cpus == 4
        assert job.account_key == "test_account"
        assert job.walltime == "01:00:00"  # Ensure the provided walltime is used

    @patch("cstar.base.system.CStarSystem.scheduler", new_callable=PropertyMock)
    @patch(
        "cstar.base.scheduler.PBSScheduler.global_max_cpus_per_node",
        new_callable=PropertyMock,
    )
    def test_create_pbs_job(self, mock_global_max_cpus, mock_scheduler):
        # Mock global_max_cpus_per_node for the scheduler
        mock_global_max_cpus.return_value = 128

        # Mock the scheduler to be a PBSScheduler with a valid queue
        mock_queue = PBSQueue(name="test_queue", max_walltime="02:00:00")
        mock_scheduler.return_value = PBSScheduler(
            queues=[mock_queue],
            primary_queue_name="test_queue",
            queue_flag="mock_flag",
        )

        # Explicitly provide `queue_name`
        job = create_scheduler_job(
            commands="echo Hello, World",
            cpus=8,
            account_key="pbs_account",
            walltime="02:00:00",
            nodes=1,
            cpus_per_node=8,
            queue_name="test_queue",  # Explicitly specify queue_name
        )

        # Ensure the returned job is a PBSJob instance
        assert isinstance(job, PBSJob), f"Expected PBSJob, got {type(job).__name__}"
        assert job.commands == "echo Hello, World"
        assert job.cpus == 8
        assert job.account_key == "pbs_account"
        assert job.walltime == "02:00:00"  # Ensure the provided walltime is used

    @patch("cstar.base.system.CStarSystem.scheduler", new_callable=PropertyMock)
    def test_unsupported_scheduler(self, mock_scheduler):
        # Mock an unsupported scheduler type
        mock_scheduler.return_value = None  # No scheduler set

        with pytest.raises(TypeError, match="Unsupported scheduler type: NoneType"):
            create_scheduler_job(
                commands="echo Hello, World",
                cpus=4,
                account_key="test_account",
                walltime="01:00:00",
            )

    def test_missing_arguments(self):
        # No need to mock scheduler here as the error is in the function arguments
        with pytest.raises(TypeError, match="missing .* required positional argument"):
            create_scheduler_job(
                cpus=4,
                account_key="test_account",
                walltime="01:00:00",
            )
