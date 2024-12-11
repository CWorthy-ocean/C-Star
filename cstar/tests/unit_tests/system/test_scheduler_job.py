import time
import pytest
import threading
from unittest.mock import MagicMock, patch, PropertyMock
from cstar.system.scheduler import (
    Scheduler,
    PBSScheduler,
    PBSQueue,
    SlurmScheduler,
    SlurmQOS,
)
from cstar.system.scheduler_job import (
    JobStatus,
    SchedulerJob,
    PBSJob,
    SlurmJob,
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
            "nodes": 1,
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

    ## Cpu distribution tests
    def test_init_without_nodes_but_with_cpus_per_node(self):
        params = self.common_job_params.copy()
        params.update(
            {
                "nodes": None,
                "cpus_per_node": 16,  # Explicit cpus_per_node
            }
        )

        job = MockSchedulerJob(**params)

        # Nodes should be calculated as ceil(cpus / cpus_per_node)
        assert job.nodes == 1  # cpus=4, cpus_per_node=16
        assert job.cpus_per_node == 16

    def test_init_with_nodes_but_without_cpus_per_node(self):
        params = self.common_job_params.copy()
        params.update(
            {
                "nodes": 2,  # Explicit nodes
                "cpus_per_node": None,  # cpus_per_node not specified
            }
        )

        job = MockSchedulerJob(**params)

        # cpus_per_node should be calculated as cpus / nodes
        assert job.nodes == 2
        assert job.cpus_per_node == 2  # cpus=4, nodes=2

    def test_init_without_nodes_or_cpus_per_node(self):
        params = self.common_job_params.copy()
        params.update(
            {
                "nodes": None,
                "cpus_per_node": None,  # Both nodes and cpus_per_node are missing
                "cpus": 128,
            }
        )
        with pytest.warns(
            UserWarning,
            match="Attempting to create scheduler job without 'nodes' and 'cpus_per_node'",
        ):
            job = MockSchedulerJob(**params)
            # Check the calculated values from _calculate_node_distribution
            assert job.nodes == 2
            assert job.cpus_per_node == 64  # cpus=128, max_cpus_per_node=64

    @pytest.mark.parametrize(
        "nodes, cpus_per_node, expected_nodes, expected_cpus_per_node",
        [
            (3, 8, 3, 8),  # Both provided as integers
            (None, 8, None, 8),  # Only cpus_per_node provided
            (3, None, 3, None),  # Only nodes provided
            (None, None, None, None),  # Neither provided
        ],
    )
    def test_init_cpus_without_distribution_requirement(
        self, nodes, cpus_per_node, expected_nodes, expected_cpus_per_node
    ):
        params = self.common_job_params.copy()
        scheduler = MockScheduler()
        scheduler.requires_task_distribution = False
        params.update(
            {
                "scheduler": scheduler,
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
            mock_status.return_value = JobStatus.COMPLETED
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
            mock_status.return_value = JobStatus.RUNNING

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
            mock_status.return_value = JobStatus.RUNNING

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
@pytest.mark.filterwarnings(
    r"ignore:WARNING.*Attempting to create scheduler job.*:UserWarning"
)
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
class TestCreateSchedulerJob:
    @patch(
        "cstar.system.manager.CStarSystemManager.scheduler", new_callable=PropertyMock
    )
    @patch("cstar.system.scheduler.SlurmQOS.max_walltime", new_callable=PropertyMock)
    def test_create_slurm_job(self, mock_max_walltime, mock_scheduler):
        # Mock max_walltime for the queue
        mock_max_walltime.return_value = "02:00:00"

        # Mock the scheduler to be a SlurmScheduler with a valid queue
        mock_queue = SlurmQOS(name="test_queue", query_name="test_queue")
        mock_scheduler.return_value = SlurmScheduler(
            queues=[mock_queue],
            primary_queue_name="test_queue",
        )

        # Explicitly provide `queue_name`
        job = create_scheduler_job(
            commands="echo Hello, World",
            cpus=4,
            nodes=1,
            account_key="test_account",
            walltime="01:00:00",
            queue_name="test_queue",  # Explicitly specify queue_name
        )

        # Ensure the returned job is a SlurmJob instance
        assert isinstance(job, SlurmJob), f"Expected SlurmJob, got {type(job).__name__}"
        assert job.commands == "echo Hello, World"
        assert job.cpus == 4
        assert job.nodes == 1
        assert job.cpus_per_node == 4
        assert job.account_key == "test_account"
        assert job.walltime == "01:00:00"  # Ensure the provided walltime is used

    @patch(
        "cstar.system.manager.CStarSystemManager.scheduler", new_callable=PropertyMock
    )
    @patch(
        "cstar.system.scheduler.PBSScheduler.global_max_cpus_per_node",
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

    @patch(
        "cstar.system.manager.CStarSystemManager.scheduler", new_callable=PropertyMock
    )
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
