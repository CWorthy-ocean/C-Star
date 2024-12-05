import time
import pytest
import threading
from unittest.mock import MagicMock, patch, PropertyMock
from cstar.base.scheduler import Scheduler
from cstar.base.scheduler_job import SchedulerJob


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
            other_scheduler_directives={"mock_directive": "mock_value"},
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
