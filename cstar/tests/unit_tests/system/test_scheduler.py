import json
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from cstar.system.scheduler import (
    Queue,
    SlurmQueue,
    PBSQueue,
    SlurmScheduler,
    PBSScheduler,
)

################################################################################


class TestQueue:
    """Unit tests for the Queue and its subclasses (SlurmQueue, PBSQueue).

    These tests cover the initialization and functionality of the base Queue class
    and its specific implementations for Slurm and PBS job schedulers.

    Tests
    -----
    test_queue_initialization
        Verify initialization of a basic Queue object.
    test_queue_initialization_with_query_name
        Ensure query_name is correctly set when explicitly provided.
    test_slurmqueue_inherits_queue
        Confirm SlurmQueue inherits initialization logic from Queue.
    test_slurmqueue_query_queue_property
        Test querying a property from a SlurmQueue using subprocess.
    test_slurmqueue_query_queue_property_error
        Verify RuntimeError is raised when SlurmQueue command fails.
    test_slurmqueue_max_walltime
        Test the max_walltime property of SlurmQueue, ensuring correct system call.
    test_slurmqueue_max_nodes
        Test the max_nodes property of SlurmQueue, ensuring correct system call.
    test_slurmqueue_priority
        Test the priority property of SlurmQueue, verifying correct output.
    test_pbsqueue_initialization
        Test initialization of a PBSQueue with max_walltime.
    test_pbsqueue_query_queue_property
        Verify PBSQueue can parse JSON output from qstat for arbitrary properties.
    test_pbsqueue_max_cpus
        Test the max_cpus property of PBSQueue for retrieving CPU limits.
    test_pbsqueue_max_mem
        Test the max_mem property of PBSQueue for retrieving memory limits.
    test_pbsqueue_priority
        Test the priority property of PBSQueue for retrieving queue priority.

    Fixtures
    --------
    mock_subprocess_run
        Mocks subprocess.run to simulate system commands without actual execution.
    """

    @pytest.fixture
    def mock_subprocess_run(self):
        """Mock subprocess.run for testing system command execution.

        This fixture ensures that subprocess.run calls are intercepted,
        allowing tests to simulate their outputs or errors without running
        actual commands.

        Yields
        ------
        mock_run : unittest.mock.MagicMock
            A mock object for subprocess.run.
        """
        with patch("subprocess.run") as mock_run:
            yield mock_run

    def test_queue_initialization(self):
        """Verify initialization of a basic Queue object."""
        queue = Queue(name="general")
        assert queue.name == "general"
        assert queue.query_name == "general"  # Default to the same name

    def test_queue_initialization_with_query_name(self):
        """Ensure query_name is correctly set when explicitly provided."""
        queue = Queue(name="general", query_name="specific")
        assert queue.name == "general"
        assert queue.query_name == "specific"

    def test_slurmqueue_inherits_queue(self):
        """Confirm SlurmQueue inherits initialization logic from Queue."""
        slurm_queue = SlurmQueue(name="general")
        assert slurm_queue.name == "general"
        assert slurm_queue.query_name == "general"  # Default to name

    def test_slurmqueue_query_queue_property(self, mock_subprocess_run):
        """Test querying a property from a SlurmQueue using subprocess.

        Uses mock_subprocess_run to simulate the output of the system command.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="60:00", stderr=""
        )
        slurm_queue = SlurmQueue(name="general")
        result = slurm_queue.query_queue_property("general", "MaxWall")
        assert result == "60:00"

        mock_subprocess_run.assert_called_once_with(
            "sacctmgr show qos general format=MaxWall --noheader",
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_slurmqueue_query_queue_property_error(self, mock_subprocess_run):
        """Verify RuntimeError is raised when SlurmQueue command fails.

        Simulates a failing command by returning a non-zero exit code.
        """
        mock_subprocess_run.return_value = MagicMock(returncode=1, stderr="Error")
        slurm_queue = SlurmQueue(name="general")

        with pytest.raises(RuntimeError, match="Command failed: Error"):
            slurm_queue.query_queue_property("general", "MaxWall")

    def test_slurmqueue_max_walltime(self, mock_subprocess_run):
        """Test the max_walltime property of SlurmQueue.

        Simulates a successful system command to retrieve the maximum walltime.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="02:00:00", stderr=""
        )
        slurm_queue = SlurmQueue(name="general")
        assert slurm_queue.max_walltime == "02:00:00"

        mock_subprocess_run.assert_called_once_with(
            "sacctmgr show qos general format=MaxWall --noheader",
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_slurmqueue_max_nodes(self, mock_subprocess_run):
        """Test the max_nodes property of SlurmQueue.

        Simulates a successful system command to retrieve the maximum nodes.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="100", stderr=""
        )
        slurm_queue = SlurmQueue(name="general")
        assert slurm_queue.max_nodes == 100

        mock_subprocess_run.assert_called_once_with(
            "sacctmgr show qos general format=MaxNodes --noheader",
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_slurmqueue_priority(self, mock_subprocess_run):
        """Test the priority property of SlurmQueue.

        Simulates a successful system command to retrieve the queue priority.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="1000", stderr=""
        )
        slurm_queue = SlurmQueue(name="general")
        assert slurm_queue.priority == "1000"

        mock_subprocess_run.assert_called_once_with(
            "sacctmgr show qos general format=Priority --noheader",
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_pbsqueue_initialization(self):
        """Test initialization of a PBSQueue with max_walltime."""
        pbs_queue = PBSQueue(name="batch", max_walltime="72:00:00")
        assert pbs_queue.name == "batch"
        assert pbs_queue.query_name == "batch"  # Default to name
        assert pbs_queue.max_walltime == "72:00:00"

    def test_pbsqueue_query_queue_property(self, mock_subprocess_run):
        """Verify PBSQueue can parse JSON output from qstat.

        Uses mock_subprocess_run to simulate qstat JSON output and validates that the
        desired property is correctly extracted.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"Queue": {"batch": {"resources_max.ncpus": "128"}}}),
        )
        pbs_queue = PBSQueue(name="batch", max_walltime="72:00:00")
        result = pbs_queue.query_queue_property("batch", "resources_max.ncpus")
        assert result == "128"

        mock_subprocess_run.assert_called_once_with(
            ["qstat", "-Qf", "-Fjson", "batch"],
            text=True,
            capture_output=True,
            check=True,
        )

    def test_pbsqueue_max_cpus(self, mock_subprocess_run):
        """Test the max_cpus property of PBSQueue.

        Simulates a successful qstat JSON output for the maximum CPUs property.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"Queue": {"batch": {"resources_max.ncpus": "128"}}}),
        )
        pbs_queue = PBSQueue(name="batch", max_walltime="72:00:00")
        assert pbs_queue.max_cpus == 128

        mock_subprocess_run.assert_called_once_with(
            ["qstat", "-Qf", "-Fjson", "batch"],
            text=True,
            capture_output=True,
            check=True,
        )

    def test_pbsqueue_max_mem(self, mock_subprocess_run):
        """Test the max_mem property of PBSQueue.

        Simulates a successful qstat JSON output for the maximum memory property.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"Queue": {"batch": {"resources_max.mem": "512mb"}}}),
        )
        pbs_queue = PBSQueue(name="batch", max_walltime="72:00:00")
        assert pbs_queue.max_mem == "512mb"

        mock_subprocess_run.assert_called_once_with(
            ["qstat", "-Qf", "-Fjson", "batch"],
            text=True,
            capture_output=True,
            check=True,
        )


################################################################################


class TestScheduler:
    """Unit tests for Scheduler class and its subclasses (SlurmScheduler, PBSScheduler).

    Tests
    -----
    test_scheduler_initialization
        Verify initialization of the Scheduler with queues and directives.
    test_scheduler_get_queue
        Ensure that queues can be retrieved by name, and missing queues raise a ValueError.
    test_slurmscheduler_global_max_cpus_per_node_success
        Confirm SlurmScheduler retrieves the maximum CPUs per node successfully.
    test_slurmscheduler_global_max_cpus_per_node_failure
        Validate SlurmScheduler handles subprocess failures when querying CPUs.
    test_slurmscheduler_global_max_mem_per_node_gb_success
        Confirm SlurmScheduler retrieves maximum memory per node in GB successfully.
    test_slurmscheduler_global_max_mem_per_node_gb_failure
        Validate SlurmScheduler handles subprocess failures when querying memory.
    test_pbsscheduler_global_max_cpus_per_node_success
        Confirm PBSScheduler retrieves the maximum CPUs per node successfully.
    test_pbsscheduler_global_max_cpus_per_node_failure
        Validate PBSScheduler handles subprocess failures when querying CPUs.
    test_pbsscheduler_global_max_mem_per_node_gb
        Parameterized test for PBSScheduler.global_max_mem_per_node_gb, covering
        various memory formats (kb, mb, gb) and invalid cases.
    test_pbsscheduler_global_max_mem_per_node_gb_failure
        Validate PBSScheduler handles subprocess failures when querying memory.

    Fixtures
    --------
    mock_subprocess_run
        Mocks subprocess.run to simulate system commands without actual execution.
    """

    @pytest.fixture
    def mock_subprocess_run(self):
        """Mock subprocess.run for testing system command execution.

        This fixture ensures that subprocess.run calls are intercepted,
        allowing tests to simulate their outputs or errors without running
        actual commands.

        Yields
        ------
        mock_run : unittest.mock.MagicMock
            A mock object for subprocess.run.
        """
        with patch("subprocess.run") as mock_run:
            yield mock_run

    def test_scheduler_initialization(self):
        """Verify initialization of the Scheduler with queues and directives."""
        queue1 = Queue(name="general")
        queue2 = Queue(name="batch")
        scheduler = SlurmScheduler(
            queue_flag="q", queues=[queue1, queue2], primary_queue_name="general"
        )

        assert scheduler.queue_flag == "q"
        assert scheduler.queues == [queue1, queue2]
        assert scheduler.primary_queue_name == "general"
        assert scheduler.queue_names == ["general", "batch"]
        assert scheduler.other_scheduler_directives == {}

    def test_scheduler_get_queue(self):
        """Ensure that queues can be retrieved by name, and missing queues raise a
        ValueError."""
        queue1 = Queue(name="general")
        queue2 = Queue(name="batch")
        scheduler = SlurmScheduler(
            queue_flag="q", queues=[queue1, queue2], primary_queue_name="general"
        )

        result = scheduler.get_queue("batch")
        assert result == queue2

        with pytest.raises(ValueError, match="not found in list of queues"):
            scheduler.get_queue("nonexistent")

    def test_slurmscheduler_global_max_cpus_per_node_success(self, mock_subprocess_run):
        """Confirm SlurmScheduler retrieves the maximum CPUs per node successfully.

        Uses mock_subprocess_run to simulate a successful system command output.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="128", stderr=""
        )
        scheduler = SlurmScheduler(
            queue_flag="q", queues=[], primary_queue_name="general"
        )

        result = scheduler.global_max_cpus_per_node
        assert result == 128

        mock_subprocess_run.assert_called_once_with(
            'scontrol show nodes | grep -o "cpu=[0-9]*" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_slurmscheduler_global_max_cpus_per_node_failure(
        self, mock_subprocess_run, capsys
    ):
        """Validate SlurmScheduler handles subprocess failures when querying CPUs.

        Captures printed error messages to ensure the failure is logged correctly.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error querying CPUs"
        )
        scheduler = SlurmScheduler(
            queue_flag="q", queues=[], primary_queue_name="general"
        )

        result = scheduler.global_max_cpus_per_node
        assert result is None

        captured = capsys.readouterr()
        assert (
            "Error querying node property. STDERR: Error querying CPUs" in captured.out
        )

    def test_slurmscheduler_global_max_mem_per_node_gb_success(
        self, mock_subprocess_run
    ):
        """Confirm SlurmScheduler retrieves maximum memory per node in GB successfully.

        Uses mock_subprocess_run to simulate a successful system command output.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="131072", stderr=""
        )
        scheduler = SlurmScheduler(
            queue_flag="q", queues=[], primary_queue_name="general"
        )

        result = scheduler.global_max_mem_per_node_gb
        assert result == 128.0  # 131072 MB -> 128 GB

        mock_subprocess_run.assert_called_once_with(
            'scontrol show nodes | grep -o "RealMemory=[0-9]*" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_slurmscheduler_global_max_mem_per_node_gb_failure(
        self, mock_subprocess_run, capsys
    ):
        """Validate SlurmScheduler handles subprocess failures when querying memory.

        Captures printed error messages to ensure the failure is logged correctly.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error querying memory"
        )
        scheduler = SlurmScheduler(
            queue_flag="q", queues=[], primary_queue_name="general"
        )

        result = scheduler.global_max_mem_per_node_gb
        assert result is None

        captured = capsys.readouterr()
        assert (
            "Error querying node property. STDERR: Error querying memory"
            in captured.out
        )

    def test_pbsscheduler_global_max_cpus_per_node_success(self, mock_subprocess_run):
        """Confirm PBSScheduler retrieves the maximum CPUs per node successfully.

        Uses mock_subprocess_run to simulate a successful system command output.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="128", stderr=""
        )
        scheduler = PBSScheduler(queue_flag="q", queues=[], primary_queue_name="batch")

        result = scheduler.global_max_cpus_per_node
        assert result == 128

        mock_subprocess_run.assert_called_once_with(
            'pbsnodes -a | grep "resources_available.ncpus" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_pbsscheduler_global_max_cpus_per_node_failure(
        self, mock_subprocess_run, capsys
    ):
        """Validate PBSScheduler handles subprocess failures when querying CPUs.

        Captures printed error messages to ensure the failure is logged correctly.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error querying CPUs"
        )
        scheduler = PBSScheduler(queue_flag="q", queues=[], primary_queue_name="batch")

        result = scheduler.global_max_cpus_per_node
        assert result is None

        captured = capsys.readouterr()
        assert (
            "Error querying node property. STDERR: Error querying CPUs" in captured.out
        )

    def test_pbsscheduler_global_max_mem_per_node_gb_failure(
        self, mock_subprocess_run, capsys
    ):
        """Validate PBSScheduler handles subprocess failures when querying memory.

        Captures printed error messages to ensure the failure is logged correctly.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error querying memory"
        )
        scheduler = PBSScheduler(queue_flag="q", queues=[], primary_queue_name="batch")

        result = scheduler.global_max_mem_per_node_gb
        assert result is None

        captured = capsys.readouterr()
        assert (
            "Error querying node property. STDERR: Error querying memory"
            in captured.out
        )

    @pytest.mark.parametrize(
        "stdout,expected",
        [
            ("1048576kb", 1.0),  # Kilobytes to gigabytes
            ("1024mb", 1.0),  # Megabytes to gigabytes
            ("2gb", 2.0),  # Already in gigabytes
            ("1234tb", None),  # Invalid format
            ("", None),  # Empty output
        ],
    )
    def test_pbsscheduler_global_max_mem_per_node_gb(self, stdout, expected):
        """Parameterized test for PBSScheduler.global_max_mem_per_node_gb.

        Tests various memory formats (kb, mb, gb) and ensures correct conversions or
        handling of invalid formats.
        """
        with patch("subprocess.run") as mock_subprocess_run:
            mock_subprocess_run.return_value = MagicMock(
                returncode=0, stdout=stdout, stderr=""
            )
            scheduler = PBSScheduler(
                queue_flag="q", queues=[], primary_queue_name="batch"
            )

            result = scheduler.global_max_mem_per_node_gb
            assert result == expected

            mock_subprocess_run.assert_called_once_with(
                'pbsnodes -a | grep "resources_available.mem" | cut -d== -f2 | sort -nr | head -1',
                shell=True,
                text=True,
                capture_output=True,
            )


class TestStrAndRepr:
    """Unit tests for the __str__ and __repr__ methods of Queue, Scheduler, and their
    respective subclasses."""

    def test_slurmqueue_str(self):
        """Test __str__ for SlurmQueue."""
        queue = SlurmQueue(name="main")
        with (
            patch.object(
                type(queue),
                "max_walltime",
                new_callable=PropertyMock,
                return_value="09:00:00",
            ),
            patch.object(
                type(queue), "max_nodes", new_callable=PropertyMock, return_value=10
            ),
            patch.object(
                type(queue), "priority", new_callable=PropertyMock, return_value=298248
            ),
        ):
            expected = (
                "SlurmQueue:\n"
                "----------\n"
                "name: main\n"
                "max_walltime: 09:00:00\n"
                "max_nodes: 10\n"
                "priority: 298248\n"
            )
            assert str(queue) == expected

    def test_slurmqueue_repr(self):
        """Test __repr__ for SlurmQueue."""
        queue = SlurmQueue(name="main")
        expected = "SlurmQueue(name='main', query_name='main')"
        assert repr(queue) == expected

    def test_pbsqueue_str(self):
        """Test __str__ for PBSQueue."""
        queue = PBSQueue(name="batch", max_walltime="72:00:00")
        with (
            patch.object(
                type(queue), "max_cpus", new_callable=PropertyMock, return_value=128
            ),
            patch.object(
                type(queue), "max_mem", new_callable=PropertyMock, return_value="512mb"
            ),
        ):
            expected = (
                "PBSQueue:\n" "--------\n" "name: batch\n" "max_walltime: 72:00:00\n"
            )
            assert str(queue) == expected

    def test_pbsqueue_repr(self):
        """Test __repr__ for PBSQueue."""
        queue = PBSQueue(name="batch", max_walltime="72:00:00")
        expected = "PBSQueue(name='batch', query_name='batch', max_walltime='72:00:00')"
        assert repr(queue) == expected

    def test_slurmscheduler_str(self):
        """Test __str__ for SlurmScheduler."""
        queues = [SlurmQueue(name="main"), SlurmQueue(name="backup")]
        scheduler = SlurmScheduler(
            queue_flag="q",
            queues=queues,
            primary_queue_name="main",
            other_scheduler_directives={"constraint": "high-memory"},
        )

        with (
            patch.object(
                type(scheduler),
                "global_max_cpus_per_node",
                new_callable=PropertyMock,
                return_value=128,
            ),
            patch.object(
                type(scheduler),
                "global_max_mem_per_node_gb",
                new_callable=PropertyMock,
                return_value=256,
            ),
        ):
            expected = (
                "SlurmScheduler\n"
                "--------------\n"
                "primary_queue: main\n"
                "queues:\nmain\nbackup\n"
                "other_scheduler_directives: {'constraint': 'high-memory'}\n"
                "global max cpus per node: 128\n"
                "global max mem per node: 256GB"
            )
            assert str(scheduler) == expected

    def test_slurmscheduler_repr(self):
        """Test __repr__ for SlurmScheduler."""
        queues = [SlurmQueue(name="main"), SlurmQueue(name="backup")]
        scheduler = SlurmScheduler(
            queue_flag="q",
            queues=queues,
            primary_queue_name="main",
            other_scheduler_directives={"constraint": "high-memory"},
        )
        expected = (
            "SlurmScheduler(queue_flag='q', queues=[SlurmQueue(name='main', query_name='main'), "
            "SlurmQueue(name='backup', query_name='backup')], primary_queue_name='main', "
            "other_scheduler_directives={'constraint': 'high-memory'})"
        )
        assert repr(scheduler) == expected

    def test_pbsscheduler_str(self):
        """Test __str__ for PBSScheduler."""
        queues = [PBSQueue(name="batch", max_walltime="72:00:00")]
        scheduler = PBSScheduler(
            queue_flag="q",
            queues=queues,
            primary_queue_name="batch",
            other_scheduler_directives={"feature": "gpu"},
        )

        with (
            patch.object(
                type(scheduler),
                "global_max_cpus_per_node",
                new_callable=PropertyMock,
                return_value=64,
            ),
            patch.object(
                type(scheduler),
                "global_max_mem_per_node_gb",
                new_callable=PropertyMock,
                return_value=128,
            ),
        ):
            expected = (
                "PBSScheduler\n"
                "------------\n"
                "primary_queue: batch\n"
                "queues:\nbatch\n"
                "other_scheduler_directives: {'feature': 'gpu'}\n"
                "global max cpus per node: 64\n"
                "global max mem per node: 128GB"
            )
            assert str(scheduler) == expected

    def test_pbsscheduler_repr(self):
        """Test __repr__ for PBSScheduler."""
        queues = [PBSQueue(name="batch", max_walltime="72:00:00")]
        scheduler = PBSScheduler(
            queue_flag="q",
            queues=queues,
            primary_queue_name="batch",
            other_scheduler_directives={"feature": "gpu"},
        )
        expected = (
            "PBSScheduler(queue_flag='q', queues=[PBSQueue(name='batch', query_name='batch', "
            "max_walltime='72:00:00')], primary_queue_name='batch', "
            "other_scheduler_directives={'feature': 'gpu'})"
        )
        assert repr(scheduler) == expected
