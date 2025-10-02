import logging
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from cstar.system.scheduler import (
    PBSQueue,
    PBSScheduler,
    Queue,
    SlurmPartition,
    SlurmQOS,
    SlurmScheduler,
)

################################################################################


class MockQueue(Queue):
    """Mock subclass of the Queue ABC used for testing."""

    def _default_max_walltime_method(self):
        """Mock implementation of the max_walltime abstractmethod for Queue."""
        pass


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
    test_slurmqos_max_walltime
        Test the max_walltime property of SlurmQOS, ensuring correct system call.
    test_slurmpartition_max_walltime
        Test the max_walltime property of SlurmQOS, ensuring correct system call.
    test_slurmqueue_max_nodes
        Test the max_nodes property of SlurmQueue, ensuring correct system call.
    test_pbsqueue_initialization
        Test initialization of a PBSQueue with max_walltime.

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
        queue = MockQueue(name="general")
        assert queue.name == "general"
        assert queue.query_name == "general"  # Default to the same name

    def test_queue_initialization_with_query_name(self):
        """Ensure query_name is correctly set when explicitly provided."""
        queue = MockQueue(name="general", query_name="specific")
        assert queue.name == "general"
        assert queue.query_name == "specific"

    def test_slurmqos_max_walltime(self, mock_subprocess_run):
        """Test the max_walltime property of SlurmQOS.

        Simulates a successful system command to retrieve the maximum walltime.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="02:00:00", stderr=""
        )
        slurm_qos = SlurmQOS(name="general")
        assert slurm_qos.max_walltime == "02:00:00"

        mock_subprocess_run.assert_called_once_with(
            "sacctmgr show qos general format=MaxWall --noheader",
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_slurmpartition_max_walltime(self, mock_subprocess_run):
        """Test the max_walltime property of SlurmPartition.

        Simulates a successful system command to retrieve the maximum walltime.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="2-01:00:00", stderr=""
        )
        slurm_ptn = SlurmPartition(name="general")
        assert slurm_ptn.max_walltime == "49:00:00"

        mock_subprocess_run.assert_called_once_with(
            "sinfo -h -o '%l' -p general",
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


################################################################################


class TestScheduler:
    """Unit tests for Scheduler class and its subclasses (SlurmScheduler, PBSScheduler).

    Tests
    -----
    test_scheduler_initialization
        Verify initialization of a Scheduler instance from a list of queues
    test_scheduler_get_queue
        Ensure that queues can be retrieved by name, and missing queues raise a ValueError.
    test_slurmscheduler_global_max_cpus_per_node_success
        Confirm SlurmScheduler queries and sets the maximum CPUs per node successfully.
    test_slurmscheduler_global_max_cpus_per_node_failure
        Validate SlurmScheduler handles subprocess failures when querying CPUs.
    test_slurmscheduler_global_max_mem_per_node_gb_success
        Confirm SlurmScheduler queries and sets maximum memory per node in GB successfully.
    test_slurmscheduler_global_max_mem_per_node_gb_failure
        Validate SlurmScheduler handles subprocess failures when querying memory.
    test_pbsscheduler_global_max_cpus_per_node_success
        Confirm PBSScheduler rqueries and sets the maximum CPUs per node successfully.
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
        """Verify initialization of a Scheduler instance from a list of queues."""
        queue1 = MockQueue(name="general")
        queue2 = MockQueue(name="batch")
        scheduler = SlurmScheduler(
            queues=[queue1, queue2], primary_queue_name="general"
        )

        assert scheduler.queues == [queue1, queue2]
        assert scheduler.primary_queue_name == "general"
        assert scheduler.queue_names == ["general", "batch"]
        assert scheduler.other_scheduler_directives == {}

    def test_scheduler_get_queue(self):
        """Ensure that queues can be retrieved by name, and missing queues raise a
        ValueError.
        """
        queue1 = MockQueue(name="general")
        queue2 = MockQueue(name="batch")
        scheduler = SlurmScheduler(
            queues=[queue1, queue2], primary_queue_name="general"
        )

        result = scheduler.get_queue("batch")
        assert result == queue2

        with pytest.raises(ValueError, match="not found in list of queues"):
            scheduler.get_queue("nonexistent")

    def test_slurmscheduler_global_max_cpus_per_node_success(self, mock_subprocess_run):
        """Confirm SlurmScheduler queries and sets the maximum CPUs per node
        successfully.

        Uses mock_subprocess_run to simulate a successful system command output.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="128", stderr=""
        )
        scheduler = SlurmScheduler(queues=[], primary_queue_name="general")

        result = scheduler.global_max_cpus_per_node
        assert result == 128

        mock_subprocess_run.assert_called_once_with(
            'scontrol show nodes | grep -o "cpu=[0-9]*" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_slurmscheduler_global_max_cpus_per_node_failure(
        self, mock_subprocess_run, caplog: pytest.CaptureFixture
    ):
        """Validate SlurmScheduler handles subprocess failures when querying CPUs.

        Captures printed error messages to ensure the failure is logged correctly.

        Mocks and Fixtures
        ------------------
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log messages

        Asserts
        -------
        - Appropriate error messages are logged
        """
        caplog.set_level(logging.INFO, logger="cstar.utils.log")

        mock_subprocess_run.return_value = MagicMock(
            returncode=2, stdout="", stderr="Error querying CPUs"
        )
        scheduler = SlurmScheduler(queues=[], primary_queue_name="general")

        result = scheduler.global_max_cpus_per_node
        assert result is None

        captured = caplog.text
        assert "Error querying node property." in captured
        assert "STDERR:\nError querying CPUs" in captured

    def test_slurmscheduler_global_max_mem_per_node_gb_success(
        self, mock_subprocess_run
    ):
        """Confirm SlurmScheduler queries and sets maximum memory per node in GB
        successfully.

        Uses mock_subprocess_run to simulate a successful system command output.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="131072", stderr=""
        )
        scheduler = SlurmScheduler(queues=[], primary_queue_name="general")

        result = scheduler.global_max_mem_per_node_gb
        assert result == 128.0  # 131072 MB -> 128 GB

        mock_subprocess_run.assert_called_once_with(
            'scontrol show nodes | grep -o "RealMemory=[0-9]*" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_slurmscheduler_global_max_mem_per_node_gb_failure(
        self, mock_subprocess_run, caplog: pytest.CaptureFixture
    ):
        """Validate SlurmScheduler handles subprocess failures when querying memory.

        Captures printed error messages to ensure the failure is logged correctly.

        Mocks and Fixtures
        ------------------
        caplog (pytest.LogCaptureFixture)
            captures log messages

        Asserts
        -------
        - Appropriate error messages are logged
        """
        caplog.set_level(logging.DEBUG, logger="cstar.base.utils.log")

        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error querying memory"
        )
        scheduler = SlurmScheduler(queues=[], primary_queue_name="general")

        result = scheduler.global_max_mem_per_node_gb
        assert result is None

        captured = caplog.text
        assert "Error querying node property." in captured
        assert "STDERR:\nError querying memory" in captured

    def test_pbsscheduler_global_max_cpus_per_node_success(self, mock_subprocess_run):
        """Confirm PBSScheduler queries and sets the maximum CPUs per node successfully.

        Uses mock_subprocess_run to simulate a successful system command output.
        """
        mock_subprocess_run.return_value = MagicMock(
            returncode=0, stdout="128", stderr=""
        )
        scheduler = PBSScheduler(queues=[], primary_queue_name="batch")

        result = scheduler.global_max_cpus_per_node
        assert result == 128

        mock_subprocess_run.assert_called_once_with(
            'pbsnodes -a | grep "resources_available.ncpus" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )

    def test_pbsscheduler_global_max_cpus_per_node_failure(
        self, mock_subprocess_run, caplog: pytest.CaptureFixture
    ):
        """Validate PBSScheduler handles subprocess failures when querying CPUs.

        Captures printed error messages to ensure the failure is logged correctly.

        Mocks and Fixtures
        ------------------
        caplog (pytest.LogCaptureFixture)
            captures log messages
        mock_subprocess_run (unittest.mock.MagicMock)
            Mocks the subprocess.run method

        Asserts
        -------
        - An appropriate error message is logged
        """
        caplog.set_level(logging.DEBUG, logger="cstar.base.utils.log")

        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error querying CPUs"
        )
        scheduler = PBSScheduler(queues=[], primary_queue_name="batch")

        result = scheduler.global_max_cpus_per_node
        assert result is None

        captured = caplog.text
        assert "Error querying node property." in captured
        assert "STDERR:\nError querying CPUs" in captured

    def test_pbsscheduler_global_max_mem_per_node_gb_failure(
        self, mock_subprocess_run, caplog: pytest.CaptureFixture
    ):
        """Validate PBSScheduler handles subprocess failures when querying memory.

        Captures printed error messages to ensure the failure is logged correctly.

        Mocks and Fixtures
        ------------------
        caplog (pytest.LogCaptureFixture)
            captures log messages
        mock_subprocess_run (unittest.mock.MagicMock)
            Mocks the subprocess.run method

        Asserts
        -------
        - An appropriate error message is logged
        """
        caplog.set_level(logging.DEBUG, logger="cstar.base.utils.log")

        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error querying memory"
        )
        scheduler = PBSScheduler(queues=[], primary_queue_name="batch")

        result = scheduler.global_max_mem_per_node_gb
        assert result is None

        captured = caplog.text
        assert "Error querying node property." in captured
        assert "STDERR:\nError querying memory" in captured

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
            scheduler = PBSScheduler(queues=[], primary_queue_name="batch")

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
    respective subclasses.
    """

    def test_slurmqos_str(self):
        """Test __str__ for SlurmQueue."""
        queue = SlurmQOS(name="main")
        with (
            patch.object(
                type(queue),
                "max_walltime",
                new_callable=PropertyMock,
                return_value="09:00:00",
            ),
        ):
            expected = "SlurmQOS:\n--------\nname: main\nmax_walltime: 09:00:00\n"
            assert str(queue) == expected

    def test_slurmqos_repr(self):
        """Test __repr__ for SlurmQOS."""
        queue = SlurmQOS(name="main")
        expected = "SlurmQOS(name='main', query_name='main')"
        assert repr(queue) == expected

    def test_slurmpartition_str(self):
        """Test __str__ for SlurmPartition."""
        queue = SlurmPartition(name="main")
        with (
            patch.object(
                type(queue),
                "max_walltime",
                new_callable=PropertyMock,
                return_value="09:00:00",
            ),
        ):
            expected = (
                "SlurmPartition:\n--------------\nname: main\nmax_walltime: 09:00:00\n"
            )
            assert str(queue) == expected

    def test_slurmpartition_repr(self):
        """Test __repr__ for SlurmPartition."""
        queue = SlurmPartition(name="main")
        expected = "SlurmPartition(name='main', query_name='main')"
        assert repr(queue) == expected

    def test_pbsqueue_str(self):
        """Test __str__ for PBSQueue."""
        queue = PBSQueue(name="batch", max_walltime="72:00:00")
        expected = "PBSQueue:\n--------\nname: batch\nmax_walltime: 72:00:00\n"
        assert str(queue) == expected

    def test_pbsqueue_repr(self):
        """Test __repr__ for PBSQueue."""
        queue = PBSQueue(name="batch", max_walltime="72:00:00")
        expected = "PBSQueue(name='batch', query_name='batch', max_walltime='72:00:00')"
        assert repr(queue) == expected

    def test_slurmscheduler_str(self):
        """Test __str__ for SlurmScheduler."""
        queues = [SlurmQOS(name="main"), SlurmQOS(name="backup")]
        scheduler = SlurmScheduler(
            queues=queues,
            primary_queue_name="main",
            other_scheduler_directives={"constraint": "high-memory"},
            documentation="https://mockscheduler.readthemocks.io",
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
                "queues:\n- main\n- backup\n"
                "other_scheduler_directives: {'constraint': 'high-memory'}\n"
                "global max cpus per node: 128\n"
                "global max mem per node: 256GB\n"
                "documentation: https://mockscheduler.readthemocks.io"
            )
            assert str(scheduler) == expected

    def test_slurmscheduler_repr(self):
        """Test __repr__ for SlurmScheduler."""
        queues = [SlurmQOS(name="main"), SlurmQOS(name="backup")]
        scheduler = SlurmScheduler(
            queues=queues,
            primary_queue_name="main",
            other_scheduler_directives={"constraint": "high-memory"},
            documentation="https://mockscheduler.readthemocks.io",
        )
        expected = (
            "SlurmScheduler(queues=[SlurmQOS(name='main', query_name='main'), "
            "SlurmQOS(name='backup', query_name='backup')], primary_queue_name='main', "
            "other_scheduler_directives={'constraint': 'high-memory'}, "
            "documentation='https://mockscheduler.readthemocks.io')"
        )
        assert repr(scheduler) == expected

    def test_pbsscheduler_str(self):
        """Test __str__ for PBSScheduler."""
        queues = [PBSQueue(name="batch", max_walltime="72:00:00")]
        scheduler = PBSScheduler(
            queues=queues,
            primary_queue_name="batch",
            other_scheduler_directives={"feature": "gpu"},
            documentation="https://mockscheduler.readthemocks.io",
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
                "queues:\n- batch\n"
                "other_scheduler_directives: {'feature': 'gpu'}\n"
                "global max cpus per node: 64\n"
                "global max mem per node: 128GB\n"
                "documentation: https://mockscheduler.readthemocks.io"
            )
            assert str(scheduler) == expected

    def test_pbsscheduler_repr(self):
        """Test __repr__ for PBSScheduler."""
        queues = [PBSQueue(name="batch", max_walltime="72:00:00")]
        scheduler = PBSScheduler(
            queues=queues,
            primary_queue_name="batch",
            other_scheduler_directives={"feature": "gpu"},
            documentation="https://mockscheduler.readthemocks.io",
        )
        expected = (
            "PBSScheduler(queues=[PBSQueue(name='batch', query_name='batch', "
            "max_walltime='72:00:00')], primary_queue_name='batch', "
            "other_scheduler_directives={'feature': 'gpu'}, "
            "documentation='https://mockscheduler.readthemocks.io')"
        )
        assert repr(scheduler) == expected
