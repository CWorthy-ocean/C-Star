import logging
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from cstar.execution.scheduler_job import (
    PBSJob,
    SchedulerJob,
    SlurmJob,
    create_scheduler_job,
)
from cstar.system.scheduler import (
    PBSQueue,
    PBSScheduler,
    Scheduler,
    SlurmQOS,
    SlurmScheduler,
)


class MockSchedulerJob(SchedulerJob):
    """Mock implementation of the `SchedulerJob` ABC for testing purposes.

    This class provides minimal functionality required for testing, including
    mocked implementations of the `status` and `submit` methods.

    Attributes
    ----------
    status : str
        A mocked status string, always returning "mock_status".
    submit : str
        A mocked method returning "mock_submit".
    """

    @property
    def status(self):
        return "mock_status"

    def submit(self):
        return "mock_submit"

    def script(self):
        pass


class MockScheduler(Scheduler):
    """Mock implementation of the `Scheduler` ABC for testing purposes.

    This class simulates a scheduler with predefined attributes and behaviors,
    including mocked queues, maximum CPUs per node, and maximum memory per node.

    Attributes
    ----------
    queues : List[MagicMock]
        A list containing a mocked queue object with a max_walltime property of 02:00:00
    primary_queue_name : str
        The name of the primary queue, set to "default_queue", the mocked queue object
    other_scheduler_directives : dict
        Mocked scheduler directives, including example mock values.
    global_max_cpus_per_node : int
        The maximum number of CPUs per node, mocked as 64.
    global_max_mem_per_node_gb : int
        The maximum memory per node in GB, mocked as 128.
    """

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
    """Tests for the base behavior of the `SchedulerJob` class using a mocked
    implementation.

    Tests
    -----
    test_initialization_defaults
        Validates that default values are correctly applied during job initialization.
    test_init_no_walltime_and_no_queue_max_walltime
        Ensures that a `ValueError` is raised when no walltime is provided, and the queue's
        max walltime is also unavailable.
    test_init_walltime_provided_but_no_queue_max_walltime
        Verifies that a user-provided walltime is used when the queue's max walltime is unavailable,
        and a warning is issued.
    test_init_no_walltime_but_queue_max_walltime_provided
        Ensures that the queue's max walltime is applied if no walltime is specified by the user.
    test_init_walltime_exceeds_max_walltime
        Confirms that a `ValueError` is raised if the provided walltime exceeds the queue's limit.
    test_init_without_nodes_but_with_cpus_per_node
        Tests automatic node calculation when nodes are not specified but `cpus_per_node` is provided.
    test_init_with_nodes_but_without_cpus_per_node
        Verifies that `cpus_per_node` is calculated automatically when nodes are provided.
    test_init_without_nodes_or_cpus_per_node
        Ensures that both nodes and `cpus_per_node` are calculated when neither is provided, using
        system defaults.
    test_init_cpus_without_distribution_requirement
        Confirms that task distribution is skipped if the scheduler does not require it.
    """

    def setup_method(self, method):
        """Sets up common job parameters for tests in `TestSchedulerJobBase`.

        This method initializes a dictionary of parameters that are shared across
        multiple test cases to initialize a MockSchedulerJob instance.

        Parameters
        ----------
        method : Callable
            The test method being executed. This parameter is part of the pytest
            `setup_method` signature but is not used in this setup.
        """
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
        """Validates that default values are correctly applied during job
        initialization.

        This test checks that when certain parameters (e.g., walltime) are omitted,
        the job uses default values, such as the queue's maximum walltime.

        Notes
        -----
        - This test suppresses warnings about unspecified walltime using
          `@pytest.mark.filterwarnings`.
        """
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
        """Ensures that a `ValueError` is raised when no walltime is provided, and the
        queue's maximum walltime is also unavailable.

        This test verifies that job initialization fails when both the walltime parameter
        and the queue's maximum walltime are undefined.

        Mocks
        -----
        MockScheduler.get_queue
            Returns a mocked queue object with `max_walltime=None`.

        Asserts
        -------
        - That a `ValueError` is raised with an appropriate error message indicating
          that no walltime could be determined.
        """
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

    def test_init_walltime_provided_but_no_queue_max_walltime(self, caplog):
        """Verifies that a user-provided walltime is applied when the queue's max
        walltime is unavailable, and a warning is logged that the user walltime cannot
        be checked.

        Mocks
        -----
        MockScheduler.get_queue
            Returns a mocked queue object with `max_walltime=None`.

        Fixtures
        --------
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log messages

        Asserts
        -------
        - That a warning is logged about the missing queue walltime.
        - That the job's walltime matches the user-provided value ("01:00:00").
        """
        with patch.object(
            MockScheduler, "get_queue", return_value=MagicMock(max_walltime=None)
        ):
            job = MockSchedulerJob(**self.common_job_params)
            caplog.set_level(logging.INFO, job.log.name)
            assert job.walltime == "01:00:00"
            assert (
                "Unable to determine the maximum allowed walltime for chosen queue"
                in caplog.text
            )

    def test_init_no_walltime_but_queue_max_walltime_provided(self, caplog):
        """Ensures that the queue's max walltime is applied when no walltime is provided
        by the user.

        This test verifies that if the user does not specify a walltime, the job uses
        the queue's maximum walltime and issues a warning about the unspecified walltime.

        Mocks & Fixtures
        ----------------
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log messages
        MockScheduler.get_queue
            Returns a mocked queue object with `max_walltime="02:00:00"`.

        Asserts
        -------
        - That a warning is logged about the unspecified walltime.
        - That the job's walltime matches the queue's maximum walltime ("02:00:00").
        """
        with patch.object(
            MockScheduler, "get_queue", return_value=MagicMock(max_walltime="02:00:00")
        ):
            params = {
                key: value
                for key, value in self.common_job_params.items()
                if key != "walltime"
            }
            job = MockSchedulerJob(**params)
            caplog.set_level(logging.INFO, logger=job.log.name)
            assert job.walltime == "02:00:00"
            assert "Walltime parameter unspecified" in caplog.text

    def test_init_walltime_exceeds_max_walltime(self):
        """Ensures that a `ValueError` is raised if the user-provided walltime exceeds
        the queue's maximum allowed walltime.

        This test checks that job initialization fails when the walltime exceeds
        the queue's `max_walltime`.

        Asserts
        -------
        - That a `ValueError` is raised with an appropriate error message indicating
          the walltime exceeds the allowed maximum.
        """
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
        """Tests automatic node calculation when nodes are not specified but
        `cpus_per_node` is provided.

        This test ensures that the number of nodes is correctly calculated as the
        ceiling of `cpus / cpus_per_node`.

        Asserts
        -------
        - That the calculated number of nodes matches the expected value.
        - That the `cpus_per_node` is assigned the provided value (16).
        """
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
        """Verifies automatic `cpus_per_node` calculation when nodes are provided, but
        `cpus_per_node` is not specified.

        This test ensures that `cpus_per_node` is calculated as the total `cpus`
        divided by the number of nodes.

        Asserts
        -------
        - That the number of nodes matches the provided value (2).
        - That the `cpus_per_node` is correctly calculated as `cpus / nodes` (2).
        """
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

    def test_init_without_nodes_or_cpus_per_node(self, caplog):
        """Ensures that both `nodes` and `cpus_per_node` are automatically calculated
        when neither is provided.

        This test verifies that the job uses the system's maximum CPUs per node to
        calculate the required task distribution.

        Mocks
        -----
        MockScheduler.get_queue
            Returns a mocked queue object with `max_walltime` set to a valid value.
        MockScheduler.global_max_cpus_per_node
            Provides the system's maximum CPUs per node (64).

        Fixtures
        --------
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log messages

        Asserts
        -------
        - That the calculated number of nodes matches the expected value (2).
        - That the calculated `cpus_per_node` matches the expected value (64).
        - An expected info message is logged
        """
        params = self.common_job_params.copy()
        params.update(
            {
                "nodes": None,
                "cpus_per_node": None,  # Both nodes and cpus_per_node are missing
                "cpus": 128,
            }
        )
        job = MockSchedulerJob(**params)
        caplog.set_level(logging.INFO, logger=job.log.name)
        # Check the calculated values from _calculate_node_distribution
        assert job.nodes == 2
        assert job.cpus_per_node == 64  # cpus=128, max_cpus_per_node=64
        assert (
            "Attempting to create scheduler job without 'nodes' and 'cpus_per_node'"
            in caplog.text
        )

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
        """Confirms that task distribution is skipped when the scheduler does not
        require explicit task distribution.

        This test uses a parameterization to evaluate different combinations of
        `nodes` and `cpus_per_node`, ensuring that these attributes are assigned
        directly without calculations when `requires_task_distribution` is `False`.

        For example, if `requires_task_distribution` is True and the user provides
        cpus=16 and cpus_per_node=4, C-Star will set `nodes=4` automatically. If
        `requires_task_distribution` is False, nodes will be set to `None`, and only
        `cpus` will be submitted to the Scheduler.

        Mocks
        -----
        MockScheduler.requires_task_distribution
            Set to `False` to simulate a scheduler that does not require task distribution.

        Asserts
        -------
        - That the `nodes` attribute matches the provided or expected value.
        - That the `cpus_per_node` attribute matches the provided or expected value.
        """
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


##
@pytest.mark.filterwarnings(
    r"ignore:WARNING.*Attempting to create scheduler job.*:UserWarning"
)
class TestCalculateNodeDistribution:
    """Tests for the method `_calculate_node_distribution`.

    The method calculates how to distribute a parallel job amongst a system's
    nodes when the user does not explicitly provide a node x cpu
    breakdown.

    These tests ensure that the correct number of nodes and CPUs per node are calculated
    based on the job's requirements and the system's maximum CPUs per node.

    Tests
    -----
    test_exact_division
        Validates that when the total required cores are an exact multiple of the cores
        per node, the calculation produces the correct number of nodes and cores per node.
    test_partial_division
        Ensures that when the total required cores are not an exact multiple of the cores
        per node, the calculation rounds up to the nearest node and adjusts the cores per
        node accordingly.
    test_single_node
        Confirms that a single node is used when the total required cores are less than or
        equal to the cores available on one node.
    test_minimum_cores
        Tests the edge case where only one core is required, ensuring that one node with
        one core is requested.
    """

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
        `tot_cores_per_node`.
        """
        n_cores_required = 300
        tot_cores_per_node = 64

        result = self.mock_job._calculate_node_distribution(
            n_cores_required, tot_cores_per_node
        )
        assert result == (5, 60), f"Expected (5, 60), got {result}"

    def test_single_node(self):
        """Test when `n_cores_required` is less than or equal to
        `tot_cores_per_node`.
        """
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
    """Tests for the `create_scheduler_job` function.

    These tests ensure that the `create_scheduler_job` function correctly creates
    instances of `SchedulerJob` subclasses (`SlurmJob` or `PBSJob`) based on the
    scheduler type and validates the provided arguments.

    Tests
    -----
    test_create_slurm_job
        Ensures that a `SlurmJob` is created when a SLURM scheduler is active, with
        all provided attributes correctly assigned.
    test_create_pbs_job
        Ensures that a `PBSJob` is created when a PBS scheduler is active, with
        all provided attributes correctly assigned.
    test_unsupported_scheduler
        Confirms that a `TypeError` is raised when the scheduler type is unsupported.
    test_missing_arguments
        Ensures that a `TypeError` is raised when required arguments are missing.
    """

    @patch(
        "cstar.system.manager.CStarSystemManager.scheduler", new_callable=PropertyMock
    )
    @patch("cstar.system.scheduler.SlurmQOS.max_walltime", new_callable=PropertyMock)
    def test_create_slurm_job(self, mock_max_walltime, mock_scheduler):
        """Ensures that a `SlurmJob` is created when a SLURM scheduler is active.

        This test verifies that the `create_scheduler_job` function correctly creates a
        `SlurmJob` instance with the provided attributes when the active scheduler is SLURM.

        Mocks
        -----
        SlurmQOS.max_walltime
            Mocked to return a maximum walltime of "02:00:00" for the queue.
        CStarSystemManager.scheduler
            Mocked to simulate a SLURM scheduler in use by the system.

        Asserts
        -------
        - That the created job is an instance of `SlurmJob`.
        - That the job's attributes (e.g., commands, CPUs, nodes, walltime) are assigned
          correctly based on the provided parameters.
        """
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
        """Ensures that a `PBSJob` is created when a PBS scheduler is active.

        This test verifies that the `create_scheduler_job` function correctly creates a
        `PBSJob` instance with the provided attributes when the active scheduler is PBS.

        Mocks
        -----
        PBSScheduler.global_max_cpus_per_node
            Mocked to return a maximum of 128 CPUs per node for the system.
        CStarSystemManager.scheduler
            Mocked to simulate a PBS scheduler on the current system.

        Asserts
        -------
        - That the created job is an instance of `PBSJob`.
        - That the job's attributes (e.g., commands, CPUs, nodes, walltime) are assigned
          correctly based on the provided parameters.
        """
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
        """Confirms that a `TypeError` is raised when calling create_scheduler_job
        without a scheduler.

        This test verifies that the `create_scheduler_job` function raises an appropriate
        error if the active scheduler is not recognized or is set to `None`.

        Mocks
        -----
        CStarSystemManager.scheduler
            Mocked to return `None`, simulating the absence of a valid scheduler.

        Asserts
        -------
        - That a `TypeError` is raised with an error message indicating the unsupported
          scheduler type.
        """
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
        """Ensures a `TypeError` is raised when required arguments are missing from
        create_scheduler_job.

        This test verifies that the `create_scheduler_job` function raises an error
        when called without the mandatory parameters.

        Asserts
        -------
        - That a `TypeError` is raised with a message indicating the missing
          required arguments.
        """
        with pytest.raises(TypeError, match="missing .* required positional argument"):
            create_scheduler_job(
                cpus=4,
                account_key="test_account",
                walltime="01:00:00",
            )
