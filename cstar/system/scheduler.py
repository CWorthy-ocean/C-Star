from abc import ABC, abstractmethod

from cstar.base.log import LoggingMixin
from cstar.base.utils import _run_cmd

################################################################################


class Queue(ABC):
    """Abstract base class for representing a generic scheduler queue.

    Attributes
    ----------
    name : str
        The name of the queue used to submit jobs, e.g. `regular`
    query_name : str, optional
        The name of the queue used for system accounting, e.g. `regular_0`.
        Defaults to the value of `name` (as the two are usually the same).
    """

    def __init__(self, name: str, query_name: str | None = None):
        """Initialize a Queue instance.

        Parameters
        ----------
        name : str
            The name of the queue.
        query_name : str, optional
            An alternative name used for querying the queue. If not provided, it defaults
            to the value of `name`.
        """
        self.name = name
        self.query_name = query_name if query_name is not None else name

    def __repr__(self) -> str:
        """Return a string represention of this queue instance."""
        return f"{self.__class__.__name__}(name={self.name!r}, query_name={self.query_name!r})"

    @property
    @abstractmethod
    def max_walltime(self):
        pass


class SlurmQueue(Queue, ABC):
    """Abstract base class for SLURM queues.

    This class represents a SLURM queue and defines the structure for retrieving
    and managing SLURM-specific queue properties, such as maximum walltime.

    This is the base class for the SlurmQOS and SlurmPartition subclasses. QoS and
    partition as queue-related flags serve similar purposes in resource allocation,
    but have different methods for querying properties.

    Attributes
    ----------
    name : str
        The name of the SLURM queue.
    query_name : str
        The name of the queue used for system accounting, e.g. `regular_0`.
        Defaults to the value of `name` (as the two are usually the same).
    max_walltime : str
        The maximum walltime allowed for jobs in this queue, formatted as "HH:MM:SS".
    """

    def __str__(self) -> str:
        """String represention of this SlurmQueue instance."""
        return (
            f"{self.__class__.__name__}:\n"
            f"{'-' * len(self.__class__.__name__)}\n"
            f"name: {self.name}\n"
            f"max_walltime: {self.max_walltime}\n"
        )

    def _parse_walltime(self, walltime_str) -> str:
        """Parse and format a SLURM walltime string into the format "HH:MM:SS".

        Parameters
        ----------
        walltime_str : str
            The walltime string to parse. This can be in one of the following formats:
            - "D-HH:MM:SS" (days, hours, minutes, seconds)
            - "HH:MM:SS" (hours, minutes, seconds)
            - "MM:SS" (minutes, seconds)

        Returns
        -------
        str
            The formatted walltime string in the "HH:MM:SS" format.
        """
        if walltime_str.count("-") == 1:  # D-HH:MM:SS
            mw_d = int(walltime_str.split("-")[0])
            mw_hms = walltime_str.split("-")[1]
        else:
            mw_d = 0
            mw_hms = walltime_str
        if mw_hms.count(":") == 1:  # MM:SS
            mw_h = 0
            mw_m, mw_s = map(int, mw_hms.split(":"))
        elif mw_hms.count(":") == 2:  # HH:MM:SS
            mw_h, mw_m, mw_s = map(int, mw_hms.split(":"))
        return f"{mw_d * 24 + mw_h:02}:{mw_m:02}:{mw_s:02}"

    @property
    @abstractmethod
    def max_walltime(self) -> str | None:
        pass


class SlurmQOS(SlurmQueue):
    """Represents a SLURM Quality of Service (QOS) queue.

    This class provides functionality to retrieve and manage SLURM QOS properties,
    such as the maximum walltime for jobs associated with a QOS.

    Attributes
    ----------
    name : str
        The name of the SLURM QOS queue.
    query_name : str
        The name of the queue used for system accounting, e.g. `regular_0`.
        Defaults to the value of `name` (as the two are usually the same).
    max_walltime : str
        The maximum walltime allowed for jobs in this QOS, formatted as "HH:MM:SS".
    """

    @property
    def max_walltime(self) -> str | None:
        """Retrieve the maximum walltime for the SLURM QOS.

        Queries the SLURM accounting manager (`sacctmgr`) to fetch the maximum walltime
        associated with this QOS. The walltime is returned in the format "HH:MM:SS".

        Returns
        -------
        str or None
            The maximum walltime in the format "HH:MM:SS", or `None` if no walltime is set.

        Raises
        ------
        RuntimeError
            If the command to query the SLURM accounting manager (`sacctmgr`) fails.
        """
        sp_cmd = f"sacctmgr show qos {self.name} format=MaxWall --noheader"
        mw = _run_cmd(sp_cmd)
        return self._parse_walltime(mw) if mw else None


class SlurmPartition(SlurmQueue):
    """Represents a SLURM partition queue.

    This class provides functionality to retrieve and manage SLURM partition properties,
    such as the maximum walltime for jobs associated with a partition.

    Attributes
    ----------
    name : str
        The name of the SLURM partition.
    query_name : str
        The name of the queue used for system accounting, e.g. `regular_0`.
        Defaults to the value of `name` (as the two are usually the same).
    max_walltime : str
        The maximum walltime allowed for jobs in this partition, formatted as "HH:MM:SS".
    """

    @property
    def max_walltime(self) -> str | None:
        """Retrieve the maximum walltime for the SLURM partition.

        Queries the SLURM scheduler (`sinfo`) to fetch the maximum walltime
        associated with this partition. The walltime is returned in the format "HH:MM:SS".

        Returns
        -------
        str or None
            The maximum walltime in the format "HH:MM:SS", or `None` if no walltime is set.

        Raises
        ------
        RuntimeError
            If the command to query the SLURM scheduler (`sinfo`) fails.
        """
        sp_cmd = f"sinfo -h -o '%l' -p {self.name}"
        mw = _run_cmd(sp_cmd)
        return self._parse_walltime(mw) if mw else None


class PBSQueue(Queue):
    """Represents a PBS queue.

    This class manages queues in a PBS (Portable Batch System) scheduler. It stores
    and manages queue-specific properties, such as the maximum walltime for jobs.

    Attributes
    ----------
    name : str
        The name of the PBS queue.
    query_name : str
        The name of the queue used for system accounting, e.g. `regular_0`.
        Defaults to the value of `name` (as the two are usually the same).
    max_walltime : str
        The maximum walltime allowed for jobs in this queue, formatted as "HH:MM:SS".
    """

    def __init__(self, name: str, max_walltime: str, query_name: str | None = None):
        """Initialize a PBSQueue instance.

        Parameters
        ----------
        name : str
            The name of the PBS queue.
        max_walltime : str
            The maximum walltime allowed for jobs in this queue, formatted as "HH:MM:SS".
        query_name : str, optional
            An alternative name used for querying the queue. Defaults to the value of `name`.
        """
        super().__init__(name)
        self._max_walltime = max_walltime

    @property
    def max_walltime(self):
        return self._max_walltime

    def __str__(self) -> str:
        """Return a readable string representation of the PBSQueue instance."""
        return (
            f"{self.__class__.__name__}:\n"
            + f"{'-' * len(self.__class__.__name__)}\n"
            + f"name: {self.name}\n"
            + f"max_walltime: {self.max_walltime}\n"
        )

    def __repr__(self) -> str:
        """Return a string representation of the PBSQueue instance."""
        base_repr = super().__repr__()
        # Strip the closing ')' and append the additional attribute
        return f"{base_repr.rstrip(')')}, max_walltime={self.max_walltime!r})"


################################################################################


class Scheduler(ABC, LoggingMixin):
    """Abstract base class for representing a job scheduler.

    This class defines the structure and common behavior for managing queues and
    job scheduling directives. Subclasses should implement specific scheduler-related
    functionality.

    Attributes
    ----------
    queues : List[Queue]
        A list of queues managed by the scheduler.
    queue_names : List[str]
        The names of all queues managed by the scheduler.
    primary_queue_name : str
        The name of the primary queue used for scheduling jobs.
    other_scheduler_directives : dict of str, optional
        Additional directives or settings used for job submission
    requires_task_distribution : bool, optional
        Whether the scheduler requires explicit specification of
        required nodes and cpus for a job, or will calculate it
        based on the number of CPUs alone. Defaults to True.
    global_max_cpus_per_node : int
        The maximum number of CPUs available per node across all queues.
    global_max_mem_per_node_gb : float
        The maximum amount of memory (in GB) available per node across all queues.

    Methods
    -------
    get_queue(name : str) -> Queue
        Retrieve a queue by name.
    """

    def __init__(
        self,
        queues: list["Queue"],
        primary_queue_name: str,
        other_scheduler_directives: dict[str, str] | None = None,
        requires_task_distribution: bool | None = True,
        documentation: str | None = None,
    ):
        """Initialize a Scheduler instance.

        Parameters
        ----------
        queues : List[Queue]
            A list of Queue instances managed by the scheduler.
        primary_queue_name : str
            The name of the primary queue used by C-Star for scheduling jobs.
        other_scheduler_directives : dict of str, optional
            Additional directives or settings used for job submission
        requires_task_distribution : bool, optional
            Whether the scheduler requires explicit specification of
            required nodes and cpus for a job, or will calculate it
            based on the number of CPUs alone. Defaults to True.
        documentation : str, optional
            Where to find additional documentation for this system's scheduler

        Raises
        ------
        ValueError
            If the primary queue name is not found in the list of queues.
        """
        self.queues = queues
        self.queue_names = [q.name for q in queues]
        self.primary_queue_name = primary_queue_name
        self.other_scheduler_directives = (
            other_scheduler_directives if other_scheduler_directives is not None else {}
        )
        self.requires_task_distribution = requires_task_distribution
        self.documentation = documentation

    def get_queue(self, name) -> Queue:
        """Retrieve a queue by name.

        Searches the list of queues managed by the scheduler for a queue with the given name.

        Parameters
        ----------
        name : str
            The name of the queue to retrieve.

        Returns
        -------
        Queue
            The queue instance matching the given name.

        Raises
        ------
        ValueError
            If the specified queue name is not found in the list of queues.
        """
        queue = next((queue for queue in self.queues if queue.name == name), None)
        if queue is None:
            raise ValueError(f"{name} not found in list of queues: {self.queue_names}")
        else:
            return queue

    def __str__(self) -> str:
        """Return a readable string representation of the PBSQueue instance."""
        queues_str = "\n".join([f"- {queue.name}" for queue in self.queues])
        return (
            f"{self.__class__.__name__}\n"
            f"{'-' * len(self.__class__.__name__)}\n"
            f"primary_queue: {self.primary_queue_name}\n"
            f"queues:\n{queues_str}\n"
            f"other_scheduler_directives: {self.other_scheduler_directives}\n"
            f"global max cpus per node: {self.global_max_cpus_per_node}\n"
            f"global max mem per node: {self.global_max_mem_per_node_gb}GB\n"
            f"documentation: {self.documentation}"
        )

    def __repr__(self) -> str:
        """Return a string representation of the PBSQueue instance."""
        base_repr = (
            f"{self.__class__.__name__}("
            f"queues={self.queues!r}, primary_queue_name={self.primary_queue_name!r}, "
            f"other_scheduler_directives={self.other_scheduler_directives!r}, "
            f"documentation={self.documentation!r})"
        )
        return base_repr

    @property
    @abstractmethod
    def global_max_cpus_per_node(self):
        """Abstract method to retrieve the maximum number of CPUs available per node
        across all queues.
        """
        pass

    @property
    @abstractmethod
    def global_max_mem_per_node_gb(self):
        """Abstract method to retrieve the maximum memory available per node across all
        queues.
        """
        pass


class SlurmScheduler(Scheduler):
    """Represents a SLURM job scheduler.

    This class provides functionality for interacting with a Slurm scheduler,
    including retrieving global properties such as the maximum number of CPUs
    and memory available per node.

    Attributes
    ----------
    queues : List[Queue]
        A list of queues managed by the SLURM scheduler.
    queue_names : List[str]
        The names of all queues managed by the scheduler.
    primary_queue_name : str
        The name of the primary queue used by C-Star for scheduling jobs.
    other_scheduler_directives : dict of str, optional
        Additional directives or settings used for job submission
    requires_task_distribution : bool, optional
        Whether the scheduler requires explicit specification of
        required nodes and cpus for a job, or will calculate it
        based on the number of CPUs alone. Defaults to True.
    global_max_cpus_per_node : int
        The maximum number of CPUs available per node across all SLURM queues.
    global_max_mem_per_node_gb : float
        The maximum amount of memory (in GB) available per node across all SLURM queues.

    Methods
    -------
    global_max_cpus_per_node
        Retrieves the maximum number of CPUs available per node.
    global_max_mem_per_node_gb
        Retrieves the maximum memory available per node in GB.
    """

    @property
    def global_max_cpus_per_node(self) -> int | None:
        """Retrieve the maximum number of CPUs available per node across all SLURM
        nodes.

        Queries the SLURM scheduler to determine the highest CPU capacity of any single node.

        Returns
        -------
        int or None
            The maximum number of CPUs available per node, or `None` if the query fails or
            returns no results.

        Raises
        ------
        RuntimeError
            If the command to query the SLURM scheduler fails.
        """
        if stdout := _run_cmd(
            'scontrol show nodes | grep -o "cpu=[0-9]*" | cut -d= -f2 | sort -nr | head -1',
            msg_err="Error querying node property.",
        ):
            return int(stdout)

        return None

    @property
    def global_max_mem_per_node_gb(self) -> float | None:
        """Retrieve the maximum memory available per node across all SLURM nodes, in
        gigabytes.

        Queries the SLURM scheduler to determine the highest memory capacity of any single node.

        Returns
        -------
        float or None
            The maximum memory available per node in gigabytes, or `None` if the query
            fails or returns no results.

        Raises
        ------
        RuntimeError
            If the command to query the SLURM scheduler fails.
        """
        if stdout := _run_cmd(
            'scontrol show nodes | grep -o "RealMemory=[0-9]*" | cut -d= -f2 | sort -nr | head -1',
            msg_err="Error querying node property.",
        ):
            return float(stdout) / (1024)

        return None


class PBSScheduler(Scheduler):
    """Represents a PBS (Portable Batch System) job scheduler.

    This class provides functionality for interacting with a PBS scheduler, including
    retrieving global properties such as the maximum number of CPUs and memory available
    per node.

    Attributes
    ----------
    queues : List[Queue]
        A list of queues managed by the PBS scheduler.
    queue_names : List[str]
        The names of all queues managed by the scheduler.
    primary_queue_name : str
        The name of the primary queue used for scheduling jobs.
    other_scheduler_directives : dict of str, optional
        Additional directives or settings used for job submission
    global_max_cpus_per_node : int
        The maximum number of CPUs available per node across all PBS queues.
    global_max_mem_per_node_gb : float
        The maximum amount of memory (in GB) available per node across all PBS queues.

    Notes
    -----
    - The `requires_task_distribution` attribute is always set to `True` for this scheduler,
      as PBS requires explicit specification of required nodes and cpus for a job.
    """

    requires_task_distribution = True

    @property
    def global_max_cpus_per_node(self) -> int | None:
        """Retrieve the maximum number of CPUs available per node across all PBS nodes.

        Queries the PBS scheduler to determine the highest CPU capacity of any single node.

        Returns
        -------
        int or None
            The maximum number of CPUs available per node, or `None` if the query fails
            or returns no results.

        Raises
        ------
        RuntimeError
            If the command to query the PBS scheduler fails.
        """
        if stdout := _run_cmd(
            'pbsnodes -a | grep "resources_available.ncpus" | cut -d= -f2 | sort -nr | head -1',
            msg_err="Error querying node property.",
        ):
            return int(stdout)

        return None

    @property
    def global_max_mem_per_node_gb(self) -> float | None:
        """Retrieve the maximum memory available per node across all PBS nodes, in
        gigabytes.

        Queries the PBS scheduler to determine the highest memory capacity of any single node.

        Returns
        -------
        float or None
            The maximum memory available per node in gigabytes, or `None` if the query
            fails or returns no results.

        Raises
        ------
        RuntimeError
            If the command to query the PBS scheduler fails.
        """
        stdout = _run_cmd(
            'pbsnodes -a | grep "resources_available.mem" | cut -d== -f2 | sort -nr | head -1',
            msg_err="Error querying node property.",
        )
        if stdout.endswith("kb"):
            return float(stdout[:-2]) / (1024**2)  # Convert kilobytes to gigabytes
        elif stdout.endswith("mb"):
            return float(stdout[:-2]) / 1024  # Convert megabytes to gigabytes
        elif stdout.endswith("gb"):
            return float(stdout[:-2])  # Already in gigabytes

        return None


################################################################################
