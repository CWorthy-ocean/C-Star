import functools
import math
import shutil
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar, Final

from cstar.base.log import LoggingMixin, get_logger
from cstar.base.utils import _run_cmd

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cstar.system.toolchain import GpuNodes

log = get_logger(__name__)

UNLIMITED_WALLTIMES: Final = frozenset({"infinite", "unlimited", "n/a"})
"""Values SLURM reports for a queue with no walltime limit."""


class QueueNotFoundError(LookupError):
    """The scheduler lists no partition or QOS of the name a walltime query asked about."""


def slurm_job_id(environ: "Mapping[str, str]") -> str:
    """Read the id of the SLURM job a process runs in from its environment.

    Parameters
    ----------
    environ : Mapping[str, str]
        The process's environment variables.

    Returns
    -------
    str
        `SLURM_JOB_ID`, else its older alias `SLURM_JOBID`; empty outside a job.
    """
    return environ.get("SLURM_JOB_ID") or environ.get("SLURM_JOBID") or ""


def in_scheduler_job(environ: "Mapping[str, str]") -> bool:
    """Tell whether a process runs inside a SLURM or PBS job, from its environment.

    Parameters
    ----------
    environ : Mapping[str, str]
        The process's environment variables.

    Returns
    -------
    bool
        True when a SLURM job id (`SLURM_JOB_ID` or `SLURM_JOBID`) or a PBS job id
        (`PBS_JOBID`) is set to a non-empty value.
    """
    return bool(slurm_job_id(environ) or environ.get("PBS_JOBID"))


def _slurm_tasks_per_node(value: str) -> list[int]:
    """Expand a `SLURM_TASKS_PER_NODE` value into one task count per node.

    ``128(x3),72`` means three nodes with 128 tasks, then one with 72.

    Raises
    ------
    ValueError
        If *value* is not in SLURM's compressed format.
    """
    counts: list[int] = []
    for item in value.split(","):
        tasks, _, repeat = item.strip().partition("(x")
        counts.extend([int(tasks)] * (int(repeat.removesuffix(")")) if repeat else 1))
    return counts


def ranks_per_node(environ: "Mapping[str, str]", n_ranks: int) -> int | None:
    """Find the most MPI ranks a launch of *n_ranks* places on one node.

    Parameters
    ----------
    environ : Mapping[str, str]
        The process's environment variables.
    n_ranks : int
        The number of MPI ranks the launch starts.

    Returns
    -------
    int or None
        *n_ranks* outside a scheduler job (every rank runs on this machine).
        Inside a job, the largest per-node task count the scheduler reports
        (`SLURM_TASKS_PER_NODE`, or the slots per host in `PBS_NODEFILE`) when
        the counts add up to *n_ranks*; otherwise (an allocation made without
        a task count) *n_ranks* spread evenly over the allocated nodes. None
        when the placement cannot be read.
    """
    if not in_scheduler_job(environ):
        return n_ranks
    try:
        if slurm_job_id(environ):
            counts = _slurm_tasks_per_node(environ["SLURM_TASKS_PER_NODE"])
        else:
            with open(environ["PBS_NODEFILE"]) as nodefile:
                hosts = [line.strip() for line in nodefile if line.strip()]
            counts = list(Counter(hosts).values())
    except (KeyError, ValueError, OSError):
        return None
    if not counts:
        return None
    if sum(counts) == n_ranks:
        return max(counts)
    return math.ceil(n_ranks / len(counts))


SCHEDULER_SLURM: Final = "SLURM"
"""The name of the SLURM scheduler family."""
SCHEDULER_PBS: Final = "PBS"
"""The name of the PBS scheduler family."""
SUBMITTABLE_FAMILIES: Final = frozenset({SCHEDULER_SLURM})
"""The scheduler families C-Star has a launcher for, and so can submit jobs to."""

VISIBLE_SCHEDULERS: Final = (
    (SCHEDULER_SLURM, "sbatch", "SLURM_"),
    (SCHEDULER_PBS, "qsub", "PBS_"),
)
"""Each scheduler family `visible_scheduler` looks for: its name, its submit command,
and the prefix of the variables it sets."""


def visible_scheduler(
    environ: "Mapping[str, str]", which: Callable[..., str | None] = shutil.which
) -> str:
    """Name the batch scheduler a machine shows, whether or not C-Star knows the system.

    A login node shows its scheduler by the submit command on its `PATH`, and a
    job also by the variables the scheduler sets.

    Parameters
    ----------
    environ : Mapping[str, str]
        The process's environment variables; its `PATH` is searched.
    which : Callable[..., str | None]
        Locates a command on a search path, as `shutil.which` does.

    Returns
    -------
    str
        `"SLURM"` or `"PBS"`, else empty.
    """
    path = environ.get("PATH")
    for family, command, prefix in VISIBLE_SCHEDULERS:
        if which(command, path=path) or any(
            name.startswith(prefix) for name in environ
        ):
            return family
    return ""


def parse_walltime(walltime_str: str) -> tuple[int, int, int]:
    """Parse a SLURM walltime in the format "D-HH:MM:SS" into a tuple
    containing [hours, minutes, seconds].

    Parameters
    ----------
    walltime_str : str
        The walltime string to parse. This can be in one of the following formats:
        - "D-HH:MM:SS" (days, hours, minutes, seconds)
        - "HH:MM:SS" (hours, minutes, seconds)
        - "MM:SS" (minutes, seconds)

    Returns
    -------
    tuple[int, int, int]

    Raises
    ------
    ValueError
        If the walltime string is not in a recognized format.
    """
    walltime_str = walltime_str.strip()
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
    else:
        raise ValueError(f"Unrecognized walltime format: {walltime_str!r}")
    return mw_d * 24 + mw_h, mw_m, mw_s


def format_walltime(walltime_str: str) -> str | None:
    """Parse and format a SLURM walltime string into the format "HH:MM:SS".

    Parameters
    ----------
    walltime_str : str
        The walltime string to parse. This can be in one of the following formats:
        - "D-HH:MM:SS" (days, hours, minutes, seconds)
        - "HH:MM:SS" (hours, minutes, seconds)
        - "MM:SS" (minutes, seconds)
        - "infinite"/"UNLIMITED"/"N/A" (no walltime limit)

    Returns
    -------
    str or None
        The formatted walltime string in the "HH:MM:SS" format, or `None` when
        SLURM reports the queue has no walltime limit.

    Raises
    ------
    ValueError
        If the value is neither a walltime nor one of SLURM's no-limit values;
        `Queue.max_walltime` then treats the limit as unknown.
    """
    if walltime_str.strip().casefold() in UNLIMITED_WALLTIMES:
        return None
    mw_h, mw_m, mw_s = parse_walltime(walltime_str)
    return f"{mw_h:02}:{mw_m:02}:{mw_s:02}"


def query_max_walltime_via_sinfo(name: str) -> str | None:
    """Retrieve the maximum walltime for the SLURM queue via sinfo.

    Queries the SLURM scheduler (`sinfo`) to fetch the maximum walltime
    associated with this partition/queue. The walltime is returned in the format "HH:MM:SS".

    Returns
    -------
    str or None
        The maximum walltime in the format "HH:MM:SS", or `None` if the partition
        has no walltime limit.

    Raises
    ------
    QueueNotFoundError
        If `sinfo` lists no partition of that name (it prints nothing, and
        exits 0).
    RuntimeError
        If `sinfo` fails.
    ValueError
        If `sinfo` reports something that is not a walltime.
    """
    sp_cmd = f"sinfo --noheader --format='%l' --partition={name}"
    # sinfo may print the partition's limit once per group of nodes
    limits = sorted(
        {line.strip() for line in _run_cmd(sp_cmd, raise_on_error=True).splitlines()}
        - {""}
    )
    if not limits:
        raise QueueNotFoundError(f"sinfo lists no partition named {name!r}")
    return format_walltime("\n".join(limits))


def _cores_per_node_via_sinfo(partition: str | None) -> list[int]:
    """Query sinfo for the physical cores per node of each node type.

    sinfo's CPU count (`%c`) counts hardware threads, and an MPI rank on each
    thread of a core slows the model, so each node type's CPUs are divided by
    its threads per core (`%Z`). `--exact` keeps node types on rows of their
    own; without it sinfo merges them into one row of minimums (`64+`), which
    could pair one node type's CPUs with another's threads.

    Parameters
    ----------
    partition : str or None
        The partition to ask about, or `None` for every partition sinfo lists.

    Returns
    -------
    list of int
        The physical cores per node of each row sinfo prints, at least one.

    Raises
    ------
    LookupError
        If sinfo returns nothing or its output cannot be parsed.
    """
    where = "any partition" if partition is None else f"partition {partition!r}"
    sp_cmd = "sinfo --noheader --exact --format='%c %Z'"
    if partition is not None:
        sp_cmd += f" --partition={partition}"
    stdout = _run_cmd(sp_cmd, msg_err=f"Error querying the cores per node of {where}.")
    cores: list[int] = []
    try:
        for line in stdout.splitlines():
            if not line.strip():
                continue
            cpus, threads = (int(field.rstrip("+")) for field in line.split())
            cores.append(cpus // max(threads, 1))
    except ValueError as ex:
        log.warning(
            f"Cannot parse cores per node {stdout!r} for {where}; "
            "treating the value as unknown."
        )
        raise LookupError(f"unparseable cores per node for {where}") from ex
    if not cores:
        raise LookupError(f"sinfo reported no cores per node for {where}")
    return cores


@functools.cache
def _cpus_per_node_via_sinfo(name: str) -> int:
    """Query sinfo for the partition's physical cores per node (smallest node type).

    Cached per partition name: the value is static hardware information and
    every scheduler job would otherwise re-run the query. Only successful
    lookups are cached -- `functools.cache` does not memoize exceptions, so a
    transient failure is retried on the next call rather than pinned.

    Raises
    ------
    LookupError
        If sinfo returns nothing or its output cannot be parsed.
    """
    return min(_cores_per_node_via_sinfo(name))


def query_max_cpus_per_node_via_sinfo(name: str) -> int | None:
    """Retrieve the CPUs per node for the SLURM partition via sinfo, counting
    physical cores, not hardware threads.

    A heterogeneous partition reports one value per node type; the smallest is
    returned so node counts derived from it are always satisfiable. Successful
    lookups are cached for the life of the process (see
    `_cpus_per_node_via_sinfo`); unknown results are not.

    Returns
    -------
    int or None
        The number of physical cores per node on the partition's smallest node
        type, or `None` if the value cannot be determined.
    """
    try:
        return _cpus_per_node_via_sinfo(name)
    except LookupError:
        return None


def query_max_walltime_via_sacctmgr(name: str) -> str | None:
    """Retrieve the maximum walltime using sacctmgr.

    Queries the SLURM accounting manager (`sacctmgr`) to fetch the maximum walltime
    associated with this QOS. The walltime is returned in the format "HH:MM:SS".
    The QOS's name is asked for too: `sacctmgr` prints nothing for a QOS that
    does not exist and an empty `MaxWall` for one with no limit, which only
    the name tells apart.

    Returns
    -------
    str or None
        The maximum walltime in the format "HH:MM:SS", or `None` if the QOS has
        no walltime limit.

    Raises
    ------
    QueueNotFoundError
        If `sacctmgr` lists no QOS of that name (it prints nothing, and exits 0).
    RuntimeError
        If `sacctmgr` fails.
    ValueError
        If `sacctmgr` reports something that is not a walltime.
    """
    sp_cmd = f"sacctmgr show qos {name} format=Name,MaxWall --noheader --parsable2"
    stdout = _run_cmd(sp_cmd, raise_on_error=True)
    rows = [line for line in stdout.splitlines() if line.strip()]
    if not rows:
        raise QueueNotFoundError(f"sacctmgr lists no QOS named {name!r}")
    mw = rows[0].partition("|")[2].strip()
    return format_walltime(mw) if mw else None


class Queue(ABC):
    """Abstract base class for representing a generic scheduler queue."""

    name: Final[str]
    """The name of the queue used to submit jobs, e.g. `regular`."""
    query_name: Final[str]
    """The name the walltime query asks about, e.g. `regular_0`, or a
    partition's QOS.

    Defaults to the value of `name` (as the two are usually the same).
    """
    _max_walltime_method: Final[Callable[[str], str | None] | None]
    """An alternative method for determining the maximum walltime. If not provided,
    defaults to a method specified by the subclass.
    """

    def __init__(
        self,
        name: str,
        query_name: str | None = None,
        max_walltime_method: Callable[[str], str | None] | None = None,
    ):
        """Initialize a Queue instance.

        Parameters
        ----------
        name : str
            The name of the queue.
        query_name : str, optional
            The name the walltime query asks about. If not provided, it defaults
            to the value of `name`.
        max_walltime_method : Callable[[str], str | None] | None, optional
            An alternative method for determining the maximum walltime. If not provided,
            defaults to a method specified by the subclass.
        """
        self.name = name
        self.query_name = query_name if query_name is not None else name
        self._max_walltime_method = max_walltime_method

    def __repr__(self) -> str:
        """Return a string representation of this queue instance."""
        return f"{self.__class__.__name__}(name={self.name!r}, query_name={self.query_name!r})"

    @property
    @abstractmethod
    def _default_max_walltime_method(self) -> Callable[[str], str | None]:
        """Return a method that will calculate a default maximum walltime."""
        pass

    @property
    def walltime_method(self) -> Callable[[str], str | None]:
        """Return the function that answers the queue's maximum walltime."""
        return self._max_walltime_method or self._default_max_walltime_method

    def query_max_walltime(self) -> str | None:
        """Ask for the queue's maximum walltime.

        Returns
        -------
        str or None
            The maximum walltime, "HH:MM:SS", or `None` if the queue has no limit.

        Raises
        ------
        QueueNotFoundError
            If the scheduler lists no queue of this queue's `query_name`.
        RuntimeError
            If the scheduler's query command fails.
        ValueError
            If the scheduler's answer is not a walltime.
        """
        return self.walltime_method(self.query_name)

    @property
    def max_walltime(self) -> str | None:
        """Return the maximum walltime for the queue, or `None` when it has no
        limit or it cannot be found out.

        Never raises for a missing queue, a failed query or an answer that is
        not a walltime: a job then asks for the walltime it was given, as for a
        queue with no limit. `cstar system check` calls `query_max_walltime` to
        tell these apart.
        """
        try:
            return self.query_max_walltime()
        except (QueueNotFoundError, RuntimeError, ValueError) as ex:
            log.warning(f"The walltime limit of queue {self.name!r} is unknown: {ex}")
            return None

    @property
    def max_cpus_per_node(self) -> int | None:
        """Return the number of CPUs available per node on the queue, when known."""
        return None


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
        The name the walltime query asks about, e.g. `regular_0`.
        Defaults to the value of `name` (as the two are usually the same).
    max_walltime: str
        The maximum walltime allowed for the queue.
    """

    def __str__(self) -> str:
        """String representation of this SlurmQueue instance."""
        return (
            f"{self.__class__.__name__}:\n"
            f"{'-' * len(self.__class__.__name__)}\n"
            f"name: {self.name}\n"
            f"max_walltime: {self.max_walltime}\n"
        )


class SlurmQOS(SlurmQueue):
    """Represents a SLURM Quality of Service (QOS) queue.

    This class provides functionality to retrieve and manage SLURM QOS properties,
    such as the maximum walltime for jobs associated with a QOS.

    Attributes
    ----------
    name : str
        The name of the SLURM QOS queue.
    query_name : str
        The name the walltime query asks about, e.g. `regular_0`.
        Defaults to the value of `name` (as the two are usually the same).
    max_walltime : str
        The maximum walltime allowed for jobs in this QOS, formatted as "HH:MM:SS".
    """

    @property
    def _default_max_walltime_method(self) -> Callable[[str], str | None]:
        return query_max_walltime_via_sacctmgr


class SlurmPartition(SlurmQueue):
    """Represents a SLURM partition queue.

    This class provides functionality to retrieve and manage SLURM partition properties,
    such as the maximum walltime for jobs associated with a partition.

    Attributes
    ----------
    name : str
        The name of the SLURM partition.
    query_name : str
        The name the walltime query asks about, e.g. `regular_0`.
        Defaults to the value of `name` (as the two are usually the same).
    max_walltime : str
        The maximum walltime allowed for jobs in this partition, formatted as "HH:MM:SS".
    """

    @property
    def _default_max_walltime_method(self) -> Callable[[str], str | None]:
        return query_max_walltime_via_sinfo

    @property
    def max_cpus_per_node(self) -> int | None:
        """CPUs per node on this partition's smallest node type, from sinfo.

        Asks about the partition's `name`, never its `query_name`: the walltime
        query may name the partition's QOS, which sinfo does not list.
        """
        return query_max_cpus_per_node_via_sinfo(self.name)


class PBSQueue(Queue):
    """Represents a PBS queue.

    This class manages queues in a PBS (Portable Batch System) scheduler. It stores
    and manages queue-specific properties, such as the maximum walltime for jobs.

    Attributes
    ----------
    name : str
        The name of the PBS queue.
    query_name : str
        The name the walltime query asks about, e.g. `regular_0`.
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
            The name the walltime query asks about. Defaults to the value of `name`.
        """
        super().__init__(name, query_name)
        self._max_walltime = max_walltime

    def query_max_walltime(self) -> str | None:
        """Return the queue's fixed maximum walltime; PBS is not asked."""
        return self._max_walltime

    @property
    def _default_max_walltime_method(self) -> Callable[[str], str | None]:
        """Irrelevant for PBSQueue."""
        raise NotImplementedError()

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
    gpu : GpuNodes or None
        The system's GPU nodes, or None when it has none registered.
    global_max_cpus_per_node : int
        The maximum number of CPUs available per node across all queues.
    global_max_mem_per_node_gb : float
        The maximum amount of memory (in GB) available per node across all queues.

    Methods
    -------
    get_queue(name : str) -> Queue
        Retrieve a queue by name.
    """

    family: ClassVar[str]
    """The scheduler family's name, as `visible_scheduler` reports it."""

    def __init__(
        self,
        queues: list["Queue"],
        primary_queue_name: str,
        other_scheduler_directives: dict[str, str] | None = None,
        requires_task_distribution: bool | None = True,
        documentation: str | None = None,
        max_cpus_per_node: int | None = None,
        gpu: "GpuNodes | None" = None,
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
        max_cpus_per_node : int, optional
            If specified, the maximum cpus used for distribution calculations
            will be fixed, instead of determined by inspecting the machine.
        gpu : GpuNodes, optional
            The system's GPU nodes, or None when it has none registered.

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
        self._max_cpus_per_node = max_cpus_per_node
        self.gpu = gpu
        """The system's GPU nodes, or None when it has none registered."""

    def get_queue(self, name: str | None) -> Queue:
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

    @property
    @abstractmethod
    def global_max_cpus_per_node(self) -> int | None:
        """Abstract method to retrieve the maximum number of CPUs available per node
        across all queues.
        """
        pass

    @property
    @abstractmethod
    def global_max_mem_per_node_gb(self) -> float | None:
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

    family: ClassVar[str] = SCHEDULER_SLURM

    @property
    def global_max_cpus_per_node(self) -> int | None:
        """Retrieve the maximum number of CPUs available per node across all SLURM
        nodes.

        The system's fixed `max_cpus_per_node` when it has one; otherwise sinfo
        is asked, until it answers, for the physical cores of the largest node
        in any partition it lists.

        Returns
        -------
        int or None
            The maximum number of CPUs available per node, or `None` if the query fails or
            returns no results.
        """
        if self._max_cpus_per_node is None:
            try:
                self._max_cpus_per_node = max(_cores_per_node_via_sinfo(None))
            except LookupError:
                return None

        return self._max_cpus_per_node

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

    family: ClassVar[str] = SCHEDULER_PBS
    requires_task_distribution = True

    @property
    def global_max_cpus_per_node(self) -> int | None:
        """Retrieve the maximum number of CPUs available per node across all PBS nodes.

        The system's fixed `max_cpus_per_node` when it has one; otherwise the PBS
        scheduler is queried, once, for the highest CPU capacity of any single node.

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
        if self._max_cpus_per_node is None:
            if stdout := _run_cmd(
                'pbsnodes -a | grep "resources_available.ncpus" | cut -d= -f2 | sort -nr | head -1',
                msg_err="Error querying node property.",
            ):
                self._max_cpus_per_node = int(stdout)

        return self._max_cpus_per_node

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
