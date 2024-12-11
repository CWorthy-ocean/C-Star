import subprocess
from enum import Enum
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

################################################################################


class Queue(ABC):
    def __init__(self, name: str, query_name: Optional[str] = None):
        self.name = name
        self.query_name = query_name if query_name is not None else name

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name!r}, query_name={self.query_name!r})"


class QueueFlag(Enum):
    PARTITION = "--partition"
    QOS = "--qos"
    Q = "-q"

    def __str__(self):
        return self.value


class SlurmQueue(Queue, ABC):
    def __str__(self):
        return (
            f"{self.__class__.__name__}:\n"
            f"{'-' * len(self.__class__.__name__)}\n"
            f"name: {self.name}\n"
            f"max_walltime: {self.max_walltime}\n"
        )

    @property
    @abstractmethod
    def max_walltime(self):
        pass

    def _parse_walltime(self, walltime_str):
        # The output of the above might be inconsistent (e.g. 02:00:00, 1-12:00:00, 30:00)
        # We should parse the output and return a consistent "HH:MM:SS":

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
        return f"{mw_d*24 + mw_h:02}:{mw_m:02}:{mw_s:02}"


class SlurmQOS(SlurmQueue):
    @property
    def max_walltime(self) -> Optional[str]:
        sp_cmd = f"sacctmgr show qos {self.name} format=MaxWall --noheader"
        sp_run = subprocess.run(sp_cmd, shell=True, text=True, capture_output=True)
        if sp_run.returncode != 0:
            raise RuntimeError(f"Command {sp_cmd} failed: {sp_run.stderr.strip()}")
        mw = sp_run.stdout.strip()
        return self._parse_walltime(mw) if mw else None


class SlurmPartition(SlurmQueue):
    @property
    def max_walltime(self) -> Optional[str]:
        sp_cmd = f"sinfo -h -o '%l' -p {self.name}"
        sp_run = subprocess.run(sp_cmd, shell=True, text=True, capture_output=True)
        if sp_run.returncode != 0:
            raise RuntimeError(f"Command {sp_cmd} failed: {sp_run.stderr.strip()}")
        mw = sp_run.stdout.strip()
        return self._parse_walltime(mw) if mw else None


class PBSQueue(Queue):
    def __init__(self, name: str, max_walltime: str, query_name: Optional[str] = None):
        super().__init__(name)
        self.max_walltime = max_walltime

    def __str__(self):
        return (
            f"{self.__class__.__name__}:\n"
            + f"{'-' * len(self.__class__.__name__)}\n"
            + f"name: {self.name}\n"
            + f"max_walltime: {self.max_walltime}\n"
        )

    def __repr__(self):
        base_repr = super().__repr__()
        # Strip the closing ')' and append the additional attribute
        return f"{base_repr.rstrip(')')}, max_walltime={self.max_walltime!r})"


################################################################################


class Scheduler(ABC):
    def __init__(
        self,
        queues: List["Queue"],
        primary_queue_name: str,
        other_scheduler_directives: Optional[Dict[str, str]] = None,
        requires_task_distribution: Optional[bool] = True,
    ):
        self.queues = queues
        self.queue_names = [q.name for q in queues]
        self.primary_queue_name = primary_queue_name
        self.other_scheduler_directives = (
            other_scheduler_directives if other_scheduler_directives is not None else {}
        )
        self.requires_task_distribution = requires_task_distribution

    def get_queue(self, name):
        queue = next((queue for queue in self.queues if queue.name == name), None)
        if queue is None:
            raise ValueError(f"{name} not found in list of queues: {self.queue_names}")
        else:
            return queue

    def __str__(self):
        queues_str = "\n".join([str(queue.name) for queue in self.queues])
        return (
            f"{self.__class__.__name__}\n"
            f"{'-' * len(self.__class__.__name__)}\n"
            f"primary_queue: {self.primary_queue_name}\n"
            f"queues:\n{queues_str}\n"
            f"other_scheduler_directives: {self.other_scheduler_directives}\n"
            f"global max cpus per node: {self.global_max_cpus_per_node}\n"
            f"global max mem per node: {self.global_max_mem_per_node_gb}GB"
        )

    def __repr__(self):
        base_repr = (
            f"{self.__class__.__name__}("
            f"queues={self.queues!r}, primary_queue_name={self.primary_queue_name!r}, "
            f"other_scheduler_directives={self.other_scheduler_directives!r})"
        )
        return base_repr

    @property
    @abstractmethod
    def global_max_cpus_per_node(self):
        pass

    @property
    @abstractmethod
    def global_max_mem_per_node_gb(self):
        pass


class SlurmScheduler(Scheduler):
    @property
    def global_max_cpus_per_node(self):
        result = subprocess.run(
            'scontrol show nodes | grep -o "cpu=[0-9]*" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            print(f"Error querying node property. STDERR: {result.stderr}")
        so = result.stdout.strip()
        return int(so) if so else None

    @property
    def global_max_mem_per_node_gb(self):
        result = subprocess.run(
            'scontrol show nodes | grep -o "RealMemory=[0-9]*" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            print(f"Error querying node property. STDERR: {result.stderr}")
        so = result.stdout.strip()
        return float(so) / (1024) if so else None


class PBSScheduler(Scheduler):
    requires_task_distribution = True

    @property
    def global_max_cpus_per_node(self):
        result = subprocess.run(
            'pbsnodes -a | grep "resources_available.ncpus" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            print(f"Error querying node property. STDERR: {result.stderr}")
        so = result.stdout.strip()
        return int(so) if so else None

    @property
    def global_max_mem_per_node_gb(self):
        result = subprocess.run(
            'pbsnodes -a | grep "resources_available.mem" | cut -d== -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            print(f"Error querying node property. STDERR: {result.stderr}")
        so = result.stdout.strip()
        if so.endswith("kb"):
            return float(so[:-2]) / (1024**2)  # Convert kilobytes to gigabytes
        elif so.endswith("mb"):
            return float(so[:-2]) / 1024  # Convert megabytes to gigabytes
        elif so.endswith("gb"):
            return float(so[:-2])  # Already in gigabytes
        else:
            return None


################################################################################
