import json
import subprocess
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

################################################################################


class Queue(ABC):
    def __init__(self, name: str, query_name: Optional[str] = None):
        self.name = name
        self.query_name = query_name if query_name is not None else name

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name!r}, query_name={self.query_name!r})"


class SlurmQueue(Queue):
    def __str__(self):
        return (
            f"{self.__class__.__name__}:\n"
            f"{'-' * len(self.__class__.__name__)}\n"
            f"name: {self.name}\n"
            f"max_walltime: {self.max_walltime}\n"
            f"max_nodes: {self.max_nodes}\n"
            f"priority: {self.priority}\n"
        )

    def query_queue_property(self, queue_name, property_name):
        result = subprocess.run(
            f"sacctmgr show qos {queue_name} format={property_name} --noheader",
            shell=True,
            text=True,
            capture_output=True,
        )

        if result.returncode != 0:
            raise RuntimeError(f"Command failed: {result.stderr.strip()}")

        stdout = result.stdout.strip()
        return stdout

    @property
    def max_walltime(self) -> Optional[str]:
        mw = self.query_queue_property(self.query_name, "MaxWall")
        return mw if mw else None

    @property
    def max_nodes(self) -> Optional[int]:
        mn = self.query_queue_property(self.query_name, "MaxNodes")
        return int(mn) if mn else None

    @property
    def priority(self) -> Optional[str]:
        pr = self.query_queue_property(self.query_name, "Priority")
        return pr if pr else None


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

    def query_queue_property(
        self, queue_name: str, property_name: str
    ) -> Optional[str]:
        # Run qstat with JSON output
        result = subprocess.run(
            ["qstat", "-Qf", "-Fjson", queue_name],
            text=True,
            capture_output=True,
            check=True,
        )
        # Parse the JSON output
        data = json.loads(result.stdout)
        # Access the requested property
        return data["Queue"][queue_name].get(property_name)

    @property
    def max_cpus(self) -> Optional[int]:
        mc = self.query_queue_property(self.query_name, "resources_max.ncpus")
        return int(mc) if mc else None

    @property
    def max_mem(self) -> Optional[str]:
        return self.query_queue_property(self.query_name, "resources_max.mem")


################################################################################


class Scheduler(ABC):
    def __init__(
        self,
        queue_flag: str,
        queues: List["Queue"],
        primary_queue_name: str,
        other_scheduler_directives: Optional[Dict[str, str]] = None,
        requires_task_distribution: Optional[bool] = True,
    ):
        self.queue_flag = queue_flag
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
            f"{self.__class__.__name__}(queue_flag={self.queue_flag!r}, "
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
