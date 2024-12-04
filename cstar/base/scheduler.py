import subprocess
import json
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    pass

################################################################################


class Queue:
    def __init__(self, name: str):
        self.name = name


class SlurmQueue(Queue):
    def __init__(self, name: str, query_name: Optional[str] = None):
        self.name = name
        self.query_name = query_name if query_name is not None else name

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
        self.query_name = query_name if query_name is not None else name
        self.max_walltime = max_walltime

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
        breakpoint()

        return data["Queue"][queue_name].get(property_name)

    @property
    def max_cpus(self) -> Optional[int]:
        mc = self.query_queue_property(self.query_name, "resources_max.ncpus")
        return int(mc) if mc else None

    # @property
    # def max_walltime(self) -> Optional[str]:
    #     return None
    # mw = self.query_queue_property(self.query_name, "resources_max.walltime")
    # return int(mw) if mw else None

    @property
    def max_mem(self) -> Optional[str]:
        return self.query_queue_property(self.query_name, "resources_max.mem")

    @property
    def priority(self) -> Optional[int]:
        pr = self.query_queue_property(self.query_name, "Priority")
        return int(pr) if pr else None


################################################################################


class Scheduler(ABC):
    def __init__(
        self,
        queue_flag: str,
        queues: List["Queue"],
        primary_queue_name: str,
        other_scheduler_directives: Optional[Dict[str, str]] = None,
    ):
        self.queue_flag = queue_flag
        self.queues = queues
        self.queue_names = [q.name for q in queues]
        self.primary_queue_name = primary_queue_name
        self.other_scheduler_directives = (
            other_scheduler_directives if other_scheduler_directives is not None else {}
        )

    def get_queue(self, name):
        queue = next((queue for queue in self.queues if queue.name == name), None)
        if queue is None:
            raise ValueError(f"{name} not found in list of queues: {self.queue_names}")
        else:
            return queue

    @abstractmethod
    @property
    def global_max_cpus_per_node(self):
        pass

    @abstractmethod
    @property
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
        so = result.stdout.strip()
        return float(so) / (1024**3) if so else None


class PBSScheduler(Scheduler):
    @property
    def global_max_cpus_per_node(self):
        result = subprocess.run(
            'pbsnodes -a | grep "resources_available.ncpus" | cut -d= -f2 | sort -nr | head -1',
            shell=True,
            text=True,
            capture_output=True,
        )
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
