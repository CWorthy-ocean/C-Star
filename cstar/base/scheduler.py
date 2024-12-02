from abc import ABC
from typing import List, Dict, Optional, TYPE_CHECKING
from cstar.base.scheduler_job import SlurmJob
import subprocess

if TYPE_CHECKING:
    from pathlib import Path

################################################################################


class Queue:
    pass


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


################################################################################


class Scheduler(ABC):
    pass


class SlurmScheduler(Scheduler):
    def __init__(
        self,
        queue_flag: str,
        queues: List["SlurmQueue"],
        primary_queue_name: str,
        other_scheduler_directives: Optional[Dict[str, str]],
    ):
        self.queue_flag = queue_flag
        self.queues = queues
        self.queue_names = [q.name for q in queues]
        self.primary_queue_name = primary_queue_name
        self.other_scheduler_directives = (
            other_scheduler_directives if other_scheduler_directives is not None else {}
        )

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

    def get_queue(self, name):
        queue = next((queue for queue in self.queues if queue.name == name), None)
        if queue is None:
            raise ValueError(f"{name} not found in list of queues: {self.queue_names}")
        else:
            return queue

    def create_job(
        self,
        commands: str,
        cpus: int,
        account_key: str,
        script_path: Optional[str | "Path"] = None,
        run_path: Optional[str | "Path"] = None,
        job_name: Optional[str] = None,
        output_file: Optional[str | "Path"] = None,
        queue_name: Optional[str] = None,
        send_email: Optional[bool] = True,
        walltime: Optional[str] = None,
    ) -> "SlurmJob":
        return SlurmJob(
            scheduler=self,
            commands=commands,
            cpus=cpus,
            account_key=account_key,
            script_path=script_path,
            run_path=run_path,
            job_name=job_name,
            output_file=output_file,
            queue_name=queue_name,
            send_email=send_email,
            walltime=walltime,
        )


################################################################################
