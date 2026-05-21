"""
Utilities for launching a Dask cluster
"""

import os
import shutil
from subprocess import check_output, check_call
from pathlib import Path

import tempfile
import time
import textwrap
import signal

import dask
from dask.distributed import Client, LocalCluster

from .config import paths, system, machine_config

# Get JupyterHub URL from environment variable, default to empty string
JUPYTERHUB_URL = os.environ.get("JUPYTERHUB_SERVICE_PREFIX", "")

class dask_cluster(object):
    """Launch or connect to a Dask cluster on SLURM, or fall back to local."""

    def __init__(
        self,
        account=None,
        n_nodes=None,
        n_tasks_per_node=None,
        wallclock=None,
        queue_name=None,
        scheduler_file=None,
        conda_env=None,
    ):
        """
        Initialize a Dask cluster.

        Parameters
        ----------
        account : str, optional
            SLURM account to charge when launching a cluster.
            Defaults to config.machine_config.account if not provided.
        n_nodes : int, optional
            Number of SLURM nodes to request. Default 4.
        n_tasks_per_node : int, optional
            Tasks per node for dask-worker.
            Defaults to config.machine_config.pes_per_node if not provided.
        wallclock : str, optional
            Wall clock time for the SLURM job (HH:MM:SS). Default "04:00:00".
        queue_name : str, optional
            SLURM QoS/queue/partition name.
            Defaults to config.machine_config.queues premium or default if not provided.
        scheduler_file : str or pathlib.Path, optional
            Existing scheduler file to connect to. If provided, skip launch.
        conda_env : str, optional
            Conda environment name to activate in the SLURM job.
            Defaults to CONDA_DEFAULT_ENV or "cstar-forge-v0".
        """
        # Defaults from machine_config, overwrite with provided args
        account = account if account is not None else machine_config.account
        n_nodes = n_nodes if n_nodes is not None else 4
        n_tasks_per_node = (
            n_tasks_per_node
            if n_tasks_per_node is not None
            else (machine_config.pes_per_node or 64)
        )
        wallclock = wallclock if wallclock is not None else "04:00:00"
        queues = machine_config.queues or {}
        queue_name = (
            queue_name
            if queue_name is not None
            else queues.get("premium") or queues.get("default") or "premium"
        )
        conda_env = (
            conda_env
            if conda_env is not None
            else os.environ.get("CONDA_DEFAULT_ENV", "cstar-forge-v0")
        )

        self.scheduler_file = scheduler_file
        self.client = None
        

        if not slurm_available():
            self.cluster = LocalCluster()
            self.client = Client(self.cluster)
            self.jobid = None
            self.dashboard_link = self.client.dashboard_link
            self.local_cluster = True
            print(f"Local cluster running at {self.client.dashboard_link}")
            return
        
        if self.scheduler_file is not None:
            self.scheduler_file = str(self.scheduler_file)
            if not os.path.exists(self.scheduler_file):
                raise FileNotFoundError(f"scheduler_file not found: {self.scheduler_file}")
            self.jobid = None
        
        else:
            if account is None:
                raise ValueError("account is required to launch a dask cluster")
            self.scheduler_file, self.jobid = self._launch_dask_cluster(
                account=account,
                n_nodes=n_nodes,
                n_tasks_per_node=n_tasks_per_node,
                wallclock=wallclock,
                queue_name=queue_name,
                conda_env=conda_env,
            )

        self.local_cluster = False
        dask.config.config["distributed"]["dashboard"][
            "link"
        ] = "{JUPYTERHUB_SERVICE_PREFIX}proxy/{host}:{port}/status"
        
        try:
            self._connect_client()
        except RuntimeError:
            # If the provided scheduler_file is stale, fall back to launching
            # a new cluster when possible.
            if self.scheduler_file is not None and account is not None:
                print(
                    "Failed to connect to existing scheduler. "
                    "Launching a new Dask cluster."
                )
                self.scheduler_file, self.jobid = self._launch_dask_cluster(
                    account=account,
                    n_nodes=n_nodes,
                    n_tasks_per_node=n_tasks_per_node,
                    wallclock=wallclock,
                    queue_name=queue_name,
                    conda_env=conda_env,
                )
                self._connect_client()
            else:
                raise
        self.dashboard_link = f"{JUPYTERHUB_URL}{self.client.dashboard_link}"


        print(f"Dashboard:\n {self.dashboard_link}")

    def _launch_dask_cluster(
        self, account, n_nodes, n_tasks_per_node, wallclock, queue_name, conda_env
    ):
        """Submit a SLURM job that starts a Dask scheduler and workers."""
        # Use scratch parent as scratch location, or fall back to environment variable
        scratch_root = paths.scratch.parent if hasattr(paths, 'scratch') else Path(os.environ.get("SCRATCH", "/tmp"))
        path_dask = scratch_root / "dask"
        path_dask_str = str(path_dask)
        os.makedirs(path_dask_str, exist_ok=True)

        scheduler_file = tempfile.mktemp(
            prefix="dask_scheduler_file.", suffix=".json", dir=path_dask_str
        )

        if system == "NERSC_perlmutter":
            sbatch_header = f"""\
            #SBATCH --job-name dask-worker
            #SBATCH --account {account}
            #SBATCH --qos={queue_name}
            #SBATCH --nodes={n_nodes}
            #SBATCH --ntasks-per-node={n_tasks_per_node}
            #SBATCH --time={wallclock}
            #SBATCH --constraint=cpu
            #SBATCH --error {path_dask_str}/dask-workers/dask-worker-%J.err
            #SBATCH --output {path_dask_str}/dask-workers/dask-worker-%J.out"""
        elif system == "RCAC_anvil":
            sbatch_header = f"""\
            #SBATCH --job-name dask-worker
            #SBATCH --account {account}
            #SBATCH --partition={queue_name}
            #SBATCH --nodes={n_nodes}
            #SBATCH --ntasks-per-node={n_tasks_per_node}
            #SBATCH --time={wallclock}
            #SBATCH --error {path_dask_str}/dask-workers/dask-worker-%J.err
            #SBATCH --output {path_dask_str}/dask-workers/dask-worker-%J.out"""
        else:
            sbatch_header = f"""\
            #SBATCH --job-name dask-worker
            #SBATCH --account {account}
            #SBATCH --nodes={n_nodes}
            #SBATCH --ntasks-per-node={n_tasks_per_node}
            #SBATCH --time={wallclock}
            #SBATCH --error {path_dask_str}/dask-workers/dask-worker-%J.err
            #SBATCH --output {path_dask_str}/dask-workers/dask-worker-%J.out"""

        if system == "NERSC_perlmutter":
            env_setup = f"""\
            module load conda
            module load python
            source $(conda info --base)/etc/profile.d/conda.sh
            conda activate {conda_env}"""
            dask_interface = "hsn0"
        elif system == "RCAC_anvil":
            env_setup = f"""\
            module load conda
            source $(conda info --base)/etc/profile.d/conda.sh
            conda activate {conda_env}"""
            dask_interface = "ib0"
        else:
            env_setup = f"""\
            module load conda
            source $(conda info --base)/etc/profile.d/conda.sh
            conda activate {conda_env}"""
            dask_interface = "hsn0"

        script = textwrap.dedent(
            f"""\
            #!/bin/bash
            {sbatch_header}

            echo "Starting scheduler..."

            scheduler_file={scheduler_file}
            rm -f $scheduler_file

            {env_setup}

            #start scheduler
            DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT=3600s \
            DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP=3600s \
            dask scheduler \
                --interface {dask_interface} \
                --scheduler-file $scheduler_file &

            dask_pid=$!

            # Wait for the scheduler to start
            sleep 5
            until [ -f $scheduler_file ]
            do
                 sleep 5
            done

            echo "Starting workers"

            #start scheduler
            DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT=3600s \
            DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP=3600s \
            srun dask-worker \
            --scheduler-file $scheduler_file \
                --interface {dask_interface} \
                --nworkers 1 

            echo "Killing scheduler"
            kill -9 $dask_pid
            """
        )

        script_file = tempfile.mktemp(prefix="launch-dask.", dir=path_dask_str)
        with open(script_file, "w") as fid:
            fid.write(script)

        print(f"spinning up dask cluster with scheduler:\n  {scheduler_file}")
        print(
            "  nodes: {nodes}, tasks_per_node: {tasks}, wallclock: {wallclock}".format(
                nodes=n_nodes,
                tasks=n_tasks_per_node,
                wallclock=wallclock,
            )
        )
        jobid = (
            check_output(f"sbatch {script_file} " + "awk '{print $1}'", shell=True)
            .decode("utf-8")
            .strip()
            .split(" ")[-1]
        )

        interrupted = False
        previous_handler = signal.getsignal(signal.SIGINT)

        def _handle_sigint(signum, frame):  # pragma: no cover - signal path
            nonlocal interrupted
            interrupted = True

        signal.signal(signal.SIGINT, _handle_sigint)
        try:
            while not os.path.exists(scheduler_file):
                if interrupted:
                    raise KeyboardInterrupt("Interrupted while waiting for scheduler file.")
                time.sleep(1)
        finally:
            signal.signal(signal.SIGINT, previous_handler)

        return scheduler_file, jobid

    def _connect_client(self, timeout=60, retries=12, delay=5):
        """Connect to an existing scheduler with retries."""
        last_exc = None
        for _ in range(retries):
            try:
                self.client = Client(scheduler_file=self.scheduler_file, timeout=timeout)
                return
            except Exception as exc:
                last_exc = exc
                time.sleep(delay)
        raise RuntimeError(
            f"Failed to connect to scheduler after {retries} attempts: {self.scheduler_file}"
        ) from last_exc

    def shutdown(self):
        """Shutdown the Dask client and any launched cluster resources."""
        if self.client is not None:
            try:
                self.client.shutdown()
            except Exception:
                pass
        if self.jobid:
            try:
                check_call(f"scancel {self.jobid}", shell=True)
            except Exception:
                pass
        if getattr(self, "cluster", None) is not None:
            try:
                self.cluster.close()
            except Exception:
                pass


def slurm_available() -> bool:
    """Return True if the SLURM scheduler command is available."""
    return shutil.which("sbatch") is not None
