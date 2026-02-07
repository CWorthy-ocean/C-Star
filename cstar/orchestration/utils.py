import os
import typing as t
from datetime import datetime, timezone

ENV_CSTAR_ORCH_DELAYS: t.Literal["CSTAR_ORCH_DELAYS"] = "CSTAR_ORCH_DELAYS"
"""Environment variable containing configurable delay for the orchestrator."""

ENV_CSTAR_ORCH_TRX_FREQ: t.Literal["CSTAR_ORCH_TRX_FREQ"] = "CSTAR_ORCH_TRX_FREQ"
"""Environment variable containing the time span for time-splitting transforms."""

ENV_CSTAR_CMD_CONVERTER_OVERRIDE: t.Literal["CSTAR_CMD_CONVERTER_OVERRIDE"] = (
    "CSTAR_CMD_CONVERTER_OVERRIDE"
)
"""Environment variable containing an overridden mapping key to apply when
converting applications into CLI commands."""

ENV_CSTAR_ORCH_RUNID: t.Literal["CSTAR_RUNID"] = "CSTAR_RUNID"
"""Environment variable containing a unique run identifier used by the orchestrator."""

ENV_CSTAR_SLURM_ACCOUNT: t.Literal["CSTAR_SLURM_ACCOUNT"] = "CSTAR_SLURM_ACCOUNT"
"""Environment variable containing the account ID to be used by the SLURM scheduler."""

ENV_CSTAR_SLURM_MAX_WALLTIME: t.Literal["CSTAR_SLURM_MAX_WALLTIME"] = (
    "CSTAR_SLURM_MAX_WALLTIME"
)
"""Environment variable containing the maximum walltime allowed by the SLURM scheduler."""

ENV_CSTAR_SLURM_QUEUE: t.Literal["CSTAR_SLURM_QUEUE"] = "CSTAR_SLURM_QUEUE"
"""Environment variable containing the SLURM priority (queue) used by the SLURM scheduler."""


def get_run_id() -> str:
    """Retrieve the current run-id.

    Generate a new run-id if not found in the environment.

    Returns
    -------
    str
    """
    new_run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return os.getenv(ENV_CSTAR_ORCH_RUNID, new_run_id)
