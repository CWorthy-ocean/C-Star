import os
import typing as t

from cstar.base.env import ENV_CSTAR_RUNID, EnvVar, generate_run_id

_GROUP_ORCH: t.Final[str] = "Orchestration"
_GROUP_DEV: t.Final[str] = "Developer Only"


ENV_CSTAR_SLURM_ACCOUNT: t.Annotated[
    t.Literal["CSTAR_SLURM_ACCOUNT"],
    EnvVar(
        "Account ID to be used by the SLURM scheduler.",
        _GROUP_ORCH,
        "",
    ),
] = "CSTAR_SLURM_ACCOUNT"
"""Environment variable containing the account ID to be used by the SLURM scheduler."""

ENV_CSTAR_SLURM_MAX_WALLTIME: t.Annotated[
    t.Literal["CSTAR_SLURM_MAX_WALLTIME"],
    EnvVar(
        "Maximum walltime allowed by the SLURM scheduler.",
        _GROUP_ORCH,
        "48:00:00",
    ),
] = "CSTAR_SLURM_MAX_WALLTIME"
"""Environment variable containing the maximum walltime allowed by the SLURM scheduler."""

ENV_CSTAR_SLURM_QUEUE: t.Annotated[
    t.Literal["CSTAR_SLURM_QUEUE"],
    EnvVar(
        "SLURM priority (queue) used by the SLURM scheduler.",
        _GROUP_ORCH,
        "",
    ),
] = "CSTAR_SLURM_QUEUE"
"""Environment variable containing the SLURM priority (queue) used by the SLURM scheduler."""


def get_run_id() -> str:
    """Retrieve the current run-id.

    Generate a new run-id if not found in the environment.

    Returns
    -------
    str
    """
    return os.getenv(ENV_CSTAR_RUNID, generate_run_id())
