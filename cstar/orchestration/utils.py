import os
import typing as t
from datetime import datetime, timezone

from cstar.base.env import EnvVar

_GROUP_ORCH: t.Final[str] = "Orchestration"
_GROUP_DEV: t.Final[str] = "Developer Only"


def generate_run_id() -> str:
    """Generate a unique run identifier based on the current time."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


ENV_CSTAR_ORCH_DELAYS: t.Annotated[
    t.Literal["CSTAR_ORCH_DELAYS"],
    EnvVar(
        "Configurable delay for the orchestrator (seconds).",
        _GROUP_ORCH,
        "0.1, 1, 2, 5, 15, 30, 60",
    ),
] = "CSTAR_ORCH_DELAYS"
"""Environment variable containing configurable delay for the orchestrator."""

ENV_CSTAR_ORCH_TRX_FREQ: t.Annotated[
    t.Literal["CSTAR_ORCH_TRX_FREQ"],
    EnvVar(
        "Time span for time-splitting transforms (e.g. Monthly, Yearly).",
        _GROUP_ORCH,
        "Monthly",
    ),
] = "CSTAR_ORCH_TRX_FREQ"
"""Environment variable containing the time span for time-splitting transforms."""

ENV_CSTAR_CMD_CONVERTER_OVERRIDE: t.Annotated[
    t.Literal["CSTAR_CMD_CONVERTER_OVERRIDE"],
    EnvVar(
        "Overridden mapping key to apply when converting applications into CLI commands.",
        _GROUP_DEV,
        "",
    ),
] = "CSTAR_CMD_CONVERTER_OVERRIDE"
"""Environment variable containing an overridden mapping key to apply when
converting applications into CLI commands."""

ENV_CSTAR_ORCH_RUNID: t.Annotated[
    t.Literal["CSTAR_RUNID"],
    EnvVar(
        description="Unique run identifier used by the orchestrator.",
        group=_GROUP_DEV,
        default_factory=lambda _: generate_run_id(),
    ),
] = "CSTAR_RUNID"
"""Environment variable containing a unique run identifier used by the orchestrator."""

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

ENV_CSTAR_ORCH_REQD_ENV: t.Annotated[
    t.Literal["CSTAR_ORCH_REQD_ENV"],
    EnvVar(
        "A comma-delimited list of required env configuration values. TEMPORARY (move to CStarEnvironment / per-platform settings?).",
        _GROUP_ORCH,
        "CSTAR_SLURM_ACCOUNT,CSTAR_SLURM_QUEUE",
    ),
] = "CSTAR_ORCH_REQD_ENV"
"""A comma-delimited list of required env configuration values. TEMPORARY (move to CStarEnvironment / per-platform settings?)."""


def get_run_id() -> str:
    """Retrieve the current run-id.

    Generate a new run-id if not found in the environment.

    Returns
    -------
    str
    """
    return os.getenv(ENV_CSTAR_ORCH_RUNID, generate_run_id())
