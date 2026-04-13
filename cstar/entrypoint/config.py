import logging
import os
import typing as t
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from cstar.base.env import get_env_item
from cstar.base.log import parse_log_level_name
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)

JOBFILE_DATE_FORMAT: t.Final[str] = "%Y%m%d_%H%M%S"


def _generate_job_name() -> str:
    """Generate a unique job name based on the current date and time."""
    now_utc = datetime.now(timezone.utc)
    formatted_now_utc = now_utc.strftime(JOBFILE_DATE_FORMAT)
    return f"cstar_worker_{formatted_now_utc}"


class JobConfig(BaseModel):
    """Configuration required to submit HPC jobs."""

    account_id: t.Annotated[str, Field(frozen=True)]
    """HPC account used for billing."""
    walltime: t.Annotated[str, Field(frozen=True)]
    """Maximum walltime allowed for job."""
    priority: t.Annotated[str, Field(frozen=True)]
    """Job priority."""
    job_name: t.Annotated[str, Field(frozen=True)] = _generate_job_name()
    """User-friendly job name."""


class ServiceConfiguration(BaseModel):
    """Configuration options for a Service."""

    as_service: bool = False
    """Determines lifetime of the service.

    When `True`, calling `execute` on the service will run continuously until
    shutdown criteria are met. When `False`, the service completes a single
    pass through the service lifecycle and automatically exits.
    """
    loop_delay: float = Field(default=0.0, ge=0.0)
    """Duration (in seconds) of a delay between iterations of the main event loop."""
    health_check_frequency: float | None = Field(default=None, ge=0.0)
    """Time (in seconds) between calls to a health check handler.

    NOTE:
    - A value of `None` disables health checks.
    - A value of `0` triggers the health check on every iteration.
    """
    log_level: int = logging.INFO
    """The logging level used by the service."""
    health_check_log_threshold: int = Field(default=10, ge=3)
    """The number of health-checks that may be missed before logging."""
    name: str = "Service"
    """A user-friendly name for logging."""

    @property
    def healthcheck_enabled(self) -> bool:
        """Return `True` when the health check frequency is non-null.

        Returns
        -------
        bool
        """
        return self.health_check_frequency is not None


def get_service_config(log_level: int | str, name: str) -> ServiceConfiguration:
    """Create a ServiceConfiguration instance using CLI arguments.

    Parameters
    ----------
    log_level : int or str
        The log level to be used by the worker

    Returns
    -------
    ServiceConfiguration
    """
    level = parse_log_level_name(log_level)

    return ServiceConfiguration(
        as_service=True,
        loop_delay=5,
        health_check_frequency=None,
        log_level=level,
        health_check_log_threshold=10,
        name=name or "Runner",
    )


def get_job_config() -> JobConfig:
    """Create and configure a `JobConfig` instance from environment variables.

    Returns
    -------
    JobConfig
    """
    account_id: str = get_env_item(ENV_CSTAR_SLURM_ACCOUNT).value
    walltime: str = get_env_item(ENV_CSTAR_SLURM_MAX_WALLTIME).value
    priority: str = get_env_item(ENV_CSTAR_SLURM_QUEUE).value

    return JobConfig(account_id=account_id, walltime=walltime, priority=priority)


def configure_environment(log: logging.Logger) -> None:
    """Configure the environment variables required by the worker.

    Parameters
    ----------
    log : logging.Logger
        A logger to log configuration details.

    Returns
    -------
    None
    """
    # ensure git works on distributed file-system, e.g. lustre
    os.environ["GIT_DISCOVERY_ACROSS_FILESYSTEM"] = "1"
    log.debug("Git discovery across file-system enabled.")
