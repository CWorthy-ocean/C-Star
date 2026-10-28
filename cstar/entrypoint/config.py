import logging

from pydantic import BaseModel, Field

from cstar.base.log import parse_log_level_name


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


def get_service_config(
    log_level: int | str, name: str = "Runner"
) -> ServiceConfiguration:
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
        name=name,
    )
