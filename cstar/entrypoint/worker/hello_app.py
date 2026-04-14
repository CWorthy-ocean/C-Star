import typing as t

from cstar.base.log import get_logger
from cstar.entrypoint.xrunner import (
    XBlueprintRunner,
    XRunnerResult,
)
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.application import ApplicationRegistry, register_app_handler
from cstar.orchestration.models import Blueprint

APP_HELLOWORLD: t.Literal["hello_world"] = "hello_world"
"""The unique identifier for the hello-world application type."""

log = get_logger(__name__)


@register_app_handler(ApplicationRegistry.BLUEPRINT.value, APP_HELLOWORLD)
class HelloWorldBlueprint(Blueprint):
    """A simple blueprint demonstrating the integration of a Blueprint and it's
    runner application.
    """

    application: str = APP_HELLOWORLD
    """The application identifier."""
    target: str
    """The person to say hello to."""


@register_app_handler(ApplicationRegistry.RUNNER.value, APP_HELLOWORLD)
class HelloWorldRunner(XBlueprintRunner[HelloWorldBlueprint]):
    """Worker class to execute a simple "Hello, world" application specified via blueprint."""

    application: str = APP_HELLOWORLD
    """The application identifier."""

    @t.override
    def __call__(self) -> XRunnerResult[HelloWorldBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        self.log.debug("Executing handler function on blueprint runner")
        if not isinstance(self.blueprint, HelloWorldBlueprint):
            raise ValueError(
                f"HelloWorldBlueprint expected. Received: {type(self.blueprint)}"
            )

        print(f"Hello, {self.blueprint.target}")
        return self.set_result(ExecutionStatus.COMPLETED)
