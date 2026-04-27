import typing as t
from dataclasses import dataclass

from cstar.applications.utils import register_application
from cstar.entrypoint.xrunner import XBlueprintRunner, XRunnerResult
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import ApplicationDefinition, Blueprint, Transform

_APP_NAME: t.Literal["hello_world"] = "hello_world"


class HelloWorldBlueprint(Blueprint):
    """A simple blueprint demonstrating the integration of a Blueprint and it's
    runner application.
    """

    application: str = _APP_NAME
    """The application identifier."""
    target: str
    """The person to say hello to."""


class HelloWorldRunner(XBlueprintRunner[HelloWorldBlueprint]):
    """Worker class to execute a simple "Hello, world" application specified via blueprint."""

    application: str = _APP_NAME
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

        print(f"Hello, {self.blueprint.target}")
        return self.set_result(ExecutionStatus.COMPLETED)


@register_application
@dataclass
class HelloWorldApplication(
    ApplicationDefinition[HelloWorldBlueprint, HelloWorldRunner]
):
    name: str = _APP_NAME
    blueprint: type[HelloWorldBlueprint] = HelloWorldBlueprint
    runner: type[HelloWorldRunner] = HelloWorldRunner
    applicable_transforms: tuple[type[Transform]] = ()
    resources_needed: t.Any = ()
    output_override_method: t.Callable = None
    long_name: str = "Hello World"
