import typing as t

from cstar.entrypoint.xrunner import XBlueprintRunner, XRunnerResult
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.application import register_blueprint, register_runner
from cstar.orchestration.models import Application, Blueprint


@register_blueprint(Application.HELLO_WORLD)
class HelloWorldBlueprint(Blueprint):
    """A simple blueprint demonstrating the integration of a Blueprint and it's
    runner application.
    """

    application: str = Application.HELLO_WORLD
    """The application identifier."""
    target: str
    """The person to say hello to."""


@register_runner(Application.HELLO_WORLD)
class HelloWorldRunner(XBlueprintRunner[HelloWorldBlueprint]):
    """Worker class to execute a simple "Hello, world" application specified via blueprint."""

    application: str = Application.HELLO_WORLD
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
