import typing as t

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerResult,
    register_application,
)
from cstar.base.adapter import SchemaAdapter
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint

APP_NAME: t.Final[str] = "hello_world"


class HelloWorldBlueprint(Blueprint):
    """A simple blueprint demonstrating the integration of a Blueprint and it's
    runner application.
    """

    application: str = APP_NAME
    """The application identifier."""
    target: str
    """The person to say hello to."""


class HelloWorldRunner(BlueprintRunner[HelloWorldBlueprint]):
    """Worker class to execute a simple "Hello, world" application specified via blueprint."""

    @t.override
    async def run(self) -> RunnerResult[HelloWorldBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        self.log.debug("Executing handler function on blueprint runner")

        print(f"Hello, {self.blueprint.target}")
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result


APP_HW_SCHEMA_1_0_0: t.Final[str] = "1.0.0"


hw_bounds = {
    "min": APP_HW_SCHEMA_1_0_0,
    "max": APP_HW_SCHEMA_1_0_0,
}
"""Schema bounds for the hello_world blueprint schema.

The schema bounds enable the migration tool to:
- automatically set version to  minimum version for a blueprint that predated versioning
- configure which version it will target for updates
"""


class HelloWorldSchemaAdapterV1V1(SchemaAdapter):
    """Schema migration sample for hello_world application.

    Adapting `1.0.0` to `1.0.0`:
    - no-op; illustrative purposes only
    """

    @classmethod
    def application(cls) -> str:
        return APP_NAME

    @classmethod
    def source(cls) -> str:
        return APP_HW_SCHEMA_1_0_0

    @classmethod
    def target(cls) -> str:
        # no migration is performed when source == target
        return APP_HW_SCHEMA_1_0_0

    @classmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        # example: self.model["channel"] = "email"
        return {**model}


@register_application
class HelloWorldApplication(
    ApplicationDefinition[HelloWorldBlueprint, HelloWorldRunner],
):
    name = APP_NAME
    long_name = APP_NAME
    runner = HelloWorldRunner
    blueprint = HelloWorldBlueprint
    applicable_transforms = ()
    migrations = (HelloWorldSchemaAdapterV1V1,)
