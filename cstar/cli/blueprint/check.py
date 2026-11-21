import typing as t

import typer

from cstar.applications.core import get_application
from cstar.cli.common import SCHEMA_ERRORS, get_migrator, report_schema_error
from cstar.orchestration.models import Blueprint, BlueprintCore
from cstar.orchestration.serialization import validate_serialized_entity
from cstar.system.migration import CstarSchemaTooNewError

app = typer.Typer()


@app.command(
    name="check",
    help="Perform content validation on a user-supplied blueprint.",
)
def check(
    path: t.Annotated[str, typer.Argument(help="Path to a blueprint file.")],
) -> None:
    """Perform content validation on a user-supplied blueprint.

    The schema version is checked first: a blueprint of a newer major than
    this build reads, or one without an automatic migration, is reported before
    its content is validated. A newer minor is validated, and reported as too
    new if it holds anything this build does not know. A blueprint that passes
    is then checked as its application checks it before a run
    (`ApplicationDefinition.blueprint_problems`).

    Raises
    ------
    typer.Exit
        If the schema version is unsupported, the blueprint needs migration, or
        its application refuses its values.
    typer.BadParameter
        If the blueprint content is invalid.
    """
    result = validate_serialized_entity(path, BlueprintCore)
    if result.item is None:
        raise typer.BadParameter(result.error_msg)

    application = result.item.application
    try:
        plan = get_migrator(application).plan(result.item.model_dump())
    except SCHEMA_ERRORS as ex:
        report_schema_error(path, ex)

    if not plan.is_compatible:
        print(
            f"Blueprint {path!r} is {application} schema {plan.source} and needs "
            f"migration to {plan.target}; run: cstar blueprint migrate {path}"
        )
        raise typer.Exit(1)

    app_def = get_application(application)
    bp_type: type[Blueprint] = app_def.blueprint
    result = validate_serialized_entity(path, bp_type)

    # a newer minor holding what this build does not know is refused as too new
    if isinstance(result.error, CstarSchemaTooNewError):
        report_schema_error(path, result.error)
    if result.item is None:
        raise typer.BadParameter(result.error_msg)

    if problems := app_def.blueprint_problems(result.item):
        typer.echo(
            f"The blueprint `{result.item.name}` has invalid values:\n  "
            + "\n  ".join(problems),
            err=True,
        )
        raise typer.Exit(1)

    print(f"The blueprint `{result.item.name}` is valid")
