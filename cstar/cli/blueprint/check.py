import typing as t

import typer

from cstar.applications.core import get_application
from cstar.cli.common import SCHEMA_ERRORS, get_migrator, report_schema_error
from cstar.orchestration.models import Blueprint, BlueprintCore
from cstar.orchestration.serialization import validate_serialized_entity

app = typer.Typer()


@app.command(
    name="check",
    help="Perform content validation on a user-supplied blueprint.",
)
def check(
    path: t.Annotated[str, typer.Argument(help="Path to a blueprint file.")],
) -> None:
    """Perform content validation on a user-supplied blueprint.

    The schema version is checked first: a blueprint newer than this build
    reads, or one without an automatic migration, is reported before its
    content is validated.

    Raises
    ------
    typer.Exit
        If the schema version is unsupported or the blueprint needs migration.
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

    bp_type: type[Blueprint] = get_application(application).blueprint
    result = validate_serialized_entity(path, bp_type)

    if result.item is None:
        raise typer.BadParameter(result.error_msg)

    print(f"The blueprint `{result.item.name}` is valid")
