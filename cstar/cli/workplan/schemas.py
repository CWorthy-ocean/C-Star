import typing as t
from pathlib import Path

import typer

from cstar.cli.blueprint.schemas import output_callback
from cstar.cli.common import write_model_schema
from cstar.orchestration.models import Workplan

app = typer.Typer()

CMD_NAME: t.Final[str] = "schemas"
HELP_SHORT = "Generate the latest workplan schema."
HELP_LONG = f"""\
{HELP_SHORT}

Writes `workplan_schema.<version>.json` to the output directory.
"""


@app.command(name=CMD_NAME, help=HELP_LONG, short_help=HELP_SHORT)
def schemas(
    output: t.Annotated[
        str,
        typer.Argument(
            help="Path where the schema will be serialized",
            dir_okay=True,
            file_okay=False,
            path_type=str,
            callback=output_callback,
        ),
    ] = "~/code/cstar/docs/schemas/wp",
) -> None:
    """Generate the latest workplan schema."""
    version = Workplan.model_fields["schema_version"].default
    path = write_model_schema(
        Workplan, Path(output) / f"workplan_schema.{version}.json"
    )
    print(f"Persisted 'workplan' schema to: {path}")
