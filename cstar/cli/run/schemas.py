import typing as t
from pathlib import Path

import typer

from cstar.cli.blueprint.schemas import output_callback
from cstar.cli.common import write_model_schema
from cstar.orchestration.manifest import RunManifest, StepRecord

app = typer.Typer()

CMD_NAME: t.Final[str] = "schemas"
HELP_SHORT = "Generate the latest run manifest and step record schemas."
HELP_LONG = f"""\
{HELP_SHORT}

Writes `run_manifest_schema.<version>.json` and `step_record_schema.<version>.json`
to the output directory.
"""

SCHEMA_MODELS: t.Final[dict[str, type[RunManifest] | type[StepRecord]]] = {
    "run_manifest": RunManifest,
    "step_record": StepRecord,
}
"""The model behind each schema file, by the stem of the file's name."""


@app.command(name=CMD_NAME, help=HELP_LONG, short_help=HELP_SHORT)
def schemas(
    output: t.Annotated[
        str,
        typer.Argument(
            help="Path where the schemas will be serialized",
            dir_okay=True,
            file_okay=False,
            path_type=str,
            callback=output_callback,
        ),
    ] = "~/code/cstar/docs/schemas/run",
) -> None:
    """Generate the latest run manifest and step record schemas."""
    for stem, model in SCHEMA_MODELS.items():
        version = model.model_fields["schema_version"].default
        path = write_model_schema(model, Path(output) / f"{stem}_schema.{version}.json")
        print(f"Persisted {stem!r} schema to: {path}")
