import json
import typing as t
from pathlib import Path

import typer

from cstar.applications.core import get_application
from cstar.base.log import get_logger
from cstar.orchestration.models import Blueprint

app = typer.Typer()
log = get_logger(__name__)

CMD_NAME: t.Final[str] = "schemas"
HELP_SHORT = "Generate the latest blueprint schemas."
HELP_LONG = f"""\
{HELP_SHORT}

Generates all built-in schemas when no application names are user-supplied.
"""

ARG_APP: t.Final[str] = "-a"
"""Argument key for passing application names"""


def output_callback(value: str) -> str:
    """Normalize the path supplied by the user by expanding user directories
    and resolving symlinks.

    Parameters
    ----------
    value : str
        The user-supplied path string.

    Returns
    -------
    str
    """
    return Path(value).expanduser().as_posix()


def generate_schema(
    bp_type: type[Blueprint],
    output_dir: Path,
) -> Path:
    """Generate a schema for the supplied blueprint.

    Parameters
    ----------
    bp_type : type[Blueprint]
        The blueprint type to use for schema generation.
    output_dir : Path
        The path to a directory where outputs will be written.

    Returns
    -------
    Path
        The path to the generated schema file.
    """
    schema = bp_type.model_json_schema()
    content = json.dumps(schema, indent=4)

    app_name = schema["properties"]["application"]["default"]
    version = schema["properties"]["schema_version"]["default"] or "1.0.0"
    file_name = f"{app_name}_schema.{version}.json"
    schema_dir = output_dir / app_name
    schema_dir.mkdir(parents=True, exist_ok=True)
    schema_path = schema_dir / file_name

    nbytes = schema_path.write_text(f"{content}\n")
    if not nbytes:
        print("Empty schema write occurred")
        raise typer.Exit(1)

    return schema_path


def all_applications() -> list[str]:
    """Return the default list of built-in applications.

    Returns
    -------
    list[str]
    """
    return [
        "plotter",
        "roms_marbl",
        "hello_world",
        "nest_ic",
        "upscaler",
    ]


@app.command(name=CMD_NAME, help=HELP_LONG, short_help=HELP_SHORT)
def schemas(
    applications: t.Annotated[
        list[str],
        typer.Option(
            ARG_APP,
            default_factory=all_applications,
            help="Supply the application name(s) to generate schemas for",
        ),
    ],
    output: t.Annotated[
        str,
        typer.Argument(
            help="Path where the schemas will be serialized",
            dir_okay=True,
            file_okay=False,
            path_type=str,
            callback=output_callback,
        ),
    ] = "~/code/cstar/docs/schemas/bp",
) -> None:
    """Generate the latest blueprint schemas."""
    output_dir = Path(output)
    try:
        apps = {app_name: get_application(app_name) for app_name in applications}
    except ValueError as ex:
        names = ", ".join(applications)
        print(f"An unknown application name was supplied: {names}")
        raise typer.Exit(1) from ex

    for app_name, app in apps.items():
        path = generate_schema(app.blueprint, output_dir)
        print(f"Persisted {app_name!r} schema to: {path}")
