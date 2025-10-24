import argparse
import json
import textwrap
import typing as t
from pathlib import Path

from cstar.cli.core import PathConverterAction, RegistryResult, cli_activity
from cstar.orchestration.models import RomsMarblBlueprint, Workplan
from cstar.system.manager import cstar_sysmgr as system_mgr


def check_or_create_path(ns: argparse.Namespace) -> None:
    """Verify a valid directory was supplied.

    If necessary, prompts the user for permission to create when interactive
    mode is enabled.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    path: Path = ns.path
    interactive: bool = ns.interactive

    if path and not path.exists() and interactive:
        do_create = input("The directory does not exist. Create it? (yes/no): ")
        if "y" in do_create.lower():
            path.mkdir(parents=True, exist_ok=True)
        else:
            msg = "Unable to create template without valid directory"
            raise ValueError(msg)

    if path and not path.exists():
        msg = f"The specified directory does not exist: {path}"
        raise ValueError(msg)


def get_inline_output(
    template_type: t.Literal["workplan", "blueprint"],
    schema: str,
    template: str,
) -> str:
    """Generate the schema and template output formatted for stdout.

    Parameters
    ----------
    template_type : str
        The template type that will be generated.
    schema : str
        The schema content for the selected model.
    template : str
        The template content for the selected model.

    Returns
    -------
    str
        Formatted output
    """
    delimiter = "*" * 80

    s_header = textwrap.dedent(
        f"""\
        {delimiter}
        * {template_type.capitalize()} schema
        {delimiter}
        """
    )
    t_header = textwrap.dedent(
        f"""\
        {delimiter}
        * {template_type.capitalize()} template
        {delimiter}
        """
    )
    return f"{s_header}{schema}\n\n{t_header}{template}"


def replace_schema_directive(template: str, schema_path: Path | None) -> str:
    """Replace the schema directive in the original template with a path
    to a newly generated schema file.

    Parameters
    ----------
    template : str
        The template content to augment
    schema_path : Path or None
        The path to a schema file

    Returns
    -------
    str
        The modified template content
    """
    template_lines = template.split("\n")
    if not template_lines[0].startswith("#"):
        return template

    if schema_path:
        schema_ref = f"# yaml-language-server: $schema=file://{schema_path}"
    else:
        schema_ref = "# yaml-language-server: $schema=<schema-uri>"

    template_lines[0] = schema_ref
    return "\n".join(template_lines)


def handle(ns: argparse.Namespace) -> None:
    """The action handler for the template-create action.

    Triggers creation of a sample template.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    check_or_create_path(ns)

    template = ns.type
    path: Path | None = ns.path

    tpl_name = f"{template}.yaml"
    schema_name = f"{template}-schema.yaml"
    tpl_source_path = system_mgr.environment.template_root / tpl_name

    if not tpl_source_path.exists():
        msg = f"Unable to read template file from `{tpl_source_path}`"
        raise ValueError(msg)

    schema_path: Path | None = None
    template_path: Path | None = None

    if path is not None:
        template_path = path / tpl_name
        schema_path = path / schema_name

    if template == "workplan":
        schema = json.dumps(Workplan.model_json_schema(), indent=4)
    else:
        schema = json.dumps(RomsMarblBlueprint.model_json_schema(), indent=4)

    original_template = tpl_source_path.read_text(encoding="utf-8")
    template_content = replace_schema_directive(original_template, schema_path)

    if template_path and schema_path:
        schema_path.write_text(schema, encoding="utf-8")
        template_path.write_text(template_content, encoding="utf-8")

        message = textwrap.dedent(
            f"""\
            {template.capitalize()} schema written to: {schema_path}
            {template.capitalize()} template written to: {template_path}
            """
        )
    else:
        message = get_inline_output(template, schema, template_content)

    print(message)


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the blueprint-template command into the CLI."""
    command: t.Literal["template"] = "template"
    action: t.Literal["create"] = "create"

    def _fn(sp: argparse._SubParsersAction) -> argparse.ArgumentParser:
        """Add a parser for the command: `cstar template create -o path/to/output.yaml` -t workplan"""
        parser: argparse.ArgumentParser = sp.add_parser(
            action,
            help="Generate an empty template.",
            description="Generate an empty template.",
        )
        parser.add_argument(
            "-o",
            "--output",
            dest="path",
            help=(
                "Output path for the blueprint. If not provided, "
                "the template is written to stdout."
            ),
            required=False,
            default=None,
            action=PathConverterAction,
        )
        parser.add_argument(
            "-t",
            "--type",
            dest="type",
            help=("The template type to create."),
            required=True,
            choices=["blueprint", "workplan"],
        )
        parser.set_defaults(template=action)
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
