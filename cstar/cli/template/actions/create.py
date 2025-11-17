import argparse
import json
import textwrap
import typing as t
from pathlib import Path

from cstar.cli.core import PathConverterAction, RegistryResult, cli_activity
from cstar.orchestration.models import RomsMarblBlueprint, Workplan

TemplateTypes: t.TypeAlias = t.Literal["workplan", "blueprint"]


def check_or_create_path(path: Path | None, interactive: bool = False) -> None:
    """Verify a valid directory was supplied.

    If necessary, prompts the user for permission to create the directory when
    interactive mode is enabled.

    Parameters
    ----------
    path : Path
        A path to a directory the generated template should be stored in
    interactive : bool
        Specify if interactive mode is enabled

    """
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
    template_type: TemplateTypes,
    schema: str,
    template_content: str,
) -> str:
    """Generate the schema and template output formatted for stdout.

    Parameters
    ----------
    template_type : str
        The template type that will be generated.
    schema : str
        The schema content for the selected model.
    template_content : str
        The content of the template for the selected model.

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
    return f"{s_header}{schema}\n\n{t_header}{template_content}"


def replace_schema_directive(template_content: str, schema_path: Path | None) -> str:
    """Replace the schema directive in the original template with a path
    to a newly generated schema file.

    Parameters
    ----------
    template_content : str
        The content to augment
    schema_path : Path or None
        The path to a schema file

    Returns
    -------
    str
        The modified template content
    """
    template_lines = template_content.split("\n")
    if template_lines[0].startswith("#"):
        template_lines.pop(0)

    uri_tag = "<schema-uri>"
    schema_ref = f"# yaml-language-server: $schema={uri_tag}"
    if schema_path is not None:
        schema_ref.replace(uri_tag, schema_path.as_posix())

    template_lines.insert(0, schema_ref)
    return "\n".join(template_lines)


async def generate_template(path: Path | None, template_type: TemplateTypes) -> str:
    """The action handler for the template-create action.

    Triggers creation of a sample template.

    Parameters
    ----------
    path : Path
        A path to a directory the generated template should be stored in
    template_type : TemplateTypes
        The type of template to be created

    Returns
    -------
    str
        The template content if the output path is None. Otherwise, a message
        describing where outputs were written.
    """
    check_or_create_path(path)

    tpl_name = f"{template_type}.yaml"
    schema_name = f"{template_type}-schema.yaml"
    subdir = "wp" if template_type == "workplan" else "bp"

    # manually locate dir; use of cstar sys_mgr results in circular reference
    fp = (
        Path(__file__).parent.parent.parent.parent
        / "additional_files/templates"
        / subdir
    )
    tpl_source_path = fp / tpl_name

    if not tpl_source_path.exists():
        msg = f"Unable to read template file from `{tpl_source_path}`"
        raise ValueError(msg)

    schema_path: Path | None = None
    template_path: Path | None = None

    if path is not None:
        template_path = path / tpl_name
        schema_path = path / schema_name

    if template_type == "workplan":
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
            {template_type.capitalize()} schema written to: {schema_path}
            {template_type.capitalize()} template written to: {template_path}
            """
        )
    else:
        message = get_inline_output(template_type, schema, template_content)

    return message


async def handle(ns: argparse.Namespace) -> None:
    """The action handler for the template-create action.

    Triggers creation of a sample template.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    msg = await generate_template(ns.path, ns.type)
    print(msg)


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the blueprint-template command into the CLI.

    Returns
    -------
    RegistryResult
        A 2-tuple containing ((command name, action name), parser function)
    """
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
