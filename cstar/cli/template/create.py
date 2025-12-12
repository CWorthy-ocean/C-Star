import enum
import json
import textwrap
import typing as t
from pathlib import Path

from typer import Argument, Option, Typer

from cstar.orchestration.models import RomsMarblBlueprint, Workplan


class TemplateType(enum.StrEnum):
    workplan = enum.auto()
    blueprint = enum.auto()


def get_inline_output(
    template_type: TemplateType,
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


def generate_template(path: Path | None, template_type: TemplateType) -> str:
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
        path.mkdir(parents=True, exist_ok=True)
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


app = Typer()


@app.command()
def generate(
    path: t.Annotated[
        Path,
        Argument(
            help="The output path for the generated document. If not provided, the template is written to stdout.",
        ),
    ],
    template_type: t.Annotated[
        TemplateType,
        Option(
            help="The type of template to create",
        ),
    ] = TemplateType.blueprint,
) -> None:
    """Generate a template document as a starting point."""
    msg = generate_template(path, template_type)
    print(msg)


def main() -> None:
    """Entrypoint for the create-template command."""
    app()


if __name__ == "__main__":
    main()
