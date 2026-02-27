import asyncio
import sys
import typing as t
from enum import StrEnum, auto
from pathlib import Path

import typer

from cstar.base.env import ENV_CSTAR_STATE_HOME, get_env_item
from cstar.base.utils import additional_files_dir
from cstar.orchestration.dag_runner import build_and_run_dag

app = typer.Typer()

BP_DEFAULT: t.Final[str] = (
    "~/code/cstar/cstar/additional_files/templates/blueprint.yaml"
)
BP_OUTDIR_DEFAULT: t.Final[str] = "output_dir: ."


class WorkplanTemplate(StrEnum):
    FANOUT = auto()
    LINEAR = auto()
    PARALLEL = auto()
    SINGLE_STEP = auto()


def create_host_workplan(
    template: WorkplanTemplate, bp_path: Path, output_path: Path, run_id: str
) -> Path:
    """Replace the default blueprint path in a template and write the
    modified workplan in a new location.
    """
    run_dir = output_path / run_id
    assets_dir = additional_files_dir()
    templates_dir = assets_dir / "templates"
    template_path = templates_dir / "wp" / f"{template}.yaml"

    bp_source_path = templates_dir / "bp/blueprint.yaml"
    bp_target_path = run_dir / bp_path.name

    # update the workplan output directory found in the template
    bp_content = bp_source_path.read_text()
    bp_content = bp_content.replace(
        BP_OUTDIR_DEFAULT, f"output_dir: {output_path.as_posix()}"
    )
    # write the modified blueprint to the working directory
    bp_target_path.parent.mkdir(parents=True, exist_ok=True)
    bp_target_path.write_text(bp_content)

    # set paths in the template workplan to the just-created blueprint path
    wp_content = template_path.read_text()
    wp_content = wp_content.replace(BP_DEFAULT, bp_target_path.as_posix())

    # write the modified workplan to the working directory.
    wp_path = run_dir / f"{template}-host.yaml"
    wp_path.parent.mkdir(parents=True, exist_ok=True)
    wp_path.write_text(wp_content)

    return wp_path


def _run(wp_path: Path, output_path: Path, run_id: str) -> None:
    """Execute the DAG synchronously."""
    try:
        asyncio.run(build_and_run_dag(wp_path, run_id, output_path))
        print(f"Completed execution of composed workplan: {wp_path}.")
    except Exception as ex:
        print(
            f"Composed workplan run at `{wp_path}` has completed unsuccessfully: {ex}"
        )


@app.command()
def compose(
    workplan: t.Annotated[
        str | None, typer.Argument(help="Path to a workplan file.")
    ] = None,
    blueprint: t.Annotated[
        str | None, typer.Argument(help="Path to a blueprint file.")
    ] = None,
    output_dir: t.Annotated[
        str,
        typer.Option(
            help="Override the output in the blueprint file(s) with this path."
        ),
    ] = get_env_item(ENV_CSTAR_STATE_HOME).value,
    run_id: t.Annotated[
        str,
        typer.Option(help="The unique identifier for an execution of the workplan."),
    ] = "...",
    template: t.Annotated[
        WorkplanTemplate | None,
        typer.Option(help="Specify the workplan template to populate."),
    ] = None,
    run_plan: t.Annotated[
        str,
        typer.Option(help="Specify `1` to execute the workplan"),
    ] = "0",
) -> Path:
    """Execute a workplan composed with a user-supplied blueprint.

    Specify a previously used run_id option to re-start a prior run.

    Returns
    -------
    Path
        The path to the workplan that was generated.
    """
    output_path = Path(output_dir).expanduser().resolve()
    wp_path = Path(workplan) if workplan is not None else None
    bp_path = Path(blueprint) if blueprint is not None else None
    template = WorkplanTemplate.SINGLE_STEP if not template else template

    if wp_path is None and bp_path is None and not template:
        print("Run aborted. A workplan, blueprint, or template must be provided")
        sys.exit(1)

    if bp_path:
        # host the blueprint in a workplan template
        wp_path = create_host_workplan(template, bp_path, output_path, run_id)
        print(f"Running template workplan at `{wp_path}` with blueprint at `{bp_path}`")
    else:
        bp_path = Path(BP_DEFAULT)
        wp_path = create_host_workplan(template, bp_path, output_path, run_id)
        print(f"Running workplan at `{wp_path}` with sample blueprint at `{bp_path}`")

    if wp_path is None or not wp_path.exists():
        raise ValueError("Workplan path is malformed.")

    if run_plan == "1":
        _run(wp_path, output_path, run_id)

    return wp_path


if __name__ == "__main__":
    typer.run(compose)
