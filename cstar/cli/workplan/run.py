import asyncio
import hashlib
import os
import textwrap
import typing as t
from pathlib import Path

import typer
import yaml
from click.core import ParameterSource
from pydantic import BaseModel, ValidationError

from cstar.base.env import (
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_RUNID,
    ENV_CSTAR_STATE_HOME,
    REMOVED_SETTINGS,
)
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.cli.common import (
    DEFAULT_LOG_LEVEL_CHOICE,
    SCHEMA_ERRORS,
    LogLevelOption,
    cb_pipeline,
    exit_on_refused_settings,
    format_validation_errors,
    localize_and_migrate,
    normalize_runid,
    present,
    schema_message,
    set_env,
)
from cstar.cli.workplan.shared import (
    colored,
    console,
    exit_on_external_problems,
    list_runs,
    preprocess_varfile,
    preprocess_vars,
)
from cstar.entrypoint.utils import (
    ARG_CLOBBER,
    ARG_CLOBBER_WORKPLAN_HELP,
    ARG_PRE_RUN,
    ARG_PRE_RUN_WORKPLAN_HELP,
    ARG_RESUME,
    ARG_RESUME_WORKPLAN_HELP,
    ARG_RUN_ID,
    ARG_VAR_HELP,
    ARG_VAR_LONG,
    ARG_VAR_SHORT,
    ARG_VARFILE_HELP,
    ARG_VARFILE_LONG,
    ARG_VARFILE_SHORT,
    OPT_CLOBBER_ALL,
)
from cstar.execution.file_system import (
    StateDirectoryManager,
    local_copy,
    write_bytes_atomic,
)
from cstar.orchestration.dag_runner import (
    ExecutiveRunSummary,
    ExecutiveStepSummary,
    NoPreparableStepsError,
    apply_clobber_overrides,
    apply_resume_overrides,
    build_and_run_dag,
    build_planner,
    check_clobber_targets,
    clobber_each_step,
    drop_spent_clobbers,
    get_launcher,
    load_external_runs,
    load_run_state,
    recorded_workplan,
    refresh_run_manifest,
    run_dag,
)
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import BlueprintCore, Step, Workplan
from cstar.orchestration.orchestration import (
    LiveWorkplan,
    ProcessHandle,
    is_transformed_workplan,
)
from cstar.orchestration.run_check import report_orphans
from cstar.orchestration.serialization import (
    deserialize,
    model_to_yaml,
    read_raw,
    try_deserialize,
    validate_serialized_entity,
)
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.orchestration.transforms import (
    PLACEHOLDER_RE,
    anchor_blueprint_path,
    external_dependencies,
)
from cstar.system.migration import CstarSchemaTooNewError

if t.TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from pydantic.fields import ComputedFieldInfo, FieldInfo

app = typer.Typer()
log = get_logger(__name__)

COMMAND_SUBMIT: t.Final[str] = "submit"
"""Name under which the workplan launch command is listed."""
ALIAS = "run"
"""Earlier name of the launch command, still registered but hidden from `--help`."""
HELP_SHORT = "Submit a workplan: schedule every step and exit."
HELP_LONG = f"""\
{HELP_SHORT}

Specify a previously used `run_id` to re-start (or reattach) to a prior run.

If a path to a blueprint is supplied, it will be executed as a single-step workplan
(the same as `cstar blueprint submit`).

Pass `--resume` to re-enter a prior run and resume its failed steps in place,
rather than re-running them from scratch. Identify the run with `--run-id`, or
with the workplan path it was started from (the run-id is derived from the
workplan name exactly as on the first run; the file must be unchanged since).

Pass `--pre-run` to perform every stage before the model launch for each step
(locally, even on scheduler systems) without launching any model. Submit the
workplan again without `--pre-run` to attach to the prepared directories and launch.
"""

CATEGORY_HEADER_COLOR: t.Final[str] = "white"
SECTION_HEADER_COLOR: t.Final[str] = "yellow"
ATTR_COLOR: t.Final[str] = "cyan"
INDENT = " "


def _ft(model_type: type[BaseModel], field_name: str) -> str:
    """Retrieve the field title from a type for a field."""
    field: FieldInfo | ComputedFieldInfo | None = None
    if field_name in model_type.model_fields:
        field = model_type.model_fields[field_name]
    if field_name in model_type.model_computed_fields:
        field = model_type.model_computed_fields[field_name]
    return str(field.title) if field else "INVALID-FIELD-NAME"


def _stepft(field_name: str) -> str:
    """Retrieve the field title from the step summary model."""
    return _ft(ExecutiveStepSummary, field_name)


def _runft(field_name: str) -> str:
    """Retrieve the field title from the run summary model."""
    return _ft(ExecutiveRunSummary, field_name)


def _hdr(header: str, color: str = "green") -> tuple[str, str]:
    """Generate header content and delimiter."""
    underline_char = "-"
    content = colored(header, color)
    delimiter = underline_char * len(header)
    return content, delimiter


def get_step_summary_display(summary: ExecutiveStepSummary) -> str:
    """Generate the executive summary display for a step from a workplan run."""
    step_header, step_underline = _hdr(f"{summary.name!r}", "green")
    assets_header, assets_underline = _hdr("Asset Paths", SECTION_HEADER_COLOR)
    job_header, job_underline = _hdr("Job Details", SECTION_HEADER_COLOR)

    handle = try_deserialize(summary.sentinel_path, ProcessHandle)
    task_prompt = _stepft("task_id")

    if handle is None:
        pid = "N/A"
    else:
        pid = handle.pid if handle else "N/A"
        if handle.launcher_name == SlurmLauncher.name:
            task_prompt = "SLURM Job ID"

    return textwrap.dedent(f"""\
        {step_header}
        {step_underline}
        {INDENT}{assets_header}
        {INDENT}{assets_underline}
        {INDENT * 2}- {_stepft("log_path")}: {summary.log_path}
        {INDENT * 2}- {_stepft("blueprint_path")}: {summary.blueprint_path}
        {INDENT * 2}- {_stepft("working_dir")}: {summary.working_dir}
        {INDENT * 2}- {_stepft("script_path")}: {summary.script_path}
        {INDENT}{job_header}
        {INDENT}{job_underline}
        {INDENT * 2}- {task_prompt}: {colored(pid, ATTR_COLOR)}
        {INDENT * 2}- {_stepft("status")}: {colored(summary.status, ATTR_COLOR)}
        """)


def get_run_summary_display(summary: ExecutiveRunSummary) -> str:
    """Generate a print-friendly summary for a workplan run."""
    prefix = "# "
    content_delimiter = "#" * 78

    header = f"{summary.workplan_name!r} Run Summary"
    run_header, run_underline = _hdr(header, CATEGORY_HEADER_COLOR)
    section_header_steps, section_del_steps = _hdr(
        _runft("steps"), CATEGORY_HEADER_COLOR
    )
    section_header_actions, section_del_actions = _hdr(
        "Further Actions", CATEGORY_HEADER_COLOR
    )

    content = textwrap.dedent(f"""\
                {prefix}{content_delimiter}
                {prefix}{run_header}
                {prefix}{run_underline}
                {prefix}{INDENT}- {_runft("run_id")}: {colored(summary.run_id, ATTR_COLOR)}
                {prefix}{INDENT}- {_runft("source_workplan")}: {summary.source_workplan}
                {prefix}{INDENT}- {_runft("final_workplan")}: {summary.final_workplan}
                {prefix}{INDENT}- {_runft("state_dir")}: {summary.state_dir}
                {prefix}
                {prefix}{section_header_steps}
                {prefix}{section_del_steps}
                <steps>
                {prefix}{section_header_actions}
                {prefix}{section_del_actions}
                <actions>
                {prefix}
                {prefix}{content_delimiter}
                """)

    step_content = [get_step_summary_display(s) for s in summary.steps]
    step_summaries = [
        f"{prefix}{INDENT}{line}" for item in step_content for line in item.split("\n")
    ]
    steps_section = (
        "\n".join(step_summaries)
        if step_summaries
        else f"{prefix}{INDENT}- No steps found"
    )

    prompts = [
        ("For run status details", f"cstar run status {summary.run_id}"),
        ("To review logs", f"cstar run log {summary.run_id} <step-name>"),
    ]

    actions_content = [f"{prefix}{INDENT}- {present(*prompt)}" for prompt in prompts]
    actions_section = "\n".join(actions_content)

    value_map = {"<steps>": steps_section, "<actions>": actions_section}
    for tpl, value in value_map.items():
        content = content.replace(tpl, value)

    return content + "\n"


def preprocess_runid(ctx: typer.Context, run_id: str) -> str:
    """Perform validation and formatting of the run-id argument.

    - verify blueprint path or run-id is supplied
    - generate a default run id from a blueprint if run-id is not supplied

    Expects a run-id already normalized by the `normalize_runid` stage that
    precedes this one in the callback pipeline.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    run_id : str
        The normalized user-supplied run-id, or an empty string.

    Returns
    -------
    str
    """
    if run_id:
        return run_id

    path: str | None = ctx.params.get("path", None)
    if isinstance(path, str):
        path = path.strip()

    if not path:
        msg = "A run-id or workplan path is required"
        raise typer.BadParameter(msg)

    run_id = WorkplanRun.get_default_run_id(path)

    msg = f"Generated a default run-id `{run_id}` from `{path}`"
    log.debug(msg)

    return run_id


def preprocess_clobber_steps(
    ctx: typer.Context,
    values: list[str],
) -> list[str]:
    """Perform validation and formatting of the `--clobber` option.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    values : list[str]
        The user-supplied step names, safe_names, or the literal token
        `all`, one per `--clobber` use.

    Returns
    -------
    list[str]
        The cleaned step names or safe_names, stripped of whitespace with any
        empty entries dropped.
    """
    return [x for value in values if (x := value.strip())]


def resolve_clobber_selection(wp_path: Path, clobber_steps: list[str]) -> list[str]:
    """Resolve the raw `--clobber` selections against the workplan.

    Expands the CLI-only token `all` into every step's safe_name and fails
    fast -- before any run machinery starts -- when a selection matches no
    step, so the user gets immediate feedback instead of a mid-run error.

    Parameters
    ----------
    wp_path : Path
        Local path to the workplan file (an original workplan, or the
        transformed workplan persisted by a prior run when re-running).
    clobber_steps : list[str]
        The cleaned `--clobber` values.

    Returns
    -------
    list[str]
        Concrete step selections to pass to `build_and_run_dag`.

    Raises
    ------
    typer.BadParameter
        If `all` is combined with step names, or a selection matches no
        step's `name` or `safe_name`.
    """
    if not clobber_steps:
        return clobber_steps

    wp = try_deserialize(wp_path, Workplan) or LiveWorkplan.try_load(wp_path)
    if wp is None:
        return clobber_steps

    if OPT_CLOBBER_ALL in clobber_steps:
        if len(clobber_steps) > 1:
            msg = f"{OPT_CLOBBER_ALL!r} cannot be combined with step names"
            raise typer.BadParameter(msg, param_hint=ARG_CLOBBER)
        return [step.safe_name for step in wp.steps]

    if unknown := check_clobber_targets(wp, clobber_steps):
        valid_names = sorted(step.name for step in wp.steps)
        msg = f"Unknown step(s) {unknown}. Valid steps: {valid_names}"
        raise typer.BadParameter(msg, param_hint=ARG_CLOBBER)

    return clobber_steps


def composed_workplan_name(blueprint: BlueprintCore) -> str:
    """The name of the workplan composed around a blueprint, which is its default run-id.

    Parameters
    ----------
    blueprint : BlueprintCore
        The blueprint the workplan wraps.

    Returns
    -------
    str

    Raises
    ------
    typer.BadParameter
        If nothing of the blueprint's name survives slugifying.
    """
    try:
        return slugify(blueprint.name)
    except ValueError as ex:
        msg = f"A run cannot be named from the blueprint's name {blueprint.name!r}"
        raise typer.BadParameter(msg) from ex


def compose_blueprint_workplan(path: str) -> str:
    """Wrap a blueprint in a single-step workplan kept under C-Star's state directory.

    The workplan is keyed on the blueprint's resolved path, so submitting the
    same blueprint again composes the same file with the same content, and two
    blueprints that share a name compose different files. It is never written
    beside the blueprint, which may be in a shared or read-only location.

    Parameters
    ----------
    path : str
        The path to a blueprint, or to anything else.

    Returns
    -------
    str
        The path to the composed workplan, or `path` unchanged when it is not a
        blueprint.

    Raises
    ------
    typer.BadParameter
        If nothing of the blueprint's name survives slugifying.
    """
    blueprint = try_deserialize(path, BlueprintCore)
    if blueprint is None:
        # path isn't a blueprint. leave it alone.
        return path

    bp_path = Path(path).expanduser().resolve()
    name = composed_workplan_name(blueprint)
    digest = hashlib.sha256(str(bp_path).encode()).hexdigest()[:12]
    wp_path = (
        StateDirectoryManager.composed_workplans_dir()
        / f"{name}-{digest}"
        / f"{name}-workplan.yaml"
    )

    wp = Workplan(
        name=name,
        description=f"Single-step workplan submitted for {bp_path}",
        steps=[
            Step(
                name=blueprint.application,
                application=blueprint.application,
                blueprint=bp_path,
            )
        ],
    )
    # atomic, so a concurrent submit of the same blueprint never reads half a file
    write_bytes_atomic(wp_path, model_to_yaml(wp).encode())
    log.debug(f"Composed a workplan for the blueprint {str(bp_path)!r} in: {wp_path}")
    return str(wp_path)


def migrate_steps(path: Path, workplan: Workplan) -> dict[str, Path]:
    """Perform automatic migration of the blueprints referenced by a workplan.

    Every step is checked before the command exits, so all blueprints with an
    unsupported schema are reported together. Neither the workplan nor its
    blueprints are rewritten: a migrated blueprint is written to a copy, which
    the run reads in place of the file the step names.

    Parameters
    ----------
    path : Path
        The path of the workplan; a step's relative blueprint path is resolved
        against its directory.
    workplan : Workplan
        The workplan to perform migrations on.

    Returns
    -------
    dict[str, Path]
        The migrated copy of each step's blueprint, by step name, for the steps
        whose blueprint was migrated.

    Raises
    ------
    typer.BadParameter
        If a blueprint does not exist or is invalid.
    typer.Exit
        If any blueprint's schema is newer than this build reads or has no
        automatic migration.
    """
    migrated: dict[str, Path] = {}
    failures: list[str] = []
    for step in workplan.steps:
        if not isinstance(step.blueprint_path, (Path, str)):
            continue

        bp_path = str(step.blueprint_path)
        if PLACEHOLDER_RE.search(bp_path):
            # The path names a file that does not exist yet: its `{{ }}`
            # placeholders are filled by `TemplateFillTransform` when the
            # workplan is prepared, so it cannot be opened here. Preparation
            # then loads the filled blueprint strictly, without schema
            # migration, so a stale blueprint behind a placeholder is reported
            # at that point instead of by this preflight.
            continue

        # a relative path is anchored to the workplan's directory, as
        # preparation will do, so the preflight opens the same file
        bp_path = str(anchor_blueprint_path(step, path.parent).blueprint_path)
        try:
            source, modified = localize_and_migrate(bp_path)
            if modified:
                migrated[step.name] = source
        except FileNotFoundError as ex:
            msg = f"Blueprint not found at path: {ex.filename}"
            raise typer.BadParameter(msg) from ex
        except SCHEMA_ERRORS as ex:
            failures.append(schema_message(bp_path, ex))
        except ValidationError as ex:
            errors = format_validation_errors(ex)
            msg = f"Blueprint {bp_path!r} is invalid. Details: {errors}"
            raise typer.BadParameter(msg) from ex

    if failures:
        for failure in failures:
            console.print(failure, soft_wrap=True, markup=False)
        raise typer.Exit(1)

    if migrated:
        listing = ", ".join(f"{name!r} ({src})" for name, src in migrated.items())
        log.debug(f"Running the migrated copies of the blueprints of steps {listing}")
    return migrated


def preprocess_path(workplan_path: str) -> str:
    """Perform validation related to the workplan path.

    A path to a blueprint is replaced by the path to the workplan composed
    around it.

    Parameters
    ----------
    workplan_path : str
        The user-supplied path to a workplan or blueprint file, or an empty
        string.

    Returns
    -------
    str
    """
    if workplan_path:
        try:
            with local_copy(workplan_path) as local_path:
                if not local_path.exists():
                    msg = f"Workplan not found at path: {workplan_path}"
                    raise typer.BadParameter(msg)

                local_path = Path(compose_blueprint_workplan(str(local_path)))

                # the transformed workplan of a run is re-entered, never prepared
                live = is_transformed_workplan(local_path)
                klass: type[Workplan] = LiveWorkplan if live else Workplan
                validation_result = validate_serialized_entity(local_path, klass)
                if isinstance(validation_result.error, CstarSchemaTooNewError):
                    raise typer.BadParameter(validation_result.error_msg)
                if not validation_result.item:
                    log.error(validation_result.error_msg)
                    msg = f"The workplan file in `{workplan_path}` is improperly formatted"
                    raise typer.BadParameter(msg)
                return str(local_path)
        except FileNotFoundError as ex:
            if is_known_run_id(workplan_path):
                msg = (
                    f"{workplan_path!r} is a run-id, not a workplan file; use "
                    f"`cstar workplan {COMMAND_SUBMIT} {ARG_RUN_ID} {workplan_path} {ARG_RESUME}`"
                )
                raise typer.BadParameter(msg) from ex

            msg = f"Workplan not found at path: {workplan_path}"
            raise typer.BadParameter(msg) from ex

    return workplan_path


def is_known_run_id(value: str) -> bool:
    """Determine whether a value is the run-id of a recorded run.

    Only a value that is already a run-id (its own slug) is looked up, so a
    mistyped file name is not mistaken for one. The lookup only checks that
    the run's latest record exists: it takes no lock and reads no record.

    Parameters
    ----------
    value : str
        The value supplied where a workplan path was expected.

    Returns
    -------
    bool
    """
    try:
        if value != slugify(value):
            return False

        return TrackingRepository().latest_path(value).exists()
    except (ValueError, OSError):
        return False


def read_slugged_workplan(path: Path) -> Workplan:
    """Read a workplan with its name normalized to a slug.

    A workplan recorded before names had to be slugs carries a free-form name
    that no longer validates, but is otherwise the same workplan.

    Parameters
    ----------
    path : Path
        The workplan file.

    Returns
    -------
    Workplan

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the content is not a valid workplan.
    """
    raw = read_raw(path)
    if not isinstance(raw, dict):
        msg = f"The content of {str(path)!r} is not a mapping"
        raise ValueError(msg)

    raw.pop("$schema", None)
    if isinstance(raw.get("name"), str):
        raw["name"] = slugify(raw["name"])

    return Workplan.model_validate(raw)


def refuse_run_id_collision(wp_path: Path, run_id: str, *, explicit: bool) -> None:
    """Refuse to re-enter a run that a different workplan started.

    A derived run-id is the workplan's name, so two different workplans that
    share a name share a run directory and the later one would overwrite the
    earlier one's state and originals. The same workplan reaching the run by
    another path (a fresh copy of a remote file, a moved checkout) is not a
    collision, so a different path is compared by content, names normalized,
    against the workplan recorded for the run. An explicit `--run-id` is taken as
    the user's choice to re-enter the run.

    Parameters
    ----------
    wp_path : Path
        The local path of the workplan about to run.
    run_id : str
        The run-id the workplan will run under.
    explicit : bool
        Whether the run-id was supplied with `--run-id` rather than derived.

    Raises
    ------
    typer.BadParameter
        If the run-id was derived and its latest record is of a different
        workplan, the supplied file cannot be read, or the recorded workplan of
        the run is missing or unreadable.
    """
    if explicit:
        return

    try:
        record = asyncio.run(TrackingRepository().get_workplan_run(run_id))
    except CstarError as ex:
        msg = f"Run-id {run_id!r} cannot be checked against its record: {ex}"
        raise typer.BadParameter(msg) from ex
    if record is None:
        return

    try:
        started_from = record.workplan_path
    except CstarError as ex:
        # the run cannot be re-entered either: scheduling reads the same manifest
        msg = (
            f"Run-id {run_id!r}, derived from the workplan name, belongs to a run "
            f"whose workplan could not be found: {ex}; pass {ARG_RUN_ID} "
            "<other-id> to start a new run, or, when that run's directory was "
            f"deleted, forget the run with `cstar admin clean {ARG_RUN_ID} {run_id}`."
        )
        raise typer.BadParameter(msg) from ex
    if started_from.resolve() == wp_path.resolve():
        return

    claim = (
        f"Run-id {run_id!r}, derived from the workplan name, belongs to a run "
        f"started from {str(started_from)!r}; the workplan at "
        f"{str(wp_path)!r} would overwrite it"
    )
    escape = f"pass {ARG_RUN_ID} <other-id> or {ARG_RUN_ID} {run_id} to re-enter"

    try:
        supplied = read_slugged_workplan(wp_path)
    except (OSError, RuntimeError, ValueError, yaml.YAMLError) as ex:
        msg = f"The workplan at {str(wp_path)!r} could not be read: {ex}"
        raise typer.BadParameter(msg) from ex

    try:
        recorded_path = recorded_workplan(record)
    except CstarError as ex:
        # the run cannot be re-entered either: scheduling reads the same manifest
        msg = (
            f"{claim}, and the manifest of that run could not be read: {ex}; pass "
            f"{ARG_RUN_ID} <other-id> to start a new run."
        )
        raise typer.BadParameter(msg) from ex

    # a deleted history must not let a different workplan overwrite the run
    if recorded_path is None:
        msg = f"{claim}, and there is no recorded workplan to compare; {escape}."
        raise typer.BadParameter(msg)

    try:
        recorded = read_slugged_workplan(recorded_path)
    except (OSError, RuntimeError, ValueError, yaml.YAMLError) as ex:
        msg = (
            f"{claim}, and the recorded workplan for that run could not be read: "
            f"{ex}; {escape}."
        )
        raise typer.BadParameter(msg) from ex

    if supplied != recorded:
        msg = (
            f"{claim} (its content differs from the workplan recorded for that "
            f"run). Pass {ARG_RUN_ID} <other-id> to run this workplan separately, "
            f"{ARG_RUN_ID} {run_id} to re-enter the run with it, or rename it."
        )
        raise typer.BadParameter(msg)


RELOCATABLE_HOMES: t.Final[frozenset[str]] = frozenset(
    {ENV_CSTAR_DATA_HOME, ENV_CSTAR_STATE_HOME}
)
"""The captured variables a reload leaves to the current shell."""


async def handle_run_reloading(run_id: str) -> WorkplanRun:
    """Locate the prior run for the run-id and restore its environment.

    The environment variables captured when the run was originally started
    are re-exported so the reload executes under matching configuration, with
    one warning naming those that differ from the current shell. The data and
    state homes are not re-exported: they say where the run is now, and a
    relocated run must keep resolving against the current ones.

    Parameters
    ----------
    run_id : str
        The run-id to reload

    Returns
    -------
    WorkplanRun
        The recorded run, carrying the original and prepared workplan paths.

    Raises
    ------
    typer.BadParameter
        - If no run record exists for the supplied run-id, or it cannot be
          read (one a newer cstar-ocean wrote that this build cannot read).
        - If the run was attached from elsewhere: it has no sentinels here and
          is never relaunched.
        - If the run's workplan or transformed workplan cannot be found.
    """
    repo = TrackingRepository()
    try:
        wp_run = await repo.get_workplan_run(run_id)
    except CstarError as ex:
        msg = f"Run {run_id!r} cannot be re-entered: {ex}"
        raise typer.BadParameter(msg) from ex
    if wp_run is None:
        msg = f"No runs with the id `{run_id}` could be found."
        raise typer.BadParameter(msg)
    if wp_run.attached:
        msg = (
            f"Run {run_id!r} was attached from {wp_run.output_path}: attached runs "
            "can be inspected and referenced, and relaunching after a move is not "
            "supported. Start a new run with `--run-id <new>` that references it "
            "(see 'Extending a finished run' in the workplan docs)."
        )
        raise typer.BadParameter(msg)

    try:
        source = wp_run.workplan_path
        _ = wp_run.trx_workplan_path
    except CstarError as ex:
        msg = f"Run {run_id!r} cannot be re-entered: {ex}"
        raise typer.BadParameter(msg) from ex

    # ensure the environment matches the prior run; a setting this build no
    # longer reads is left out rather than re-exported to every step
    recorded = {
        name: value
        for name, value in wp_run.environment.items()
        if name not in RELOCATABLE_HOMES and name not in REMOVED_SETTINGS
    }
    differing = {
        name: value for name, value in recorded.items() if os.environ.get(name) != value
    }
    if differing:
        listing = ", ".join(
            f"{name} (now {os.environ.get(name)!r}, recorded {value!r})"
            for name, value in differing.items()
        )
        log.warning(
            f"Re-exporting recorded settings that differ from this shell: {listing}"
        )
    os.environ.update(recorded)

    msg = f"Re-starting run-id {run_id!r} with workplan originating in {source!r}"
    log.info(msg)

    return wp_run


def verify_live_source(trx_path: Path, wp_run: WorkplanRun) -> None:
    """Ensure a transformed workplan supplied by path is the one `wp_run` executes.

    A transformed workplan re-enters the run it belongs to. A copy of it
    elsewhere, such as the one an attempt carried, is a plan no record executes
    and is refused until a run can be attached from disk.

    Parameters
    ----------
    trx_path : Path
        The transformed workplan path supplied on the command line.
    wp_run : WorkplanRun
        The recorded run being re-entered.

    Raises
    ------
    typer.BadParameter
        If the supplied path is not the transformed workplan the record executes.
    """
    recorded = wp_run.trx_workplan_path
    if trx_path.resolve() == recorded.resolve():
        return

    msg = (
        f"{str(trx_path)!r} is a transformed workplan, but run {wp_run.run_id!r} "
        f"executes {str(recorded)!r}; pass {ARG_RUN_ID} {wp_run.run_id} without a "
        "workplan path to re-enter the run as recorded"
    )
    raise typer.BadParameter(msg)


def verify_resume_source(wp_path: Path, wp_run: WorkplanRun) -> None:
    """Ensure the workplan supplied with `--resume` is the one `wp_run` started from.

    The path only identifies the run; its content is never re-transformed, so a
    file that changed since the run started is rejected rather than silently
    resumed under the recorded plan. When no recorded copy of the workplan is on
    disk (older attempts may have been deleted), the run is resumed as recorded.

    Parameters
    ----------
    wp_path : Path
        The workplan path supplied on the command line.
    wp_run : WorkplanRun
        The recorded run being re-entered.

    Raises
    ------
    typer.BadParameter
        If the run's manifest or recorded workplan is unreadable, or the
        supplied workplan no longer matches it.
    """
    escape = (
        f"Pass --run-id {wp_run.run_id} without a workplan path to resume the run "
        "as recorded"
    )
    try:
        recorded_path = recorded_workplan(wp_run)
    except CstarError as ex:
        msg = (
            f"Unable to verify {str(wp_path)!r} against run {wp_run.run_id!r}: "
            f"{ex}. {escape}."
        )
        raise typer.BadParameter(msg) from ex

    if recorded_path is None:
        log.warning(
            f"No recorded workplan of run {wp_run.run_id!r} was found to verify "
            f"{str(wp_path)!r} against, as older attempts may have been deleted; "
            "resuming the run's recorded plan, so an edit of that file is not "
            "applied."
        )
        return

    # a carried copy is verbatim and a legacy backup a re-serialization of the
    # parsed workplan, so compare models either way; the suffix names the format
    try:
        recorded = deserialize(recorded_path, Workplan)
        supplied = deserialize(wp_path, Workplan)
    except Exception as ex:
        msg = (
            f"Unable to verify {str(wp_path)!r} against run {wp_run.run_id!r}: "
            f"{ex}. {escape}."
        )
        raise typer.BadParameter(msg) from ex

    if supplied != recorded:
        msg = (
            f"The workplan at {str(wp_path)!r} differs from the one run {wp_run.run_id!r} "
            f"was started from ({str(recorded_path)!r}): it was edited since; "
            f"{ARG_RESUME} re-enters the recorded run. "
            f"{escape}, or supply a new --run-id to start a fresh run."
        )
        raise typer.BadParameter(msg)

    msg = f"Workplan {str(wp_path)!r} matches the recorded workplan for run {wp_run.run_id!r}"
    log.debug(msg)


def is_live_workplan_file(path: str) -> bool:
    """Whether a path names the transformed workplan a run persists.

    Parameters
    ----------
    path : str
        The user-supplied path, possibly empty.

    Returns
    -------
    bool
    """
    return bool(path) and Path(path).is_file() and is_transformed_workplan(Path(path))


def workplan_resubmit(source: str) -> str:
    """The command that submits a workplan again.

    Parameters
    ----------
    source : str
        The workplan to name in the command.

    Returns
    -------
    str
    """
    return f"cstar workplan {COMMAND_SUBMIT} {source}"


def submit_workplan(
    path: str,
    run_id: str,
    *,
    clobber: list[str],
    resume: bool,
    pre_run: bool,
    user_vars: "Mapping[str, str]",
    explicit_run_id: bool,
    resubmit: "Callable[[str], str]",
    clobber_hint: "Callable[[Sequence[str]], str]",
) -> None:
    """Schedule the run of a workplan, or re-enter a recorded run, and report it.

    Parameters
    ----------
    path : str
        The local path to the workplan to run, or to its transformed workplan to
        re-enter its run. Empty to re-enter the run of `run_id` as recorded.
    run_id : str
        The run-id the workplan runs under.
    clobber : list[str]
        The names of the steps whose prior state is cleared, or the literal
        token `all`.
    resume : bool
        Whether to re-enter the run and resume its failed steps in place.
    pre_run : bool
        Whether to perform every stage before the model launch without
        launching a model.
    user_vars : Mapping[str, str]
        The user-defined variables the workplan is filled with.
    explicit_run_id : bool
        Whether `run_id` was supplied by the user rather than derived.
    resubmit : Callable[[str], str]
        Names the command that submits this run's source again, given the
        workplan the run was made from (a `<workplan>` placeholder when it is not
        known), for the follow-up steps printed.
    clobber_hint : Callable[[Sequence[str]], str]
        Spells this command's options that re-run the named steps, for the
        refusal of a re-entry over edited steps.

    Raises
    ------
    typer.BadParameter
        If the inputs are invalid or the run cannot be re-entered.
    typer.Exit
        If the run could not be scheduled.
    """
    live = is_live_workplan_file(path)
    reload = resume or not path or live

    if reload:
        wp_run = asyncio.run(handle_run_reloading(run_id))
        if path and live:
            verify_live_source(Path(path), wp_run)
        elif path:
            verify_resume_source(Path(path), wp_run)
        original_path, trx_path = wp_run.workplan_path, wp_run.trx_workplan_path
        path = str(trx_path)

    try:
        with local_copy(path) as wp_path:
            clobber = resolve_clobber_selection(wp_path, clobber)

            if reload:
                wp = LiveWorkplan.load(wp_path)
                if resume and wp.pre_run:
                    msg = (
                        f"Run {run_id!r} is a pre-run; retry its failed steps with "
                        f"`{resubmit('<workplan>')} {ARG_PRE_RUN}` instead"
                    )
                    raise typer.BadParameter(msg, param_hint=ARG_RESUME)
                pre_run = wp.pre_run
                asyncio.run(drop_spent_clobbers(wp, wp_run))
                apply_clobber_overrides(wp, clobber)
                if resume:
                    # load the state once: refresh the manifest, then apply it
                    launcher = get_launcher(wp)
                    status = asyncio.run(load_run_state(run_id, launcher))
                    refresh_run_manifest(wp_run.output_path, run_id, status)
                    asyncio.run(apply_resume_overrides(wp, status))
                external = asyncio.run(
                    load_external_runs(wp, get_launcher(wp, force_local=pre_run))
                )
                exit_on_external_problems(external, external_dependencies(wp))
                planner = build_planner(wp, external.tasks())
                report_orphans(wp_run.output_path, run_id, wp)

                wp_run = asyncio.run(
                    run_dag(
                        original_path,
                        trx_path,
                        run_id,
                        planner,
                        user_variables=user_vars,
                    ),
                )
            else:
                refuse_run_id_collision(
                    wp_path,
                    run_id,
                    explicit=explicit_run_id,
                )
                sources = migrate_steps(wp_path, deserialize(wp_path, Workplan))
                wp_run = asyncio.run(
                    build_and_run_dag(
                        wp_path,
                        run_id,
                        user_variables=user_vars,
                        clobber_steps=clobber,
                        pre_run=pre_run,
                        blueprint_sources=sources,
                        clobber_hint=clobber_hint,
                    ),
                )

            summary = asyncio.run(ExecutiveRunSummary.from_run(wp_run))
            console.print(get_run_summary_display(summary))
    except (typer.BadParameter, typer.Exit):
        raise
    except NoPreparableStepsError as ex:
        console.print(str(ex))
        return
    except CstarExpectationFailed as ex:
        msg = f"An invalid request was made: {ex}"
        log.exception(msg)
        print(msg)
        raise typer.Exit(1) from ex
    except CstarSchemaTooNewError as ex:
        # a file of the run (the workplan, its manifest or a blueprint) was
        # written by a newer release; the message names it and the upgrade
        log.debug("A file of the run is newer than this build reads", exc_info=True)
        raise typer.BadParameter(f"Run {run_id!r} cannot be continued: {ex}") from ex
    except ValidationError as ex:
        # a step blueprint (or the workplan) that fails strict validation: the
        # message says what is wrong, so the traceback is only logged for debugging
        log.debug("Invalid inputs encountered while running the dag", exc_info=True)
        msg = f"{ex.title} failed validation: {format_validation_errors(ex)}"
        raise typer.BadParameter(msg) from ex
    except ValueError as ex:
        msg = "Invalid inputs encountered while running the dag"
        log.exception(msg)
        raise typer.BadParameter(str(ex)) from ex
    except Exception as ex:
        msg = f"Workplan run {run_id!r} has failed: {ex}"
        log.exception(msg)
        print(msg)
        raise typer.Exit(3) from ex

    console.print(f"{summary.workplan_name!r} run scheduling has completed")
    if pre_run:
        console.print(
            "Pre-run scheduled. Submit "
            f"`{resubmit(str(summary.source_workplan))} {ARG_RUN_ID} {summary.run_id}` "
            f"without {ARG_PRE_RUN} to attach to the prepared directories and launch."
        )


@app.command(name=COMMAND_SUBMIT, help=HELP_LONG, short_help=HELP_SHORT)
def submit(
    ctx: typer.Context,
    run_id: t.Annotated[
        str,
        typer.Option(
            ARG_RUN_ID,
            help=(
                "The unique identifier for an execution of the workplan; "
                "defaults to the workplan's name."
            ),
            autocompletion=list_runs,
            callback=cb_pipeline(
                normalize_runid, preprocess_runid, set_env(ENV_CSTAR_RUNID)
            ),
        ),
    ] = "",
    user_variables: t.Annotated[
        list[str],
        typer.Option(
            ARG_VAR_LONG,
            ARG_VAR_SHORT,
            help=ARG_VAR_HELP,
            callback=preprocess_vars,
        ),
    ] = [],
    user_variables_path: t.Annotated[
        Path | None,
        typer.Option(
            ARG_VARFILE_LONG,
            ARG_VARFILE_SHORT,
            help=ARG_VARFILE_HELP,
            callback=preprocess_varfile,
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    path: t.Annotated[
        str,
        typer.Argument(
            help="Path to a workplan or blueprint file.",
            callback=preprocess_path,
            is_eager=True,
        ),
    ] = "",
    log_level: LogLevelOption = DEFAULT_LOG_LEVEL_CHOICE,
    clobber: t.Annotated[
        list[str],
        typer.Option(
            ARG_CLOBBER,
            help=ARG_CLOBBER_WORKPLAN_HELP,
            callback=preprocess_clobber_steps,
        ),
    ] = [],
    resume: t.Annotated[
        bool,
        typer.Option(ARG_RESUME, help=ARG_RESUME_WORKPLAN_HELP),
    ] = False,
    pre_run: t.Annotated[
        bool,
        typer.Option(ARG_PRE_RUN, help=ARG_PRE_RUN_WORKPLAN_HELP),
    ] = False,
) -> None:
    """Execute a workplan.

    Specify a previously used run_id option to re-start a prior run.
    """
    exit_on_refused_settings()

    if user_variables and user_variables_path is not None:
        msg = "`--var` and `--varfile` must not be supplied together"
        raise typer.BadParameter(msg)

    live = is_live_workplan_file(path)

    problems = [
        msg
        for condition, msg in (
            (resume and clobber, f"{ARG_RESUME} cannot be combined with {ARG_CLOBBER}"),
            (resume and pre_run, f"{ARG_RESUME} cannot be combined with {ARG_PRE_RUN}"),
            (
                pre_run and not path,
                (
                    f"{ARG_PRE_RUN} requires a workplan path: reloading a run by "
                    "--run-id replays its recorded plan as-is"
                ),
            ),
            (
                pre_run and live,
                (
                    f"{ARG_PRE_RUN} needs the authored workplan, not its transformed "
                    "copy, which replays the recorded plan as-is"
                ),
            ),
            (
                resume and (user_variables or user_variables_path is not None),
                (
                    f"{ARG_RESUME} re-enters a prior run with its recorded variables; "
                    f"{ARG_VAR_LONG}/{ARG_VARFILE_LONG} cannot be supplied"
                ),
            ),
        )
        if condition
    ]
    if problems:
        raise typer.BadParameter("; ".join(problems))

    submit_workplan(
        path,
        run_id,
        clobber=clobber,
        resume=resume,
        pre_run=pre_run,
        user_vars=t.cast("Mapping[str, str]", ctx.obj),
        explicit_run_id=ctx.get_parameter_source("run_id")
        is not ParameterSource.DEFAULT,
        resubmit=workplan_resubmit,
        clobber_hint=clobber_each_step,
    )


if __name__ == "__main__":
    typer.run(submit)
