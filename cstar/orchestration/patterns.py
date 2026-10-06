"""Pure generators and normalizers for recurring workplan shapes.

Each generator builds the `Step` objects (and any external `runs`) for one
shape -- time-chunked runs, a spin-up ramp, forge-then-run, an upscaling chain
-- without reading blueprints or touching the file system, so a UI can drive it
and show the result before anything is written. `normalize_legacy` rewrites
deprecated or path-based spellings of an existing workplan into their current
form and reports every rewrite.
"""

import copy
import itertools
import re
import typing as t
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from pydantic import Field, model_validator

from cstar.applications.roms_marbl.transforms import (
    ContinuanceDirective,
    NestingDirective,
    RestartFile,
    restart_timestamp,
)
from cstar.base.utils import WALLTIME_RE, deep_merge
from cstar.orchestration.models import (
    BLUEPRINT_METADATA_FIELDS,
    ConfiguredBaseModel,
    DeferredBlueprintRef,
    InlineBlueprintRef,
    RunRef,
    Step,
    StepRef,
    Workplan,
)

if t.TYPE_CHECKING:
    from cstar.applications.core import EmittedBlueprint

TIMESTAMP_FMT: t.Final[str] = "%Y-%m-%d %H:%M:%S"
"""The canonical timestamp form written into a `continue-from` directive."""

_LEGACY_OUTPUT_PATTERN: t.Final[re.Pattern[str]] = re.compile(
    r"/joined_output(?=[/;\s]|$)"
)
"""A `joined_output` path segment of the legacy output layout."""

_WORD_BREAKS: t.Final[re.Pattern[str]] = re.compile(r"[\W_]+")
"""A run of characters that separates the words of a step name."""

INLINE_APPLICATIONS: t.Final[frozenset[str]] = frozenset(
    {"nest_ic", "upscaler", "hello_world"}
)
"""Applications whose blueprint is fully described by a step's overrides."""

_UPSCALER: t.Final[str] = "upscaler"
"""The application name of the upscaler."""


class Generated(t.NamedTuple):
    """The result of a workplan-shape generator."""

    steps: list[Step]
    """The generated steps, in dependency order."""
    runs: dict[str, RunRef]
    """The external run aliases the steps reference."""


class StepSource(ConfiguredBaseModel):
    """Where a restart comes from: another step, or a fixed path."""

    step: str = Field(default="")
    """A local step name, or a `<step>@<alias>` token for a step of another run."""

    run_id: str = Field(default="")
    """The run-id to declare for the alias when `step` names an external alias."""

    path: str = Field(default="")
    """A fixed directory or file path; mutually exclusive with `step`."""

    timestamp: datetime | None = Field(default=None)
    """The restart to select instead of the latest in the source."""

    @model_validator(mode="after")
    def _one_source(self) -> "StepSource":
        """Require exactly one of `step` and `path`, and `run_id` only for an
        external step.
        """
        if bool(self.step) == bool(self.path):
            msg = "A restart source needs exactly one of `step` and `path`"
            raise ValueError(msg)
        if self.run_id and not (self.step and StepRef.parse(self.step).is_external):
            msg = "`run_id` is only meaningful when `step` names a step of another run"
            raise ValueError(msg)
        return self


class Change(ConfiguredBaseModel):
    """One rewrite made by `normalize_legacy`."""

    step: str
    """The name of the step that was rewritten."""
    field: str
    """The dotted path of the rewritten field within the step."""
    before: str
    """The value before the rewrite."""
    after: str
    """The value after the rewrite."""
    reason: str
    """Why the rewrite was made."""


Window: t.TypeAlias = tuple[datetime, datetime]
"""A half-open simulation window as `(start, end)`."""


def _require_span(start: datetime, end: datetime) -> None:
    """Reject a span that is empty or runs backwards."""
    if end <= start:
        msg = f"end ({end}) must be after start ({start})"
        raise ValueError(msg)


def month_windows(start: datetime, end: datetime) -> list[Window]:
    """Split a span at calendar-month boundaries.

    The first and last windows are partial when `start` or `end` is not on a
    month boundary; no empty window is produced.

    Parameters
    ----------
    start : datetime
        The start of the span.
    end : datetime
        The end of the span; must be after `start`.

    Returns
    -------
    list[tuple[datetime, datetime]]
    """
    _require_span(start, end)

    windows: list[Window] = []
    cursor = start
    while cursor < end:
        year, month = (
            (cursor.year + 1, 1)
            if cursor.month == 12
            else (cursor.year, cursor.month + 1)
        )
        boundary = datetime(year, month, 1, tzinfo=cursor.tzinfo)
        windows.append((cursor, min(boundary, end)))
        cursor = boundary
    return windows


def fixed_windows(start: datetime, end: datetime, length: timedelta) -> list[Window]:
    """Split a span into windows of a fixed length; the last may be shorter.

    Parameters
    ----------
    start : datetime
        The start of the span.
    end : datetime
        The end of the span; must be after `start`.
    length : timedelta
        The length of each window; must be positive.

    Returns
    -------
    list[tuple[datetime, datetime]]
    """
    _require_span(start, end)
    if length <= timedelta(0):
        msg = f"window length must be positive, got {length}"
        raise ValueError(msg)

    windows: list[Window] = []
    cursor = start
    while cursor < end:
        windows.append((cursor, min(cursor + length, end)))
        cursor += length
    return windows


def equal_windows(start: datetime, end: datetime, n: int) -> list[Window]:
    """Split a span into `n` windows of equal length.

    Parameters
    ----------
    start : datetime
        The start of the span.
    end : datetime
        The end of the span; must be after `start`.
    n : int
        The number of windows; must be positive.

    Returns
    -------
    list[tuple[datetime, datetime]]
    """
    _require_span(start, end)
    if n < 1:
        msg = f"the number of windows must be at least 1, got {n}"
        raise ValueError(msg)

    edges = [start + (end - start) * i // n for i in range(n)] + [end]
    if any(a >= b for a, b in itertools.pairwise(edges)):
        msg = f"cannot split {start} to {end} into {n} non-empty windows"
        raise ValueError(msg)
    return list(itertools.pairwise(edges))


def _source_directive(source: StepSource) -> dict[str, t.Any]:
    """Render a restart source as a `continue-from` directive config."""
    config: dict[str, t.Any] = (
        {ContinuanceDirective.KEY_STEP: str(StepRef.parse(source.step))}
        if source.step
        else {ContinuanceDirective.KEY_PATH: source.path}
    )
    if source.timestamp is not None:
        config[ContinuanceDirective.KEY_TIMESTAMP] = source.timestamp.strftime(
            TIMESTAMP_FMT
        )
    return config


def _chain(
    base: Step,
    windows: Sequence[Window],
    extras: Sequence[Mapping[str, t.Any]],
    *,
    prefix: str,
    first_source: StepSource | None,
    walltime: str | Callable[[datetime, datetime], str] | None,
    restart_cadence: Mapping[str, t.Any] | None,
    chain_directive: bool,
) -> Generated:
    """Build one chained step per window; shared by `chunk_steps` and `spinup_ramp`.

    Parameters
    ----------
    base : Step
        The step the chunks are derived from.
    windows : Sequence[tuple[datetime, datetime]]
        The `(start, end)` of each chunk; only the end is written.
    extras : Sequence[Mapping[str, Any]]
        Per-window blueprint overrides merged on top of the base overrides.
    prefix : str
        The step name prefix.
    first_source : StepSource | None
        Where the first chunk continues from.
    walltime : str | Callable[[datetime, datetime], str] | None
        The SLURM max walltime, or a function of the window.
    restart_cadence : Mapping[str, Any] | None
        `basic_output_settings` namelist overrides applied to every chunk.
    chain_directive : bool
        Whether to chain chunks with `continue-from` directives as well as
        `depends_on`.

    Returns
    -------
    Generated
    """
    if not windows:
        msg = "at least one window is required"
        raise ValueError(msg)
    if not prefix.strip():
        msg = "the step name prefix must not be empty"
        raise ValueError(msg)
    if first_source is not None and not chain_directive:
        msg = "a first restart source needs `chain_directive=True`"
        raise ValueError(msg)

    width = max(2, len(str(len(windows))))
    names = [f"{prefix}-{i:0{width}d}" for i in range(1, len(windows) + 1)]
    cont_key = ContinuanceDirective.key()

    runs: dict[str, RunRef] = {}
    first_deps = list(base.depends_on)
    if first_source is not None and first_source.step:
        ref = StepRef.parse(first_source.step)
        if ref.is_external and first_source.run_id:
            runs[ref.run] = RunRef(run_id=first_source.run_id)
        if str(ref) not in first_deps:
            first_deps.append(str(ref))

    steps: list[Step] = []
    for i, ((start, end), extra) in enumerate(zip(windows, extras, strict=True)):
        overrides = deep_merge(
            base.blueprint_overrides,
            {"runtime_params": {"end_date": end}},
        )
        if restart_cadence:
            overrides = deep_merge(
                overrides,
                {
                    "namelist_overrides": {
                        "basic_output_settings": dict(restart_cadence)
                    }
                },
            )
        overrides = deep_merge(overrides, dict(extra))

        compute = base.compute_overrides
        if walltime is not None:
            wt = walltime(start, end) if callable(walltime) else walltime
            if not re.fullmatch(WALLTIME_RE, wt):
                msg = (
                    f"walltime {wt!r} for window {start} to {end} does not match "
                    "the SLURM walltime format (e.g. 03:00:00 or 1-12:00:00)"
                )
                raise ValueError(msg)
            compute = deep_merge(compute, {"slurm": {"max_walltime": wt}})

        directives = copy.deepcopy(base.directives)
        if chain_directive:
            if i > 0:
                directives[cont_key] = {ContinuanceDirective.KEY_STEP: names[i - 1]}
            elif first_source is not None:
                directives[cont_key] = _source_directive(first_source)

        steps.append(
            Step(
                name=names[i],
                application=base.application,
                blueprint=base.blueprint_path,
                depends_on=first_deps if i == 0 else [names[i - 1]],
                blueprint_overrides=overrides,
                compute_overrides=copy.deepcopy(compute),
                workflow_overrides=copy.deepcopy(base.workflow_overrides),
                directives=directives,
            )
        )

    return Generated(steps=steps, runs=runs)


def chunk_steps(
    base: Step,
    windows: Sequence[Window],
    *,
    prefix: str,
    first_source: StepSource | None = None,
    walltime: str | Callable[[datetime, datetime], str] | None = None,
    restart_cadence: Mapping[str, t.Any] | None = None,
    chain_directive: bool = True,
) -> Generated:
    """Split one run into chained steps, one per time window.

    Each chunk overrides only `runtime_params.end_date`; the start of a chunk
    comes from the restart of the previous one. The blueprint file is never
    read.

    Parameters
    ----------
    base : Step
        The step the chunks are derived from.
    windows : Sequence[tuple[datetime, datetime]]
        The `(start, end)` of each chunk.
    prefix : str
        The step name prefix; chunk `i` is named `<prefix>-<i>`, zero-padded.
    first_source : StepSource | None
        Where the first chunk continues from; by default it starts from the
        blueprint's own initial conditions.
    walltime : str | Callable[[datetime, datetime], str] | None
        The SLURM max walltime of every chunk, or a function of its window.
    restart_cadence : Mapping[str, Any] | None
        `basic_output_settings` namelist overrides applied to every chunk.
    chain_directive : bool
        Chain chunks with `continue-from` directives as well as `depends_on`;
        `False` for applications without directives.

    Returns
    -------
    Generated
    """
    return _chain(
        base,
        windows,
        [{} for _ in windows],
        prefix=prefix,
        first_source=first_source,
        walltime=walltime,
        restart_cadence=restart_cadence,
        chain_directive=chain_directive,
    )


def spinup_ramp(
    base: Step,
    ramp: Sequence[tuple[timedelta, float]],
    *,
    start: datetime,
    prefix: str,
    first_source: StepSource | None = None,
    restart_cadence: Mapping[str, t.Any] | None = None,
) -> Generated:
    """Chain a spin-up whose time step grows from one segment to the next.

    Parameters
    ----------
    base : Step
        The step the segments are derived from.
    ramp : Sequence[tuple[timedelta, float]]
        The `(duration, dt)` of each segment, in order.
    start : datetime
        The start of the first segment; later segments follow consecutively.
    prefix : str
        The step name prefix.
    first_source : StepSource | None
        Where the first segment continues from.
    restart_cadence : Mapping[str, Any] | None
        `basic_output_settings` namelist overrides applied to every segment.

    Returns
    -------
    Generated
    """
    if any(duration <= timedelta(0) for duration, _ in ramp):
        msg = "every ramp duration must be positive"
        raise ValueError(msg)

    windows: list[Window] = []
    cursor = start
    for duration, _ in ramp:
        windows.append((cursor, cursor + duration))
        cursor += duration

    return _chain(
        base,
        windows,
        [{"namelist_overrides": {"time_stepping": {"dt": dt}}} for _, dt in ramp],
        prefix=prefix,
        first_source=first_source,
        walltime=None,
        restart_cadence=restart_cadence,
        chain_directive=True,
    )


def forge_then_run(
    forge_blueprint: Path,
    emitted: "EmittedBlueprint",
    *,
    names: tuple[str, str] = ("forge", "roms_marbl"),
) -> Generated:
    """Generate a forge step followed by the run of the blueprint it emits.

    Parameters
    ----------
    forge_blueprint : Path
        The forge blueprint file.
    emitted : EmittedBlueprint
        The blueprint the forge step publishes.
    names : tuple[str, str]
        The names of the forge and run steps.

    Returns
    -------
    Generated
    """
    forge_name, run_name = names
    forge_step = Step(
        name=forge_name,
        application="forge",
        blueprint=str(forge_blueprint.expanduser().resolve()),
    )
    run_step = Step(
        name=run_name,
        application=emitted.application,
        depends_on=[forge_name],
        blueprint=DeferredBlueprintRef(from_step=forge_name, filename=emitted.filename),
        # a deferred blueprint cannot be inspected at submit time; size the step
        # from the emitted blueprint's partitioning
        compute_overrides={"slurm": {"num_cpus": emitted.cpus_needed}},
    )
    return Generated(steps=[forge_step, run_step], runs={})


def _check_unchunked(levels: Sequence[Step]) -> None:
    """Refuse levels that look like time chunks of one run."""
    by_name = {step.name: step for step in levels}
    for step in levels:
        cont = step.directives.get(ContinuanceDirective.key())
        source = (
            by_name.get(str(cont.get(ContinuanceDirective.KEY_STEP, "")))
            if isinstance(cont, Mapping)
            else None
        )
        if source is not None and source.blueprint_path == step.blueprint_path:
            msg = (
                f"Level {step.name!r} continues from {source.name!r} on the same "
                "blueprint; upscaling time-chunked levels is not yet supported"
            )
            raise ValueError(msg)


def upscale_chain(
    levels: Sequence[Step],
    *,
    use_pio_of: Callable[[Step], bool],
) -> Generated:
    """Generate the upscaling steps that feed each grid level back to its parent.

    For each adjacent pair of levels, an `upscaler` step converts the child's
    output and a copy of the parent (`<parent>_up`) re-runs with the result as
    its `cdr_forcing`. A later pair upscales from the previous pair's re-run
    parent rather than the original.

    Parameters
    ----------
    levels : Sequence[Step]
        The `roms_marbl` steps, innermost grid first.
    use_pio_of : Callable[[Step], bool]
        Whether a level's build uses ParallelIO, which the upscaler must match.

    Returns
    -------
    Generated

    Raises
    ------
    ValueError
        If fewer than two levels are given, a level is not `roms_marbl`, is
        time-chunked, or a parent already sets `cdr_forcing`.
    """
    if len(levels) < 2:
        msg = "upscaling needs at least two levels"
        raise ValueError(msg)
    if bad := [s.name for s in levels if s.application != "roms_marbl"]:
        msg = f"upscaling levels other than roms_marbl is not yet supported: {bad}"
        raise ValueError(msg)
    _check_unchunked(levels)
    if with_cdr := [
        s.name for s in levels[1:] if "cdr_forcing" in s.blueprint_overrides
    ]:
        msg = (
            f"Parent level(s) {with_cdr} already set `cdr_forcing`; combining it with "
            "upscaled forcing is not yet supported"
        )
        raise ValueError(msg)

    steps: list[Step] = []
    child_source = levels[0]
    for child, parent in itertools.pairwise(levels):
        upscale_name = f"upscale_{child.name}_{parent.name}"
        steps.append(
            Step(
                name=upscale_name,
                application=_UPSCALER,
                blueprint=InlineBlueprintRef(),
                depends_on=[child_source.name],
                blueprint_overrides={
                    "uscl_file_location": "{{output_dir: " + child_source.name + "}}",
                    "pio": use_pio_of(parent),
                },
            )
        )
        parent_up = Step(
            name=f"{parent.name}_up",
            application=parent.application,
            blueprint=parent.blueprint_path,
            depends_on=[*parent.depends_on, upscale_name],
            blueprint_overrides=deep_merge(
                parent.blueprint_overrides,
                {
                    "cdr_forcing": {
                        "data": [
                            {
                                "location": "{{output_dir: "
                                + upscale_name
                                + "}}/upscaled_cdr.nc"
                            }
                        ]
                    }
                },
            ),
            compute_overrides=copy.deepcopy(parent.compute_overrides),
            workflow_overrides=copy.deepcopy(parent.workflow_overrides),
            directives=copy.deepcopy(parent.directives),
        )
        steps.append(parent_up)
        child_source = parent_up

    safe_names: dict[str, list[str]] = {}
    for step in [*levels, *steps]:
        safe_names.setdefault(step.safe_name, []).append(step.name)
    if collisions := [names for names in safe_names.values() if len(names) > 1]:
        msg = f"Generated step names collide with existing ones: {collisions}"
        raise ValueError(msg)

    return Generated(steps=steps, runs={})


def _show(value: t.Any) -> str:
    """Render a value for a `Change`."""
    return value if isinstance(value, str) else str(value)


def _covered(in_file: t.Any, in_overrides: t.Any) -> bool:
    """Whether the overrides set everything a blueprint file sets under a key."""
    if not isinstance(in_file, Mapping):
        return True
    if not isinstance(in_overrides, Mapping):
        return False
    return all(
        k in in_overrides and _covered(v, in_overrides[k]) for k, v in in_file.items()
    )


def _is_inline_candidate(step: Step) -> bool:
    """Whether a step's blueprint file adds nothing beyond its overrides."""
    if not isinstance(step.blueprint_path, str | Path):
        return False

    path = Path(step.blueprint_path)
    if not path.is_file():
        return False

    try:
        data = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError):
        return False

    if (
        not isinstance(data, Mapping)
        or data.get("application") not in INLINE_APPLICATIONS
    ):
        return False
    if data["application"] != step.application:
        return False

    return all(
        key in step.blueprint_overrides
        and _covered(value, step.blueprint_overrides[key])
        for key, value in data.items()
        if key not in BLUEPRINT_METADATA_FIELDS
    )


def _rewrite_legacy_output(
    value: t.Any, field: str, emit: Callable[[str, str, str], None]
) -> t.Any:
    """Rewrite `/joined_output` to `/output` in every string under `value`."""
    if isinstance(value, str):
        rewritten = _LEGACY_OUTPUT_PATTERN.sub("/output", value)
        if rewritten != value:
            emit(field, value, rewritten)
        return rewritten
    if isinstance(value, dict):
        return {
            k: _rewrite_legacy_output(v, f"{field}.{k}", emit) for k, v in value.items()
        }
    if isinstance(value, list):
        return [
            _rewrite_legacy_output(v, f"{field}[{i}]", emit)
            for i, v in enumerate(value)
        ]
    return value


def _task_dir_key(name: str) -> str:
    """Return the form in which a step name and its task directory compare equal.

    Task directories written by earlier layouts spell word breaks with `-`
    where the step name has `_`, so the two are treated alike.
    """
    return _WORD_BREAKS.sub("-", name.casefold()).strip("-")


def _task_step(
    token: str, tasks_dir: Path, owners: Mapping[str, str], own: str
) -> str | None:
    """Return the step whose task directory contains `token`, if any."""
    path = Path(token)
    if not path.is_absolute():
        return None
    try:
        parts = path.relative_to(tasks_dir).parts
    except ValueError:
        return None
    owner = owners.get(_task_dir_key(parts[0])) if parts else None
    return owner if owner != own else None


def _normalize_step(
    step: Step,
    owners: Mapping[str, str],
    run_root: Path | None,
) -> tuple[Step, list[Change]]:
    """Rewrite one step; see `normalize_legacy`."""
    changes: list[Change] = []

    def record(field: str, before: t.Any, after: t.Any, reason: str) -> None:
        changes.append(
            Change(
                step=step.name,
                field=field,
                before=_show(before),
                after=_show(after),
                reason=reason,
            )
        )

    directives = copy.deepcopy(step.directives)
    overrides = copy.deepcopy(step.blueprint_overrides)
    depends_on = list(step.depends_on)
    nest_key, cont_key = NestingDirective.key(), ContinuanceDirective.key()
    nest = directives.get(nest_key)
    nest = nest if isinstance(nest, dict) else None

    if nest is not None and NestingDirective.KEY_RST_PATH in nest:
        field = f"directives.{nest_key}.{NestingDirective.KEY_RST_PATH}"
        value = nest[NestingDirective.KEY_RST_PATH]
        if cont_key in directives:
            record(
                field,
                value,
                value,
                f"conflict: {cont_key} already present; "
                f"{NestingDirective.KEY_RST_PATH} left in place",
            )
        else:
            del nest[NestingDirective.KEY_RST_PATH]
            directives[cont_key] = {ContinuanceDirective.KEY_PATH: value}
            record(
                field,
                value,
                f"directives.{cont_key}.{ContinuanceDirective.KEY_PATH}",
                f"{NestingDirective.KEY_RST_PATH} is deprecated; the restart moved to {cont_key}",
            )
            if not nest:
                del directives[nest_key]
                nest = None

    if (
        nest is not None
        and NestingDirective.KEY_BRY_PATH in nest
        and NestingDirective.KEY_PATH not in nest
        and NestingDirective.KEY_STEP not in nest
    ):
        renamed = {
            (NestingDirective.KEY_PATH if k == NestingDirective.KEY_BRY_PATH else k): v
            for k, v in nest.items()
        }
        nest.clear()
        nest.update(renamed)
        record(
            f"directives.{nest_key}.{NestingDirective.KEY_BRY_PATH}",
            nest[NestingDirective.KEY_PATH],
            f"directives.{nest_key}.{NestingDirective.KEY_PATH}",
            f"{NestingDirective.KEY_BRY_PATH} is a deprecated alias of {NestingDirective.KEY_PATH}",
        )

    def legacy(field: str, before: str, after: str) -> None:
        record(
            field,
            before,
            after,
            "`joined_output` is the legacy output directory; use `output`",
        )

    path_keys = (
        ContinuanceDirective.KEY_PATH,
        NestingDirective.KEY_PATH,
        NestingDirective.KEY_BRY_PATH,
        NestingDirective.KEY_RST_PATH,
    )
    for key in (cont_key, nest_key):
        config = directives.get(key)
        if isinstance(config, dict):
            for path_key in path_keys:
                if isinstance(config.get(path_key), str):
                    config[path_key] = _rewrite_legacy_output(
                        config[path_key], f"directives.{key}.{path_key}", legacy
                    )
    overrides = _rewrite_legacy_output(overrides, "blueprint_overrides", legacy)

    if run_root is not None:
        tasks_dir = run_root / "tasks"

        def depend_on(name: str) -> None:
            if name not in depends_on:
                depends_on.append(name)
                record(
                    "depends_on",
                    "",
                    name,
                    "required by the step reference that replaced a task path",
                )

        cont = directives.get(cont_key)
        if isinstance(cont, dict) and isinstance(
            cont.get(ContinuanceDirective.KEY_PATH), str
        ):
            path = cont[ContinuanceDirective.KEY_PATH]
            if owner := _task_step(path, tasks_dir, owners, step.name):
                rewritten: dict[str, t.Any] = {ContinuanceDirective.KEY_STEP: owner}
                rewritten.update(
                    {
                        k: v
                        for k, v in cont.items()
                        if k != ContinuanceDirective.KEY_PATH
                    }
                )
                restart_ts = restart_timestamp(path)
                if (
                    restart_ts is not None
                    and ContinuanceDirective.KEY_TIMESTAMP not in rewritten
                ):
                    rewritten[ContinuanceDirective.KEY_TIMESTAMP] = restart_ts.strftime(
                        TIMESTAMP_FMT
                    )
                directives[cont_key] = rewritten
                record(
                    f"directives.{cont_key}.{ContinuanceDirective.KEY_PATH}",
                    path,
                    ", ".join(f"{k}: {v}" for k, v in rewritten.items()),
                    "path lies in the task directory of a step of this workplan",
                )
                depend_on(owner)

        nest = directives.get(nest_key)
        if isinstance(nest, dict) and isinstance(
            nest.get(NestingDirective.KEY_PATH), str
        ):
            value = nest[NestingDirective.KEY_PATH]
            tokens = [
                tok.strip()
                for tok in value.split(NestingDirective.SOURCE_DELIMITER)
                if tok.strip()
            ]
            owned = [_task_step(tok, tasks_dir, owners, step.name) for tok in tokens]
            if tokens and all(owned):
                names = [name for name in owned if name]
                step_value = NestingDirective.SOURCE_DELIMITER.join(names)
                rewritten = {
                    (
                        NestingDirective.KEY_STEP
                        if k == NestingDirective.KEY_PATH
                        else k
                    ): (step_value if k == NestingDirective.KEY_PATH else v)
                    for k, v in nest.items()
                }
                directives[nest_key] = rewritten
                record(
                    f"directives.{nest_key}.{NestingDirective.KEY_PATH}",
                    value,
                    f"{NestingDirective.KEY_STEP}: {step_value}",
                    "path lies in the task directory of a step of this workplan",
                )
                for name in dict.fromkeys(names):
                    depend_on(name)

    blueprint: t.Any = step.blueprint_path
    if _is_inline_candidate(step):
        record(
            "blueprint",
            step.blueprint_path,
            InlineBlueprintRef.TOKEN,
            "the step's overrides already set everything the blueprint file sets",
        )
        blueprint = InlineBlueprintRef()

    updated = step.model_copy(
        update={
            "directives": directives,
            "blueprint_overrides": overrides,
            "depends_on": depends_on,
            "blueprint_path": blueprint,
        },
        deep=True,
    )
    return updated, changes


def normalize_legacy(
    wp: Workplan, *, run_root: Path | None = None
) -> tuple[Workplan, list[Change]]:
    """Rewrite deprecated and path-based spellings of a workplan.

    The rewrites are: a `nest-from` `rst_path` becomes a `continue-from`
    `path`; `bry_path` becomes `path`; the legacy `joined_output` directory
    becomes `output`; with `run_root`, a path into the task directory of a
    step of this workplan becomes a `step` reference (with the `timestamp` of a
    restart file named by a `continue-from` path); and a blueprint file that
    adds nothing beyond the step's overrides becomes `inline`.

    Parameters
    ----------
    wp : Workplan
        The workplan to normalize; it is not modified.
    run_root : Path | None
        The run directory the workplan's paths point into, enabling the
        path-to-step rewrite.

    Returns
    -------
    tuple[Workplan, list[Change]]
        The rewritten workplan and every rewrite made.
    """
    keyed: dict[str, list[str]] = {}
    for step in wp.steps:
        keyed.setdefault(_task_dir_key(step.name), []).append(step.name)
    owners = {key: names[0] for key, names in keyed.items() if len(names) == 1}

    steps: list[Step] = []
    changes: list[Change] = []
    for step in wp.steps:
        rewritten, step_changes = _normalize_step(step, owners, run_root)
        steps.append(rewritten)
        changes.extend(step_changes)

    normalized = Workplan.model_validate({**copy.deepcopy(dict(wp)), "steps": steps})
    return normalized, changes


def available_restarts(directory: Path) -> list[datetime]:
    """List the restart timestamps found under a directory.

    Parameters
    ----------
    directory : Path
        The directory to search, whole and partitioned restart files alike.

    Returns
    -------
    list[datetime]
        The sorted, unique timestamps; empty if the directory does not exist.
    """
    if not directory.is_dir():
        return []

    found = {
        rst.timestamp
        for partitioned in (False, True)
        for rst in RestartFile.candidates(directory, partitioned=partitioned)
    }
    return sorted(found)


def predicted_restarts(
    windows: Sequence[Window],
    restart_cadence: Mapping[str, t.Any] | None,
) -> list[datetime]:
    """Predict the restart timestamps a chunked run will write.

    Parameters
    ----------
    windows : Sequence[tuple[datetime, datetime]]
        The `(start, end)` of each chunk; every end is a restart.
    restart_cadence : Mapping[str, Any] | None
        `basic_output_settings` overrides; an `output_period_rst` (seconds)
        adds a restart every period from each window's start, unless
        `monthly_restarts` is set.

    Returns
    -------
    list[datetime]
        The sorted, unique timestamps.
    """
    found = {end for _, end in windows}

    cadence = restart_cadence or {}
    period = cadence.get("output_period_rst")
    if period and period > 0 and not cadence.get("monthly_restarts", False):
        step = timedelta(seconds=period)
        for start, end in windows:
            moment = start + step
            while moment < end:
                found.add(moment)
                moment += step

    return sorted(found)
