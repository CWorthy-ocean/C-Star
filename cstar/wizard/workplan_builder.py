"""The wizard's Workplan page.

The page builds a :class:`~cstar.orchestration.models.Workplan` draft from
widgets: workplan metadata and the runs it references, a compute target, and a
pane per step.  Every edit regathers the draft (``_rebuild``), validates it
with the same model ``cstar workplan check`` loads, and refreshes the YAML
preview, the sticky bar and the per-card status chips.  The page never writes
a loaded file: saving always goes through an explicit target path.

Pure helpers (blueprint facts, namelist field discovery, the YAML text) sit at
the top; :class:`_StepPane` owns one step's widgets; :class:`WorkplanBuilderPage`
owns the cards and the draft.  The constructor signature and the ``.widget`` /
``.blueprint_app`` attributes are the contract with the shell
(:func:`cstar.wizard.ui.shell.app`).
"""

from __future__ import annotations

import base64
import copy
import functools
import html
import json
import sys
import tempfile
import threading
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import yaml
from pydantic import BaseModel, ValidationError

from cstar.applications.core import get_application, get_application_name
from cstar.applications.forge.blueprint import DEFAULT_APPLICATION as FORGE
from cstar.applications.hello_world import APP_NAME as HELLO_WORLD
from cstar.applications.nest_ic import APP_NAME as NEST_IC
from cstar.applications.roms_marbl.models import APP_NAME as ROMS_MARBL
from cstar.applications.roms_marbl.transforms import (
    ContinuanceDirective,
    NestingDirective,
)
from cstar.applications.upscaler import APP_NAME as UPSCALER
from cstar.base.utils import deep_merge, slugify
from cstar.entrypoint.utils import ARG_PRE_RUN, ARG_RUN_ID, ARG_VAR_LONG
from cstar.orchestration.check import deep_check
from cstar.orchestration.compute_environment import (
    ComputeEnvironment,
    resolve_compute_environment,
)
from cstar.orchestration.dag_runner import get_launcher, prune_unpreparable_steps
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.launch.slurm import SlurmComputeSpec, SlurmLauncher
from cstar.orchestration.models import (
    RUN_ALIAS_SEPARATOR,
    DeferredBlueprintRef,
    InlineBlueprintRef,
    RunRef,
    Step,
    StepRef,
    Workplan,
)
from cstar.orchestration.patterns import (
    INLINE_APPLICATIONS,
    TIMESTAMP_FMT,
    Change,
    Generated,
    StepSource,
    available_restarts,
    chunk_steps,
    equal_windows,
    fixed_windows,
    forge_then_run,
    month_windows,
    normalize_legacy,
    spinup_ramp,
    upscale_chain,
)
from cstar.orchestration.serialization import deserialize, model_to_yaml
from cstar.orchestration.transforms import PLACEHOLDER_RE, mustache
from cstar.roms.namelist import namelist_schema_for_ref
from cstar.system.manager import get_registered_sys_contexts, get_sysmgr
from cstar.system.scheduler import SlurmScheduler
from cstar.wizard.ui import components
from cstar.wizard.ui.catalog_bar import CONFIRM_TIMEOUT
from cstar.wizard.ui.labels import label_for
from cstar.wizard.wizard import (
    _STREAM_READ_SIZE,
    _base_type,
    _drain_stream_buffer,
    _make_field_widget,
    _read_field_widget,
    _schedule_coroutine,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from pydantic.fields import FieldInfo

    from cstar.applications.core import EmittedBlueprint
    from cstar.system.scheduler import Scheduler

PAGE = "workplan-builder"
"""The label-glossary page and the ``page=`` argument of the UI kit."""

WORKPLAN_SCHEMA_REF = (
    "https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/"
    "docs/schemas/wp/workplan_schema.1.0.0.json"
)
"""The JSON schema editors validate a workplan file against."""

APPLICATIONS = (ROMS_MARBL, FORGE, NEST_IC, UPSCALER, HELLO_WORLD)
"""The applications a step may be authored for."""

CHECK, RUN = "check", "run"
"""The ``cstar workplan`` subcommands the Run subsection streams."""

FREQUENT_RESTARTS = {"output_period_rst": 86400.0, "monthly_restarts": False}
"""``basic_output_settings`` overrides that write a restart every simulated day."""

_UNVERIFIABLE = (
    "no run record found",
    "blueprint file not found",
    "no file found",
    "no such file",
    "unable to load workplan",
    "unable to locate",
    "not been submitted",
)
"""Fragments (lower-case) of problems that mean "cannot be read here", not "wrong"."""

_PLACEHOLDER_SCOPES = ("input_dir", "output_dir")
"""The directory scopes a ``{{scope: step}}`` placeholder can name."""

# step blueprint source kinds, as (label, value)
SOURCE_CATALOG_ROMS = "catalog_roms"
SOURCE_CATALOG_FORGE = "catalog_forge"
SOURCE_PATH = "path"
SOURCE_UPLOAD = "upload"
SOURCE_DEFERRED = "deferred"
SOURCE_INLINE = "inline"
SOURCE_CURRENT = "current"
_SOURCES = [
    ("catalog roms_marbl", SOURCE_CATALOG_ROMS),
    ("catalog forge", SOURCE_CATALOG_FORGE),
    ("path", SOURCE_PATH),
    ("upload", SOURCE_UPLOAD),
    ("deferred (from a step)", SOURCE_DEFERRED),
    ("inline (no file)", SOURCE_INLINE),
    ("current blueprint page config", SOURCE_CURRENT),
]
_FILE_SOURCES = frozenset(
    {
        SOURCE_CATALOG_ROMS,
        SOURCE_CATALOG_FORGE,
        SOURCE_PATH,
        SOURCE_UPLOAD,
        SOURCE_CURRENT,
    }
)

# compute-target radio options
TARGET_LOCAL = "Local"
TARGET_SLURM = "SLURM"
TARGET_NONE = "Not specified"
MACHINE_CUSTOM = "custom"

_KIND_NONE = "none"
_KIND_STEP = "step"
_KIND_PATH = "path"

_FORM_EXCLUDED = frozenset(
    {"name", "description", "application", "state", "schema_version", "working_dir"}
)
"""Blueprint fields a generated inline form leaves out."""


# ---------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------
def _caption(key: str, default: str) -> str:
    """A button caption from the glossary's ``buttons.<key>`` entry."""
    return label_for(f"buttons.{key}", default, page=PAGE).label


def _show(widget: Any, visible: bool) -> None:
    """Show or hide a widget (a ``field_row`` mirrors its widget's display)."""
    widget.layout.display = "" if visible else "none"


def _esc(text: object) -> str:
    """HTML-escape ``text``."""
    return html.escape(str(text))


def _as_datetime(value: object) -> datetime | None:
    """Coerce a YAML scalar (datetime, date or ISO text) to a datetime, else ``None``."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


@dataclass(frozen=True)
class BlueprintFacts:
    """What the builder reads from a readable blueprint file."""

    application: str = ""
    start_date: datetime | None = None
    end_date: datetime | None = None
    cpus_needed: int = 0
    use_pio: bool | None = None
    roms_ref: str | None = None


@functools.lru_cache(maxsize=128)
def _read_facts(path: str, mtime_ns: int) -> BlueprintFacts | None:
    """Read a blueprint file's facts; keyed on mtime so edits are seen."""
    try:
        raw = yaml.safe_load(Path(path).read_text())
    except (OSError, yaml.YAMLError):
        return None
    if not isinstance(raw, dict):
        return None

    def _section(name: str) -> dict[str, Any]:
        value = raw.get(name)
        return value if isinstance(value, dict) else {}

    partitioning = _section("partitioning")
    cpus = partitioning.get("n_cores")
    if not cpus:
        nx, ny = partitioning.get("n_procs_x"), partitioning.get("n_procs_y")
        cpus = nx * ny if isinstance(nx, int) and isinstance(ny, int) else 0
    roms = _section("code").get("roms")
    roms = roms if isinstance(roms, dict) else {}
    runtime = _section("runtime_params")
    use_pio = partitioning.get("use_pio")
    return BlueprintFacts(
        application=str(raw.get("application", "")),
        start_date=_as_datetime(runtime.get("start_date")),
        end_date=_as_datetime(runtime.get("end_date")),
        cpus_needed=int(cpus) if isinstance(cpus, int) else 0,
        use_pio=use_pio if isinstance(use_pio, bool) else None,
        roms_ref=str(roms.get("commit") or roms.get("branch") or "") or None,
    )


def blueprint_facts(path: str | Path) -> BlueprintFacts | None:
    """The facts of the blueprint at ``path``, or ``None`` when unreadable."""
    try:
        resolved = Path(path).expanduser()
        return _read_facts(str(resolved), resolved.stat().st_mtime_ns)
    except OSError:
        return None


@functools.lru_cache(maxsize=32)
def _read_emitted(
    path: str, application: str, mtime_ns: int
) -> EmittedBlueprint | None:
    """The blueprint an application's run of ``path`` will emit, if it emits one."""
    try:
        app = get_application(application)
        return app.emitted_blueprint(deserialize(Path(path), app.blueprint))
    except Exception:  # an unreadable producer is simply not predictable
        return None


def emitted_for(path: str | Path, application: str) -> EmittedBlueprint | None:
    """What ``application`` would emit when run on the blueprint at ``path``."""
    try:
        resolved = Path(path).expanduser()
        return _read_emitted(str(resolved), application, resolved.stat().st_mtime_ns)
    except OSError:
        return None


@functools.lru_cache(maxsize=16)
def namelist_fields(ref: str | None) -> dict[str, dict[str, FieldInfo]]:
    """The namelist sections (and their fields) of the schema for a ucla-roms ref."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        schema = namelist_schema_for_ref(ref)
    sections: dict[str, dict[str, FieldInfo]] = {}
    for name, info in schema.model_fields.items():
        candidates: list[Any] = [
            info.annotation,
            *getattr(info.annotation, "__args__", ()),
        ]
        for model in candidates:
            if isinstance(model, type) and issubclass(model, BaseModel):
                sections[name] = dict(cast("type[BaseModel]", model).model_fields)
                break
    return sections


def _field_default(info: FieldInfo) -> Any:
    """A widget-safe default: ``None`` for a required field (it has none)."""
    return None if info.is_required() else info.get_default(call_default_factory=True)


def _pop_path(data: dict[str, Any], *keys: str) -> Any:
    """Pop a nested key, pruning parents left empty; ``None`` if absent."""
    parents: list[dict[str, Any]] = [data]
    for key in keys[:-1]:
        child = parents[-1].get(key)
        if not isinstance(child, dict):
            return None
        parents.append(child)
    if keys[-1] not in parents[-1]:
        return None
    value = parents[-1].pop(keys[-1])
    for parent, key in zip(reversed(parents[:-1]), reversed(keys[:-1]), strict=True):
        if not parent[key]:
            del parent[key]
    return value


def _placeholder_steps(value: object) -> list[str]:
    """The step tokens named by ``{{scope: step}}`` placeholders under ``value``."""
    found: list[str] = []
    for body in PLACEHOLDER_RE.findall(json.dumps(value, default=str)):
        scope, sep, token = body.partition(":")
        if sep and token.strip() and token.strip() not in found:
            found.append(token.strip())
    return found


def _parse_datetime(text: str) -> datetime | str:
    """A datetime for ISO ``text``; ``{{placeholder}}`` text is kept as written."""
    parsed = _as_datetime(text)
    if parsed is not None:
        return parsed
    if PLACEHOLDER_RE.search(text):
        return text
    msg = f"{text!r} is not an ISO date or date-time (e.g. 2012-02-01 or 2012-02-01 06:00:00)"
    raise ValueError(msg)


def _pairs(options: Iterable[Any]) -> list[tuple[str, Any]]:
    """``options`` as ``(label, value)`` pairs (a mixed list would not be read as pairs)."""
    return [o if isinstance(o, tuple) else (str(o), o) for o in options]


def _set_options(widget: Any, options: Iterable[Any], keep: Iterable[str] = ()) -> None:
    """Set a widget's options without losing its selection.

    The selection (plus ``keep``) stays an option even when no longer offered,
    so a stale reference stays visible for validation rather than vanishing.
    Nothing is touched when the option set is unchanged; a Dropdown with no
    selection takes its first option.
    """
    current = widget.value
    held = [current] if isinstance(current, str) else list(current or ())
    wanted = _pairs(options)
    offered = {v for _label, v in wanted}
    wanted += [(v, v) for v in dict.fromkeys([*held, *keep]) if v and v not in offered]
    if list(widget.options) != wanted:
        widget.options = wanted
        widget.value = current
    if widget.value is None and wanted and isinstance(current, str | type(None)):
        if hasattr(widget, "index"):
            widget.value = wanted[0][1]


def _add_option(widget: Any, *values: str) -> None:
    """Append ``values`` to a widget's options when missing (selection kept)."""
    offered = {v for _label, v in _pairs(widget.options)}
    missing = [v for v in values if v and v not in offered]
    if missing:
        current = widget.value
        widget.options = [*_pairs(widget.options), *_pairs(missing)]
        widget.value = current


def format_walltime(hours: float) -> str:
    """``HH:MM:SS`` (or ``D-HH:MM:SS``) for ``hours``, rounded up to a whole minute."""
    minutes = max(1, -(-round(hours * 3600) // 60))
    days, minutes = divmod(minutes, 24 * 60)
    clock = f"{minutes // 60:02d}:{minutes % 60:02d}:00"
    return f"{days}-{clock}" if days else clock


def _is_unverifiable(problem: str) -> bool:
    """Whether a deep-check problem only says something is unreadable on this machine."""
    lowered = problem.casefold()
    return any(fragment in lowered for fragment in _UNVERIFIABLE)


def yaml_text(workplan: Workplan) -> str:
    """The workplan's file text: the schema header plus its YAML."""
    return f"# yaml-language-server: $schema={WORKPLAN_SCHEMA_REF}\n" + model_to_yaml(
        workplan
    )


def live_system_name() -> str:
    """The name of the machine the wizard runs on, or ``""`` when unknown."""
    try:
        return get_sysmgr().name
    except Exception:  # off-HPC the manager may refuse to build
        return ""


@functools.lru_cache(maxsize=1)
def slurm_machines() -> dict[str, Scheduler]:
    """The registered systems that submit through SLURM, with their schedulers."""
    machines: dict[str, Scheduler] = {}
    for ctx in get_registered_sys_contexts():
        try:
            scheduler = ctx.create_scheduler()
        except Exception:
            continue
        if isinstance(scheduler, SlurmScheduler):
            machines[ctx.name] = scheduler
    return machines


@functools.lru_cache(maxsize=1)
def unsupported_machines() -> list[str]:
    """The registered systems with a scheduler but no workplan launcher."""
    return sorted(
        ctx.name
        for ctx in get_registered_sys_contexts()
        if (sched := ctx.create_scheduler()) is not None
        and not isinstance(sched, SlurmScheduler)
    )


# ---------------------------------------------------------------------------
# step pane
# ---------------------------------------------------------------------------
class _NamelistRow:
    """One ``section / field / value`` row of a pane's namelist-override table."""

    def __init__(self, pane: _StepPane) -> None:
        """Build the row's widgets; the pane supplies the schema and the callback."""
        W = pane.W
        self.pane = pane
        self.base: type = str
        self.section = W.Dropdown(layout=W.Layout(width="190px"))
        self.field = W.Dropdown(layout=W.Layout(width="190px"))
        self.holder = W.HBox([])
        self.remove_btn = W.Button(icon="trash", tooltip="Remove this override")
        self.widget = W.HBox([self.section, self.field, self.holder, self.remove_btn])
        self.section.observe(self._on_section, names="value")
        self.field.observe(self._on_field, names="value")
        self.remove_btn.on_click(lambda _b: pane._remove_row(self))
        self.refresh_sections()

    def refresh_sections(self) -> None:
        """Reload the section options from the pane's current schema."""
        with self.pane.page.suspended():
            _set_options(self.section, list(self.pane.schema()))
            self._load_fields()

    def _load_fields(self) -> None:
        """Reload the field options for the selected section."""
        fields = self.pane.schema().get(self.section.value, {})
        _set_options(self.field, list(fields))
        self._load_value(None, use_default=True)

    def _load_value(self, value: Any, *, use_default: bool) -> None:
        """Build the value widget for the selected field."""
        info = self.pane.schema().get(self.section.value, {}).get(self.field.value)
        if info is None:
            self.holder.children = []
            return
        default = _field_default(info)
        self.base = _base_type(info.annotation, default)
        widget = _make_field_widget(
            self.pane.W, "", self.base, default if use_default else value
        )
        widget.description = ""
        widget.observe(self.pane._changed, names="value")
        self.holder.children = [widget]

    def _on_section(self, _change: Any) -> None:
        if self.pane.page.is_suspended:
            return
        with self.pane.page.suspended():
            self._load_fields()
        self.pane._changed()

    def _on_field(self, _change: Any) -> None:
        if self.pane.page.is_suspended:
            return
        with self.pane.page.suspended():
            self._load_value(None, use_default=True)
        self.pane._changed()

    def accepts(self, section: str, name: str, value: Any) -> bool:
        """Whether the row can carry ``value`` for ``section.name`` losslessly."""
        info = self.pane.schema().get(section, {}).get(name)
        if info is None:
            return False
        base = _base_type(info.annotation, _field_default(info))
        widget = _make_field_widget(self.pane.W, "", base, value)
        try:
            return bool(_read_field_widget(widget, base) == value)
        except (TypeError, ValueError):
            return False

    def populate(self, section: str, name: str, value: Any) -> None:
        """Show ``section.name = value`` (the caller checked :meth:`accepts`)."""
        with self.pane.page.suspended():
            self.section.value = section
            self._load_fields()
            self.field.value = name
            self._load_value(value, use_default=False)

    def value(self) -> tuple[str, str, Any] | None:
        """The ``(section, field, value)`` this row sets, or ``None`` when blank."""
        if not (self.section.value and self.field.value and self.holder.children):
            return None
        return (
            self.section.value,
            self.field.value,
            _read_field_widget(self.holder.children[0], self.base),
        )


class _StepPane:
    """The widgets and gather/populate logic of one workplan step."""

    def __init__(self, page: WorkplanBuilderPage, name: str = "") -> None:
        """Build a pane for a new step called ``name``."""
        W = page.W
        self.page = page
        self.W = W
        self.error = ""
        self._last_name = name
        self._dep_order: list[str] = []
        self._uploaded_path = ""
        self._current_path = ""
        self._cpus_touched = False
        self._app_fallback: str = ROMS_MARBL
        self._form_app = ""
        self._form: dict[str, tuple[Any, type, Any]] = {}
        self._form_touched: set[str] = set()
        self._passthrough_directives: dict[str, Any] = {}
        self._passthrough_compute: dict[str, Any] = {}
        self._rows: list[_NamelistRow] = []
        self._schema_ref: str | None = None
        self.implied: list[str] = []
        self.problem_lines: list[str] = []

        def text(**kw: Any) -> Any:
            return W.Text(continuous_update=False, layout=W.Layout(width="420px"), **kw)

        self.name = text(value=name)
        self.source = W.Dropdown(options=_SOURCES, value=SOURCE_PATH)
        self.catalog_roms = W.Dropdown(options=[])
        self.catalog_forge = W.Dropdown(options=[])
        self.path = text(placeholder="/path/to/blueprint.yaml")
        self.upload = W.FileUpload(accept=".yml,.yaml", multiple=False)
        self.upload_note = W.HTML("")
        self.producer = W.Dropdown(options=[])
        self.filename = text(placeholder="B_<name>.yaml")
        self.application = W.Dropdown(options=list(APPLICATIONS), value=ROMS_MARBL)
        self.current_note = W.HTML("")
        self.source_note = W.HTML("")

        self.depends_on = W.SelectMultiple(
            options=[], rows=4, layout=W.Layout(width="420px")
        )
        self.locked_deps = W.HTML("")

        # roms_marbl overrides
        self.start_note = W.HTML("")
        self.end_date = text(placeholder="ISO date, blank = from the blueprint")
        self.cdr_location = text(placeholder="path to a CDR forcing netCDF")
        self.roms_branch = text(placeholder="ucla-roms branch")
        self.roms_commit = text(placeholder="ucla-roms commit")
        self.add_row_btn = W.Button(
            description=_caption("add_override", "Add namelist override"), icon="plus"
        )
        self.rows_box = W.VBox([])
        # other applications' generated form
        self.form_box = W.VBox([])
        self.raw_overrides = W.Textarea(
            placeholder="blueprint overrides as YAML, merged last",
            layout=W.Layout(width="520px", height="90px"),
        )

        # directives
        self.cont_kind = W.Dropdown(
            options=[(k, k) for k in (_KIND_NONE, _KIND_STEP, _KIND_PATH)],
            value=_KIND_NONE,
        )
        self.cont_step = W.Dropdown(options=[])
        self.cont_path = text(placeholder="restart directory or file")
        self.cont_pick = W.Dropdown(options=[""])
        self.cont_timestamp = text(placeholder="YYYY-MM-DD HH:MM:SS, blank = latest")
        self.directive_note = W.HTML("")
        self.nest_kind = W.Dropdown(
            options=[(k, k) for k in (_KIND_NONE, _KIND_STEP, _KIND_PATH)],
            value=_KIND_NONE,
        )
        self.nest_rows: list[Any] = []
        self.nest_box = W.VBox([])
        self.add_nest_btn = W.Button(
            description=_caption("add_source", "Add boundary source"), icon="plus"
        )

        # compute overrides
        self.num_cpus = W.IntText(value=0, layout=W.Layout(width="140px"))
        self.max_walltime = text(placeholder="HH:MM:SS")
        self.queue_name = text()
        self.account_name = text()
        self.cpus_per_node = W.IntText(value=0, layout=W.Layout(width="140px"))
        self.local_walltime = text(placeholder="HH:MM:SS")

        self.status = W.HTML("")
        self.up_btn = W.Button(icon="arrow-up", tooltip="Move up")
        self.down_btn = W.Button(icon="arrow-down", tooltip="Move down")
        self.dup_btn = W.Button(icon="copy", tooltip="Duplicate")
        self.del_btn = W.Button(icon="trash", tooltip="Delete", button_style="danger")
        self.up_btn.on_click(lambda _b: page._move(self, -1))
        self.down_btn.on_click(lambda _b: page._move(self, 1))
        self.dup_btn.on_click(lambda _b: page._duplicate(self))
        self.del_btn.on_click(lambda _b: page._delete(self))
        self.add_row_btn.on_click(lambda _b: self._add_row())
        self.add_nest_btn.on_click(lambda _b: self._add_nest_row(_KIND_STEP, ""))

        for widget in (
            self.depends_on,
            self.end_date,
            self.cdr_location,
            self.roms_branch,
            self.roms_commit,
            self.raw_overrides,
            self.cont_kind,
            self.cont_step,
            self.cont_path,
            self.cont_timestamp,
            self.nest_kind,
            self.max_walltime,
            self.queue_name,
            self.account_name,
            self.cpus_per_node,
            self.local_walltime,
        ):
            widget.observe(self._changed, names="value")
        self.num_cpus.observe(self._on_cpus, names="value")
        self.name.observe(self._on_name, names="value")
        self.source.observe(self._on_blueprint_change, names="value")
        for widget in (
            self.catalog_roms,
            self.catalog_forge,
            self.path,
            self.producer,
            self.filename,
        ):
            widget.observe(self._on_blueprint_change, names="value")
        self.upload.observe(self._on_upload, names="value")
        self.application.observe(self._on_application, names="value")
        self.cont_pick.observe(self._on_pick, names="value")
        self.cont_kind.observe(self._on_directive_kind, names="value")
        self.cont_path.observe(self._on_directive_kind, names="value")
        self.cont_step.observe(self._on_directive_kind, names="value")
        self.nest_kind.observe(self._on_directive_kind, names="value")

        def row(key: str, widget: Any, **kw: Any) -> Any:
            return components.field_row(W, f"step.{key}", widget, page=PAGE, **kw)

        sub = components.subsection
        self.widget = W.VBox(
            [
                W.HBox(
                    [
                        self.status,
                        self.up_btn,
                        self.down_btn,
                        self.dup_btn,
                        self.del_btn,
                    ]
                ),
                row("name", self.name),
                sub(
                    W,
                    "step.blueprint",
                    row("source", self.source),
                    row("catalog_roms", self.catalog_roms),
                    row("catalog_forge", self.catalog_forge),
                    row("path", self.path),
                    row("upload", self.upload, extra=(self.upload_note,)),
                    row("producer", self.producer),
                    row("filename", self.filename),
                    self.current_note,
                    row("application", self.application),
                    self.source_note,
                    page=PAGE,
                ),
                row("depends_on", self.depends_on, extra=(self.locked_deps,)),
                sub(
                    W,
                    "step.overrides",
                    self.start_note,
                    row("end_date", self.end_date),
                    W.VBox([self.rows_box, self.add_row_btn]),
                    row("cdr_location", self.cdr_location),
                    row("roms_branch", self.roms_branch),
                    row("roms_commit", self.roms_commit),
                    self.form_box,
                    row("raw_overrides", self.raw_overrides),
                    page=PAGE,
                ),
                sub(
                    W,
                    "step.directives",
                    row("cont_kind", self.cont_kind),
                    row("cont_step", self.cont_step),
                    row("cont_path", self.cont_path),
                    row("cont_pick", self.cont_pick),
                    row("cont_timestamp", self.cont_timestamp),
                    row("nest_kind", self.nest_kind),
                    W.VBox([self.nest_box, self.add_nest_btn]),
                    self.directive_note,
                    page=PAGE,
                ),
                sub(
                    W,
                    "step.compute",
                    row("num_cpus", self.num_cpus),
                    row("max_walltime", self.max_walltime),
                    row("queue_name", self.queue_name),
                    row("account_name", self.account_name),
                    row("cpus_per_node", self.cpus_per_node),
                    row("local_walltime", self.local_walltime),
                    page=PAGE,
                ),
            ]
        )
        self._directives_section = self.widget.children[4]
        self._overrides_section = self.widget.children[3]
        with page.suspended():
            self.refresh_choices()
            self._sync()

    # ---- schema and facts ------------------------------------------------
    def schema(self) -> dict[str, dict[str, FieldInfo]]:
        """The namelist sections for this step's ucla-roms ref."""
        return namelist_fields(self._schema_ref)

    def facts(self) -> BlueprintFacts | None:
        """The facts of the step's blueprint file, when it names a readable one."""
        path, _ = self.resolve()
        return blueprint_facts(path) if path else None

    def resolve(self) -> tuple[str, list[str]]:
        """The step's blueprint file path (``""`` for none) and any problems."""
        kind = self.source.value
        if kind == SOURCE_PATH:
            return self.path.value.strip(), []
        if kind == SOURCE_UPLOAD:
            return self._uploaded_path, []
        if kind == SOURCE_CURRENT:
            return self._current_path, []
        if kind == SOURCE_CATALOG_FORGE:
            catalog, name = self.page.catalog, self.catalog_forge.value
            return (str(catalog.forge_blueprint_path(name)), []) if name else ("", [])
        if kind == SOURCE_CATALOG_ROMS:
            catalog, name = self.page.catalog, self.catalog_roms.value
            if not name:
                return "", []
            directory = catalog.roms_marbl_blueprint_path(name)
            found = [
                p
                for p in sorted(directory.glob("*.y*ml"))
                if (f := blueprint_facts(p)) and f.application == ROMS_MARBL
            ]
            if len(found) == 1:
                return str(found[0]), []
            problem = (
                f"catalog blueprint {name!r} holds {len(found)} roms_marbl blueprint "
                f"files ({directory}); pick one with the path source"
            )
            return "", [problem]
        return "", []

    @property
    def is_file_source(self) -> bool:
        """Whether the step's blueprint is a file."""
        return self.source.value in _FILE_SOURCES

    @property
    def app(self) -> str:
        """The step's application name."""
        return str(self.application.value)

    # ---- observers -------------------------------------------------------
    def _changed(self, _change: Any = None) -> None:
        """Any edit: refresh the dependent widgets and regather the draft."""
        if self.page.is_suspended:
            return
        with self.page.suspended():
            self._sync()
        self.page._rebuild()

    def _on_name(self, change: Any) -> None:
        if self.page.is_suspended:
            return
        old, new = self._last_name, str(change["new"]).strip()
        self._last_name = new
        if old and new and old != new:
            self.page._rename_refs(old, new)
        self._changed()

    def _on_cpus(self, _change: Any) -> None:
        if not self.page.is_suspended:
            self._cpus_touched = True
        self._changed()

    def _on_upload(self, _change: Any) -> None:
        files = self.upload.value
        if self.page.is_suspended or not files:
            return
        item = (
            files[0] if isinstance(files, list | tuple) else next(iter(files.values()))
        )
        staged = (
            Path(tempfile.mkdtemp(prefix="cstar-wizard-")) / Path(item["name"]).name
        )
        staged.write_bytes(bytes(item["content"]))
        self._uploaded_path = str(staged)
        self.upload_note.value = f"<code>{_esc(staged)}</code>"
        self._on_blueprint_change()

    def _on_blueprint_change(self, _change: Any = None) -> None:
        """The blueprint source changed: re-derive everything that reads the file."""
        if self.page.is_suspended:
            return
        self.reselect()
        self.page._rebuild()

    def reselect(self) -> None:
        """Re-derive the application, prefills and visibility from the source."""
        with self.page.suspended():
            self.refresh_choices()
            if self.source.value == SOURCE_CURRENT:
                self._use_current_config()
            self._sync_application()
            self._prefill()
            self._sync()

    def _use_current_config(self) -> None:
        """Save the blueprint page's config and use that file as the blueprint."""
        wizard = self.page.wizard
        config = getattr(wizard, "config", None)
        if config is None:
            self.current_note.value = components.banner(
                "warn", "The Blueprint page has no valid configuration to use."
            )
            self._current_path = ""
            return
        target = Path(wizard.save_path.value)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._current_path = str(config.to_yaml(target))
        self.current_note.value = (
            f"Saved the Blueprint page's configuration to "
            f"<code>{_esc(self._current_path)}</code>"
        )

    def _on_application(self, _change: Any) -> None:
        if self.page.is_suspended:
            return
        with self.page.suspended():
            self._app_fallback = self.app
            self._sync()
        self.page._rebuild()

    def _on_directive_kind(self, _change: Any) -> None:
        self._changed()

    def _on_pick(self, change: Any) -> None:
        """Choosing a restart writes its timestamp into the text field."""
        if not self.page.is_suspended:
            self.cont_timestamp.value = change["new"] or ""

    # ---- derived state ---------------------------------------------------
    def _sync_application(self) -> None:
        """Offer the application choices the source allows; derive it from a file."""
        kind = self.source.value
        derived = ""
        if self.is_file_source:
            path, _ = self.resolve()
            facts = blueprint_facts(path) if path else None
            derived = facts.application if facts else ""
            if not derived and path:
                try:
                    derived = get_application_name(Path(path).expanduser())
                except Exception:
                    derived = ""
        if kind == SOURCE_INLINE:
            choices = [a for a in APPLICATIONS if a in INLINE_APPLICATIONS]
        else:
            choices = list(APPLICATIONS)
        wanted = derived or (self.app if self.app in choices else self._app_fallback)
        if wanted not in choices and (derived or kind != SOURCE_INLINE):
            choices.append(wanted)
        pairs = _pairs(choices)
        if list(self.application.options) != pairs:
            self.application.options = pairs
        self.application.value = wanted if wanted in choices else choices[0]
        self.application.disabled = bool(derived)

    def _prefill(self) -> None:
        """Prefill ``num_cpus`` from the blueprint (or the predicted emitted one)."""
        if self._cpus_touched or self.num_cpus.value:
            return
        cpus = 0
        if self.source.value == SOURCE_DEFERRED:
            emitted = self.page.emitted_for_token(self.producer.value)
            if emitted is not None:
                cpus = emitted.cpus_needed
                if not self.filename.value:
                    self.filename.value = emitted.filename
                if self.app == ROMS_MARBL or self.app not in APPLICATIONS:
                    self.application.value = emitted.application
        elif facts := self.facts():
            cpus = facts.cpus_needed
        if cpus:
            self.num_cpus.value = cpus

    def _sync(self) -> None:
        """Show what applies to the source/application; refresh derived widgets."""
        kind, app = self.source.value, self.app
        _show(self.catalog_roms, kind == SOURCE_CATALOG_ROMS)
        _show(self.catalog_forge, kind == SOURCE_CATALOG_FORGE)
        _show(self.path, kind == SOURCE_PATH)
        _show(self.upload, kind == SOURCE_UPLOAD)
        _show(self.producer, kind == SOURCE_DEFERRED)
        _show(self.filename, kind == SOURCE_DEFERRED)
        _show(self.current_note, kind == SOURCE_CURRENT)
        self.refresh_choices()

        facts = self.facts()
        if (facts.roms_ref if facts else None) != self._schema_ref:
            self._schema_ref = facts.roms_ref if facts else None
            for row in self._rows:
                row.refresh_sections()
        roms = app == ROMS_MARBL
        for widget in (
            self.end_date,
            self.cdr_location,
            self.roms_branch,
            self.roms_commit,
            self.rows_box,
            self.add_row_btn,
        ):
            _show(widget, roms)
        self.start_note.value = (
            f"<span class='forge-hint'>start_date <code>{facts.start_date}</code> "
            "comes from the blueprint (or the restart a directive locates); "
            "it is never written here.</span>"
            if roms and facts and facts.start_date
            else ""
        )
        self.end_date.placeholder = (
            f"blueprint: {facts.end_date}"
            if facts and facts.end_date
            else "ISO date, blank = from the blueprint"
        )
        if not roms:
            self._build_form(app)
        _show(self.form_box, not roms and app != FORGE)

        directives = self._app_directives(app)
        has_cont = ContinuanceDirective in directives
        has_nest = NestingDirective in directives
        _show(self._directives_section, has_cont or has_nest)
        cont = self.cont_kind.value
        for widget in (self.cont_kind,):
            _show(widget, has_cont)
        _show(self.cont_step, has_cont and cont == _KIND_STEP)
        _show(self.cont_path, has_cont and cont == _KIND_PATH)
        _show(self.cont_timestamp, has_cont and cont != _KIND_NONE)
        self._sync_restarts(has_cont)
        _show(self.nest_kind, has_nest)
        _show(self.nest_box, has_nest and self.nest_kind.value != _KIND_NONE)
        _show(self.add_nest_btn, has_nest and self.nest_kind.value != _KIND_NONE)

        slurm = self.page.compute_target.value != TARGET_LOCAL
        for widget in (
            self.max_walltime,
            self.queue_name,
            self.account_name,
            self.cpus_per_node,
        ):
            _show(widget, slurm)
        _show(self.local_walltime, not slurm)
        _show(self.num_cpus, slurm)
        self.locked_deps.value = (
            "<span class='forge-hint'>implied: "
            + ", ".join(f"<code>{_esc(d)}</code>" for d in self.implied)
            + "</span>"
            if self.implied
            else ""
        )

    @staticmethod
    def _app_directives(app: str) -> tuple[Any, ...]:
        """The directives ``app`` declares (none for an unknown application)."""
        try:
            return tuple(get_application(app).directives)
        except Exception:
            return ()

    def _sync_restarts(self, has_cont: bool) -> None:
        """Offer the restarts present at the source as a picker, when readable."""
        directory = self.page.restart_dir(self._cont_source())
        restarts = available_restarts(directory) if directory else []
        options = [""] + [r.strftime(TIMESTAMP_FMT) for r in restarts]
        picker = has_cont and self.cont_kind.value != _KIND_NONE and bool(restarts)
        current = self.cont_timestamp.value.strip()
        _set_options(self.cont_pick, options, [current] if current else [])
        self.cont_pick.value = current if current in self.cont_pick.options else ""
        _show(self.cont_pick, picker)
        _show(
            self.cont_timestamp,
            has_cont and self.cont_kind.value != _KIND_NONE and not picker,
        )

    def _cont_source(self) -> str:
        """The ``continue-from`` source: a step token or a path (``""`` for none)."""
        if self.cont_kind.value == _KIND_STEP:
            return self.cont_step.value or ""
        if self.cont_kind.value == _KIND_PATH:
            return self.cont_path.value.strip()
        return ""

    # ---- choices ---------------------------------------------------------
    def refresh_choices(self) -> None:
        """Reload every dropdown whose options come from the page or catalog."""
        page = self.page
        _set_options(self.catalog_roms, page.roms_blueprint_names())
        _set_options(self.catalog_forge, page.forge_blueprint_names())
        steps = page.step_choices(self)
        choose = [("(choose a step)", ""), *steps]
        _set_options(self.producer, choose)
        _set_options(self.cont_step, choose)
        _set_options(self.depends_on, steps)
        for row in self.nest_rows:
            if row.kind == _KIND_STEP:
                _set_options(row.widget, choose)
        for widget, _base, extra in self._form.values():
            if extra is not None:
                _set_options(
                    extra,
                    [("from step…", ""), *page.placeholder_choices(self)],
                )

    def rename_reference(self, old: str, new: str) -> None:
        """Follow a rename of step ``old`` to ``new`` in every reference."""
        with self.page.suspended():
            for widget in (self.producer, self.cont_step):
                if widget.value == old:
                    _add_option(widget, new)
                    widget.value = new
            _add_option(self.depends_on, new)
            self.depends_on.value = tuple(
                new if d == old else d for d in self.depends_on.value
            )
            self._dep_order = [new if d == old else d for d in self._dep_order]
            for row in self.nest_rows:
                if row.widget.value == old:
                    _add_option(row.widget, new)
                    row.widget.value = new

    def rename_alias(self, old: str, new: str) -> None:
        """Follow a rename of run alias ``old`` in every ``step@alias`` token."""

        def retoken(token: str) -> str:
            step, sep, alias = token.partition(RUN_ALIAS_SEPARATOR)
            return f"{step}{sep}{new}" if sep and alias == old else token

        with self.page.suspended():
            for widget in (self.producer, self.cont_step):
                if widget.value != retoken(widget.value or ""):
                    _add_option(widget, retoken(widget.value))
                    widget.value = retoken(widget.value)
            tokens = tuple(retoken(d) for d in self.depends_on.value)
            _add_option(self.depends_on, *tokens)
            self.depends_on.value = tokens
            self._dep_order = [retoken(d) for d in self._dep_order]
            for row in self.nest_rows:
                if row.kind == _KIND_STEP and row.widget.value != retoken(
                    row.widget.value
                ):
                    _add_option(row.widget, retoken(row.widget.value))
                    row.widget.value = retoken(row.widget.value)

    # ---- namelist rows, nest rows, generated form ------------------------
    def _add_row(self) -> _NamelistRow:
        row = _NamelistRow(self)
        self._rows.append(row)
        self.rows_box.children = [*self.rows_box.children, row.widget]
        self._changed()
        return row

    def _remove_row(self, row: _NamelistRow) -> None:
        self._rows.remove(row)
        self.rows_box.children = [r.widget for r in self._rows]
        self._changed()

    def _add_nest_row(self, kind: str, value: str) -> None:
        W = self.W
        if kind == _KIND_STEP:
            widget = W.Dropdown(options=[], layout=W.Layout(width="300px"))
            with self.page.suspended():
                _set_options(
                    widget, self.page.step_choices(self), [value] if value else []
                )
                if value:
                    widget.value = value
        else:
            widget = W.Text(
                value=value,
                continuous_update=False,
                placeholder="boundary directory",
                layout=W.Layout(width="420px"),
            )
        remove = W.Button(icon="trash", tooltip="Remove this source")
        holder = W.HBox([widget, remove])
        item = type("_NestRow", (), {})()
        item.kind, item.widget, item.holder = kind, widget, holder
        widget.observe(self._changed, names="value")
        remove.on_click(lambda _b: self._remove_nest_row(item))
        self.nest_rows.append(item)
        self.nest_box.children = [r.holder for r in self.nest_rows]
        if not self.page.is_suspended:
            self._changed()

    def _remove_nest_row(self, item: Any) -> None:
        self.nest_rows.remove(item)
        self.nest_box.children = [r.holder for r in self.nest_rows]
        self._changed()

    def _build_form(self, app: str) -> None:
        """Generate the form for an application's blueprint fields."""
        if app == self._form_app:
            return
        self._form_app = app
        self._form, self._form_touched = {}, set()
        W = self.W
        rows: list[Any] = []
        try:
            fields = get_application(app).blueprint.model_fields
        except Exception:
            fields = {}
        for name, info in fields.items():
            if name in _FORM_EXCLUDED:
                continue
            required = info.is_required()
            default = _field_default(info)
            base = _base_type(info.annotation, default)
            widget = _make_field_widget(
                W, f"{name}{' *' if required else ''}", base, default
            )
            widget.observe(lambda _c, n=name: self._form_edited(n), names="value")
            picker = None
            if base is str:
                picker = W.Dropdown(
                    options=[("from step…", "")], layout=W.Layout(width="190px")
                )
                picker.observe(
                    lambda _c, n=name: self._insert_placeholder(n), names="value"
                )
            self._form[name] = (widget, base, picker)
            rows.append(W.HBox([widget] + ([picker] if picker is not None else [])))
        self.form_box.children = rows
        self.refresh_choices()

    def _form_edited(self, name: str) -> None:
        if not self.page.is_suspended:
            self._form_touched.add(name)
        self._changed()

    def _insert_placeholder(self, name: str) -> None:
        widget, _base, picker = self._form[name]
        if self.page.is_suspended or not picker.value:
            return
        with self.page.suspended():
            widget.value = f"{widget.value}{picker.value}"
            self._form_touched.add(name)
            picker.value = ""
        self._changed()

    @property
    def required_form_fields(self) -> list[str]:
        """The generated form's required fields."""
        try:
            fields = get_application(self.app).blueprint.model_fields
        except Exception:
            return []
        return [n for n, i in fields.items() if n in self._form and i.is_required()]

    # ---- gather ----------------------------------------------------------
    def gather(self) -> Step:
        """Build the :class:`Step` the widgets describe.

        Raises
        ------
        ValueError
            If the widgets cannot form a step (the message says why).
        """
        kind, app = self.source.value, self.app
        blueprint: str | DeferredBlueprintRef | InlineBlueprintRef
        if kind == SOURCE_DEFERRED:
            if not self.producer.value:
                raise ValueError("choose the step that produces the blueprint")
            blueprint = DeferredBlueprintRef(
                from_step=self.producer.value, filename=self.filename.value.strip()
            )
        elif kind == SOURCE_INLINE:
            blueprint = InlineBlueprintRef()
        else:
            path, problems = self.resolve()
            if problems or not path:
                raise ValueError(problems[0] if problems else "choose a blueprint")
            blueprint = path
        overrides = self._gather_overrides(app)
        directives = self._gather_directives()
        compute = self._gather_compute()
        implied = self._implied(blueprint, overrides, directives, app)
        self.implied = [d for d in implied if d not in self.depends_on.value]
        depends = self._order([*self.depends_on.value, *implied])
        return Step(
            name=self.name.value,
            application=app,
            blueprint=blueprint,
            depends_on=depends,
            blueprint_overrides=overrides,
            compute_overrides=compute,
            directives=directives,
        )

    def _order(self, items: list[str]) -> list[str]:
        """``items`` de-duplicated, keeping the order they were first seen in."""
        unique = list(dict.fromkeys(items))
        known = [d for d in self._dep_order if d in unique]
        self._dep_order = known + [d for d in unique if d not in known]
        return list(self._dep_order)

    def _implied(
        self,
        blueprint: object,
        overrides: Mapping[str, Any],
        directives: Mapping[str, Any],
        app: str,
    ) -> list[str]:
        """The dependencies the blueprint ref, placeholders and directives imply."""
        implied: list[str] = []
        if isinstance(blueprint, DeferredBlueprintRef):
            implied.append(blueprint.from_step)
        implied.extend(_placeholder_steps(overrides))
        implied.extend(_placeholder_steps(directives))
        for directive in self._app_directives(app):
            config = directives.get(directive.key())
            if isinstance(config, dict):
                implied.extend(directive.referenced_steps(config))
        return list(dict.fromkeys(implied))

    def _gather_overrides(self, app: str) -> dict[str, Any]:
        """The ``blueprint_overrides`` the widgets describe, raw YAML merged last."""
        out: dict[str, Any] = {}
        if app == ROMS_MARBL:
            if end := self.end_date.value.strip():
                out["runtime_params"] = {"end_date": _parse_datetime(end)}
            for row in self._rows:
                if (item := row.value()) is not None:
                    section, field, value = item
                    out.setdefault("namelist_overrides", {}).setdefault(section, {})[
                        field
                    ] = value
            if location := self.cdr_location.value.strip():
                out["cdr_forcing"] = {"data": [{"location": location}]}
            roms = {
                key: v
                for key, widget in (
                    ("branch", self.roms_branch),
                    ("commit", self.roms_commit),
                )
                if (v := widget.value.strip())
            }
            if roms:
                out["code"] = {"roms": roms}
        elif app != FORGE:
            for name, (widget, base, _picker) in self._form.items():
                value = _read_field_widget(widget, base)
                required = name in self.required_form_fields
                if name in self._form_touched or (required and value not in ("", None)):
                    out[name] = value
        raw_text = self.raw_overrides.value.strip()
        if raw_text:
            raw = yaml.safe_load(raw_text)
            if not isinstance(raw, dict):
                raise ValueError("raw overrides must be a YAML mapping")
            out = deep_merge(out, raw, replace_lists=True)
        return out

    def _gather_directives(self) -> dict[str, Any]:
        """The step's directives; unmodelled ones are carried through as written."""
        directives = copy.deepcopy(self._passthrough_directives)
        if self.cont_kind.value != _KIND_NONE:
            cont: dict[str, Any] = {}
            if self.cont_kind.value == _KIND_STEP:
                if not self.cont_step.value:
                    raise ValueError("choose the step to continue from")
                cont[ContinuanceDirective.KEY_STEP] = self.cont_step.value
            else:
                if not self.cont_path.value.strip():
                    raise ValueError("enter the path to continue from")
                cont[ContinuanceDirective.KEY_PATH] = self.cont_path.value.strip()
            stamp = self.cont_timestamp.value.strip()
            if stamp:
                parsed = _as_datetime(stamp)
                cont[ContinuanceDirective.KEY_TIMESTAMP] = (
                    parsed.strftime(TIMESTAMP_FMT) if parsed else stamp
                )
            directives[ContinuanceDirective.key()] = cont
        if self.nest_kind.value != _KIND_NONE:
            values = [str(r.widget.value).strip() for r in self.nest_rows]
            values = [v for v in values if v]
            if not values:
                raise ValueError("add at least one boundary source for nest-from")
            directives[NestingDirective.key()] = {
                (
                    NestingDirective.KEY_STEP
                    if self.nest_kind.value == _KIND_STEP
                    else NestingDirective.KEY_PATH
                ): NestingDirective.SOURCE_DELIMITER.join(values)
            }
        return directives

    def _gather_compute(self) -> dict[str, Any]:
        """The ``compute_overrides``; blanks are omitted, unmodelled keys kept."""
        modelled: dict[str, Any] = {}
        slurm = {
            key: value
            for key, value in (
                ("num_cpus", self.num_cpus.value or None),
                ("max_walltime", self.max_walltime.value.strip()),
                ("queue_name", self.queue_name.value.strip()),
                ("account_name", self.account_name.value.strip()),
                ("cpus_per_node", self.cpus_per_node.value or None),
            )
            if value
        }
        if slurm:
            modelled["slurm"] = slurm
        if local := self.local_walltime.value.strip():
            modelled["local"] = {"max_walltime": local}
        return deep_merge(copy.deepcopy(self._passthrough_compute), modelled)

    def problems(self) -> list[str]:
        """Problems the model would not report until check time."""
        lines = list(self.resolve()[1])
        if self.source.value == SOURCE_DEFERRED and not self.num_cpus.value:
            lines.append(
                "a deferred blueprint cannot be sized by the launcher: set num_cpus"
            )
        if not self.is_file_source and self.source.value != SOURCE_DEFERRED:
            missing = [
                n
                for n in self.required_form_fields
                if n not in self._form_touched
                and _read_field_widget(*self._form[n][:2]) in ("", None)
            ]
            if missing and self.app != ROMS_MARBL:
                lines.append(f"inline blueprint fields missing: {', '.join(missing)}")
        self.problem_lines = lines
        return lines

    # ---- titles ----------------------------------------------------------
    def title(self) -> str:
        """``<name> · <application> · <window>`` for the accordion header."""
        window = ""
        if self.app == ROMS_MARBL:
            facts = self.facts()
            end = self.end_date.value.strip() or (
                str(facts.end_date) if facts and facts.end_date else ""
            )
            start = str(facts.start_date) if facts and facts.start_date else ""
            window = f"{start} to {end}" if start and end else end
        return components.accordion_title(
            self.name.value.strip() or "(unnamed step)", self.app, window
        )

    # ---- populate --------------------------------------------------------
    def populate(self, step: Step) -> None:
        """Show ``step`` in the widgets (workflow overrides are not shown)."""
        page = self.page
        with page.suspended():
            self.name.value = step.name
            self._last_name = step.name
            blueprint = step.blueprint_path
            self._app_fallback = step.application
            if isinstance(blueprint, DeferredBlueprintRef):
                self.source.value = SOURCE_DEFERRED
                _add_option(self.producer, blueprint.from_step)
                self.producer.value = blueprint.from_step
                self.filename.value = blueprint.filename
            elif isinstance(blueprint, InlineBlueprintRef):
                self.source.value = SOURCE_INLINE
            else:
                self.source.value = SOURCE_PATH
                self.path.value = str(blueprint)
            self._sync_application()
            if not self.application.disabled:
                _add_option(self.application, step.application)
                self.application.value = step.application
            self._cpus_touched = True
            self._schema_ref = None
            self._sync()
            self._populate_overrides(step)
            self._populate_directives(step)
            self._populate_compute(step)
            implied = self._implied(
                blueprint, step.blueprint_overrides, step.directives, step.application
            )
            explicit = [d for d in step.depends_on if d not in implied]
            _add_option(self.depends_on, *step.depends_on)
            self.depends_on.value = tuple(explicit)
            self._dep_order = list(step.depends_on)
            self._sync()

    def _populate_overrides(self, step: Step) -> None:
        """Split the overrides into modelled widgets and the raw remainder."""
        rest: dict[str, Any] = copy.deepcopy(dict(step.blueprint_overrides))
        app = step.application
        self.raw_overrides.value = ""
        self._rows, self._form_app, self._form_touched = [], "", set()
        self.rows_box.children = []
        if app == ROMS_MARBL:
            end = _pop_path(rest, "runtime_params", "end_date")
            self.end_date.value = "" if end is None else str(end)
            for section, fields in list(rest.get("namelist_overrides", {}).items()):
                if not isinstance(fields, dict):
                    continue
                for name, value in list(fields.items()):
                    row = _NamelistRow(self)
                    if row.accepts(section, name, value):
                        row.populate(section, name, value)
                        self._rows.append(row)
                        _pop_path(rest, "namelist_overrides", section, name)
            self.rows_box.children = [r.widget for r in self._rows]
            cdr = rest.get("cdr_forcing")
            if (
                isinstance(cdr, dict)
                and set(cdr) == {"data"}
                and isinstance(cdr["data"], list)
                and len(cdr["data"]) == 1
                and isinstance(cdr["data"][0], dict)
                and set(cdr["data"][0]) == {"location"}
                and isinstance(cdr["data"][0]["location"], str)
            ):
                self.cdr_location.value = cdr["data"][0]["location"]
                del rest["cdr_forcing"]
            for key, widget in (
                ("branch", self.roms_branch),
                ("commit", self.roms_commit),
            ):
                value = _pop_path(rest, "code", "roms", key)
                widget.value = value if isinstance(value, str) else ""
                if value is not None and not isinstance(value, str):
                    _set_back = rest.setdefault("code", {}).setdefault("roms", {})
                    _set_back[key] = value
        elif app != FORGE:
            self._build_form(app)
            for name, (widget, base, _picker) in self._form.items():
                if name not in rest:
                    continue
                probe = _make_field_widget(self.W, "", base, rest[name])
                if _read_field_widget(probe, base) == rest[name]:
                    widget.value = probe.value
                    self._form_touched.add(name)
                    del rest[name]
        if rest:
            self.raw_overrides.value = yaml.safe_dump(rest, sort_keys=False)

    def _populate_directives(self, step: Step) -> None:
        """Show the directives the widgets model; keep the rest as written."""
        directives: dict[str, Any] = copy.deepcopy(dict(step.directives))
        self._passthrough_directives = {}
        self.cont_kind.value = self.nest_kind.value = _KIND_NONE
        self.cont_path.value = self.cont_timestamp.value = ""
        self.nest_rows = []
        self.nest_box.children = []
        cont = directives.pop(ContinuanceDirective.key(), None)
        if cont is not None:
            step_key, path_key, stamp_key = (
                ContinuanceDirective.KEY_STEP,
                ContinuanceDirective.KEY_PATH,
                ContinuanceDirective.KEY_TIMESTAMP,
            )
            if (
                isinstance(cont, dict)
                and (step_key in cont) != (path_key in cont)
                and set(cont) <= {step_key, path_key, stamp_key}
            ):
                if step_key in cont:
                    self.cont_kind.value = _KIND_STEP
                    _add_option(self.cont_step, str(cont[step_key]))
                    self.cont_step.value = str(cont[step_key])
                else:
                    self.cont_kind.value = _KIND_PATH
                    self.cont_path.value = str(cont[path_key])
                stamp = cont.get(stamp_key, "")
                parsed = _as_datetime(stamp) if stamp != "" else None
                self.cont_timestamp.value = (
                    parsed.strftime(TIMESTAMP_FMT) if parsed else str(stamp)
                )
            else:
                self._passthrough_directives[ContinuanceDirective.key()] = cont
        nest = directives.pop(NestingDirective.key(), None)
        if nest is not None:
            step_key, path_key = NestingDirective.KEY_STEP, NestingDirective.KEY_PATH
            if (
                isinstance(nest, dict)
                and len(nest) == 1
                and (step_key in nest or path_key in nest)
            ):
                kind = _KIND_STEP if step_key in nest else _KIND_PATH
                self.nest_kind.value = kind
                for token in str(
                    nest[step_key if kind == _KIND_STEP else path_key]
                ).split(NestingDirective.SOURCE_DELIMITER):
                    if token.strip():
                        self._add_nest_row(kind, token.strip())
            else:
                self._passthrough_directives[NestingDirective.key()] = nest
        self._passthrough_directives.update(directives)
        self.directive_note.value = (
            "<span class='forge-hint'>Kept as written: "
            + _esc(", ".join(self._passthrough_directives))
            + "</span>"
            if self._passthrough_directives
            else ""
        )

    def _populate_compute(self, step: Step) -> None:
        """Show the modelled compute overrides; keep the rest as written."""
        rest: dict[str, Any] = copy.deepcopy(dict(step.compute_overrides))
        for key, widget in (
            ("num_cpus", self.num_cpus),
            ("cpus_per_node", self.cpus_per_node),
        ):
            value = _pop_path(rest, "slurm", key)
            widget.value = value if isinstance(value, int) and value >= 0 else 0
            if value is not None and widget.value != value:
                rest.setdefault("slurm", {})[key] = value
        for key, widget in (
            ("max_walltime", self.max_walltime),
            ("queue_name", self.queue_name),
            ("account_name", self.account_name),
        ):
            value = _pop_path(rest, "slurm", key)
            widget.value = value if isinstance(value, str) else ""
            if value is not None and not isinstance(value, str):
                rest.setdefault("slurm", {})[key] = value
        local = _pop_path(rest, "local", "max_walltime")
        self.local_walltime.value = local if isinstance(local, str) else ""
        self._passthrough_compute = rest


# ---------------------------------------------------------------------------
# the page
# ---------------------------------------------------------------------------
class _FirstSource:
    """Form fields choosing where a generated chain's first step continues from."""

    def __init__(self, page: WorkplanBuilderPage) -> None:
        """Build the fields; ``page`` supplies step choices and declared runs."""
        W = page.W
        self.page = page
        self.kind = W.Dropdown(
            options=[
                ("the blueprint's own", _KIND_NONE),
                ("a step", _KIND_STEP),
                ("a path", _KIND_PATH),
            ],
            value=_KIND_NONE,
        )
        self.step = W.Dropdown(options=[], layout=W.Layout(width="260px"))
        self.path = W.Text(continuous_update=False, layout=W.Layout(width="260px"))
        self.timestamp = W.Text(
            placeholder="YYYY-MM-DD HH:MM:SS, blank = latest",
            continuous_update=False,
            layout=W.Layout(width="260px"),
        )
        self.kind.observe(lambda _c: self._sync(), names="value")
        self._sync()

    def rows(self) -> list[Any]:
        """The field rows, in display order."""
        W = self.page.W
        return [
            components.field_row(W, f"first.{key}", widget, page=PAGE)
            for key, widget in (
                ("kind", self.kind),
                ("step", self.step),
                ("path", self.path),
                ("timestamp", self.timestamp),
            )
        ]

    def _sync(self) -> None:
        _show(self.step, self.kind.value == _KIND_STEP)
        _show(self.path, self.kind.value == _KIND_PATH)
        _show(self.timestamp, self.kind.value != _KIND_NONE)

    def refresh(self) -> None:
        """Reload the step choices."""
        _set_options(self.step, [("(choose a step)", ""), *self.page.step_choices()])

    def source(self) -> StepSource | None:
        """The chosen source, or ``None`` for the blueprint's own initial conditions.

        Raises
        ------
        ValueError
            If the choice is incomplete or the timestamp is not a date.
        """
        if self.kind.value == _KIND_NONE:
            return None
        stamp = self.timestamp.value.strip()
        parsed = _as_datetime(stamp) if stamp else None
        if stamp and parsed is None:
            raise ValueError(f"{stamp!r} is not an ISO date or date-time")
        if self.kind.value == _KIND_PATH:
            return StepSource(path=self.path.value.strip(), timestamp=parsed)
        token = self.step.value
        run_id = ""
        if token and (ref := StepRef.parse(token)).is_external:
            run_id = next(
                (
                    r.run_id.value.strip()
                    for r in self.page.run_rows
                    if r.alias.value.strip() == ref.run
                ),
                "",
            )
        return StepSource(step=token, run_id=run_id, timestamp=parsed)


class _RunRow:
    """One row of the Runs table: an alias bound to a run-id."""

    def __init__(
        self, page: WorkplanBuilderPage, alias: str = "", run_id: str = ""
    ) -> None:
        """Build the row; the page is called back on every edit."""
        W = page.W
        self.page = page
        self.alias = W.Text(
            value=alias,
            placeholder="alias",
            continuous_update=False,
            layout=W.Layout(width="130px"),
        )
        self.run_id = W.Text(
            value=run_id,
            placeholder="run-id or {{var}}",
            continuous_update=False,
            layout=W.Layout(width="230px"),
        )
        self.pick = W.Dropdown(
            options=[("recorded runs…", "")], layout=W.Layout(width="260px")
        )
        self.steps = W.Text(
            value="",
            placeholder="step names, if not recorded here",
            continuous_update=False,
            layout=W.Layout(width="230px"),
        )
        self.status = W.HTML("")
        self.remove_btn = W.Button(icon="trash", tooltip="Remove this run")
        self.widget = W.HBox(
            [self.alias, self.run_id, self.pick, self.steps, self.remove_btn]
        )
        self.holder = W.VBox([self.widget, self.status])
        self.alias.observe(self._on_alias, names="value")
        self.run_id.observe(self._on_run_id, names="value")
        self.steps.observe(self._on_run_id, names="value")
        self.pick.observe(self._on_pick, names="value")
        self.remove_btn.on_click(lambda _b: page._remove_run(self))
        self._last_alias = alias

    def _on_alias(self, change: Any) -> None:
        if self.page.is_suspended:
            return
        old, new = self._last_alias, str(change["new"]).strip()
        self._last_alias = new
        if old and new and old != new:
            self.page._rename_alias(old, new)
        self.page._runs_changed(self)

    def _on_run_id(self, _change: Any) -> None:
        if not self.page.is_suspended:
            self.page._runs_changed(self)

    def _on_pick(self, change: Any) -> None:
        if self.page.is_suspended or not change["new"]:
            return
        with self.page.suspended():
            self.run_id.value = change["new"]
        self.page._runs_changed(self)


class WorkplanBuilderPage:
    """The Workplan page: build a workplan from catalog blueprints and steps.

    Parameters
    ----------
    blueprint_app
        The Blueprint page's app, shared so the two pages see the same catalog
        and the blueprint being edited.
    W
        The ``ipywidgets`` module; imported lazily when omitted.
    """

    def __init__(self, blueprint_app: Any, *, W: Any = None) -> None:
        """Build the page's widget tree."""
        if W is None:
            import ipywidgets

            W = ipywidgets
        self.W = W
        self.blueprint_app = blueprint_app

        self.draft: Workplan | None = None
        self.problems: list[str] = []
        self.loaded_from: Path | None = None
        self.changes: list[Change] = []
        self.dropped: list[str] = []
        self.original: Workplan | None = None
        self.panes: list[_StepPane] = []
        self.run_rows: list[_RunRow] = []
        self.run_steps: dict[str, list[str]] = {}
        self.run_workplans: dict[str, Any] = {}
        self.recorded_runs: list[Any] = []
        self._suspended = 0
        self._accordion: Any = None
        self._shown: list[_StepPane] = []
        self._save_touched = False
        self._saved_text = ""
        self._compute_kept: dict[str, Any] = {}
        self._compute_extra: dict[str, Any] = {}
        self._confirm_timer: Any = None
        self._awaiting_confirm = False
        self._pre_run_offered = False
        self._reverted: set[str] = set()
        self._changes_key: object = None

        self._build_start()
        self._build_workplan()
        self._build_compute()
        self._build_steps()
        self._build_review()
        self._build_recipes()
        self._build_diagnostics()
        self._build_run()
        self._build_layout()
        with self.suspended():
            self._refresh_saved()
            self._sync_compute()
            self.add_step()
            self._refresh_recipes()
        self._rebuild()

    # ---- state access ----------------------------------------------------
    @property
    def is_suspended(self) -> bool:
        """Whether observers are muted (the page is writing its own widgets)."""
        return bool(self._suspended)

    @contextmanager
    def suspended(self):  # type: ignore[no-untyped-def]
        """Mute observers while the page writes widgets itself."""
        self._suspended += 1
        try:
            yield
        finally:
            self._suspended -= 1

    @property
    def wizard(self) -> Any:
        """The Blueprint page's wizard, or ``None`` while its catalog failed to load."""
        return getattr(self.blueprint_app, "inner", None)

    @property
    def catalog(self) -> Any:
        """The catalog the Blueprint page currently uses (read on demand: Reload swaps it)."""
        return getattr(self.wizard, "catalog", None)

    def roms_blueprint_names(self) -> list[str]:
        """The catalog's roms_marbl blueprint names."""
        return list(getattr(self.catalog, "roms_marbl_blueprint_names", []))

    def forge_blueprint_names(self) -> list[str]:
        """The catalog's forge blueprint names."""
        return list(getattr(self.catalog, "forge_blueprint_names", []))

    def external_tokens(self) -> list[str]:
        """``step@alias`` tokens for the steps of the declared runs."""
        return [
            f"{step}{RUN_ALIAS_SEPARATOR}{alias}"
            for alias, steps in self.run_steps.items()
            for step in steps
        ]

    def step_choices(self, exclude: _StepPane | None = None) -> list[str]:
        """The step names (and external tokens) a reference may name."""
        names = [
            p.name.value.strip()
            for p in self.panes
            if p is not exclude and p.name.value.strip()
        ]
        return [*names, *self.external_tokens()]

    def placeholder_choices(
        self, exclude: _StepPane | None = None
    ) -> list[tuple[str, str]]:
        """``(label, "{{scope: step}}")`` options for a path field's picker."""
        return [
            (f"{scope}: {step}", mustache(f"{scope}: {step}"))
            for step in self.step_choices(exclude)
            for scope in _PLACEHOLDER_SCOPES
        ]

    def pane_named(self, name: str) -> _StepPane | None:
        """The pane whose step is called ``name``."""
        return next((p for p in self.panes if p.name.value.strip() == name), None)

    def producer_blueprint(self, token: str) -> tuple[str, str] | None:
        """``(application, path)`` of the blueprint behind a step token, if known."""
        ref = StepRef.parse(token) if token else None
        if ref is None:
            return None
        if not ref.is_external:
            pane = self.pane_named(ref.step)
            if pane is None or not pane.is_file_source:
                return None
            path, _ = pane.resolve()
            return (pane.app, path) if path else None
        live = self.run_workplans.get(ref.run)
        if live is not None and ref.step in live:
            step = live[ref.step]
            if isinstance(step.blueprint_path, str | Path):
                return step.application, str(step.blueprint_path)
        return None

    def emitted_for_token(self, token: str) -> EmittedBlueprint | None:
        """The blueprint a producer step is predicted to emit."""
        try:
            found = self.producer_blueprint(token)
        except ValueError:
            return None
        return emitted_for(found[1], found[0]) if found else None

    def restart_dir(self, source: str) -> Path | None:
        """The directory holding a ``continue-from`` source's restarts, when known."""
        if not source:
            return None
        try:
            ref = StepRef.parse(source)
        except ValueError:
            return Path(source).expanduser()
        if ref.is_external and (live := self.run_workplans.get(ref.run)) is not None:
            if ref.step in live:
                return Path(live[ref.step].fsm.output_dir)
            return None
        if not ref.is_external and (Path(source).expanduser().is_dir()):
            return Path(source).expanduser()
        return None

    # ---- start card ------------------------------------------------------
    def _build_start(self) -> None:
        W = self.W
        self.load_dd = W.Dropdown(options=[], layout=W.Layout(width="320px"))
        self.load_btn = W.Button(description=_caption("load", "Load"), icon="upload")
        self.refresh_saved_btn = W.Button(icon="refresh", tooltip="Rescan the catalog")
        self.load_path = W.Text(
            placeholder="/path/to/workplan.yaml", layout=W.Layout(width="420px")
        )
        self.load_path_btn = W.Button(
            description=_caption("load", "Load"), icon="upload"
        )
        self.upload = W.FileUpload(accept=".yml,.yaml", multiple=False)
        self.upload_btn = W.Button(description=_caption("load", "Load"), icon="upload")
        self.new_btn = W.Button(
            description=_caption("new", "New workplan"), icon="file"
        )
        self.load_status = W.HTML("")
        self.load_btn.on_click(lambda _b: self._load_saved())
        self.refresh_saved_btn.on_click(lambda _b: self._refresh_saved())
        self.load_path_btn.on_click(
            lambda _b: self._load_from_path(self.load_path.value)
        )
        self.upload_btn.on_click(lambda _b: self._load_upload())
        self.new_btn.on_click(lambda _b: self._new())

    def saved_workplans(self) -> dict[str, Path]:
        """The workplans saved in the catalog, by name."""
        directory = getattr(self.catalog, "workplans_dir", None)
        if directory is None or not Path(directory).is_dir():
            return {}
        return {p.stem: p for p in sorted(Path(directory).glob("*.yaml"))}

    def _refresh_saved(self) -> None:
        saved = self.saved_workplans()
        with self.suspended():
            self.load_dd.options = [(n, str(p)) for n, p in saved.items()]

    def _set_load_status(self, message: str, *, error: bool = False) -> None:
        kind = "forge-msg-err" if error else "forge-msg-ok"
        self.load_status.value = f"<span class='{kind}'>{_esc(message)}</span>"

    def _load_saved(self) -> None:
        self._refresh_saved()
        if self.load_dd.value:
            self._load_from_path(self.load_dd.value)

    def _load_from_path(self, path_text: str) -> None:
        path = Path(path_text.strip()).expanduser()
        try:
            workplan = deserialize(path, Workplan)
        except Exception as ex:
            self._set_load_status(f"{type(ex).__name__}: {ex}", error=True)
            return
        self.load_workplan(workplan, path)

    def _load_upload(self) -> None:
        files = self.upload.value
        if not files:
            self._set_load_status("Choose a file to upload first.", error=True)
            return
        item = (
            files[0] if isinstance(files, list | tuple) else next(iter(files.values()))
        )
        try:
            raw = yaml.safe_load(bytes(item["content"]))
            if isinstance(raw, dict):
                raw.pop("$schema", None)
            workplan = Workplan.model_validate(raw)
        except Exception as ex:
            self._set_load_status(f"{type(ex).__name__}: {ex}", error=True)
            return
        self.load_workplan(workplan, None)

    def load_workplan(self, workplan: Workplan, source: Path | None) -> None:
        """Replace the draft with ``workplan`` (normalized), without touching its file.

        Parameters
        ----------
        workplan
            The workplan as read.
        source
            The file it came from, or ``None`` for an upload.
        """
        normalized, changes = normalize_legacy(workplan, run_root=None)
        self.original, self.changes = workplan, changes
        self.loaded_from = source.expanduser().resolve() if source else None
        self.dropped = [
            f"{s.name}: workflow_overrides ({', '.join(s.workflow_overrides)})"
            for s in workplan.steps
            if s.workflow_overrides
        ]
        self.dropped += [
            f"runs.{alias}.start_at (the orchestrator pins it at schedule time)"
            for alias, ref in workplan.runs.items()
            if ref.start_at is not None
        ]
        with self.suspended():
            self._populate(normalized)
        self._set_load_status(
            f"Loaded {source or 'upload'}: {len(normalized.steps)} steps, "
            f"{len(changes)} change(s) applied"
        )
        self._rebuild()

    def _new(self) -> None:
        with self.suspended():
            self.loaded_from, self.original, self.changes, self.dropped = (
                None,
                None,
                [],
                [],
            )
            self._populate(None)
        self._set_load_status("Started a new workplan.")
        self._rebuild()

    def _populate(self, workplan: Workplan | None) -> None:
        """Fill every widget from ``workplan`` (``None`` = a blank one step plan)."""
        self.name.value = workplan.name if workplan else ""
        self.description.value = workplan.description if workplan else ""
        self.runtime_vars.value = ", ".join(workplan.runtime_vars) if workplan else ""
        self.run_rows, self.run_steps, self.run_workplans = [], {}, {}
        for alias, ref in (workplan.runs if workplan else {}).items():
            row = _RunRow(self, alias, ref.run_id)
            self.run_rows.append(row)
        self.runs_box.children = [r.holder for r in self.run_rows]
        for step in workplan.steps if workplan else ():
            for token in [*step.depends_on]:
                self._note_token(token)
            self._note_token(
                step.blueprint_path.from_step
                if isinstance(step.blueprint_path, DeferredBlueprintRef)
                else ""
            )
        for row in self.run_rows:
            row.steps.value = ", ".join(self.run_steps.get(row.alias.value, []))
            self._load_recorded(row)
        self._populate_compute(workplan.compute_environment if workplan else {})
        self.panes = []
        for step in workplan.steps if workplan else ():
            pane = _StepPane(self)
            self.panes.append(pane)
            pane.populate(step)
        if workplan is None:
            self.add_step()
        self._refresh_steps_view()
        self._save_touched = False
        self._saved_text = ""
        self._run_id_touched = False
        self._reverted = set()
        self._sync_save_path()

    def _note_token(self, token: str) -> None:
        """Record the step of a ``step@alias`` token as known for its alias."""
        try:
            ref = StepRef.parse(token) if token else None
        except ValueError:
            return
        if ref is not None and ref.is_external:
            names = self.run_steps.setdefault(ref.run, [])
            if ref.step not in names:
                names.append(ref.step)

    # ---- workplan card ---------------------------------------------------
    def _build_workplan(self) -> None:
        W = self.W
        self.name = W.Text(continuous_update=False, layout=W.Layout(width="420px"))
        self.description = W.Textarea(layout=W.Layout(width="520px", height="60px"))
        self.runtime_vars = W.Text(
            placeholder="comma-separated names",
            continuous_update=False,
            layout=W.Layout(width="420px"),
        )
        self.runs_box = W.VBox([])
        self.add_run_btn = W.Button(
            description=_caption("add_run", "Add run"), icon="plus"
        )
        self.refresh_runs_btn = W.Button(
            description=_caption("refresh_runs", "Refresh runs"), icon="refresh"
        )
        self.runs_status = W.HTML("")
        for widget in (self.name, self.description, self.runtime_vars):
            widget.observe(self._edited, names="value")
        self.name.observe(self._on_name, names="value")
        self.add_run_btn.on_click(lambda _b: self._add_run())
        self.refresh_runs_btn.on_click(lambda _b: self.refresh_runs())

    def _edited(self, _change: Any = None) -> None:
        if not self.is_suspended:
            self._rebuild()

    def _on_name(self, _change: Any) -> None:
        if not self.is_suspended:
            self._sync_save_path()

    def _add_run(self) -> None:
        row = _RunRow(self)
        self.run_rows.append(row)
        self.runs_box.children = [r.holder for r in self.run_rows]

    def _remove_run(self, row: _RunRow) -> None:
        self.run_rows.remove(row)
        self.run_steps.pop(row.alias.value, None)
        self.run_workplans.pop(row.alias.value, None)
        self.runs_box.children = [r.holder for r in self.run_rows]
        self._rebuild()

    def _rename_alias(self, old: str, new: str) -> None:
        """Follow a rename of run alias ``old`` in every ``step@alias`` token."""
        for mapping in (self.run_steps, self.run_workplans):
            if old in mapping:
                mapping[new] = mapping.pop(old)
        with self.suspended():
            for pane in self.panes:
                pane.rename_alias(old, new)

    def _runs_changed(self, row: _RunRow) -> None:
        """A run row was edited: reload what is known about its run."""
        with self.suspended():
            typed = [s.strip() for s in row.steps.value.split(",") if s.strip()]
            self.run_steps[row.alias.value.strip()] = typed
            self._load_recorded(row)
        self._rebuild()

    def _load_recorded(self, row: _RunRow) -> None:
        """Load the steps of the recorded run matching ``row``'s run-id."""
        alias, run_id = row.alias.value.strip(), row.run_id.value.strip()
        record = next((r for r in self.recorded_runs if r.run_id == run_id), None)
        if not alias or record is None:
            row.status.value = ""
            return
        try:
            from cstar.orchestration.orchestration import LiveWorkplan

            live = deserialize(record.trx_workplan_path, LiveWorkplan)
        except Exception as ex:
            row.status.value = f"<span class='forge-msg-warn'>run recorded, but its workplan could not be read: {_esc(ex)}</span>"
            return
        self.run_workplans[alias] = live
        typed = self.run_steps.get(alias, [])
        names = [s.name for s in live.steps]
        self.run_steps[alias] = [*names, *[t for t in typed if t not in names]]
        statuses = {n: self._step_status(run_id, n) for n in names}
        unfinished = [n for n, s in statuses.items() if s != "Done"]
        row.status.value = ", ".join(
            f"<code>{_esc(n)}</code> {_esc(s)}" for n, s in statuses.items()
        ) + (" " + components.chip("not finished", "warn") if unfinished else "")

    @staticmethod
    def _step_status(run_id: str, step: str) -> str:
        """The persisted status of a recorded step, or ``recorded`` when unreadable."""
        try:
            from cstar.orchestration.orchestration import ProcessHandle
            from cstar.orchestration.state import StateRepository

            handle = deserialize(
                StateRepository.sentinel_path(step, run_id=run_id), ProcessHandle
            )
            return handle.status.name
        except Exception:
            return "recorded"

    def refresh_runs(self) -> None:
        """Load the recorded runs (newest first) into every row's picker."""
        _schedule_coroutine(self._load_runs_async())

    async def _load_runs_async(self) -> None:
        try:
            from cstar.orchestration.tracking import TrackingRepository

            runs = await TrackingRepository().list_latest_runs()
        except Exception as ex:
            self.runs_status.value = (
                f"<span class='forge-msg-warn'>Could not list runs: {_esc(ex)}</span>"
            )
            return
        self.recorded_runs = sorted(runs, key=lambda r: r.start_at, reverse=True)
        options = [("recorded runs…", "")] + [
            (f"{r.run_id}  ({r.start_at:%Y-%m-%d %H:%M})", r.run_id)
            for r in self.recorded_runs
        ]
        with self.suspended():
            for row in self.run_rows:
                row.pick.options = options
                self._load_recorded(row)
        self.runs_status.value = f"{len(self.recorded_runs)} recorded run(s)"
        self._rebuild()

    # ---- compute card ----------------------------------------------------
    def _build_compute(self) -> None:
        W = self.W
        machines = list(slurm_machines())
        live = live_system_name()
        self.compute_target = W.RadioButtons(
            options=[TARGET_LOCAL, TARGET_SLURM, TARGET_NONE],
            value=TARGET_SLURM if live in machines else TARGET_NONE,
        )
        self.machine = W.Dropdown(
            options=[*machines, MACHINE_CUSTOM],
            value=live
            if live in machines
            else (machines[0] if machines else MACHINE_CUSTOM),
        )
        self.queue = W.Dropdown(options=[], layout=W.Layout(width="200px"))
        self.queue_text = W.Text(
            continuous_update=False, layout=W.Layout(width="200px")
        )
        self.account = W.Text(continuous_update=False, layout=W.Layout(width="260px"))
        self.walltime = W.Text(
            placeholder="HH:MM:SS",
            continuous_update=False,
            layout=W.Layout(width="140px"),
        )
        self.cpus_per_node = W.IntText(value=0, layout=W.Layout(width="140px"))
        self.machine_note = W.HTML(
            "<span class='forge-hint'>"
            + ", ".join(_esc(m) for m in unsupported_machines())
            + " submit through PBS, which the workplan launcher does not support."
            "</span>"
            if unsupported_machines()
            else ""
        )
        self.env_note = W.HTML("")
        self.compute_note = W.HTML("")
        for widget in (
            self.compute_target,
            self.machine,
            self.queue,
            self.queue_text,
            self.account,
            self.walltime,
            self.cpus_per_node,
        ):
            widget.observe(self._on_compute, names="value")

    def _on_compute(self, change: Any) -> None:
        if self.is_suspended:
            return
        self._compute_kept = {}
        with self.suspended():
            self._sync_compute()
            for pane in self.panes:
                pane._sync()
        self._rebuild()

    def _sync_compute(self) -> None:
        """Show the fields of the chosen target; refresh the machine's queues."""
        target = self.compute_target.value
        slurm = target == TARGET_SLURM
        custom = self.machine.value == MACHINE_CUSTOM
        scheduler = slurm_machines().get(self.machine.value)
        queues = list(scheduler.queue_names) if scheduler else []
        if scheduler and scheduler.primary_queue_name in queues:
            queues.remove(scheduler.primary_queue_name)
            queues.insert(0, scheduler.primary_queue_name)
        _set_options(self.queue, [("(environment)", ""), *queues])
        for widget in (self.machine, self.account, self.walltime):
            _show(widget, slurm)
        _show(self.queue, slurm and not custom)
        _show(self.queue_text, slurm and custom)
        _show(self.cpus_per_node, slurm and custom)
        _show(self.machine_note, slurm)
        _show(self.env_note, slurm)
        fallbacks = []
        for label, getter in (
            ("queue", SlurmLauncher.configured_queue),
            ("account", SlurmLauncher.configured_account),
            ("walltime", SlurmLauncher.configured_walltime),
        ):
            try:
                value = getter()
            except Exception:
                value = ""
            fallbacks.append(f"{label} <code>{_esc(value) or 'unset'}</code>")
        self.env_note.value = (
            "<span class='forge-hint' style='color:#888'>Blank fields fall back "
            f"to the environment: {', '.join(fallbacks)}</span>"
        )
        self.compute_note.value = (
            "<span class='forge-hint'>Kept as written (the file sets workplan-wide "
            "SLURM defaults without choosing a launcher).</span>"
            if self._compute_kept
            else ""
        )

    def _populate_compute(self, raw: Mapping[str, Any]) -> None:
        """Show a workplan's ``compute_environment`` in the compute widgets."""
        self._compute_kept, self._compute_extra = {}, {}
        for widget in (self.account, self.walltime, self.queue_text):
            widget.value = ""
        self.cpus_per_node.value = 0
        try:
            env = resolve_compute_environment(dict(raw))
        except Exception:
            env = ComputeEnvironment()
            self._compute_kept = dict(raw)
        if env.launcher == LocalLauncher.name:
            self.compute_target.value = TARGET_LOCAL
        elif env.launcher == SlurmLauncher.name:
            self.compute_target.value = TARGET_SLURM
            machines = slurm_machines()
            self.machine.value = (
                env.system
                if env.system in machines
                else (
                    MACHINE_CUSTOM if env.system or not machines else self.machine.value
                )
            )
            self._sync_compute()
            if env.slurm is not None:
                spec = env.slurm.model_dump(exclude_defaults=True, exclude_none=True)
                self.account.value = spec.pop("account_name", "")
                self.walltime.value = spec.pop("max_walltime", "")
                queue = spec.pop("queue_name", "")
                _add_option(self.queue, queue)
                self.queue.value = queue
                self.queue_text.value = queue
                self.cpus_per_node.value = spec.pop("cpus_per_node", 0)
                self._compute_extra = spec
        else:
            self.compute_target.value = TARGET_NONE
            if raw and not self._compute_kept:
                self._compute_kept = dict(raw)
        self._sync_compute()

    def _gather_compute(self) -> dict[str, Any]:
        """The ``compute_environment`` mapping (empty for "Not specified").

        Raises
        ------
        ValueError
            If the SLURM fields do not form a valid environment.
        """
        target = self.compute_target.value
        if target == TARGET_NONE:
            return copy.deepcopy(self._compute_kept)
        if target == TARGET_LOCAL:
            env = ComputeEnvironment(launcher=LocalLauncher.name)
        else:
            custom = self.machine.value == MACHINE_CUSTOM
            queue = (self.queue_text if custom else self.queue).value.strip()
            spec = {
                **self._compute_extra,
                **{
                    key: value
                    for key, value in (
                        ("account_name", self.account.value.strip()),
                        ("max_walltime", self.walltime.value.strip()),
                        ("queue_name", queue),
                        (
                            "cpus_per_node",
                            (self.cpus_per_node.value or None) if custom else None,
                        ),
                    )
                    if value
                },
            }
            env = ComputeEnvironment(
                launcher=SlurmLauncher.name,
                system="" if custom else self.machine.value,
                slurm=SlurmComputeSpec.model_validate(spec) if spec else None,
            )
        data = env.model_dump(exclude_defaults=True, exclude_none=True)
        resolve_compute_environment(data)
        return data

    # ---- steps card ------------------------------------------------------
    def _build_steps(self) -> None:
        W = self.W
        self.steps_holder = W.VBox([])
        self.add_step_btn = W.Button(
            description=_caption("add_step", "Add step"), icon="plus"
        )
        self.add_step_btn.on_click(lambda _b: self.add_step())

    def add_step(self, name: str = "") -> _StepPane:
        """Append a new, blank step pane and open it."""
        pane = _StepPane(self, name or self._unique_name("step"))
        pane.source.value = (
            SOURCE_CATALOG_ROMS if self.roms_blueprint_names() else SOURCE_PATH
        )
        self.panes.append(pane)
        pane.reselect()
        self._refresh_steps_view(open_pane=pane)
        if not self.is_suspended:
            self._rebuild()
        return pane

    def _unique_name(self, base: str) -> str:
        taken = {p.name.value.strip() for p in self.panes}
        if base not in taken:
            return base
        i = 2
        while f"{base}-{i}" in taken:
            i += 1
        return f"{base}-{i}"

    def _refresh_steps_view(self, open_pane: _StepPane | None = None) -> None:
        """Rebuild the accordion for the current panes, keeping open panes open."""
        previous = self._accordion
        was_open = (
            {
                id(p): acc.selected_index == 0
                for p, acc in zip(self._shown, previous.panes, strict=True)
            }
            if previous is not None
            else {}
        )
        self._accordion = components.open_accordion(
            self.W, [p.widget for p in self.panes], [p.title() for p in self.panes]
        )
        self._shown = list(self.panes)
        for pane, acc in zip(self.panes, self._accordion.panes, strict=True):
            if was_open.get(id(pane)) or pane is open_pane:
                acc.selected_index = 0
        self.steps_holder.children = [self._accordion]

    def _move(self, pane: _StepPane, delta: int) -> None:
        i = self.panes.index(pane)
        j = i + delta
        if 0 <= j < len(self.panes):
            self.panes[i], self.panes[j] = self.panes[j], self.panes[i]
            self._refresh_steps_view()
            self._rebuild()

    def _duplicate(self, pane: _StepPane) -> None:
        try:
            step = pane.gather()
        except Exception as ex:
            pane.status.value = (
                f"<span class='forge-msg-err'>Cannot duplicate: {_esc(ex)}</span>"
            )
            return
        copy_pane = _StepPane(self)
        with self.suspended():
            copy_pane.populate(
                step.model_copy(update={"name": self._unique_name(f"{step.name}-copy")})
            )
        self.panes.insert(self.panes.index(pane) + 1, copy_pane)
        self._refresh_steps_view(open_pane=copy_pane)
        self._rebuild()

    def _delete(self, pane: _StepPane) -> None:
        self.panes.remove(pane)
        self._refresh_steps_view()
        self._rebuild()

    def _rename_refs(self, old: str, new: str) -> None:
        for pane in self.panes:
            pane.rename_reference(old, new)

    # ---- review card -----------------------------------------------------
    def _build_review(self) -> None:
        W = self.W
        self.validation = W.HTML("")
        self.preview = W.HTML("")
        self.save_path = W.Text(continuous_update=False, layout=W.Layout(width="520px"))
        self.save_btn = W.Button(
            description=_caption("save", "Save"), icon="save", button_style="primary"
        )
        self.save_status = W.HTML("")
        self.download_link = W.HTML("")
        self.changes_box = W.VBox([])
        self.save_path.observe(self._on_save_path, names="value")
        self.save_btn.on_click(lambda _b: self._on_save())

    def _on_save_path(self, _change: Any) -> None:
        if not self.is_suspended:
            self._save_touched = True
            self._reset_confirm()

    def default_save_path(self) -> Path:
        """Where Save writes by default: the catalog's ``workplans/<slug>.yaml``."""
        try:
            slug = slugify(self.name.value)
        except ValueError:
            slug = "workplan"
        catalog = self.catalog
        if catalog is not None and not getattr(catalog, "read_only", False):
            return Path(catalog.workplans_dir) / f"{slug}.yaml"
        base = self.loaded_from.parent if self.loaded_from else Path.cwd()
        return base / f"{slug}.yaml"

    def _sync_save_path(self) -> None:
        if self._save_touched:
            return
        with self.suspended():
            self.save_path.value = str(self.default_save_path())

    def _reset_confirm(self) -> None:
        self._awaiting_confirm = False
        self.save_btn.description = _caption("save", "Save")
        self.save_btn.button_style = "primary"
        self._confirm_timer = None

    def _schedule_reset(self) -> Any:
        """Reset the confirm state after the timeout (on the loop when one runs)."""
        import asyncio

        try:
            return asyncio.get_running_loop().call_later(
                CONFIRM_TIMEOUT, self._reset_confirm
            )
        except RuntimeError:
            timer = threading.Timer(CONFIRM_TIMEOUT, self._reset_confirm)
            timer.daemon = True
            timer.start()
            return timer

    def _is_loaded_file(self, target: Path) -> bool:
        try:
            return self.loaded_from is not None and target.resolve() == self.loaded_from
        except OSError:
            return False

    def _on_save(self) -> None:
        """Save the draft; overwriting the loaded file takes a second click."""
        if self.draft is None:
            self.save_status.value = "<span class='forge-msg-err'>The draft is invalid; fix the problems above before saving.</span>"
            return
        target = Path(self.save_path.value.strip()).expanduser()
        if self._is_loaded_file(target) and not self._awaiting_confirm:
            self._awaiting_confirm = True
            self.save_btn.description = _caption(
                "confirm_overwrite", "Confirm overwrite"
            )
            self.save_btn.button_style = "danger"
            if self._confirm_timer is not None:
                self._confirm_timer.cancel()
            self._confirm_timer = self._schedule_reset()
            return
        if self._confirm_timer is not None:
            self._confirm_timer.cancel()
        self._reset_confirm()
        self.save(target)

    def save(self, target: Path) -> Path | None:
        """Write the draft's file text to ``target``; the status line reports it."""
        if self.draft is None:
            return None
        text = yaml_text(self.draft)
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text)
        except OSError as ex:
            self.save_status.value = f"<span class='forge-msg-err'>{_esc(ex)}</span>"
            return None
        self._saved_text = text
        self.save_status.value = (
            f"<span class='forge-msg-ok'>Saved {_esc(target)}</span>"
        )
        self._refresh_saved()
        return target

    # ---- recipes card ----------------------------------------------------
    def _build_recipes(self) -> None:
        """Build the recipe forms: chunk, ramp, forge then run, upscale."""
        W = self.W
        self.recipe_hint = W.HTML("")

        def text(**kw: Any) -> Any:
            return W.Text(continuous_update=False, layout=W.Layout(width="260px"), **kw)

        # chunk a run in time
        self.chunk_base = W.Dropdown(options=[], layout=W.Layout(width="260px"))
        self.chunk_start = text(placeholder="ISO date, from the blueprint")
        self.chunk_end = text(placeholder="ISO date, from the blueprint")
        self.chunk_mode = W.Dropdown(
            options=[
                ("calendar months", "monthly"),
                ("fixed days", "days"),
                ("equal parts", "equal"),
            ],
            value="monthly",
        )
        self.chunk_value = W.IntText(value=1, layout=W.Layout(width="100px"))
        self.chunk_prefix = text()
        self.chunk_first = _FirstSource(self)
        self.chunk_walltime = text(placeholder="HH:MM:SS")
        self.chunk_hours_per_day = W.FloatText(
            value=0.0, layout=W.Layout(width="100px")
        )
        self.chunk_cadence = W.Checkbox(value=False, indent=False)
        self.chunk_btn = W.Button(
            description=_caption("add_steps", "Add steps"), icon="plus"
        )
        self.chunk_status = W.HTML("")
        # spin-up ramp
        self.ramp_base = W.Dropdown(options=[], layout=W.Layout(width="260px"))
        self.ramp_start = text(placeholder="ISO date, from the blueprint")
        self.ramp_prefix = text(value="spinup")
        self.ramp_first = _FirstSource(self)
        self.ramp_cadence = W.Checkbox(value=False, indent=False)
        self.ramp_rows: list[tuple[Any, Any, Any]] = []
        self.ramp_box = W.VBox([])
        self.ramp_add_btn = W.Button(
            description=_caption("add_ramp_row", "Add segment"), icon="plus"
        )
        self.ramp_btn = W.Button(
            description=_caption("add_steps", "Add steps"), icon="plus"
        )
        self.ramp_status = W.HTML("")
        # forge inputs, then run
        self.forge_source = W.Dropdown(options=[], layout=W.Layout(width="260px"))
        self.forge_path = text(placeholder="/path/to/x.forge_blueprint.yaml")
        self.forge_btn = W.Button(
            description=_caption("add_steps", "Add steps"), icon="plus"
        )
        self.forge_status = W.HTML("")
        # upscale a nested chain
        self.upscale_levels = W.TagsInput(allowed_tags=[], allow_duplicates=False)
        self.upscale_btn = W.Button(
            description=_caption("add_steps", "Add steps"), icon="plus"
        )
        self.upscale_status = W.HTML("")

        self.chunk_base.observe(self._on_chunk_base, names="value")
        self.ramp_base.observe(self._on_ramp_base, names="value")
        self.chunk_btn.on_click(
            lambda _b: self._generate(self.chunk_status, self._chunk)
        )
        self.ramp_btn.on_click(lambda _b: self._generate(self.ramp_status, self._ramp))
        self.forge_btn.on_click(
            lambda _b: self._generate(self.forge_status, self._forge)
        )
        self.upscale_btn.on_click(
            lambda _b: self._generate(self.upscale_status, self._upscale)
        )
        self.ramp_add_btn.on_click(lambda _b: self._add_ramp_row(7.0, 100.0))
        self._add_ramp_row(7.0, 100.0)

    def _add_ramp_row(self, days: float, dt: float) -> None:
        W = self.W
        days_w = W.FloatText(value=days, layout=W.Layout(width="110px"))
        dt_w = W.FloatText(value=dt, layout=W.Layout(width="110px"))
        remove = W.Button(icon="trash", tooltip="Remove this segment")
        entry = (days_w, dt_w, remove)
        holder = W.HBox([W.HTML("days"), days_w, W.HTML("dt (s)"), dt_w, remove])
        self.ramp_rows.append(entry)
        self.ramp_box.children = [*self.ramp_box.children, holder]

        def _remove(_b: Any) -> None:
            i = self.ramp_rows.index(entry)
            self.ramp_rows.pop(i)
            self.ramp_box.children = [
                c for j, c in enumerate(self.ramp_box.children) if j != i
            ]

        remove.on_click(_remove)

    def roms_step_names(self) -> list[str]:
        """The names of the roms_marbl steps (the bases the time recipes accept)."""
        return [
            p.name.value.strip()
            for p in self.panes
            if p.name.value.strip() and p.app == ROMS_MARBL
        ]

    def _refresh_recipes(self) -> None:
        """Reload the recipe dropdowns from the current steps and catalog."""
        names = self.roms_step_names()
        _set_options(self.chunk_base, [("(choose a step)", ""), *names])
        _set_options(self.ramp_base, [("(choose a step)", ""), *names])
        _set_options(
            self.forge_source,
            [("a path…", ""), *self.forge_blueprint_names()],
        )
        _show(self.forge_path, not self.forge_source.value)
        if list(self.upscale_levels.allowed_tags) != names:
            self.upscale_levels.allowed_tags = names
        self.chunk_first.refresh()
        self.ramp_first.refresh()
        self.recipe_hint.value = (
            ""
            if names
            else "<span class='forge-hint'>The chunk, ramp and upscale recipes "
            "start from a roms_marbl step: add one in the Steps card first.</span>"
        )
        for widget in (self.chunk_btn, self.ramp_btn, self.upscale_btn):
            widget.disabled = not names

    def _on_chunk_base(self, _change: Any) -> None:
        if self.is_suspended:
            return
        self._prefill_window(self.chunk_base.value, self.chunk_start, self.chunk_end)
        if self.chunk_base.value:
            self.chunk_prefix.value = self.chunk_base.value

    def _on_ramp_base(self, _change: Any) -> None:
        if self.is_suspended:
            return
        self._prefill_window(self.ramp_base.value, self.ramp_start, None)

    def _prefill_window(self, base: str, start: Any, end: Any) -> None:
        """Prefill a recipe's window from the base step's blueprint, when readable."""
        pane = self.pane_named(base) if base else None
        facts = pane.facts() if pane else None
        if facts is None:
            return
        override = pane.end_date.value.strip() if pane else ""
        if facts.start_date:
            start.value = str(facts.start_date)
        if end is not None:
            end.value = override or (str(facts.end_date) if facts.end_date else "")

    def _generate(self, status: Any, build: Callable[[], Generated]) -> None:
        """Run a generator and append its steps; its ValueError becomes the status."""
        try:
            generated = build()
            self._append_generated(generated)
        except ValueError as ex:
            status.value = f"<span class='forge-msg-err'>{_esc(ex)}</span>"
            return
        status.value = (
            f"<span class='forge-msg-ok'>Added {len(generated.steps)} step(s): "
            f"{_esc(', '.join(s.name for s in generated.steps))}. The base step "
            "stays; delete it if it should not run.</span>"
        )

    def _append_generated(self, generated: Generated) -> None:
        """Append generated steps as panes and merge the runs they declare."""
        taken = {p.name.value.strip() for p in self.panes}
        if clash := [s.name for s in generated.steps if s.name in taken]:
            msg = f"step name(s) already in use: {', '.join(clash)}; choose another prefix"
            raise ValueError(msg)
        declared = {r.alias.value.strip(): r for r in self.run_rows}
        for alias, ref in generated.runs.items():
            row = declared.get(alias)
            if row is not None and row.run_id.value.strip() != ref.run_id:
                msg = (
                    f"run alias {alias!r} already names run "
                    f"{row.run_id.value.strip()!r}, not {ref.run_id!r}"
                )
                raise ValueError(msg)
        with self.suspended():
            for alias, ref in generated.runs.items():
                if alias not in declared:
                    row = _RunRow(self, alias, ref.run_id)
                    self.run_rows.append(row)
                    self._load_recorded(row)
            self.runs_box.children = [r.holder for r in self.run_rows]
            for step in generated.steps:
                pane = _StepPane(self)
                self.panes.append(pane)
                pane.populate(step)
            self._refresh_steps_view()
        self._rebuild()

    def _unique_names(self, *bases: str) -> tuple[str, ...]:
        taken = {p.name.value.strip() for p in self.panes}
        names: list[str] = []
        for base in bases:
            name, i = base, 2
            while name in taken:
                name, i = f"{base}-{i}", i + 1
            taken.add(name)
            names.append(name)
        return tuple(names)

    def _base_step(self, name: str) -> Step:
        pane = self.pane_named(name) if name else None
        if pane is None:
            raise ValueError("choose the base step")
        return pane.gather()

    def _chunk(self) -> Generated:
        base = self._base_step(self.chunk_base.value)
        start, end = (
            self._window_date(w.value, label)
            for w, label in ((self.chunk_start, "start"), (self.chunk_end, "end"))
        )
        mode, value = self.chunk_mode.value, self.chunk_value.value
        if mode == "monthly":
            windows = month_windows(start, end)
        elif mode == "days":
            windows = fixed_windows(start, end, timedelta(days=value))
        else:
            windows = equal_windows(start, end, value)
        fixed, per_day = (
            self.chunk_walltime.value.strip(),
            self.chunk_hours_per_day.value,
        )
        if fixed and per_day:
            raise ValueError(
                "give a fixed walltime or hours per simulated day, not both"
            )
        walltime: str | Callable[[datetime, datetime], str] | None = fixed or None
        if per_day:

            def per_day_walltime(first: datetime, last: datetime) -> str:
                return format_walltime(per_day * (last - first).total_seconds() / 86400)

            walltime = per_day_walltime

        return chunk_steps(
            base,
            windows,
            prefix=self.chunk_prefix.value.strip(),
            first_source=self.chunk_first.source(),
            walltime=walltime,
            restart_cadence=FREQUENT_RESTARTS if self.chunk_cadence.value else None,
        )

    def _ramp(self) -> Generated:
        base = self._base_step(self.ramp_base.value)
        start = self._window_date(self.ramp_start.value, "start")
        ramp = [(timedelta(days=d.value), dt.value) for d, dt, _r in self.ramp_rows]
        return spinup_ramp(
            base,
            ramp,
            start=start,
            prefix=self.ramp_prefix.value.strip(),
            first_source=self.ramp_first.source(),
            restart_cadence=FREQUENT_RESTARTS if self.ramp_cadence.value else None,
        )

    def _forge(self) -> Generated:
        name = self.forge_source.value
        path = (
            str(self.catalog.forge_blueprint_path(name))
            if name
            else self.forge_path.value.strip()
        )
        if not path:
            raise ValueError("choose a forge blueprint")
        emitted = emitted_for(path, FORGE)
        if emitted is None:
            raise ValueError(f"cannot read a forge blueprint at {path!r}")
        forge_name, run_name = self._unique_names(FORGE, ROMS_MARBL)
        return forge_then_run(Path(path), emitted, names=(forge_name, run_name))

    def _upscale(self) -> Generated:
        names = list(self.upscale_levels.value)
        levels = [self._base_step(n) for n in names]

        def use_pio(step: Step) -> bool:
            pane = self.pane_named(step.name)
            facts = pane.facts() if pane else None
            return True if facts is None or facts.use_pio is None else facts.use_pio

        return upscale_chain(levels, use_pio_of=use_pio)

    @staticmethod
    def _window_date(text: str, label: str) -> datetime:
        parsed = _as_datetime(text.strip())
        if parsed is None:
            raise ValueError(
                f"enter the window {label} as an ISO date (e.g. 2012-01-01)"
            )
        return parsed

    # ---- diagnostics -----------------------------------------------------
    def _build_diagnostics(self) -> None:
        W = self.W
        self.deep_btn = W.Button(
            description=_caption("deep_check", "Deep check"), icon="search"
        )
        self.diagnostics = W.HTML("")
        self.readiness = W.HTML("")
        self.deep_btn.on_click(lambda _b: self.deep_check())
        self.readiness_kept = 0

    def deep_check(self) -> None:
        """Run the deep check in the kernel (see :meth:`_deep_check_async`)."""
        _schedule_coroutine(self._deep_check_async())

    def parsed_vars(self) -> dict[str, str]:
        """The ``k=v`` runtime variable values typed under Run."""
        pairs = (p.partition("=") for p in self.run_vars.value.split(",") if p.strip())
        return {k.strip(): v.strip() for k, _sep, v in pairs}

    async def _deep_check_async(self) -> None:
        """Resolve the draft as ``cstar workplan run`` would, listing every problem."""
        draft = self.draft
        if draft is None:
            self.diagnostics.value = components.banner(
                "warn", "Fix the problems listed above before running a deep check."
            )
            return
        self.deep_btn.disabled = True
        self.diagnostics.value = "<i>checking…</i>"
        try:
            resolved, problems = await deep_check(
                draft, self.parsed_vars(), lambda: get_launcher(draft)
            )
        except Exception as ex:  # e.g. SLURM requested on a machine without it
            resolved, problems = None, [f"{type(ex).__name__}: {ex}"]
        finally:
            self.deep_btn.disabled = False
        problems = [*problems, *self._drift_problems()]
        unverifiable = [p for p in problems if _is_unverifiable(p)]
        failed = [p for p in problems if p not in unverifiable]
        parts: list[str] = []
        if failed:
            parts.append(
                components.banner(
                    "err",
                    "<b>Deep check found problems:</b><br>"
                    + "<br>".join(f"&nbsp;&nbsp;{_esc(p)}" for p in failed),
                )
            )
        if unverifiable:
            parts.append(
                components.banner(
                    "warn",
                    "<b>Not verifiable here:</b><br>"
                    + "<br>".join(f"&nbsp;&nbsp;{_esc(p)}" for p in unverifiable)
                    + "<br><span class='forge-hint'>The blueprint or the run record "
                    "is not readable on this machine; the run checks it again.</span>",
                )
            )
        if not problems and resolved is not None:
            parts.append(
                components.banner(
                    "ok", "<b>Deep check passed.</b> Deferred steps are not checked."
                )
            )
        self.diagnostics.value = "".join(parts)

    def _drift_problems(self) -> list[str]:
        """Deferred steps whose ``num_cpus`` differs from the producer's prediction."""
        lines: list[str] = []
        for pane in self.panes:
            if pane.source.value != SOURCE_DEFERRED or not pane.num_cpus.value:
                continue
            emitted = self.emitted_for_token(pane.producer.value)
            if emitted is not None and emitted.cpus_needed != pane.num_cpus.value:
                lines.append(
                    f"step {pane.name.value.strip()!r}: num_cpus is "
                    f"{pane.num_cpus.value} but {pane.producer.value!r} is predicted "
                    f"to emit a blueprint needing {emitted.cpus_needed}"
                )
        return lines

    def _update_readiness(self) -> None:
        """List what a pre-run would prepare and what it would skip."""
        self.readiness_kept = 0
        if self.draft is None:
            self.readiness.value = ""
            return
        try:
            pruned, reasons = prune_unpreparable_steps(self.draft)
        except Exception as ex:  # an application that cannot be imported here
            self.readiness.value = f"<span class='forge-hint'>Pre-run readiness unavailable: {_esc(ex)}</span>"
            return
        kept = [s.name for s in pruned.steps]
        self.readiness_kept = len(kept)
        lines = [
            "<b>will be prepared:</b> "
            + (", ".join(f"<code>{_esc(n)}</code>" for n in kept) or "nothing")
        ]
        lines += [
            f"<b>skipped ({_esc(reason)}):</b> "
            + ", ".join(f"<code>{_esc(n)}</code>" for n in names)
            for reason, names in reasons.items()
        ]
        self.readiness.value = "<br>".join(lines)

    # ---- run -------------------------------------------------------------
    def _build_run(self) -> None:
        W = self.W
        self.run_id = W.Text(continuous_update=False, layout=W.Layout(width="320px"))
        self.run_vars = W.Text(
            placeholder="name=value, name=value",
            continuous_update=False,
            layout=W.Layout(width="420px"),
        )
        self.pre_run_first = W.Checkbox(value=False, indent=False)
        self.check_btn = W.Button(description=_caption("check", "Check"), icon="check")
        self.run_btn = W.Button(
            description=_caption("run", "Run"), icon="play", button_style="primary"
        )
        self.run_status = W.HTML("")
        self._run_id_touched = False
        # same log styling and bottom-pinned scrolling as the Blueprint page's Run
        self.run_output = W.Output(
            layout=W.Layout(
                border="1px solid #ccc",
                padding="6px",
                max_height="380px",
                overflow="auto",
                display="flex",
                flex_flow="column-reverse",
            )
        )
        self.run_output.add_class("forge-run-log")
        self._run_log_style = W.HTML(
            "<style>.forge-run-log > .jp-OutputArea "
            "{ flex: 0 0 auto; height: auto; max-height: none; }</style>"
        )
        self.run_id.observe(self._on_run_id, names="value")
        self.check_btn.on_click(lambda _b: self._start_run(CHECK))
        self.run_btn.on_click(lambda _b: self._start_run(RUN))

    def _on_run_id(self, _change: Any) -> None:
        if not self.is_suspended:
            self._run_id_touched = True

    def _sync_run(self) -> None:
        """Track the default run-id and decide whether Pre-run first is on offer."""
        if not self._run_id_touched:
            try:
                default = slugify(self.name.value)
            except ValueError:
                default = ""
            if self.run_id.value != default:
                self.run_id.value = default
        offered = self.compute_target.value == TARGET_SLURM and self.readiness_kept > 0
        if offered != self._pre_run_offered:
            self._pre_run_offered = offered
            self.pre_run_first.value = offered
        _show(self.pre_run_first, offered)

    def run_command(self, action: str, path: Path) -> list[str]:
        """The CLI command for ``action`` on the saved workplan at ``path``.

        Uses the ``cstar`` script installed beside the running interpreter, so
        the subprocess stays in this environment; otherwise runs the CLI as a
        module.
        """
        exe = Path(sys.executable).with_name("cstar")
        cmd = [str(exe)] if exe.exists() else [sys.executable, "-m", "cstar.cli.cli"]
        cmd += ["workplan", action, str(path)]
        if action == RUN and self.run_id.value.strip():
            cmd += [ARG_RUN_ID, self.run_id.value.strip()]
        for key, value in self.parsed_vars().items():
            cmd += [ARG_VAR_LONG, f"{key}={value}"]
        if action == RUN and self.pre_run_first.value and self._pre_run_offered:
            cmd.append(ARG_PRE_RUN)
        return cmd

    def ensure_saved(self) -> Path | None:
        """Save the draft when needed and return the file to run.

        The loaded file is never overwritten here: when the target is that
        file and it differs from the draft, saving needs the confirm click.
        """
        if self.draft is None:
            return None
        target = Path(self.save_path.value.strip()).expanduser()
        text = yaml_text(self.draft)
        if target.is_file() and target.read_text() == text:
            return target
        if self._is_loaded_file(target):
            self.run_status.value = (
                "<span class='forge-msg-err'>The draft differs from the loaded file: "
                "choose another save path, or Save and confirm the overwrite.</span>"
            )
            return None
        return self.save(target)

    def _start_run(self, action: str) -> None:
        if self.draft is None:
            self.run_status.value = "<span class='forge-msg-err'>Nothing to run: the draft is invalid.</span>"
            return
        _schedule_coroutine(self._run_async(action))

    async def _run_async(self, action: str) -> None:
        """Save if needed, run the CLI and stream its output into the log."""
        import asyncio

        self.run_btn.disabled = self.check_btn.disabled = True
        self.run_output.clear_output(wait=True)
        cmd: list[str] | None = None
        try:
            path = self.ensure_saved()
            if path is None:
                return
            cmd = self.run_command(action, path)
            self.run_status.value = f"<i>running: {_esc(' '.join(cmd))}</i>"
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
            stream = proc.stdout
            assert stream is not None  # PIPE was requested
            buf = b""
            while chunk := await stream.read(_STREAM_READ_SIZE):
                lines, buf = _drain_stream_buffer(buf + chunk)
                for line in lines:
                    self.run_output.append_stdout(line)
            lines, buf = _drain_stream_buffer(buf, at_eof=True)
            for line in lines:
                self.run_output.append_stdout(line)
            code = await proc.wait()
            self.run_status.value = self._finished_html(action, path, code)
        except Exception as ex:
            where = f" while running: {_esc(' '.join(cmd))}" if cmd else ""
            self.run_status.value = f"<span class='forge-msg-err'>{type(ex).__name__}: {_esc(ex)}{where}</span>"
        finally:
            self.run_btn.disabled = self.check_btn.disabled = False

    def _finished_html(self, action: str, path: Path, code: int) -> str:
        if code != 0:
            return f"<span class='forge-msg-err'>exited with code {code}</span>"
        run_id = self.run_id.value.strip()
        if action == CHECK:
            return "<span class='forge-msg-ok'>&check; check finished</span>"
        status_cmd = (
            f"cstar workplan status {run_id}"
            if run_id
            else "cstar workplan status <run-id>"
        )
        if ARG_PRE_RUN in self.run_command(action, path):
            follow = " ".join(
                c for c in self.run_command(action, path) if c != ARG_PRE_RUN
            ).replace(str(Path(sys.executable).with_name("cstar")), "cstar")
            return (
                "<span class='forge-msg-ok'>&check; prepared (pre-run).</span> Then "
                f"submit it with <code>{_esc(follow)}</code>; follow it with "
                f"<code>{_esc(status_cmd)}</code>"
            )
        return (
            "<span class='forge-msg-ok'>&check; scheduled.</span> Follow it with "
            f"<code>{_esc(status_cmd)}</code>"
        )

    # ---- normalizer report -----------------------------------------------
    def _revert(self, step_name: str) -> None:
        """Restore a step's pane from the file as loaded, undoing its rewrites."""
        original = next(
            (
                s
                for s in (self.original.steps if self.original else ())
                if s.name == step_name
            ),
            None,
        )
        pane = self.pane_named(step_name)
        if original is None or pane is None:
            return
        pane.populate(original)
        self._reverted.add(step_name)
        self._rebuild()

    # ---- layout ----------------------------------------------------------
    def _build_layout(self) -> None:
        W = self.W
        self.sticky_bar = W.HTML("")
        self.card_chips = {
            key: W.HTML("")
            for key in ("start", "workplan", "compute", "steps", "recipes", "review")
        }

        def row(key: str, widget: Any, **kw: Any) -> Any:
            return components.field_row(W, key, widget, page=PAGE, **kw)

        sub = components.subsection
        card = components.card
        chips = self.card_chips
        start = card(
            W,
            "start",
            row("load_dd", self.load_dd, extra=(self.load_btn, self.refresh_saved_btn)),
            row("load_path", self.load_path, extra=(self.load_path_btn,)),
            row("upload", self.upload, extra=(self.upload_btn,)),
            W.HBox([self.new_btn, self.load_status]),
            num=1,
            chips_widget=chips["start"],
            page=PAGE,
        )
        workplan = card(
            W,
            "workplan",
            row("name", self.name),
            row("description", self.description),
            row("runtime_vars", self.runtime_vars),
            sub(
                W,
                "workplan.runs",
                self.runs_box,
                W.HBox([self.add_run_btn, self.refresh_runs_btn, self.runs_status]),
                page=PAGE,
            ),
            num=2,
            chips_widget=chips["workplan"],
            page=PAGE,
        )
        compute = card(
            W,
            "compute",
            row("compute_target", self.compute_target),
            row("machine", self.machine, extra=(self.machine_note,)),
            row("queue", self.queue, extra=(self.queue_text,)),
            row("account", self.account),
            row("walltime", self.walltime),
            row("cpus_per_node", self.cpus_per_node),
            self.env_note,
            self.compute_note,
            num=3,
            chips_widget=chips["compute"],
            page=PAGE,
        )
        steps = card(
            W,
            "steps",
            self.steps_holder,
            self.add_step_btn,
            num=4,
            chips_widget=chips["steps"],
            page=PAGE,
        )
        recipes = card(
            W,
            "recipes",
            self.recipe_hint,
            sub(
                W,
                "recipes.chunk",
                row("chunk_base", self.chunk_base),
                row("chunk_start", self.chunk_start),
                row("chunk_end", self.chunk_end),
                row("chunk_mode", self.chunk_mode, extra=(self.chunk_value,)),
                row("chunk_prefix", self.chunk_prefix),
                *self.chunk_first.rows(),
                row("chunk_walltime", self.chunk_walltime),
                row("chunk_hours_per_day", self.chunk_hours_per_day),
                row("chunk_cadence", self.chunk_cadence),
                W.HBox([self.chunk_btn, self.chunk_status]),
                page=PAGE,
            ),
            sub(
                W,
                "recipes.ramp",
                row("ramp_base", self.ramp_base),
                row("ramp_start", self.ramp_start),
                row("ramp_prefix", self.ramp_prefix),
                self.ramp_box,
                self.ramp_add_btn,
                *self.ramp_first.rows(),
                row("ramp_cadence", self.ramp_cadence),
                W.HBox([self.ramp_btn, self.ramp_status]),
                page=PAGE,
            ),
            sub(
                W,
                "recipes.forge",
                row("forge_source", self.forge_source),
                row("forge_path", self.forge_path),
                W.HBox([self.forge_btn, self.forge_status]),
                page=PAGE,
            ),
            sub(
                W,
                "recipes.upscale",
                row("upscale_levels", self.upscale_levels),
                W.HBox([self.upscale_btn, self.upscale_status]),
                page=PAGE,
            ),
            num=5,
            chips_widget=chips["recipes"],
            page=PAGE,
        )
        review = card(
            W,
            "review",
            self.validation,
            sub(W, "review.report", self.changes_box, page=PAGE),
            sub(
                W,
                "review.diagnostics",
                W.HBox([self.deep_btn]),
                self.diagnostics,
                self.readiness,
                page=PAGE,
            ),
            sub(W, "review.preview", self.preview, page=PAGE),
            sub(
                W,
                "review.save",
                row("save_path", self.save_path, extra=(self.save_btn,)),
                self.save_status,
                self.download_link,
                page=PAGE,
            ),
            sub(
                W,
                "review.run",
                row("run_id", self.run_id),
                row("run_vars", self.run_vars),
                row("pre_run_first", self.pre_run_first),
                W.HBox([self.check_btn, self.run_btn, self.run_status]),
                self._run_log_style,
                self.run_output,
                page=PAGE,
            ),
            num=6,
            chips_widget=chips["review"],
            required_chip=False,
            page=PAGE,
        )
        intro = W.HTML(
            "<p class='forge-hint'>Compose steps into a workplan, check it, save it, "
            "and run it. Load an existing workplan to edit a copy; the loaded file "
            "is never changed unless you confirm an overwrite.</p>"
        )
        self.widget = W.VBox(
            [
                components.style_widget(W),
                self.sticky_bar,
                intro,
                start,
                workplan,
                compute,
                steps,
                recipes,
                review,
            ]
        )
        self.sticky_bar.add_class("forge-sticky-html")
        self.widget.add_class("forge-app")
        self.widget.add_class("forge-workplan")

    # ---- gather / rebuild ------------------------------------------------
    def _gather(self) -> Workplan | None:
        """Build the draft from the widgets; ``self.problems`` lists what is wrong."""
        problems: list[str] = []
        steps: list[Step] = []
        owners: list[_StepPane] = []
        for pane in self.panes:
            pane.error = ""
            try:
                steps.append(pane.gather())
                owners.append(pane)
            except Exception as ex:
                pane.error = str(ex)
                problems.append(
                    f"step {pane.name.value.strip() or '(unnamed)'!r}: {ex}"
                )
            problems.extend(
                f"step {pane.name.value.strip() or '(unnamed)'!r}: {line}"
                for line in pane.problems()
            )
        runs = {
            row.alias.value.strip(): RunRef(run_id=row.run_id.value.strip())
            for row in self.run_rows
            if row.alias.value.strip() and row.run_id.value.strip()
        }
        compute: dict[str, Any] = {}
        try:
            compute = self._gather_compute()
        except Exception as ex:
            problems.append(f"compute: {ex}")
        runtime_vars = [
            v.strip() for v in self.runtime_vars.value.split(",") if v.strip()
        ]
        data = {
            "name": self.name.value.strip(),
            "description": self.description.value.strip(),
            "runs": runs,
            "steps": steps,
            "compute_environment": compute,
            "runtime_vars": runtime_vars,
        }
        try:
            workplan = Workplan.model_validate(data)
        except ValidationError as ex:
            for err in ex.errors():
                loc = list(err["loc"])
                if loc == ["steps"] and any(p.error for p in self.panes):
                    continue  # the panes already say why there are no steps
                label = ".".join(str(p) for p in loc)
                if loc[:1] == ["steps"] and len(loc) > 1 and isinstance(loc[1], int):
                    if loc[1] < len(owners):
                        owners[loc[1]].error = err["msg"]
                        label = f"step {owners[loc[1]].name.value.strip()!r}"
                problems.append(f"{label}: {err['msg']}".strip(": "))
            self.problems = problems
            return None
        self.problems = problems
        return workplan

    def _rebuild(self) -> None:
        """Regather the draft and refresh every derived display."""
        if self.is_suspended:
            return
        with self.suspended():
            for pane in self.panes:
                pane.refresh_choices()
            self._refresh_recipes()
            self.draft = self._gather()
            self._update_view()

    def _update_view(self) -> None:
        """Refresh titles, chips, sticky bar, preview and the change report."""
        problems, draft = self.problems, self.draft
        valid = draft is not None and not problems
        for i, pane in enumerate(self.panes):
            flag = "  ⚠" if pane.error or pane.problem_lines else ""
            self._accordion.set_title(i, pane.title() + flag)
            pane.status.value = (
                components.chip("● problem", "warn")
                if pane.error or pane.problem_lines
                else components.chip("● ok", "ok")
            )
        chips = self.card_chips
        chips["steps"].value = components.chip(
            f"● {len(self.panes)} step(s)", "ok" if valid else "warn"
        )
        chips["workplan"].value = components.chip(
            "● Complete"
            if self.name.value.strip() and self.description.value.strip()
            else "● Name and description needed",
            "ok"
            if self.name.value.strip() and self.description.value.strip()
            else "warn",
        )
        chips["compute"].value = components.chip(
            f"● {self.compute_target.value}", "info"
        )
        chips["start"].value = (
            components.chip(f"● {len(self.changes)} change(s)", "info")
            if self.changes
            else ""
        )
        chips["review"].value = components.chip(
            "● Ready" if valid else "● Invalid", "ok" if valid else "warn"
        )
        links = "".join(
            f"<a class='step' href='#forge-sec-{key}'>{i} {title}</a>"
            for i, (key, title) in enumerate(
                (
                    ("start", "Start"),
                    ("workplan", "Workplan"),
                    ("compute", "Compute"),
                    ("steps", "Steps"),
                    ("recipes", "Recipes"),
                    ("review", "Review"),
                ),
                start=1,
            )
        )
        summary = (
            f"<b>{_esc(self.name.value.strip() or '(unnamed)')}</b> &middot; "
            f"{len(self.panes)} step(s)"
        )
        self.sticky_bar.value = (
            f"{links}<span class='summary'>{summary}</span><span class='sp'></span>"
            + components.chip(
                "● Valid" if valid else "● Invalid", "ok" if valid else "warn"
            )
        )
        self.validation.value = (
            components.banner(
                "err",
                "<b>Problems:</b><br>"
                + "<br>".join(f"&nbsp;&nbsp;{_esc(p)}" for p in problems[:12]),
            )
            if problems
            else components.banner("ok", "<b>The workplan is valid.</b>")
        )
        if draft is not None:
            text = yaml_text(draft)
            self.preview.value = f"<pre>{_esc(text)}</pre>"
            self.download_link.value = self._download_html(draft, text)
        else:
            self.preview.value = "<i>(no valid draft yet)</i>"
            self.download_link.value = ""
        self._render_changes()
        self._update_readiness()
        self._sync_run()
        self._sync_save_path()

    @staticmethod
    def _download_html(workplan: Workplan, text: str) -> str:
        """A data-URI download link for the workplan's file text."""
        fname = f"{slugify(workplan.name)}.yaml"
        b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
        return (
            f'⬇ <a class="forge-dl-btn" download="{fname}" '
            f'href="data:text/yaml;base64,{b64}">Download <code>{_esc(fname)}</code></a>'
        )

    def _render_changes(self) -> None:
        """List what loading changed, with a revert button per rewritten step.

        Reverting restores the whole step's pane from the file as loaded.
        """
        key = (
            tuple(repr(c) for c in self.changes),
            tuple(self.dropped),
            frozenset(self._reverted),
        )
        if key == self._changes_key:
            return
        self._changes_key = key
        W = self.W
        rows: list[Any] = []
        if self.changes or self.dropped:
            rows.append(
                W.HTML(
                    "<span class='forge-hint'>Applied when the file was loaded "
                    "(the file itself is untouched).</span>"
                )
            )
        for change in self.changes:
            reverted = change.step in self._reverted
            html_row = W.HTML(
                f"<code>{_esc(change.step)}</code> <code>{_esc(change.field)}</code>: "
                f"{_esc(change.before)} &rarr; {_esc(change.after)} "
                f"<span class='forge-hint'>{_esc(change.reason)}</span>"
            )
            button = W.Button(
                description=_caption("revert", "revert"),
                tooltip="Restore this step as it was in the file",
                disabled=reverted,
            )
            button.on_click(lambda _b, name=change.step: self._revert(name))
            rows.append(W.HBox([html_row, button]))
        for line in self.dropped:
            rows.append(
                W.HTML(
                    f"<code>{_esc(line)}</code> <span class='forge-hint'>dropped: "
                    "set at run time, not authored</span>"
                )
            )
        self.changes_box.children = rows


__all__ = ["WORKPLAN_SCHEMA_REF", "WorkplanBuilderPage", "yaml_text"]
