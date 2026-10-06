import functools
import itertools
import os
import re
import typing as t
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import (
    BaseModel,
    ValidationError,
)

from cstar.applications.core import (
    ApplicationDefinition,
    Transform,
    get_app_for_blueprint,
    get_application,
)
from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.log import LoggingMixin, get_logger
from cstar.base.utils import deep_merge
from cstar.execution.file_system import JobFileSystemManager, local_copy
from cstar.orchestration.adapter import DIRECTIVES_FILENAME, prepare_directive_file
from cstar.orchestration.compute_environment import (
    ComputeEnvironment,
    resolve_compute_environment,
)
from cstar.orchestration.launch.common import is_foreign_handle
from cstar.orchestration.launch.slurm import SlurmComputeSpec
from cstar.orchestration.models import (
    Blueprint,
    DeferredBlueprintRef,
    KeyValueStore,
    RunRef,
    StepRef,
    Workplan,
)
from cstar.orchestration.orchestration import (
    LiveStep,
    LiveWorkplan,
    ProcessHandle,
    Status,
    Task,
    synthesize_blueprint,
)
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun

if t.TYPE_CHECKING:
    from cstar.entrypoint.runner import BlueprintRunner
    from cstar.orchestration.orchestration import Launcher

log = get_logger(__name__)

TRANSFORMS: dict[str, list[Transform[LiveStep]]] = defaultdict(list)
"""Storage for transform registrations."""


def register_transform(application: str, transform: Transform[t.Any]) -> None:
    """Register a transform for an application.

    Parameters
    ----------
    application : str
        The application name.
    transform : Transform
        The transform instance to register for the application.
    """
    TRANSFORMS[application].append(transform)


def get_transforms(application: str) -> list[Transform[t.Any]]:
    """Retrieve a list of transforms to be applied for an application.

    Parameters
    ----------
    application : str
        The application name.

    Returns
    -------
    list[Transform]
        A list containing transforms
    """
    return TRANSFORMS.get(application, [])


INLINE_BLUEPRINT_FILENAME: t.Final[str] = "blueprint.yaml"
"""Name of the blueprint file materialized into an inline step's work directory."""

PLACEHOLDER_RE = re.compile(r"\{\{([^}]+)\}\}")
"""Pattern matching double-brace template placeholders.

Captures the full content between braces so dispatch logic can distinguish
plain variable references (``{{my_var}}``) from scope references
(``{{<scope>: step_name}}``).
"""


class SplitFrequency(StrEnum):
    Daily = "daily"
    Weekly = "weekly"
    Monthly = "monthly"


def _dailies(
    start_date: datetime,
    end_date: datetime,
) -> Iterable[tuple[datetime, datetime]]:
    """Get daily time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, start_date.day)
    while current_date < end_date:
        day_start = current_date
        day_end = day_start + timedelta(days=1)
        yield (day_start, day_end)
        current_date = day_end


def _weeklies(
    start_date: datetime,
    end_date: datetime,
) -> Iterable[tuple[datetime, datetime]]:
    """Get weekly time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, start_date.day)
    while current_date < end_date:
        week_start = current_date
        week_end = week_start + timedelta(days=7)
        yield (week_start, week_end)
        current_date = week_end


def _monthlies(
    start_date: datetime,
    end_date: datetime,
) -> Iterable[tuple[datetime, datetime]]:
    """Get monthly time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, 1)
    while current_date < end_date:
        month_start = current_date

        if month_start.month == 12:
            month_end = datetime(current_date.year + 1, 1, 1)
        else:
            month_end = datetime(current_date.year, month_start.month + 1, 1)

        yield (month_start, month_end)
        current_date = month_end


SLICE_FUNCTIONS = defaultdict(
    lambda: _monthlies,
    {
        SplitFrequency.Daily.value: _dailies,
        SplitFrequency.Weekly.value: _weeklies,
        SplitFrequency.Monthly.value: _monthlies,
    },
)


def get_time_slices(
    start_date: datetime,
    end_date: datetime,
    frequency: str = SplitFrequency.Monthly.value,
) -> Iterable[tuple[datetime, datetime]]:
    """Get the time slices for the given start and end dates.

    Parameters
    ----------
    start_date : datetime
        The start date.
    end_date : datetime
        The end date.
    frequency : str
        The desired frequency (daily, weekly, monthly).

    Returns
    -------
    Iterable[tuple[datetime, datetime]]
        Iterable containing 2-tuples of (start_date, end_date).
    """
    slice_fn = SLICE_FUNCTIONS[frequency]
    time_slices = list(slice_fn(start_date, end_date))

    # adjust when the start date is not the first day of the month
    if start_date > time_slices[0][0]:
        time_slices[0] = (start_date, time_slices[0][1])

    # adjust when the end date is not the last day of the month
    if end_date < time_slices[-1][1]:
        time_slices[-1] = (time_slices[-1][0], end_date)

    return time_slices


def mustache(s: str) -> str:
    """Return the string formatted with enclosing double-curly-brackets
    in the mustache template style.
    """
    return f"{{{{{s}}}}}"


def _parse_ref(token: str) -> StepRef:
    """Parse a step reference token, reporting a malformed one as a `CstarError`."""
    try:
        return StepRef.parse(token)
    except ValueError as ex:
        raise CstarError(str(ex)) from ex


def fsm_resolver(
    lookup: Callable[[str], JobFileSystemManager],
    step_name: str,
    scope: str,
) -> str:
    """A resolver for attributes available on a file-system manager.

    Parameters
    ----------
    lookup : Callable[[str], JobFileSystemManager]
        Maps a step reference token to the related file-system manager; raises
        `KeyError` for an unknown step of the current workplan.
    step_name : str
        The step reference token to retrieve an FSM for.
    scope : str
        The scope to be resolved (which attribute on the FSM instance).
    """
    try:
        fsm = lookup(step_name)
    except KeyError as ex:
        msg = f"Unable to resolve {scope!r} for unknown step {step_name!r}"
        raise KeyError(msg) from ex

    if value := getattr(fsm, scope, None):
        return str(value)

    ph = mustache(f"{scope}: {step_name}")
    msg = f"Unable to resolve {scope!r} for placeholder {ph!r}"
    raise KeyError(msg)


def get_fsm_resolver(
    steps: Sequence[LiveStep],
    external: "ExternalRuns",
) -> Callable[[str, str], str]:
    """Create a resolver function for file-system manager attributes.

    Parameters
    ----------
    steps : Sequence[LiveStep]
        The steps to be used in the creation of the resolver.
    external : ExternalRuns
        The registry used to resolve `<step>@<alias>` tokens.

    Returns
    -------
    Callable[[str, str], str]
        Callable accepting the step name and scope as parameters and returning
        the resolved value.
    """
    fsm_map: dict[str, JobFileSystemManager] = {s.name: s.fsm for s in steps}

    def lookup(token: str) -> JobFileSystemManager:
        ref = _parse_ref(token)
        return external.step(ref).fsm if ref.is_external else fsm_map[ref.step]

    return functools.partial(fsm_resolver, lookup)


class TemplateFillTransform:
    """Fill ``{{<scope>: <placeholder>}}`` template strings in a step.

    Replaces placeholders anywhere in the step (blueprint path, overrides,
    directives) and dispatches each one to one of two resolvers:

    - **variable resolver** — handles plain ``{{name}}`` tokens by looking up
      *name* in the caller-supplied mapping (e.g. user-defined runtime variables).
    - **scope resolver** — handles ``{{<scope>: step_name}}`` tokens by returning
      the resolved value for the named step.  Must be bound via
      :meth:`with_scoped_resolver` before any step that uses this syntax is
      processed.

    The transform yields a single updated step; the original step is not
    mutated. :meth:`fill_text` applies the same replacement to any string.
    """

    _variable_resolver: Callable[[str], str] | None = None
    _scoped_resolver: Callable[[str, str], str] | None = None

    def __init__(
        self,
        variable_resolver: Callable[[str], str] | None = None,
        scoped_resolver: Callable[[str, str], str] | None = None,
    ) -> None:
        """Initialize the transform.

        Parameters
        ----------
        variable_resolver : Callable[[str], str] | None
            Maps a plain placeholder name to its replacement string.
        scoped_resolver : Callable[[str, str], Path] | None
            Maps a step name to a resolver handling job paths.  Required only
            when ``blueprint_overrides`` contains ``{{<scope>: <step>}}`` tokens.
        """
        self._variable_resolver = variable_resolver
        self._scoped_resolver = scoped_resolver

    def with_scoped_resolver(
        self,
        resolver: Callable[[str, str], str],
    ) -> "TemplateFillTransform":
        """Return a new instance with the given resolver bound.

        Parameters
        ----------
        resolver : Callable[[str, str], str]
            Use this resolver to resolve values with a specific scope.

        Returns
        -------
        TemplateFillTransform
        """
        return TemplateFillTransform(
            self._variable_resolver,
            resolver,
        )

    @staticmethod
    def suffix() -> str:
        """Return the suffix used when persisting a resource modified by this transform."""
        return "tmpl"

    def _resolve(self, content: str) -> str:
        """Dispatch a single placeholder's inner content to the correct resolver.

        Parameters
        ----------
        content : str
            The text captured between ``{{`` and ``}}``, stripped of
            surrounding whitespace.

        Returns
        -------
        str
            The resolved replacement string.

        Raises
        ------
        ValueError
            If the appropriate resolver has not been provided.
        """
        parts = content.split(":", maxsplit=1)
        if len(parts) > 1:
            scope, ph = [x.strip() for x in parts]
            if not self._scoped_resolver:
                msg = f"No {scope!r} resolver provided for placeholder {ph!r}"
                raise ValueError(msg)

            return self._scoped_resolver(ph, scope)

        if self._variable_resolver is None:
            msg = f"No variable resolver provided for placeholder '{mustache(content)}'"
            raise ValueError(msg)
        return self._variable_resolver(content)

    def fill_text(self, content: str) -> str:
        """Replace every placeholder in a string.

        Parameters
        ----------
        content : str
            The text to fill.

        Returns
        -------
        str
            The text with every placeholder replaced.

        Raises
        ------
        CstarExpectationFailed
            If placeholders remain after filling or were malformed.
        """
        matches = PLACEHOLDER_RE.findall(content)
        if not matches:
            return content

        # use set to replace all occurrences at once
        for match in set(matches):
            content = content.replace(mustache(match), self._resolve(match))

        if PLACEHOLDER_RE.findall(content):
            raise CstarExpectationFailed(
                "Some templated values were not filled or placeholders were malformed."
            )

        return content

    def _fill(self, step: LiveStep) -> LiveStep:
        content = step.model_dump_json(by_alias=True)
        filled = self.fill_text(content)
        return step if filled == content else LiveStep.model_validate_json(filled)

    def __call__(self, step: LiveStep) -> Sequence[LiveStep]:
        """Apply template filling to every string in a step.

        Parameters
        ----------
        step : LiveStep
            The step whose placeholders (blueprint path, overrides,
            directives) will be filled.

        Returns
        -------
        Iterable[LiveStep]
            A single-element iterable containing the updated step.
        """
        return (LiveStep.from_step(self._fill(step)),)

    @property
    def variable_resolver(self) -> Callable[[str], str] | None:
        return self._variable_resolver

    @property
    def scoped_resolver(self) -> Callable[[str, str], str] | None:
        return self._scoped_resolver


def fill_runs(
    runs: Mapping[str, RunRef],
    fill_transform: TemplateFillTransform | None,
) -> dict[str, RunRef]:
    """Fill the `{{name}}` placeholders in the run-ids of external runs.

    Parameters
    ----------
    runs : Mapping[str, RunRef]
        The external runs of a workplan, keyed by alias.
    fill_transform : TemplateFillTransform | None
        The transform used to fill placeholders.

    Returns
    -------
    dict[str, RunRef]
        Copies of `runs` with filled run-ids.

    Raises
    ------
    CstarExpectationFailed
        If a run-id contains a placeholder and no transform was supplied.
    """
    filled: dict[str, RunRef] = {}
    for alias, run in runs.items():
        if fill_transform is not None:
            run_id = fill_transform.fill_text(run.run_id)
        elif PLACEHOLDER_RE.search(run.run_id):
            msg = (
                f"Run alias {alias!r} uses a placeholder in its run-id "
                f"({run.run_id!r}) but no values are available to fill it"
            )
            raise CstarExpectationFailed(msg)
        else:
            run_id = run.run_id
        filled[alias] = RunRef.model_validate({**run.model_dump(), "run_id": run_id})
    return filled


def external_dependencies(workplan: Workplan) -> list[StepRef]:
    """List the steps of external runs that a workplan depends on.

    The workplan model guarantees that every external reference is also a
    declared dependency, so this is the full set of external steps to refresh
    and gate.

    Parameters
    ----------
    workplan : Workplan
        The workplan to inspect.

    Returns
    -------
    list[StepRef]
        The distinct external references in `depends_on`, in first-seen order.
    """
    refs = (
        StepRef.parse(entry) for step in workplan.steps for entry in step.depends_on
    )
    return list(dict.fromkeys(ref for ref in refs if ref.is_external))


async def resolve_external_runs(
    workplan: Workplan,
    fill_transform: TemplateFillTransform | None,
    launcher: "Callable[[], Launcher[t.Any]]",
) -> "ExternalRuns":
    """Build the registry of a workplan's external runs and refresh their steps.

    Parameters
    ----------
    workplan : Workplan
        The workplan declaring `runs`.
    fill_transform : TemplateFillTransform | None
        The transform used to fill placeholders in run-ids.
    launcher : Callable[[], Launcher[t.Any]]
        Supplies the launcher used to query status; called only when the
        workplan depends on a step of an external run, so a workplan without
        external dependencies never builds a launcher.

    Returns
    -------
    ExternalRuns
    """
    external = ExternalRuns(fill_runs(workplan.runs, fill_transform))
    if refs := external_dependencies(workplan):
        await external.refresh(refs, launcher())
    return external


_THandle = t.TypeVar("_THandle", bound=ProcessHandle)


class _Probe(t.NamedTuple):
    """The state of an external step observed by `ExternalRuns.refresh`."""

    step: LiveStep
    """The step as defined by the transformed workplan of the external run."""
    handle: ProcessHandle
    """The handle persisted by the run that submitted the step."""
    status: Status
    """The status reported by the launcher."""
    launcher_name: str
    """The name of the launcher that reported the status."""
    supports_foreign_dependencies: bool
    """Whether that launcher can wait on an in-progress handle of another run."""


class ExternalRuns:
    """The registry of other workplan runs a workplan refers to.

    Every lookup of an external run, step or handle goes through here. Records
    and workplans are read lazily and cached per alias; nothing is ever written
    (a sentinel written here would land in the current run's state directory).
    """

    def __init__(self, runs: Mapping[str, RunRef]) -> None:
        """Initialize the registry.

        Parameters
        ----------
        runs : Mapping[str, RunRef]
            The external runs keyed by alias, with run-ids already filled.
        """
        self._runs = dict(runs)
        self._records: dict[str, WorkplanRun] = {}
        self._workplans: dict[str, LiveWorkplan] = {}
        self._probes: dict[StepRef, _Probe] = {}
        self._errors: dict[StepRef, str] = {}

    def _declared(self, alias: str) -> RunRef:
        """Return the run declared as `alias`."""
        if alias not in self._runs:
            msg = (
                f"Run alias {alias!r} is not declared under `runs` "
                f"(declared: {sorted(self._runs)})"
            )
            raise CstarError(msg)
        return self._runs[alias]

    def _describe(self, ref: StepRef) -> str:
        """Name a step, its alias and its run-id for use in messages."""
        run_id = self._runs[ref.run].run_id if ref.run in self._runs else "?"
        return f"step {ref.step!r} of run {run_id!r} (alias {ref.run!r})"

    def record(self, alias: str) -> WorkplanRun:
        """Return the tracking record of the run declared as `alias`.

        Parameters
        ----------
        alias : str
            The run alias.

        Returns
        -------
        WorkplanRun
            The record matching the run's `start_at` when pinned, otherwise
            the latest record of the run-id.

        Raises
        ------
        CstarError
            If the alias is undeclared or no record exists for the run.
        """
        if alias not in self._records:
            run = self._declared(alias)
            try:
                found = TrackingRepository().get_workplan_run_sync(
                    run.run_id, run_date=run.start_at
                )
            except ValueError as ex:
                msg = f"Run alias {alias!r} has an invalid run-id {run.run_id!r}: {ex}"
                raise CstarError(msg) from ex
            if found is None:
                msg = f"No run record found for alias {alias!r} (run-id {run.run_id!r})"
                raise CstarError(msg)
            self._records[alias] = found
        return self._records[alias]

    def workplan(self, alias: str) -> LiveWorkplan:
        """Return the transformed workplan recorded for the run declared as `alias`.

        Raises
        ------
        CstarError
            If the run has no record or its workplan cannot be loaded.
        """
        if alias not in self._workplans:
            record = self.record(alias)
            try:
                self._workplans[alias] = deserialize(
                    record.trx_workplan_path, LiveWorkplan
                )
            except (FileNotFoundError, ValueError, yaml.YAMLError) as ex:
                msg = (
                    f"Unable to load workplan for run-id {record.run_id!r} from "
                    f"{str(record.trx_workplan_path)!r}: {ex}"
                )
                raise CstarError(msg) from ex
        return self._workplans[alias]

    def step(self, ref: StepRef) -> LiveStep:
        """Return a step of an external run.

        Raises
        ------
        CstarError
            If the run cannot be resolved or has no step of that name.
        """
        workplan = self.workplan(ref.run)
        if ref.step not in workplan:
            msg = (
                f"Step {ref.step!r} not found in run {self._declared(ref.run).run_id!r} "
                f"(alias {ref.run!r})"
            )
            raise CstarError(msg)
        return workplan[ref.step]

    @t.overload
    def handle(self, ref: StepRef) -> ProcessHandle: ...

    @t.overload
    def handle(self, ref: StepRef, klass: type[_THandle]) -> _THandle: ...

    def handle(
        self,
        ref: StepRef,
        klass: type[ProcessHandle] = ProcessHandle,
    ) -> ProcessHandle:
        """Return the handle persisted by the run that submitted a step.

        Parameters
        ----------
        ref : StepRef
            The external step.
        klass : type[ProcessHandle]
            The handle type to load; a launcher's `handle_klass()` when the
            handle is to be passed to that launcher.

        Raises
        ------
        CstarError
            If the run cannot be resolved or the step was never submitted.
        """
        record = self.record(ref.run)
        path = StateRepository.sentinel_path(ref.step, run_id=record.run_id)
        try:
            return deserialize(path, klass)
        except FileNotFoundError as ex:
            msg = (
                f"Step {ref.step!r} of run {record.run_id!r} (alias {ref.run!r}) "
                f"has not been submitted (no sentinel at {str(path)!r})"
            )
            raise CstarError(msg) from ex

    async def refresh(
        self,
        refs: Iterable[StepRef],
        launcher: "Launcher[t.Any]",
    ) -> None:
        """Query the current status of external steps.

        A step that cannot be resolved (missing or unreadable record, workplan
        or sentinel, or a failed status query) is recorded as a problem rather
        than raised, so `problem` can report every reference.

        Parameters
        ----------
        refs : Iterable[StepRef]
            The external steps to query.
        launcher : Launcher[t.Any]
            The launcher used to query status.
        """
        for ref in refs:
            self._probes.pop(ref, None)
            self._errors.pop(ref, None)
            try:
                step = self.step(ref)
                handle = self.handle(ref, launcher.handle_klass())
                status = await launcher.query_status(handle)
            except (CstarError, ValueError, OSError, yaml.YAMLError) as ex:
                # a corrupt sentinel or workplan, or a failed query, is a
                # problem with this reference rather than a crash
                log.debug(f"Unable to refresh external step {ref}: {ex}")
                self._errors[ref] = str(ex)
                continue

            self._probes[ref] = _Probe(
                step,
                handle,
                status,
                launcher.name,
                launcher.supports_foreign_dependencies,
            )

        statuses = ", ".join(
            f"{ref}={p.status.name}" for ref, p in self._probes.items()
        )
        log.debug(f"Refreshed external step status: {statuses or 'none'}")

    def tasks(self) -> dict[str, Task[ProcessHandle]]:
        """Return the refreshed external steps as tasks keyed by their token.

        The handles are copies carrying the refreshed status; the persisted
        sentinels are never touched.

        Returns
        -------
        dict[str, Task[ProcessHandle]]
            A task per refreshed step, keyed by `str(ref)`.
        """
        return {
            str(ref): Task(
                step=probe.step,
                handle=probe.handle.model_copy(update={"status": probe.status}),
            )
            for ref, probe in self._probes.items()
        }

    def statuses(self) -> dict[str, Status]:
        """Return the status found by the last refresh, keyed by step token."""
        return {str(ref): probe.status for ref, probe in self._probes.items()}

    def errors(self) -> dict[str, str]:
        """Return why each step whose last refresh failed could not be resolved.

        Returns
        -------
        dict[str, str]
            The message of each failure, keyed by `str(ref)`.
        """
        return {str(ref): message for ref, message in self._errors.items()}

    def is_done(self, ref: StepRef) -> bool:
        """Return `True` when the last refresh found the step `Done`."""
        probe = self._probes.get(ref)
        return probe is not None and probe.status == Status.Done

    def problem(self, ref: StepRef) -> str:
        """Describe why a step cannot be depended upon.

        A step is usable when it is done, or when it is in progress under a
        launcher that can wait on a handle created by another run.

        Parameters
        ----------
        ref : StepRef
            The external step.

        Returns
        -------
        str
            The problem, or an empty string when the step is usable.
        """
        if ref.run not in self._runs:
            msg = (
                f"step {ref.step!r} uses run alias {ref.run!r}, which is not "
                f"declared under `runs` (declared: {sorted(self._runs)})"
            )
            return msg

        what = self._describe(ref)
        if error := self._errors.get(ref):
            return error

        if (probe := self._probes.get(ref)) is None:
            return f"{what}: status unknown (not refreshed)"

        if probe.status == Status.Done:
            return ""

        if (
            Status.is_in_progress(probe.status)
            and probe.supports_foreign_dependencies
            and not is_foreign_handle(probe.handle, probe.launcher_name)
        ):
            return ""

        msg = (
            f"{what} is {probe.status.name} (handle launcher "
            f"{probe.handle.launcher_name or 'unknown'!r}; this system uses "
            f"launcher {probe.launcher_name!r}) and cannot be depended upon"
        )
        if not Status.is_terminal(probe.status):
            run_id = self._runs[ref.run].run_id
            msg += f"; run `cstar workplan status {run_id}` to refresh its status"
        return msg

    def pinned(self) -> dict[str, RunRef]:
        """Return the runs with `start_at` set to the resolved record's start.

        Only the aliases that `refresh` resolved are pinned. An alias that no
        step refers to (or whose last refresh failed) is passed through as
        authored, so an unused or stale `runs` entry cannot abort scheduling;
        a failed reference is reported by `problem` instead.

        Returns
        -------
        dict[str, RunRef]
        """
        resolved = {ref.run for ref in self._probes}
        return {
            alias: (
                run.model_copy(update={"start_at": self.record(alias).start_at})
                if alias in resolved
                else run
            )
            for alias, run in self._runs.items()
        }


def lookup_step(workplan: LiveWorkplan, token: str) -> LiveStep:
    """Find a step of the workplan, or of one of its external runs.

    Parameters
    ----------
    workplan : LiveWorkplan
        The workplan to search; its `runs` resolve external references.
    token : str
        A step name, or `<step>@<alias>` for a step of an external run.

    Returns
    -------
    LiveStep

    Raises
    ------
    KeyError
        If the token names a step of `workplan` that does not exist.
    CstarError
        If the token is malformed or an external step cannot be resolved.
    """
    ref = _parse_ref(token)
    if ref.is_external:
        return ExternalRuns(workplan.runs).step(ref)

    if ref.step not in workplan:
        msg = f"Unable to locate step {ref.step!r} in workplan"
        raise KeyError(msg)
    return workplan[ref.step]


class WorkplanTransformer(LoggingMixin):
    """Transform a workplan by applying transforms to its steps."""

    original: Workplan
    """The original, pre-transformation workplan."""

    _transformed: Workplan | None = None
    """The post-transformation workplan."""

    DERIVED_PATH_SUFFIX: t.Literal["_trx"] = "_trx"
    """Suffix appended to the original workplan path when generating a derived path."""

    def __init__(
        self,
        wp: Workplan,
        fill_transform: TemplateFillTransform | None = None,
        external: ExternalRuns | None = None,
    ) -> None:
        """Initialize the instance.

        Parameters
        ----------
        wp : Workplan
            The workplan to transform.
        fill_transform : TemplateFillTransform | None
            The transform used to fill template placeholders.
        external : ExternalRuns | None
            The registry of external runs the workplan refers to; an empty
            registry when omitted, so every external reference is a problem.
        """
        self.original = Workplan(**wp.model_dump(by_alias=True))
        self.fill_transform = fill_transform
        self.external = external if external is not None else ExternalRuns({})

    @property
    def is_modified(self) -> bool:
        """Return `True` if the transformed workplan differs from the original.

        Returns
        -------
        bool
        """
        lhs = self.original.model_dump()
        rhs = self.transformed.model_dump() if self.transformed else {}

        return lhs != rhs

    @property
    def transformed(self) -> Workplan:
        """Return the transformed workplan.

        Returns
        -------
        Workplan
        """
        if self._transformed is None:
            self._transformed = self.apply()
        return self._transformed

    @staticmethod
    def derived_path(
        source: Path,
        target_dir: Path | None = None,
        suffix: str = DERIVED_PATH_SUFFIX,
        extension: str | None = None,
    ) -> Path:
        """Generate a new path name derived from the source path.

        If no target directory is specified, the derived path will be in the
        same directory as the source path.

        Parameters
        ----------
        source : Path
            The source path.
        target_dir : Path | None, optional
            An alternate parent directory to place the file
        suffix : str, optional
            A suffix to append to the source file name, by default "_trx"

        Returns
        -------
        Path

        Raises
        ------
        ValueError
            If the source and target paths are identical
        """
        if not target_dir and not suffix:
            msg = f"Identical source and target will destroy the source: `{source}`"
            raise ValueError(msg)

        directory = target_dir or source.parent
        filename = Path(source.name).with_stem(f"{source.stem}{suffix}")
        if extension:
            filename = filename.with_suffix(extension)

        return directory / filename

    def apply(self) -> Workplan:
        """Create a new workplan with appropriate transforms applied.

        Returns
        -------
        Workplan
        """
        if self._transformed:
            return self._transformed

        # ensure consistent output targets for all steps in the workplan
        live_steps = [LiveStep.from_step(s) for s in self.original.steps]

        # fill template placeholders before any other transform operates on overrides
        if self.fill_transform is not None:
            resolver = get_fsm_resolver(live_steps, self.external)
            fill = self.fill_transform.with_scoped_resolver(resolver)

            live_steps = [filled for step in live_steps for filled in fill(step)]

        transformed_steps: list[LiveStep] = []
        named_dep_map: dict[str, str] = {}

        app_names = {step.application for step in live_steps}
        app_transforms: dict[str, Sequence[type[Transform[LiveStep]]]] = {
            app_name: get_application(app_name).applicable_transforms
            for app_name in app_names
        }

        problems = collect_directive_problems(live_steps)
        problems.extend(
            problem
            for ref in external_dependencies(self.original)
            if (problem := self.external.problem(ref))
        )
        if problems:
            summary = f"{len(problems)} directive problem(s) found in workplan:"
            msg = "\n".join([summary, *(f"- {problem}" for problem in problems)])
            raise ValueError(msg)

        override_transform = OverrideTransform()
        env = resolve_compute_environment(self.original.compute_environment)

        for step in live_steps:
            active_transforms = [
                trx for trx in app_transforms[step.application] if trx.is_active()
            ]

            if (step.is_deferred or step.is_inline) and active_transforms:
                active_names = [trx.__name__ for trx in active_transforms]
                msg = (
                    f"Application transform(s) {', '.join(active_names)} cannot be "
                    f"applied to step {step.name!r} because its blueprint is "
                    "deferred or inline and does not exist as a file at "
                    "schedule time"
                )
                raise CstarExpectationFailed(msg)

            if active_transforms:
                # Schedule-time application transforms operate on a
                # materialized, merged blueprint written to disk (a
                # feature-flagged path); system overrides are baked in first.
                # Do not restructure this pipeline.
                step = apply_automatic_overrides(step)
                for trx_klass in active_transforms:
                    trx = trx_klass()
                    transform_result = trx(step)

                    # apply overrides generated by the transformation
                    overridden_steps = list(
                        itertools.chain.from_iterable(
                            map(override_transform, transform_result),
                        ),
                    )
                    final_step_name = overridden_steps[-1].name
                    if final_step_name != step.name:
                        named_dep_map[step.name] = final_step_name
                    # children have materialized blueprints (overrides already
                    # baked in); record their cpu requirement directly
                    transformed_steps.extend(
                        _inject_compute_defaults(
                            _inject_cpus(
                                child,
                                child.blueprint.cpus_needed,
                                single_node=child.blueprint.single_node,
                            ),
                            env,
                        )
                        for child in overridden_steps
                    )
            else:
                # Common path: no blueprint file is rewritten; the step
                # keeps its original blueprint path and carries an
                # apply-overrides directive to be resolved at runtime.
                transformed_steps.append(
                    _inject_compute_defaults(
                        package_runtime_overrides(preflight_overrides(step)), env
                    )
                )

        # remap dependency references to point to the last child of each split parent
        for trx_step in transformed_steps:
            depends_on = {str(d) for d in trx_step.depends_on}
            if to_update := depends_on.intersection(named_dep_map):
                depends_on.update(named_dep_map[x] for x in to_update)
                depends_on.difference_update(to_update)
                trx_step.depends_on.clear()
                trx_step.depends_on.extend(depends_on)

        # deferred blueprint references must point at steps that still exist;
        # a split producer writes its outputs into child-step directories
        for trx_step in transformed_steps:
            if trx_step.is_deferred:
                ref = t.cast("DeferredBlueprintRef", trx_step.blueprint_path)
                if ref.from_step in named_dep_map:
                    msg = (
                        f"Step {trx_step.name!r} defers its blueprint to step "
                        f"{ref.from_step!r}, which was split "
                        "into sub-steps by an application transform; step "
                        "references are not yet remapped across split steps"
                    )
                    raise CstarExpectationFailed(msg)

        update: dict[str, t.Any] = {
            "steps": transformed_steps,
            "name": f"{self.original.name} (transformed)",
        }
        if self.original.runs:
            # pin each external run to the record resolved now
            update["runs"] = self.external.pinned()

        self._transformed = self.original.model_copy(update=update)

        return self._transformed


class OverrideTransform(Transform[LiveStep]):
    """Transform that overrides a step by returning a blueprint with all overridden attributes applied."""

    _system_overrides: dict[str, t.Any]
    _replace_lists: bool

    def __init__(
        self,
        sys_overrides: dict[str, t.Any] | None = None,
        *,
        replace_lists: bool = False,
    ) -> None:
        """Initialize the instance.

        Parameters
        ----------
        sys_overrides : dict[str, t.Any] | None
            System-level blueprint overrides that will be applied after
            the user-supplied values.
        replace_lists : bool
            If True, a list in `sys_overrides` replaces the blueprint's list
            outright instead of merging element-wise.
        """
        self._system_overrides = sys_overrides or {}
        self._replace_lists = replace_lists

    def apply(
        self,
        bp: Blueprint,
        overrides: dict[str, t.Any] | None = None,
    ) -> Blueprint:
        """Apply all overrides from a blueprint.

        Generate a new blueprint with overrides applied and an empty set of overrides.
        Store the newly generated blueprint in the output directory.

        Parameters
        ----------
        bp : Blueprint
            The blueprint to apply overrides to
        overrides : dict[str, t.Any] | None
            A dictionary containing overrides for attributes of a blueprint.

        Returns
        -------
        Blueprint
            The blueprint with all overrides applied.
        """
        overrides = overrides.copy() if overrides else {}

        model = bp.model_dump(
            exclude={"$schema"},
        )

        # system-level overrides take precedence over step-level overrides
        merged = deep_merge(model, overrides)
        merged = deep_merge(
            merged, self._system_overrides, replace_lists=self._replace_lists
        )

        overridden = {**overrides, **self._system_overrides}
        description = (
            f"{bp.description}; overridden keys [{', '.join(overridden.keys())}]"
        )
        merged.update(description=description)
        bp_type = type(bp)
        return bp_type(**merged)

    def __call__(self, step: LiveStep) -> Sequence[LiveStep]:
        """Apply the transform to a step.

        Parameters
        ----------
        step : Step
            The step to be transformed

        Returns
        -------
        Sequence[Step]
            Zero-to-many steps resulting from applying the transform.
        """
        bp_path = Path(step.blueprint_path)

        app: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
            get_application(step.application)
        )
        bp_type = app.blueprint

        blueprint: Blueprint = deserialize(bp_path, bp_type)

        updated_bp = self.apply(blueprint, step.blueprint_overrides)
        update: dict[str, t.Any] = {
            "blueprint_overrides": {},
            "working_dir": updated_bp.effective_working_dir,
        }

        live_step = LiveStep.from_step(step, update=update)

        bp_renamed = bp_path.with_stem(f"{bp_path.stem}.{self.suffix()}").name
        bp_path = live_step.fsm.run_dir / bp_renamed
        serialize(bp_path, updated_bp)

        live_step.blueprint_path = bp_path
        return (live_step,)

    @staticmethod
    def suffix() -> str:
        """Return a suffix used when persisting a resource modified by this transform.

        Returns
        -------
        str
        """
        return "ovrd"


def get_system_overrides(step: LiveStep) -> dict[str, t.Any]:
    """Create a step-specific mapping of system-level overrides
    that will be applied to orchestrated steps.

    Returns
    -------
    LiveStep
        The transformed step.
    """
    return {"working_dir": step.fsm.root_dir}


def apply_automatic_overrides(step: LiveStep) -> LiveStep:
    """Materialize system overrides for the schedule-time transform pipeline.

    Automatically overrides the output directory specified in a blueprint
    to write to the C-Star home directories, baking the result into a
    rewritten blueprint file. Only steps entering the materialized
    application-transform pipeline need this; every other step persists its
    overrides via `package_runtime_overrides` instead.

    See `cstar.execution.file_system.JobFileSystemManager` for more detail
    on the available set of home directories.

    Returns
    -------
    LiveStep
        The transformed step.
    """
    sys_overrides = get_system_overrides(step)
    if not sys_overrides:
        return step

    override_transform = OverrideTransform(sys_overrides=sys_overrides)
    overridden_step_result = override_transform(step)

    return overridden_step_result[0]


def preflight_overrides(step: LiveStep) -> LiveStep:
    """Validate a step's overrides and enrich its CPU requirement.

    For a step whose blueprint already exists at schedule time, builds the
    merged blueprint in memory (without persisting it): constructing the
    merged blueprint validates it, preserving schedule-time fail-fast
    behavior for bad overrides. A deferred step's blueprint is not available
    at schedule time, so it is returned unchanged; its overrides are
    validated only when the runtime `apply-overrides` directive resolves and
    applies them.

    Because a deferred (or otherwise unavailable) blueprint cannot be
    inspected at submit time, the scheduler reads the persisted
    `compute_overrides` instead of the blueprint to determine CPU
    requirements. When the step's `compute_overrides` does not already
    declare `["slurm"]["num_cpus"]`, this injects it from the merged
    blueprint's `cpus_needed`, so the transformed workplan records the
    requirement regardless of whether the blueprint can be read later.

    Parameters
    ----------
    step : LiveStep
        The step to preflight.

    Returns
    -------
    LiveStep
        The step unchanged (deferred, or CPU count already declared), or
        with `compute_overrides` enriched with the CPU requirement.
    """
    if step.is_deferred:
        return step

    merged = OverrideTransform(sys_overrides=get_system_overrides(step)).apply(
        step.blueprint, dict(step.blueprint_overrides)
    )

    return _inject_cpus(step, merged.cpus_needed, single_node=merged.single_node)


def _inject_cpus(step: LiveStep, cpus: int, single_node: bool = False) -> LiveStep:
    """Record a step's cpu requirement in its `compute_overrides`.

    Declared values win; the requirement is injected only when
    `["slurm"]["num_cpus"]` is absent. When the blueprint declares itself
    `single_node`, that is recorded alongside as `["slurm"]["single_node"]`
    so the launcher can clamp the cpu count to one node's capacity without
    reading the blueprint.

    Parameters
    ----------
    step : LiveStep
        The step to enrich.
    cpus : int
        The cpu requirement read from the step's blueprint.
    single_node : bool, optional
        Whether the blueprint confines its work to one node. Only recorded
        when `True`; a `False` value leaves the overrides untouched so that
        an explicitly declared `single_node` is never overwritten.

    Returns
    -------
    LiveStep
        The step with `compute_overrides` recording the cpu requirement.

    Raises
    ------
    CstarExpectationFailed
        If the step declares a non-mapping `slurm` compute override.
    """
    declared = step.compute_overrides.get("slurm", {})
    if not isinstance(declared, Mapping):
        msg = (
            f"Step {step.name!r} declares a non-mapping `slurm` compute "
            f"override: {declared!r}"
        )
        raise CstarExpectationFailed(msg)

    injected: dict[str, t.Any] = {"num_cpus": cpus}
    if single_node:
        injected["single_node"] = True

    new_overrides = deep_merge(
        {"slurm": injected},
        dict(step.compute_overrides),
    )
    return LiveStep.from_step(step, update={"compute_overrides": new_overrides})


def _inject_compute_defaults(step: LiveStep, env: ComputeEnvironment) -> LiveStep:
    """Record the workplan-wide SLURM defaults in a step's `compute_overrides`.

    Declared step values win; the workplan's defaults fill in the rest.

    Parameters
    ----------
    step : LiveStep
        The step to enrich.
    env : ComputeEnvironment
        The workplan's compute environment.

    Returns
    -------
    LiveStep
        The step with `compute_overrides` carrying the workplan's SLURM
        defaults, or the step itself when the workplan declares none.
    """
    if env.slurm is None:
        return step

    defaults = env.slurm.model_dump(exclude_defaults=True)
    declared = step.compute_overrides.get("slurm", {})
    if isinstance(declared, Mapping) and declared.get("single_node"):
        # a single-node step cannot take a workplan-wide node count
        defaults.pop("num_nodes", None)

    new_overrides = deep_merge({"slurm": defaults}, dict(step.compute_overrides))
    try:
        SlurmComputeSpec.model_validate(new_overrides["slurm"])
    except ValidationError as ex:
        msg = (
            f"Step {step.name!r}: the workplan's compute_environment.slurm defaults "
            f"conflict with the step's compute overrides: {ex.errors()[0]['msg']}"
        )
        raise ValueError(msg) from ex
    return LiveStep.from_step(step, update={"compute_overrides": new_overrides})


def effective_blueprint(step: LiveStep) -> Blueprint:
    """Load a step's blueprint with its packaged runtime overrides applied.

    A transformed step carries its overrides in an `apply-overrides`
    directive rather than a rewritten blueprint file, so code inspecting
    the step's configuration (e.g. another step reading its outputs layout)
    must merge that pending payload to see what will actually run.

    Parameters
    ----------
    step : LiveStep
        The step whose blueprint content is requested.

    Returns
    -------
    Blueprint
        The blueprint with any packaged runtime overrides merged in memory.

    Raises
    ------
    BlueprintDeferredError
        If the step's blueprint is deferred and does not exist yet.
    CstarExpectationFailed
        If the step's blueprint is inline but no overrides were packaged
        (the step was not transformed first), or they are incomplete.
    """
    config = step.directives.get(ApplyOverridesDirective.key(), {})
    overrides = (
        config.get(ApplyOverridesDirective.KEY_OVERRIDES)
        if isinstance(config, Mapping)
        else None
    )

    # an inline step's content lives only in the packaged payload once
    # transformed; its blueprint_overrides are empty by then
    if step.is_inline:
        if not isinstance(overrides, Mapping):
            msg = (
                f"Step {step.name!r} declares an inline blueprint but has no "
                "packaged overrides; the step was not transformed first"
            )
            raise CstarExpectationFailed(msg)
        return synthesize_blueprint(step, overrides)

    blueprint = step.blueprint
    if isinstance(overrides, Mapping):
        blueprint = OverrideTransform().apply(blueprint, dict(overrides))

    return blueprint


def package_runtime_overrides(step: LiveStep) -> LiveStep:
    """Package schedule-time overrides into a runtime `apply-overrides` directive.

    This is the override-persistence path for every step outside the
    feature-flagged schedule-time application-transform pipeline, deferred or
    not: rather than baking overrides into a rewritten blueprint file at
    schedule time, the user-supplied `blueprint_overrides` and the system
    overrides are merged and stored on the step's directives, to be applied
    by an `apply-overrides` directive on the compute node instead.

    Directive configuration is not validated here; `collect_directive_problems`
    validates every step's directives, once, before any step is packaged (see
    `WorkplanTransformer.apply`).

    Returns
    -------
    LiveStep
        The transformed step.
    """
    sys_overrides = {
        key: value.as_posix() if isinstance(value, Path) else value
        for key, value in get_system_overrides(step).items()
    }
    overrides = deep_merge(dict(step.blueprint_overrides), sys_overrides)

    # apply-overrides must precede every other directive: directives run in
    # mapping order, and until the working_dir override is applied, a content
    # directive (e.g. continue-from) would persist its intermediate blueprint
    # into the raw blueprint's working_dir, which may not be writable.
    directives = {
        ApplyOverridesDirective.key(): {
            ApplyOverridesDirective.KEY_OVERRIDES: overrides,
            ApplyOverridesDirective.KEY_APPLICATION: step.application,
        },
        **{
            key: value
            for key, value in step.directives.items()
            if key != ApplyOverridesDirective.key()
        },
    }
    update: dict[str, t.Any] = {"blueprint_overrides": {}, "directives": directives}
    return LiveStep.from_step(step, update=update)


def materialize_inline_blueprints(steps: Sequence[LiveStep]) -> list[LiveStep]:
    """Write each inline step's blueprint to its work directory.

    Runs from `prepare_workplan` rather than `WorkplanTransformer.apply`: a
    schedule-time check must write nothing to disk, and a reloaded or resumed
    run reads the transformed workplan, so the file path must be recorded
    there. Launchers and `cstar blueprint run` then see an ordinary,
    complete blueprint file.

    The steps must already be transformed: `effective_blueprint` builds the
    content from the packaged `apply-overrides` directive, which already
    carries every override and the `working_dir`. That directive is dropped
    once baked into the file so the overrides are not applied a second time.

    Parameters
    ----------
    steps : Sequence[LiveStep]
        The transformed steps of the workplan.

    Returns
    -------
    list[LiveStep]
        The steps, with each inline step replaced by one that references its
        materialized blueprint file.

    Raises
    ------
    CstarExpectationFailed
        If an inline step has no packaged overrides, or its overrides do
        not form a complete blueprint.
    """
    materialized: list[LiveStep] = []

    for step in steps:
        if not step.is_inline:
            materialized.append(step)
            continue

        path = step.fsm.run_dir / INLINE_BLUEPRINT_FILENAME
        serialize(path, effective_blueprint(step))

        directives = {
            key: value
            for key, value in step.directives.items()
            if key != ApplyOverridesDirective.key()
        }
        update = {"blueprint": path, "directives": directives}
        materialized.append(LiveStep.from_step(step, update=update))

    return materialized


def _canonical(token: str) -> str:
    """Return a step reference token in canonical form; a malformed one unchanged."""
    try:
        return str(StepRef.parse(token))
    except ValueError:
        return token


def _ancestor_map(steps: Sequence[LiveStep]) -> dict[str, set[str]]:
    """Map each step's name to the set of its transitive `depends_on` ancestors.

    Parameters
    ----------
    steps : Sequence[LiveStep]
        The steps to map.

    Returns
    -------
    dict[str, set[str]]
        Step name to the set of every step name reachable by following
        `depends_on` edges, direct or indirect. External `<step>@<alias>`
        tokens are leaves: they appear in the sets, in canonical form, but have
        no entries of their own. Terminates on a dependency cycle via a
        visited set rather than looping forever.
    """
    depends_on = {step.name: [_canonical(d) for d in step.depends_on] for step in steps}
    ancestors: dict[str, set[str]] = {}
    for name in depends_on:
        seen: set[str] = set()
        stack = list(depends_on.get(name, ()))
        while stack:
            candidate = stack.pop()
            if candidate in seen:
                continue
            seen.add(candidate)
            stack.extend(depends_on.get(candidate, ()))
        ancestors[name] = seen
    return ancestors


def allowed_directives(application: str) -> dict[str, type["Directive"]]:
    """Return the directives a step of `application` may declare, by key.

    `apply-overrides` is available to every application; the rest come from
    `ApplicationDefinition.directives`.
    """
    return {
        ApplyOverridesDirective.key(): ApplyOverridesDirective,
        **{d.key(): d for d in get_application(application).directives},
    }


def collect_directive_problems(steps: Sequence[LiveStep]) -> list[str]:
    """Validate every step's directives against its application and the DAG.

    Called once by `WorkplanTransformer.apply`, before any step is
    packaged, so every knowable directive misconfiguration is reported
    together at schedule time instead of one at a time on the compute node.
    For each step, a directive key must belong to `apply-overrides` or to
    the step's application (`ApplicationDefinition.directives`); its config
    must be a mapping; the directive's own `validate_directives` is
    consulted for config-shape and sibling-directive problems; and every
    step name returned by the directive's `referenced_steps` must name
    another step in `steps` that is a transitive `depends_on` ancestor of
    the referencing step. A reference to a step of an external run
    (`<step>@<alias>`) must likewise be an ancestor; whether that step is
    usable is gated once per dependency by `WorkplanTransformer.apply`.

    Parameters
    ----------
    steps : Sequence[LiveStep]
        The workplan's steps, in schedule order.

    Returns
    -------
    list[str]
        One message per problem found, each prefixed with the step and
        directive it concerns; empty when every step's directives are valid.
    """
    ancestors = _ancestor_map(steps)
    step_names = {step.name for step in steps}
    problems: list[str] = []

    for step in steps:
        allowed = allowed_directives(step.application)

        for key, config in step.directives.items():
            directive_cls = allowed.get(key)
            if directive_cls is None:
                allowed_keys = ", ".join(sorted(allowed))
                problems.append(
                    f"step {step.name!r} directive {key!r}: not a directive of "
                    f"application {step.application!r}; allowed: {allowed_keys}"
                )
                continue

            if not isinstance(config, Mapping):
                problems.append(
                    f"step {step.name!r} directive {key!r}: configuration must "
                    f"be a mapping, got {type(config).__name__}"
                )
                continue

            problems.extend(
                f"step {step.name!r} directive {key!r}: {problem}"
                for problem in directive_cls.validate_directives(config, step)
            )

            for token in directive_cls.referenced_steps(config):
                try:
                    ref = StepRef.parse(token)
                except ValueError as ex:
                    problems.append(f"step {step.name!r} directive {key!r}: {ex}")
                    continue

                if not ref.is_external and ref.step not in step_names:
                    problems.append(
                        f"step {step.name!r} directive {key!r}: references "
                        f"unknown step {token!r}"
                    )
                elif str(ref) not in ancestors[step.name]:
                    problems.append(
                        f"step {step.name!r} directive {key!r}: step {token!r} "
                        f"is not an upstream dependency of step {step.name!r} "
                        "(via depends_on)"
                    )

    return problems


class Directive(Transform[LiveStep], t.Protocol):
    _config: Mapping[str, t.Any]
    """Contract of a transform that can be used as a directive."""
    _workplan: LiveWorkplan | None = None
    """The workplan instance containing contextual information for the directive."""

    def __init__(
        self,
        config: dict[str, t.Any],
        *,
        workplan: LiveWorkplan | None = None,
    ) -> None:
        """Initialize the instance.

        Parameters
        ----------
        config : dict[str, t.Any] | None
            A dictionary containing configuration for the directive.
        """
        if not config:
            msg = "Configuration must be provided"
            raise ValueError(msg)

        self._config = config
        self._workplan = workplan

    @classmethod
    def key(cls) -> str:
        """Return a string that will be used to identify the appropriate configuration
        for the directive in any Mapping.

        Returns
        -------
        str
        """
        ...

    @classmethod
    def validate_directives(
        cls, config: Mapping[str, t.Any], step: LiveStep
    ) -> Sequence[str]:
        """Validate this directive's config at schedule time.

        Called by `collect_directive_problems` before a workplan is
        submitted, so a directive can reject a malformed configuration, or
        one that conflicts with another directive on the same step (e.g. two
        directives that would both set `initial_conditions`, found via
        `step.directives`), at `cstar workplan` time rather than on the
        compute node. Default: no problems.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.
        step : LiveStep
            The step the directive is configured on; `step.directives`
            carries the step's full directives mapping (this directive's
            key included) for checking conflicts with sibling directives.

        Returns
        -------
        Sequence[str]
            Problem messages describing why `config` is invalid; empty when
            `config` is fine.
        """
        return ()

    @classmethod
    def referenced_steps(cls, config: Mapping[str, t.Any]) -> Sequence[str]:
        """Return the workplan step names this directive's config refers to.

        Lets schedule-time validation check a directive's step references
        against the workplan DAG without knowing each directive's config
        layout. Default: no step references.

        Parameters
        ----------
        config : Mapping[str, t.Any]
            This directive's own configuration mapping.

        Returns
        -------
        Sequence[str]
        """
        return ()

    @property
    def workplan(self) -> LiveWorkplan:
        """Return the live workplan.

        Returns
        -------
        LiveWorkplan

        Raises
        ------
        CStarError
            If the workplan was not injected into the directives by the runner.
        """
        if self._workplan:
            return self._workplan

        raise CstarError("Directive did not receive workplan")


class OverrideDirective(Directive, OverrideTransform):
    """Base class for directives that generate blueprint overrides from
    their configuration and apply them as an `OverrideTransform`.
    """

    _overrides: dict[str, t.Any]

    REPLACE_LISTS: t.ClassVar[bool] = False
    """Whether lists in the generated overrides replace the blueprint's lists
    outright. Directives that locate a complete set of files (e.g. boundary
    or restart files) set this so no stale entries survive; the default keeps
    the element-wise merge that user overrides rely on."""

    def __init__(
        self,
        config: dict[str, t.Any],
        *,
        workplan: LiveWorkplan | None = None,
    ) -> None:
        """Initialize the instance.

        Parameters
        ----------
        config : dict[str, t.Any] | None
            A dictionary containing configuration for the directive.
        workplan : LiveWorkplan | None
            The workplan instance containing contextual information for the directive.
        """
        Directive.__init__(self, config, workplan=workplan)
        OverrideTransform.__init__(
            self, self._generate_overrides(), replace_lists=self.REPLACE_LISTS
        )

    def _generate_overrides(self) -> dict[str, t.Any]:
        """Generate any system overrides required by the directive.

        Returns
        -------
        dict[str, t.Any]
        """
        return {}


class ApplyOverridesDirective(OverrideDirective):
    """A directive that applies schedule-time overrides to a blueprint that
    only exists at runtime.

    Steps with deferred blueprints cannot have their `blueprint_overrides` or
    system overrides baked into the blueprint when the workplan is scheduled;
    this directive applies them on the compute node instead.
    """

    KEY_OVERRIDES: t.Final[str] = "overrides"
    """Key containing the overrides to apply to the blueprint."""
    KEY_APPLICATION: t.Final[str] = "application"
    """Key containing the application declared by the step."""

    @classmethod
    def key(cls) -> str:
        return "apply-overrides"

    def _generate_overrides(self) -> dict[str, t.Any]:
        """Return the overrides packaged into the directive configuration.

        Returns
        -------
        dict[str, t.Any]

        Raises
        ------
        ValueError
            If the configuration does not contain an overrides mapping.
        """
        overrides = self._config.get(self.KEY_OVERRIDES)
        if not isinstance(overrides, Mapping):
            msg = (
                f"Directive {self.key()!r} requires a mapping under "
                f"key {self.KEY_OVERRIDES!r}"
            )
            raise ValueError(msg)
        return dict(overrides)

    def apply(
        self,
        bp: Blueprint,
        overrides: dict[str, t.Any] | None = None,
    ) -> Blueprint:
        """Apply overrides after verifying the blueprint matches the
        application declared by the step.

        Parameters
        ----------
        bp : Blueprint
            The blueprint to apply overrides to
        overrides : dict[str, t.Any] | None
            A dictionary containing overrides for attributes of a blueprint.

        Returns
        -------
        Blueprint
            The blueprint with all overrides applied.

        Raises
        ------
        CstarExpectationFailed
            If the blueprint's application differs from the one declared
            by the step.
        """
        expected = self._config.get(self.KEY_APPLICATION)
        if expected and bp.application != expected:
            msg = (
                f"Blueprint declares application {bp.application!r} but the "
                f"step declared {expected!r}"
            )
            raise CstarExpectationFailed(msg)
        return super().apply(bp, overrides)


class DirectiveConfig(BaseModel):
    directive_map: t.ClassVar[dict[str, type[Directive]]] = {}
    """Lookup for all registered directives."""

    directives: KeyValueStore
    """Generic configuration container for an instance of a directive."""

    @classmethod
    def apply_directives(
        cls,
        directive_uri: str,
        blueprint_uri: str,
    ) -> str:
        """Apply the specified directives to the blueprint and
        return the path to the final, transformed blueprint.

        Parameters
        ----------
        directive_uri : str
            The URI to configuration for directives the runner must execute.
        blueprint_uri : str
            The user-supplied blueprint URI specifying the blueprint to preprocess.

        Returns
        -------
        str
        """
        with (
            local_copy(directive_uri) as local_path,
            local_copy(blueprint_uri) as local_bp,
        ):
            model = deserialize(local_path, DirectiveConfig)

            directives = model.directives
            if not directives:
                return blueprint_uri

            directive_map = DirectiveConfig.directive_map
            workplan: LiveWorkplan | None = None
            if os.getenv(ENV_CSTAR_RUNID, None):
                workplan = DirectiveConfig.load_workplan()

            app = get_app_for_blueprint(Path(local_bp))
            blueprint = t.cast("Blueprint", deserialize(local_bp, app.blueprint))

            step = LiveStep(
                name="directive-step",
                application=app.name,
                blueprint=local_bp,
                working_dir=blueprint.effective_working_dir,
            )

            # apply-overrides must run before any content directive so the
            # working_dir override lands before an intermediate blueprint is
            # persisted; enforce it here since directive files may predate
            # the ordering guaranteed by `package_runtime_overrides`.
            ordered = sorted(
                directives.items(),
                key=lambda item: item[0] != ApplyOverridesDirective.key(),
            )
            transforms = [
                directive_map[key](
                    config=t.cast("dict[str, dict[str, t.Any]]", config),
                    workplan=workplan,
                )
                for key, config in ordered
            ]
            for transform in transforms:
                step = transform(step)[0]

        return str(step.blueprint_path)

    @classmethod
    def load_workplan(
        cls,
    ) -> LiveWorkplan:
        """Load the transformed workplan for the active run.

        The run is identified by the run-id exported in the environment.

        Returns
        -------
        LiveWorkplan
            The transformed workplan recorded for the active run.

        Raises
        ------
        RuntimeError
            If no run-id is exported in the environment, no run record exists
            for it, or the recorded workplan cannot be loaded. In the last case
            the underlying `FileNotFoundError`, `ValueError` or `yaml.YAMLError`
            is chained as the cause and its message is included.
        """
        run_id = os.getenv(ENV_CSTAR_RUNID, None)
        run: WorkplanRun | None = None

        if run_id:
            repo = TrackingRepository()
            run = repo.get_workplan_run_sync(run_id)
        else:
            msg = f"No run-id could be found in environment variable: {ENV_CSTAR_RUNID}"
            raise RuntimeError(msg)

        if not run:
            msg = f"Workplan context could not be provided to directives for run: {run_id}"
            raise RuntimeError(msg)

        wp_path = run.trx_workplan_path
        try:
            return deserialize(wp_path, LiveWorkplan)
        except (FileNotFoundError, ValueError, yaml.YAMLError) as ex:
            msg = f"Unable to load workplan for run-id {run_id!r} from {str(wp_path)!r}: {ex}"
            raise RuntimeError(msg) from ex

    @classmethod
    def restore_directive_file(cls, path: str | Path) -> Path:
        """Rewrite a missing directive file from the workplan recorded for the run.

        A step's directive file is written into its work directory when the
        step is submitted, so it can be lost while the step waits in a
        scheduler queue. The directives are also recorded in the transformed
        workplan persisted for the run, which is used here to restore the file
        at the path the step was submitted with.

        Parameters
        ----------
        path : str | Path
            The path to the missing directive file.

        Returns
        -------
        Path
            The path to the restored directive file.

        Raises
        ------
        CstarError
            If the file cannot be restored: no run is active, the workplan
            recorded for the run cannot be loaded, no step in it writes its
            directives to `path`, or the file cannot be written.
        """
        if not os.getenv(ENV_CSTAR_RUNID, ""):
            msg = (
                f"no run-id is set in {ENV_CSTAR_RUNID}; directives are only "
                "restored for a step running as part of a workplan"
            )
            raise CstarError(msg)

        target = Path(path).expanduser().resolve()

        try:
            workplan = cls.load_workplan()
        except (RuntimeError, ValueError) as ex:
            msg = f"the workplan recorded for this run could not be loaded: {ex}"
            raise CstarError(msg) from ex

        for step in workplan.steps:
            if step.fsm.run_dir / DIRECTIVES_FILENAME != target:
                continue

            try:
                restored = prepare_directive_file(step)
            except OSError as ex:
                msg = f"the directive file for step {step.name!r} could not be written: {ex}"
                raise CstarError(msg) from ex

            msg = (
                f"Restored missing directive file for step {step.name!r} from "
                f"the workplan recorded for this run: {str(restored)!r}"
            )
            log.warning(msg)
            return restored

        msg = (
            f"no step in the workplan for this run writes directives to {str(target)!r}"
        )
        raise CstarError(msg)

    @classmethod
    def register(cls, key: str, directive: type[Directive]) -> None:
        DirectiveConfig.directive_map[key] = directive


def resolve_deferred_blueprint(ref: DeferredBlueprintRef) -> Path:
    """Locate the blueprint generated by the producing step of a deferred
    blueprint reference.

    Requires workplan context (`ENV_CSTAR_RUNID`), so this can only run once
    the workplan has been scheduled.

    Parameters
    ----------
    ref : DeferredBlueprintRef
        The deferred reference naming the producing step; the step may belong
        to an external run (`<step>@<alias>`).

    Returns
    -------
    Path
        The path to the generated blueprint.

    Raises
    ------
    CstarError
        If the producing step cannot be found in the workplan, or a unique
        blueprint file cannot be located in its output directory.
    """
    workplan = DirectiveConfig.load_workplan()
    try:
        producer = lookup_step(workplan, ref.from_step)
    except KeyError as ex:
        msg = (
            f"Deferred blueprint references step {ref.from_step!r}, which "
            "does not exist in the workplan"
        )
        raise CstarError(msg) from ex

    output_dir = producer.fsm.output_dir

    if ref.filename:
        candidate = output_dir / ref.filename
        if not candidate.is_file():
            msg = (
                f"Step {ref.from_step!r} did not produce the expected "
                f"blueprint: {candidate}"
            )
            raise CstarError(msg)
        return candidate

    candidates = sorted(
        path
        for pattern in ("*.yml", "*.yaml", "*.json")
        for path in output_dir.glob(pattern)
    )
    if not candidates:
        msg = f"Step {ref.from_step!r} did not produce a blueprint in: {output_dir}"
        raise CstarError(msg)
    if len(candidates) > 1:
        names = ", ".join(path.name for path in candidates)
        msg = (
            f"Multiple candidate blueprints found in the output of step "
            f"{ref.from_step!r} ({names}); specify `filename` in the "
            "deferred blueprint reference"
        )
        raise CstarError(msg)

    return candidates[0]


DirectiveConfig.register(ApplyOverridesDirective.key(), ApplyOverridesDirective)
