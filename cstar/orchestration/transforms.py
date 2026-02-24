import itertools
import os
import typing as t
from collections import defaultdict
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path

from cstar.base.feature import (
    ENV_FF_ORCH_TRX_TIMESPLIT,
    is_feature_enabled,
)
from cstar.base.log import LoggingMixin
from cstar.base.utils import (
    DEFAULT_OUTPUT_ROOT_NAME,
    deep_merge,
    slugify,
)
from cstar.orchestration.models import (
    Application,
    RomsMarblBlueprint,
    Workplan,
)
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.utils import ENV_CSTAR_ORCH_TRX_FREQ


class Transform(t.Protocol):
    """Protocol for a class that transforms a step into one or more
    new steps.
    """

    def __call__(self, step: LiveStep) -> t.Iterable[LiveStep]:
        """Apply the transform to a step.

        Parameters
        ----------
        step : Step
            The step to be transformed

        Returns
        -------
        Iterable[Step]
            Zero-to-many steps resulting from applying the transform.
        """
        ...

    @staticmethod
    def suffix() -> str:
        """Return the standard prefix to be used when persisting
        a resource modified by this transform.
        """
        ...


TRANSFORMS: dict[str, list[Transform]] = defaultdict(list)
"""Storage for transform registrations."""


def register_transform(application: str, transform: Transform) -> None:
    """Register a transform for an application.

    Parameters
    ----------
    application : str
        The application name.
    transform : Transform
        The transform instance to register for the application.
    """
    TRANSFORMS[application].append(transform)


def get_transforms(application: str) -> list[Transform]:
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


def _dailies(
    start_date: datetime, end_date: datetime
) -> t.Iterable[tuple[datetime, datetime]]:
    """Get daily time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, start_date.day)
    while current_date < end_date:
        day_start = current_date
        day_end = day_start + timedelta(days=1)
        yield (day_start, day_end)
        current_date = day_end


def _weeklies(
    start_date: datetime, end_date: datetime
) -> t.Iterable[tuple[datetime, datetime]]:
    """Get weekly time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, start_date.day)
    while current_date < end_date:
        week_start = current_date
        week_end = week_start + timedelta(days=7)
        yield (week_start, week_end)
        current_date = week_end


def _monthlies(
    start_date: datetime, end_date: datetime
) -> t.Iterable[tuple[datetime, datetime]]:
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


class SplitFrequency(StrEnum):
    Daily = "daily"
    Weekly = "weekly"
    Monthly = "monthly"


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
) -> t.Iterable[tuple[datetime, datetime]]:
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


class WorkplanTransformer(LoggingMixin):
    """Transform a workplan by applying transforms to its steps."""

    original: Workplan
    """The original, pre-transformation workplan."""

    _transformed: Workplan | None = None
    """The post-transformation workplan."""

    DERIVED_PATH_SUFFIX: t.Literal["_trx"] = "_trx"
    """Suffix appended to the original workplan path when generating a derived path."""

    def __init__(self, wp: Workplan, transform: Transform) -> None:
        """Initialize the instance."""
        self.original = Workplan(**wp.model_dump(by_alias=True))
        self.transform_fn = transform
        self._transformed: Workplan | None = None

    @property
    def is_modified(self) -> bool:
        """Return `True` if the transformed workplan differs from the original.

        Returns
        -------
        bool
        """
        dump_a = self.original.model_dump()
        dump_b = self.transformed.model_dump()

        return dump_a != dump_b

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
        steps: t.Iterable[LiveStep] = list(
            map(override_output_directory, live_steps),
        )

        if is_feature_enabled(ENV_FF_ORCH_TRX_TIMESPLIT):
            split_steps: list[LiveStep] = []
            named_dep_map: dict[str, str] = {}

            for step in steps:
                transformed_steps = list(self.transform_fn(step))
                named_dep_map[step.name] = transformed_steps[-1].name
                split_steps.extend(transformed_steps)

            for step in split_steps:
                depends_on = {str(d) for d in step.depends_on}

                if to_update := depends_on.intersection(named_dep_map):
                    depends_on.update(named_dep_map[x] for x in to_update)
                    depends_on.difference_update(to_update)

                    step.depends_on.clear()
                    step.depends_on.extend(depends_on)

            steps = split_steps

        # apply any overrides produced in the transform function
        override_transform = OverrideTransform()
        steps = list(itertools.chain.from_iterable(map(override_transform, steps)))

        self._transformed = self.original.model_copy(
            update={
                "steps": steps,
                "name": f"{self.original.name} (transformed)",
            },
        )

        return self._transformed


class RomsMarblTimeSplitter(Transform):
    """A step tranformation that splits a ROMS-MARBL simulation into
    multiple sub-steps based on the timespan covered by the simulation.
    """

    frequency: str
    """The step splitting frequency used to generate new time steps."""

    def __init__(self, frequency: str = SplitFrequency.Monthly.value) -> None:
        """Initialize the transform instance."""
        self.frequency = os.getenv(ENV_CSTAR_ORCH_TRX_FREQ, frequency)

    def __call__(self, step: LiveStep) -> t.Iterable[LiveStep]:
        """Split a step into multiple sub-steps.

        Parameters
        ----------
        step : Step
            The step to split.

        Returns
        -------
        Iterable[Step]
            Steps for each subtask resulting from the split.
        """
        blueprint = deserialize(step.blueprint_path, RomsMarblBlueprint)
        start_date = blueprint.runtime_params.start_date
        end_date = blueprint.runtime_params.end_date

        bp_path = step.fsm.work_dir / Path(step.blueprint_path).name
        serialize(bp_path, blueprint)

        time_slices = list(get_time_slices(start_date, end_date, self.frequency))
        n_slices = len(time_slices)

        if end_date <= start_date:
            msg = "end_date must be after start_date"
            raise ValueError(msg)

        depends_on = step.depends_on
        last_restart_file: Path | None = None
        output_root_name = DEFAULT_OUTPUT_ROOT_NAME

        for i, (sd, ed) in enumerate(time_slices):
            bp_copy = RomsMarblBlueprint(
                **blueprint.model_dump(
                    exclude_unset=True,
                    exclude_defaults=True,
                    exclude_computed_fields=True,
                ),
            )

            compact_sd = sd.strftime("%Y%m%d%H%M%S")
            compact_ed = ed.strftime("%Y%m%d%H%M%S")

            dynamic_name = f"{i + 1:03d}_{step.safe_name}_{compact_sd}_{compact_ed}"
            child_step_name = slugify(dynamic_name)

            child_fs = step.fsm.get_subtask_manager(child_step_name)

            description = f"Subtask {i + 1} of {n_slices}; Timespan: {sd} to {ed}; {bp_copy.description}"
            overrides = {
                "name": dynamic_name,
                "description": description,
                "runtime_params": {
                    "start_date": sd,
                    "end_date": ed,
                    "output_dir": child_fs.root.as_posix(),  # child_fs.output_dir,
                },
            }

            if last_restart_file:
                rst_path = last_restart_file.as_posix()
                overrides["initial_conditions"] = {"data": [{"location": rst_path}]}

            child_bp_path = child_fs.work_dir / f"{child_step_name}_bp.yaml"
            serialize(child_bp_path, bp_copy)

            updates = {
                "blueprint": child_bp_path.as_posix(),
                "blueprint_overrides": overrides,
                "depends_on": depends_on,
                "name": child_step_name,
            }
            child_step = LiveStep.from_step(step, parent=step, update=updates)

            yield child_step
            if i == len(time_slices) - 1:
                break

            # use dependency on the prior substep to chain all the dynamic steps
            depends_on = [child_step.name]

            # Use the last restart file as initial conditions for the follow-up step
            # - reset file names are formatted as: <stem>_rst.YYYYMMDDHHMMSS.{partition}.nc
            reset_file_name = f"{output_root_name}_rst.{compact_ed}.000.nc"

            step_output_dir = child_fs.output_dir
            restart_file_path = step_output_dir / reset_file_name

            # use output dir of the last step as the input for the next step
            last_restart_file = restart_file_path

    @staticmethod
    def suffix() -> str:
        """Return the standard prefix to be used when persisting
        a resource modified by this transform.
        """
        return "split"


class OverrideTransform(Transform):
    """Transform that overrides a step by returning a blueprint with all overridden attributes applied."""

    _system_overrides: dict[str, t.Any] = {}

    def __init__(self, sys_overrides: dict[str, t.Any] = {}) -> None:
        """Initialize the instance.

        Parameters
        ----------
        sys_overrides : dict[str, t.Any]
            System-level blueprint overrides that will be applied after
            the user-supplied values.
        """
        self._system_overrides = sys_overrides

    def apply(
        self, bp: RomsMarblBlueprint, overrides: dict[str, t.Any] = {}
    ) -> RomsMarblBlueprint:
        """Apply all overrides from a blueprint.

        Generate a new blueprint with overrides applied and an empty set of overrides.
        Store the newly generated blueprint in the output directory.

        Parameters
        ----------
        bp : RomsMarblBlueprint
            The blueprint to apply overrides to
        overrides : dict[str, t.Any]
            A dictionary containing overrides for attributes of a blueprint.

        Returns
        -------
        RomsMarblBlueprint
            The blueprint with all overrides applied.
        """
        overrides = overrides.copy()

        model = bp.model_dump(exclude_defaults=True, exclude_unset=True)

        # system-level overrides take precedence over step-level overrides
        changeset = deep_merge(overrides, self._system_overrides)
        merged = deep_merge(model, changeset)

        description = (
            f"{bp.description}; overridden keys [{', '.join(changeset.keys())}]"
        )
        merged.update(description=description)

        return RomsMarblBlueprint(**merged)

    def __call__(self, step: LiveStep) -> t.Iterable[LiveStep]:
        """Apply the transform to a step.

        Parameters
        ----------
        step : Step
            The step to be transformed

        Returns
        -------
        Iterable[Step]
            Zero-to-many steps resulting from applying the transform.
        """
        bp_path = Path(step.blueprint_path)
        blueprint = deserialize(bp_path, RomsMarblBlueprint)

        updated_bp = self.apply(blueprint, step.blueprint_overrides)

        live_step = LiveStep.from_step(
            step,
            update={
                "blueprint_overrides": {},
                "work_dir": updated_bp.runtime_params.output_dir,
            },
        )

        bp_renamed = bp_path.with_stem(f"{bp_path.stem}.{self.suffix()}").name
        live_step.blueprint_path = live_step.fsm.work_dir / bp_renamed

        serialize(live_step.blueprint_path, updated_bp)
        return [live_step]

    @staticmethod
    def suffix() -> str:
        """Return the standard prefix to be used when persisting
        a resource modified by this transform.
        """
        return "ovrd"


def override_output_directory(step: LiveStep) -> LiveStep:
    """Automatically override the output directory specified in a blueprint
    to write to the C-Star home directories.

    See `cstar.execution.file_system.DirectoryManager` for more detail
    on the available set of home directories.

    Returns
    -------
    list[Step]
        The transformed steps.
    """
    sys_overrides = {"runtime_params": {"output_dir": step.fsm.root}}
    override_transform = OverrideTransform(sys_overrides)
    overridden_step_result = iter(override_transform(step))

    return next(overridden_step_result)


register_transform(Application.ROMS_MARBL.value, RomsMarblTimeSplitter())
