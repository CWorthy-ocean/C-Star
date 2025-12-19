import os
import typing as t
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from cstar.base.feature import is_feature_enabled
from cstar.orchestration.models import (
    ChildStep,
    CodeRepository,
    RomsMarblBlueprint,
    Step,
    Workplan,
)
from cstar.orchestration.roms_dot_in import get_runtime_setting_value
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.utils import ENV_CSTAR_ORC_TRX_FREQ, deep_merge, slugify


class Transform(t.Protocol):
    """Protocol for a class that transforms a step into one or more
    new steps.
    """

    def __call__(self, step: Step) -> t.Iterable[Step]:
        """Perform a transformation on the input step.

        Parameters
        ----------
        step : Step
            The step to split.

        Returns
        -------
        Iterable[Step]
            The sub-steps.
        """
        ...


TRANSFORMS: dict[str, list[Transform]] = defaultdict(list)
"""Storage for transform registrations."""


def register_transform(application: str, transform: Transform) -> None:
    """Register a splitter for an application.

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


SLICE_FUNCTIONS = defaultdict(
    lambda: _monthlies,
    {
        "daily": _dailies,
        "weekly": _weeklies,
        "monthly": _monthlies,
    },
)


def get_time_slices(
    start_date: datetime,
    end_date: datetime,
    frequency: str = "monthly",
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


class WorkplanTransformer:
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
        suffix: str = "_trx",
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
            raise ValueError(
                f"Identical source and target may result in overwriting the source: `{source}`"
            )

        directory = target_dir if target_dir else source.parent
        filename = Path(source.name).with_stem(f"{source.stem}{suffix}")

        return directory / filename

    def apply(self) -> Workplan:
        """Create a new workplan with appropriate transforms applied.

        Returns
        -------
        Workplan
        """
        if not is_feature_enabled("ORC_TRANSFORM_AUTO"):
            return self.original

        if self._transformed:
            return self._transformed

        steps = []
        for step in self.original.steps:
            if not self.transform_fn:
                steps.append(step)
                continue

            transformed_steps = list(self.transform_fn(step))
            steps.extend(transformed_steps)

            # replace dependencies on the original step with the last transformed step
            tweaks = (s for s in self.original.steps if step.name in s.depends_on)
            for tweak in tweaks:
                tweak.depends_on.remove(step.name)
                tweak.depends_on.append(transformed_steps[-1].name)

        wp_attrs = self.original.model_dump()
        wp_attrs.update(
            {"steps": steps, "name": f"{self.original.name} (with transforms)"}
        )

        self._transformed = Workplan(**wp_attrs)
        return t.cast(Workplan, self._transformed)


class RomsMarblTimeSplitter(Transform):
    """A step tranformation that splits a ROMS-MARBL simulation into
    multiple sub-steps based on the timespan covered by the simulation.
    """

    def _get_output_base_name(self, repo: CodeRepository) -> str:
        """Parse the `.in` input file from the repository to identify
        the base name for outputs of the simulation.

        Returns
        -------
        str

        Raises
        ------
        TypeError
            If the .in file does not contain a well-formatted value for
            output_base_name
        """
        value = get_runtime_setting_value(repo, "output_base_name")
        if isinstance(value, list):
            msg = "Invalid output_base_name found. Expected a single value."
            raise TypeError(msg)
        return value

    def __call__(self, step: Step) -> t.Iterable[Step]:
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

        job_fs = step.file_system(blueprint)
        job_fs.prepare()

        serialize(job_fs.root / Path(step.blueprint_path).name, blueprint)

        frequency = os.getenv(ENV_CSTAR_ORC_TRX_FREQ, "monthly")
        time_slices = list(get_time_slices(start_date, end_date, frequency=frequency))
        n_slices = len(time_slices)

        # if (start_date - end_date).total_seconds() < timedelta(days=30).total_seconds():
        #     # TODO: leave this ask discussion driver. must determine
        #     # what conditions might preclude automatic splitting.
        #     # yield step
        #     # return
        #     return [step]

        if end_date <= start_date:
            raise ValueError("end_date must be after start_date")

        depends_on = step.depends_on
        last_restart_file: Path | None = None
        output_root_name = get_runtime_setting_value(
            blueprint.code.run_time, "output_root_name"
        )

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

            dynamic_name = f"{i + 1:02d}_{step.safe_name}_{compact_sd}_{compact_ed}"
            child_step_name = slugify(dynamic_name)

            subtask_root = (
                job_fs.tasks_dir
            )  # subtask_fs = RomsJobFileSystem(subtask_root)
            subtask_out_dir = subtask_root / child_step_name

            description = (
                f"Subtask {i + 1} of {n_slices}; Simulation covering "
                f"timespan from `{sd}` to `{ed}` - {blueprint.description}"
            )
            overrides = {
                "description": description,
                "runtime_params": {
                    "name": dynamic_name,
                    "start_date": sd,
                    "end_date": ed,
                    "output_dir": subtask_out_dir.as_posix(),
                },
            }
            bp_copy.runtime_params.start_date = sd
            bp_copy.runtime_params.end_date = ed
            bp_copy.runtime_params.output_dir = subtask_out_dir
            bp_copy.name = dynamic_name
            bp_copy.description = (
                f"subtask {i + 1}: {sd} to {ed} - {blueprint.description}"
            )

            if last_restart_file:
                rst_path = last_restart_file.as_posix()
                bp_copy.initial_conditions.data[0].location = rst_path
                overrides["initial_conditions"] = {"data": [{"location": rst_path}]}

            child_bp_path = subtask_out_dir / f"{child_step_name}_blueprint.yaml"

            serialize(child_bp_path, bp_copy)

            attributes = step.model_dump(
                exclude={
                    "blueprint",
                    "name",
                    "depends_on",
                    "blueprint_overrides",
                }
            )

            child_step = ChildStep(
                **attributes,
                name=child_step_name,
                blueprint=child_bp_path.as_posix(),
                depends_on=depends_on,
                parent=step.name,
                blueprint_overrides=overrides,  # type: ignore[arg-type]
            )
            child_step.file_system(bp_copy).prepare()

            yield child_step
            if i == len(time_slices) - 1:
                break

            # use dependency on the prior substep to chain all the dynamic steps
            depends_on = [child_step.name]

            # Use the last restart file as initial conditions for the follow-up step
            # - reset file names are formatted as: <stem>_rst.YYYYMMDDHHMMSS.{partition}.nc
            reset_file_name = f"{output_root_name}_rst.{compact_ed}.000.nc"

            step_output_dir = child_step.file_system(bp_copy).output_dir
            restart_file_path = step_output_dir / reset_file_name

            # use output dir of the last step as the input for the next step
            last_restart_file = restart_file_path


class BlueprintOverrider:
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

        model = bp.model_dump(exclude_unset=True)

        # system-level overrides take precedence over step-level overrides
        changeset = deep_merge(overrides, self._system_overrides)
        merged = deep_merge(model, changeset)

        description = (
            f"{bp.description} - overrides applied to [{', '.join(changeset.keys())}]"
        )
        merged.update(description=description)

        return RomsMarblBlueprint(**merged)


register_transform("roms_marbl", RomsMarblTimeSplitter())
