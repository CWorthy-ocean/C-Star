import os
import typing as t
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import HttpUrl

from cstar.orchestration.models import ChildStep, RomsMarblBlueprint, Step, Workplan
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.utils import deep_merge, slugify


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


def register_transform(application: str, transform: Transform) -> None:
    """Register a splitter for an application.

    Parameters
    ----------
    application : str
        The application name.
    transform : Splitter
        The transform instance.
    """
    TRANSFORMS[application].append(transform)


def get_transforms(application: str) -> list[Transform]:
    """Retrieve a transform for an application.

    Parameters
    ----------
    application : str
        The application name.

    Returns
    -------
    Splitter | None
        The transform instance, or None if not found.
    """
    return TRANSFORMS.get(application, [])


def _dailies(
    start_date: datetime, end_date: datetime
) -> t.Iterable[tuple[datetime, datetime]]:
    """Get the daily time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, start_date.day)
    while current_date < end_date:
        day_start = current_date
        day_end = day_start + timedelta(days=1)
        yield (day_start, day_end)
        current_date = day_end


def _monthlies(
    start_date: datetime, end_date: datetime
) -> t.Iterable[tuple[datetime, datetime]]:
    """Get the monthly time slices for the given start and end dates."""
    current_date = datetime(start_date.year, start_date.month, 1)
    while current_date < end_date:
        month_start = current_date

        if month_start.month == 12:
            month_end = datetime(current_date.year + 1, 1, 1)
        else:
            month_end = datetime(current_date.year, month_start.month + 1, 1)

        yield (month_start, month_end)
        current_date = month_end


def get_time_slices(
    start_date: datetime,
    end_date: datetime,
    frequency: t.Literal["daily", "monthly"] = "monthly",
) -> t.Iterable[tuple[datetime, datetime]]:
    """Get the time slices for the given start and end dates.

    Parameters
    ----------
    start_date : datetime
        The start date.
    end_date : datetime
        The end date.

    Returns
    -------
    Iterable[tuple[datetime, datetime]]
        The time slices.
    """
    time_slices = list(
        _dailies(start_date, end_date)
        if frequency == "daily"
        else _monthlies(start_date, end_date)
    )

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

    ENABLED_ENV_VAR: t.ClassVar[t.Final[str]] = "CSTAR_ORCHESTRATOR_ENABLE_TRANSFORMS"
    """Environment variable to control whether transforms are enabled."""

    DERIVED_PATH_SUFFIX: t.ClassVar[t.Final[str]] = "_trx"
    """Suffix appended to the original workplan path when generating a derived path."""

    def __init__(self, wp: Workplan, transform: Transform):
        self.original = Workplan(**wp.model_dump(by_alias=True))
        self.transform_fn = transform
        self._transformed: Workplan | None = None

    @property
    def enabled(self) -> bool:
        """Check local configuration to determine if transforms are enabled.

        Defaults to `True` when no configuration is found.

        Returns
        -------
        bool
        """
        return os.getenv(WorkplanTransformer.ENABLED_ENV_VAR, "1") == "1"

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
        """Return the transformed workplan."""
        if self._transformed is None:
            self._transformed = self.apply()
        return self._transformed

    @staticmethod
    def derived_path(source: Path, target_dir: Path | None = None) -> Path:
        """Generate a new path name derived from the original workplan path.

        If no target directory is specified, the resulting path will be alongside
        the original file.
        """
        stem = f"{source.stem}{WorkplanTransformer.DERIVED_PATH_SUFFIX}"
        if target_dir is not None:
            return target_dir / source.with_stem(stem)
        return source.with_stem(stem)

    def apply(self) -> Workplan:
        """Create a new workplan with appropriate transforms applied.

        Returns
        -------
        Workplan
        """
        if not self.enabled:
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
        return self._transformed


class RomsMarblTimeSplitter(Transform):
    """A step tranformation that splits a ROMS-MARBL simulation into
    multiple sub-steps based on the timespan covered by the simulation.
    """

    def _get_location_stem(self, blueprint: RomsMarblBlueprint) -> str:
        """Identify the stem of the initial conditions file.

        Parameters
        ----------
        blueprint : RomsMarblBlueprint
            The blueprint to extract initial conditions from.

        Returns
        -------
        str
            The stem of the initial conditions file.
        """
        location = blueprint.initial_conditions.data[0].location

        if isinstance(location, HttpUrl):
            if location.path is None:
                raise RuntimeError("Initial conditions location is not a valid path")
            location = location.path

        return Path(location).stem

    def _get_blueprint_overrides(
        self,
        step_name: str,
        sd: datetime,
        ed: datetime,
        step_output_dir: Path,
        depends_on: list[str],
    ) -> dict[str, t.Any]:
        """Create a dictionary that will override the blueprint runtime parameters
        of a step.

        Parameters
        ----------
        step_name : str
            The name of the step.
        sd : datetime
            The start date.
        ed : datetime
            The end date.
        step_output_dir : Path
            The output directory of the step.
        depends_on : list[str]
            The dependencies of the step.

        Returns
        -------
        dict[str, t.Any]
            The overrides.
        """
        return {
            "name": step_name,
            "blueprint_overrides": {
                "runtime_params": {
                    "start_date": sd.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_date": ed.strftime("%Y-%m-%d %H:%M:%S"),
                    "output_dir": step_output_dir.as_posix(),
                }
            },
            "depends_on": depends_on,
        }

    def _get_ic_overrides(self, last_output_path: Path) -> dict[str, t.Any]:
        """Create a dictionary that will override the initial conditions of a step.

        Parameters
        ----------
        last_output_path : Path
            The path to the last output file.

        Returns
        -------
        dict[str, t.Any]
            The overrides.
        """
        return {
            "blueprint_overrides": {
                "initial_conditions": {
                    "data": [
                        {
                            "location": last_output_path.as_posix(),
                            # "partitioned": "true",
                        }
                    ]
                }
            }
        }

    def output_root(
        self,
        step_name: str,
        bp: RomsMarblBlueprint,
    ) -> Path:
        """The step-relative directory for writing outputs."""
        # runtime_overrides = t.cast(
        #     dict[str, str], self.blueprint_overrides.get("runtime_params", {})
        # )
        # output_dir: str | Path = runtime_overrides.get("output_dir", "")

        # if output_dir:
        #     # runtime override will always take precedence
        #     return Path(output_dir)

        # run_id = os.getenv("CSTAR_RUNID")

        # use the blueprint root path if it hasn't been overridden
        return Path(bp.runtime_params.output_dir) / slugify(step_name)

    def __call__(self, step: Step) -> t.Iterable[Step]:
        """Split a step into multiple sub-steps.

        Parameters
        ----------
        step : Step
            The step to split.

        Returns
        -------
        Iterable[Step]
            The sub-steps.
        """
        blueprint = deserialize(step.blueprint_path, RomsMarblBlueprint)
        # step.bp = blueprint
        start_date = blueprint.runtime_params.start_date
        end_date = blueprint.runtime_params.end_date

        # use the directory of the parent as the base...
        output_root = self.output_root(step.name, blueprint)
        work_root = output_root / "work"
        task_root = output_root / "tasks"

        time_slices = list(get_time_slices(start_date, end_date))
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

            unique_name = f"{i + 1:02d}_{step.name}_{compact_sd}_{compact_ed}"
            child_step_name = slugify(unique_name)

            subtask_root = task_root / child_step_name
            description = (
                f"Subtask {i + 1} of {n_slices}; Simulation covering "
                f"timespan from `{sd}` to `{ed}` - {blueprint.description}"
            )
            overrides = {
                "description": description,
                "runtime_params": {
                    "start_date": sd,
                    "end_date": ed,
                    "output_dir": subtask_root.as_posix(),
                },
            }
            # bp_copy.runtime_params.start_date = sd
            # bp_copy.runtime_params.end_date = ed
            # bp_copy.runtime_params.output_dir = subtask_root
            # bp_copy.description = (
            #     f"subtask {i + 1}: {sd} to {ed} - {blueprint.description}"
            # )

            if last_restart_file:
                # bp_copy.initial_conditions.data[
                #     0
                # ].location = last_restart_file.as_posix()
                overrides["initial_conditions"] = {
                    "data": [{"location": last_restart_file.as_posix()}]
                }

            store_at = work_root / f"{child_step_name}_blueprint.yaml"
            serialize(store_at, bp_copy)

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
                blueprint=store_at.as_posix(),
                depends_on=depends_on,
                parent=step.name,
                blueprint_overrides=overrides,  # type: ignore[arg-type]
            )

            yield child_step
            if i == len(time_slices) - 1:
                break

            # use dependency on the prior substep to chain all the dynamic steps
            depends_on = [child_step.name]

            # Use the last restart file as initial conditions for the follow-up step
            # - reset file names are formatted as: <stem>_rst.YYYYMMDDHHMMSS.{partition}.nc
            reset_file_name = f"output_rst.{compact_ed}.000.nc"
            # restart_file_path = child_step.working_dir / "output" / reset_file_name
            restart_file_path = subtask_root / "output" / reset_file_name

            # use output dir of the last step as the input for the next step
            last_restart_file = restart_file_path


class OverrideTransform(Transform):
    """Transform that overrides a step by returning a blueprint with all overridden attributes applied."""

    @staticmethod
    def _output_dir(step: Step, blueprint: RomsMarblBlueprint) -> Path:
        runtime_params = t.cast(
            dict[str, str], step.blueprint_overrides.get("runtime_params", {})
        )
        output_dir_override: str = runtime_params.get("output_dir", "")

        if not output_dir_override:
            return Path(blueprint.runtime_params.output_dir)

        return Path(output_dir_override)

    def _get_overridden_blueprint(
        self, step: Step, blueprint: RomsMarblBlueprint
    ) -> RomsMarblBlueprint:
        """Apply all overrides from a blueprint.

        Generate a new blueprint with overrides applied and an empty set of overrides.
        Store the newly generated blueprint in the output directory.
        """
        kvs = step.blueprint_overrides
        bp_attrs = blueprint.model_dump(exclude_unset=True)
        description = f"{blueprint.description}\n - overrides applied"

        bp_attrs.update(description=description)
        merged = deep_merge(bp_attrs, kvs)

        return RomsMarblBlueprint(**merged)

    def _persist_overridden_blueprint(
        self, blueprint: RomsMarblBlueprint, step: Step
    ) -> Path:
        """Persist the overridden blueprint to the output directory."""
        output_dir = self._output_dir(step, blueprint)
        work_dir = output_dir / "work"

        store_at = work_dir / f"{blueprint.name}.yaml"
        serialize(store_at, blueprint)

        return store_at

    def __call__(self, step: Step) -> t.Iterable[Step]:
        """Split a step into multiple sub-steps.

        Parameters
        ----------
        step : Step
            The step to split.

        Returns
        -------
        Iterable[Step]
            The sub-steps.
        """
        bp = deserialize(step.blueprint_path, RomsMarblBlueprint)
        new_bp = self._get_overridden_blueprint(step, bp)
        new_bp_path = self._persist_overridden_blueprint(new_bp, step)

        attributes = step.model_dump(exclude={"blueprint", "blueprint_overrides"})
        new_step = Step(
            **attributes,
            blueprint=new_bp_path.as_posix(),
        )
        return [new_step]


register_transform("roms_marbl", RomsMarblTimeSplitter())
# register_transform("roms_marbl", OverrideTransform())
# register_transform("sleep", RomsMarblTimeSplitter())
