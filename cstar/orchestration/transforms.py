import typing as t
from datetime import datetime
from pathlib import Path

from pydantic import HttpUrl

from cstar.orchestration.models import ChildStep, RomsMarblBlueprint, Step
from cstar.orchestration.serialization import deserialize
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


TRANSFORMS: dict[str, Transform] = {}


def register_transform(application: str, transform: Transform) -> None:
    """Register a splitter for an application.

    Parameters
    ----------
    application : str
        The application name.
    transform : Splitter
        The transform instance.
    """
    TRANSFORMS[application] = transform


def get_transform(application: str) -> Transform | None:
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
    return TRANSFORMS.get(application)


def get_time_slices(
    start_date: datetime, end_date: datetime
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
    current_date = datetime(start_date.year, start_date.month, 1)

    time_slices = []
    while current_date < end_date:
        month_start = current_date

        if month_start.month == 12:
            month_end = datetime(current_date.year + 1, 1, 1)
        else:
            month_end = datetime(
                current_date.year,
                month_start.month + 1,
                1,
                hour=0,
                minute=0,
                second=0,
            )

        time_slices.append((month_start, month_end))
        current_date = month_end

    # adjust when the start date is not the first day of the month
    if start_date > time_slices[0][0]:
        time_slices[0] = (start_date, time_slices[0][1])

    # adjust when the end date is not the last day of the month
    if end_date < time_slices[-1][1]:
        time_slices[-1] = (time_slices[-1][0], end_date)

    return time_slices


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
            location = Path(location.path)

        return location.stem

    def _get_default_overrides(
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
                "runtime_params": {
                    "initial_conditions": {
                        "location": last_output_path.as_posix(),
                    }
                }
            }
        }

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
        blueprint = deserialize(step.blueprint, RomsMarblBlueprint)
        start_date = blueprint.runtime_params.start_date
        end_date = blueprint.runtime_params.end_date

        # use the directory of the parent as the base...
        output_root = step.tasks_dir(blueprint)
        time_slices = get_time_slices(start_date, end_date)

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

        for sd, ed in time_slices:
            compact_sd = sd.strftime("%Y%m%d%H%M%S")
            compact_ed = ed.strftime("%Y%m%d%H%M%S")

            step_name = slugify(f"{step.name}_{compact_sd}-{compact_ed}")

            # reset file names are formatted as: <stem>_rst.YYYYMMDDHHMMSS.{partition}.nc
            # restart_file = step_output_root / f"{ic_stem}_rst.{compact_sd}.*.nc"

            # restart_files = list(step_output_dir.glob(".*_rst.??????????????.*.nc"))
            # restart_files.sort(reverse=True)
            # last_restart_file = restart_files[0] if restart_files else "not-found"

            updates = self._get_default_overrides(
                step_name, sd, ed, output_root / step_name, depends_on
            )

            # adjust initial conditions after the first step
            if last_restart_file is not None:
                updates = deep_merge(updates, self._get_ic_overrides(last_restart_file))

            child_step = ChildStep(
                **{**step.model_dump(), **updates, "parent": step.name}
            )
            yield child_step

            # use dependency on the prior substep to chain all the dynamic steps
            depends_on = [child_step.name]

            # use output dir of the last step as the input for the next step
            last_restart_file = child_step.restart_path(blueprint)


register_transform("roms_marbl", RomsMarblTimeSplitter())
