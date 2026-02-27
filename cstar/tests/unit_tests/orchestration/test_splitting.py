import os
import typing as t
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.utils import DEFAULT_OUTPUT_ROOT_NAME
from cstar.orchestration.models import Application, Step, Workplan
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.transforms import (
    RomsMarblTimeSplitter,
    get_time_slices,
    get_transforms,
)


@pytest.fixture
def single_step_workplan(
    tmp_path: Path,
    bp_templates_dir: Path,
) -> Workplan:
    """Generate a workplan that contains a single step.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files

    Returns
    -------
    Workplan
    """
    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    default_output_dir = "output_dir: ."

    bp_path = tmp_path / "blueprint.yaml"
    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(
        default_output_dir, f"output_dir: {tmp_path.as_posix()}"
    )
    bp_content.replace("sleep", Application.ROMS_MARBL.value)
    bp_path.write_text(bp_content)

    return Workplan(
        name="single-step-workplan",
        description="A workplan with a single step.",
        steps=[
            Step(
                name="s-00",
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
        ],
    )


def test_time_splitting() -> None:
    """Verify that the time splitting function honors the start and end dates.

    Ensures that no assumptions are made about the 1st and last days of the month.
    """
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)

    time_slices = list(get_time_slices(start_date, end_date))
    assert len(time_slices) == 12
    assert time_slices[0][0] == start_date
    assert time_slices[-1][1] == end_date

    for i, (curr_start, curr_end) in enumerate(reversed(time_slices[1:-1])):
        assert curr_start < curr_end, (
            "Splitter may have produced reversed time boundaries"
        )

        assert curr_start == datetime(curr_end.year, curr_end.month - 1, 1)


@pytest.mark.parametrize(
    ("application", "transform_fn"),
    [
        ("roms_marbl", RomsMarblTimeSplitter()),
        (Application.ROMS_MARBL.value, RomsMarblTimeSplitter()),
    ],
)
def test_roms_marbl_transform_registry(
    application: str, transform_fn: t.Callable[[Step], t.Iterable[Step]]
) -> None:
    """Verify that the transform registry returns the expected transform."""
    with mock.patch.dict(
        "cstar.orchestration.transforms.TRANSFORMS",
        {application: transform_fn},
        clear=True,
    ):
        transform = get_transforms(application)

    assert transform is not None
    assert isinstance(transform, RomsMarblTimeSplitter)


@pytest.mark.parametrize(
    "application",
    ["sleep", Application.SLEEP.value, "unknown-app-id"],
)
def test_sleep_transform_registry(application: str) -> None:
    """Verify that the transform registry returns no transforms for sleep or unknown applications.

    Confirm that querying the registry does not raise an exception.
    """
    transform = get_transforms(application)

    assert not transform


def test_splitter(single_step_workplan: Workplan) -> None:
    """Verify the splitter returns the expected steps for a given time range."""
    transform = RomsMarblTimeSplitter()

    step = LiveStep.from_step(single_step_workplan.steps[0])

    with mock.patch.dict(os.environ, {ENV_CSTAR_RUNID: "12345"}, clear=True):
        transformed_steps = list(transform(step))

    # one step transforms into 12 monthly steps
    assert len(transformed_steps) == 12

    output_directories: list[str] = []

    for i, step in enumerate(transformed_steps[:-1]):
        successor = transformed_steps[i + 1]
        runtime_params = t.cast("dict", step.blueprint_overrides)["runtime_params"]
        succ_runtime_params = t.cast("dict", successor.blueprint_overrides)[
            "runtime_params"
        ]

        # verify start and end dates are valid
        sd = runtime_params["start_date"]
        if not isinstance(sd, datetime):
            sd = datetime.strptime(sd, "%Y%m%d %H%M%S")
        ed = runtime_params["end_date"]
        if not isinstance(ed, datetime):
            ed = datetime.strptime(ed, "%Y%m%d %H%M%S")
        assert sd < ed

        output_dir = runtime_params["output_dir"]
        output_directories.append(output_dir)

        # verify each step uses output from the prior step as initial conditions
        if i > 0:
            succ_init_cond = successor.blueprint_overrides.get("initial_conditions", {})
            ic_loc_successor = succ_init_cond.get("data", [{}])[0].get("location", "")  # type: ignore[union-attr,index,operator]

            assert str(output_dir) in ic_loc_successor  # type: ignore[union-attr,index,operator]

            # verify the initial conditions reference the prior step's time slice
            compact_sd = ed.strftime("%Y%m%d%H%M%S")

            expected = f"output/{DEFAULT_OUTPUT_ROOT_NAME}_rst.{compact_sd}.000.nc"
            assert expected in str(ic_loc_successor)

        # verify successor starts right where current step ends
        sd_successor = succ_runtime_params["start_date"]
        # sd_successor = datetime.strptime(sd_successor_str, "%Y-%m-%d %H:%M:%S")
        assert sd_successor == ed

    # verify all output directories are unique
    assert len(output_directories) == len(set(output_directories))
