import typing as t
from datetime import datetime
from pathlib import Path

import pytest

from cstar.orchestration.models import Application, Step, Workplan
from cstar.orchestration.transforms import (
    RomsMarblTimeSplitter,
    get_time_slices,
    get_transform,
)


@pytest.fixture
def single_step_workplan(tmp_path: Path) -> Workplan:
    """Generate a workplan."""
    bp_tpl_path = (
        Path(__file__).parent.parent.parent.parent
        / "additional_files/templates/bp"
        / "blueprint.yaml"
    )
    default_output_dir: t.Literal["output_dir: ."] = "output_dir: ."

    bp_path = tmp_path / "blueprint.yaml"
    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(default_output_dir, tmp_path.as_posix())
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


def test_time_splitting():
    """Verify that the time splitting function honors the start and end dates.

    Ensures that no assumptions are made about the 1st and last days of the month.
    """
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)

    time_slices = get_time_slices(start_date, end_date)
    assert len(time_slices) == 12
    assert time_slices[0][0] == start_date
    assert time_slices[-1][1] == end_date

    for i, (curr_start, curr_end) in enumerate(reversed(time_slices[1:-1])):
        assert curr_start < curr_end, (
            "Splitter may have produced reversed time boundaries"
        )

        assert curr_start == datetime(curr_end.year, curr_end.month - 1, 1)


@pytest.mark.parametrize(
    "application",
    [
        "roms_marbl",
        Application.ROMS_MARBL.value,
    ],
)
def test_roms_marbl_transform_registry(application: str):
    """Verify that the transform registry returns the expected transform."""
    transform = get_transform(application)

    assert transform is not None
    assert isinstance(transform, RomsMarblTimeSplitter)


@pytest.mark.parametrize(
    "application",
    ["sleep", Application.SLEEP.value, "unknown-app-id"],
)
def test_sleep_transform_registry(application: str):
    """Verify that the transform registry returns no transforms for sleep or unknown applications.

    Confirm that querying the registry does not raise an exception.
    """
    transform = get_transform(application)

    assert not transform


@pytest.mark.parametrize(
    "application",
    [
        Application.ROMS_MARBL.value,
    ],
)
def test_roms_marbl_splitter(application: str):
    """Verify that the transform registry returns the expected transform."""
    transform = get_transform(application)

    assert transform is not None
    assert isinstance(transform, RomsMarblTimeSplitter)


def test_splitter(single_step_workplan: Workplan) -> None:
    """Verify the splitter returns the expected steps for a given time range."""
    transform = RomsMarblTimeSplitter()

    step = single_step_workplan.steps[0]
    transformed_steps = list(transform(step))

    # one step transforms into 12 monthly steps
    assert len(transformed_steps) == 12

    output_directories: list[str] = []
    # expected_stem = "grid"

    for i, step in enumerate(transformed_steps[:-1]):
        successor = transformed_steps[i + 1]
        runtime_params = t.cast(dict, step.blueprint_overrides)["runtime_params"]
        succ_runtime_params = t.cast(dict, successor.blueprint_overrides)[
            "runtime_params"
        ]

        # verify start and end dates are valid
        sd_str = runtime_params["start_date"]
        ed_str = runtime_params["end_date"]
        sd = datetime.strptime(sd_str, "%Y-%m-%d %H:%M:%S")
        ed = datetime.strptime(ed_str, "%Y-%m-%d %H:%M:%S")
        assert sd < ed

        output_dir = runtime_params["output_dir"]
        output_directories.append(output_dir)

        # verify each step uses output from the prior step as initial conditions
        if i > 0:
            ic_successor = succ_runtime_params["initial_conditions"]["location"]
            assert str(output_dir) in ic_successor

            # verify the initial conditions reference the prior step's time slice
            compact_sd = sd.strftime("%Y%m%d%H%M%S")
            # assert f"{expected_stem}_rst.{compact_sd}.*.nc" in ic_successor
            assert f"outputs/.*_rst.{compact_sd}.*.nc" in ic_successor
            # TODO: replace this assert after completing testing
            # assert f"_rst.*.nc" in ic_successor

        # verify successor starts right where current step ends
        sd_successor_str = succ_runtime_params["start_date"]
        sd_successor = datetime.strptime(sd_successor_str, "%Y-%m-%d %H:%M:%S")
        assert sd_successor == ed

    # verify all output directories are unique
    assert len(output_directories) == len(set(output_directories))
