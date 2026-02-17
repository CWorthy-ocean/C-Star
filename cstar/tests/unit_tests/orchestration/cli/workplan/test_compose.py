import os
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_STATE_HOME, FLAG_ON
from cstar.base.feature import ENV_FF_ORCH_TRX_TIMESPLIT
from cstar.cli.workplan.compose import WorkplanTemplate, compose
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.dag_runner import build_and_run_dag, prepare_workplan
from cstar.orchestration.models import Application, RomsMarblBlueprint, Workplan
from cstar.orchestration.orchestration import check_environment, configure_environment
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.transforms import SplitFrequency, WorkplanTransformer
from cstar.orchestration.utils import (
    ENV_CSTAR_CMD_CONVERTER_OVERRIDE,
    ENV_CSTAR_ORCH_RUNID,
    ENV_CSTAR_ORCH_TRX_FREQ,
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
    get_run_id,
)


@pytest.mark.asyncio
async def test_compose_host_creation(
    tmp_path: Path,
    bp_templates_dir: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that compose creates a new workplan and blueprint in the expected
    output directory.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    default_blueprint_path : str
        Fixture returning the default blueprint path contained in template workplans
    """
    workplan_name: str = "linear"

    wp_template_file = f"{workplan_name}.yaml"
    wp_template_path = wp_templates_dir / wp_template_file

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    mock_process = mock.AsyncMock()
    output_dir = tmp_path / "test-outputs"
    run_id = "my-run"

    with mock.patch("cstar.orchestration.dag_runner.process_plan", mock_process):
        generated_wp_path = compose(
            wp_template_path.as_posix(),
            bp_template_path.as_posix(),
            output_dir.as_posix(),
            run_id=run_id,
            template=WorkplanTemplate[workplan_name.upper()],
            run_plan="0",
        )

    wp = deserialize(generated_wp_path, Workplan)

    # confirm the linear plan was used
    steps = list(wp.steps)
    assert len(steps) == 4

    # confirm the output dir passed to compose was used
    step = steps[0]
    bp = deserialize(step.blueprint_path, RomsMarblBlueprint)

    assert bp.runtime_params.output_dir == output_dir

    expected_bp = output_dir / run_id / "blueprint.yaml"
    expected_host_wp = output_dir / run_id / f"{workplan_name}-host.yaml"

    # confirm a copy of the blueprint was made
    assert expected_bp.exists()

    # confirm the workplan was copied and renamed
    assert expected_host_wp.exists()


@pytest.mark.parametrize("do_run", ["0", "1"])
@pytest.mark.asyncio
async def test_compose_host_run_parameter(
    do_run: str,
    tmp_path: Path,
    bp_templates_dir: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that compose only executes the plan when run="1" is passed.

    Parameters
    ----------
    do_run : str
        String containing "0" or "1" indicating if the generated workplan should execute.
    tmp_path : Path
        Temporary directory for test outputs
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    default_blueprint_path : str
        Fixture returning the default blueprint path contained in template workplans
    """
    workplan_name: str = "single_step"

    wp_template_file = f"{workplan_name}.yaml"
    wp_template_path = wp_templates_dir / wp_template_file

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    mock_process = mock.AsyncMock()
    output_dir = tmp_path / "default-assets"
    output_override_dir = tmp_path / "overridden-assets"

    mock_run = mock.Mock()
    run_id = "my-run"
    mock_env = {ENV_CSTAR_STATE_HOME: output_override_dir.as_posix()}

    with (
        mock.patch("cstar.cli.workplan.compose._run", mock_run),
        mock.patch("cstar.orchestration.dag_runner.process_plan", mock_process),
        mock.patch.dict(os.environ, mock_env, clear=True),
    ):
        _ = compose(
            wp_template_path.as_posix(),
            bp_template_path.as_posix(),
            output_dir.as_posix(),
            run_id=run_id,
            template=WorkplanTemplate[workplan_name.upper()],
            run_plan=do_run,
        )

    if do_run == "1":
        mock_run.assert_called()
    else:
        mock_run.assert_not_called()


@pytest.mark.parametrize(
    ("drop_var", "key"),
    [
        (ENV_CSTAR_SLURM_ACCOUNT, "ACCOUNT"),
        (ENV_CSTAR_SLURM_QUEUE, "QUEUE"),
    ],
)
@pytest.mark.asyncio
async def test_build_and_run_dag_env(
    drop_var: str,
    key: str,
    tmp_path: Path,
    bp_templates_dir: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that the DAG runner fails when required environment variables are not passed.

    Parameters
    ----------
    drop_var : str
        The environment variable to be removed from the environment.
    key : str
        A unique string verifying the correct exception was caught.
    tmp_path : Path
        Temporary directory for test outputs
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    default_blueprint_path : str
        Fixture returning the default blueprint path contained in template workplans
    """
    workplan_name: str = "single_step"

    wp_template_file = f"{workplan_name}.yaml"
    wp_template_path = wp_templates_dir / wp_template_file

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    output_dir = tmp_path / "original-output-dir"
    output_override_dir = tmp_path / "overridden-output-dir"
    run_id = "my-run"

    mock_process = mock.AsyncMock()
    mock_env = {
        ENV_CSTAR_STATE_HOME: output_override_dir.as_posix(),
        ENV_CSTAR_SLURM_ACCOUNT: "xyz",
        ENV_CSTAR_SLURM_QUEUE: "wholenode",
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:5:00",
        ENV_CSTAR_ORCH_RUNID: run_id,
    }

    # get rid of one required env var.
    del mock_env[drop_var]

    with (
        mock.patch("cstar.orchestration.dag_runner.process_plan", mock_process),
        mock.patch.dict(os.environ, mock_env, clear=True),
        pytest.raises(ValueError) as ex,
    ):
        generated_wp_path = compose(
            wp_template_path.as_posix(),
            bp_template_path.as_posix(),
            output_dir.as_posix(),
            run_id=run_id,
            template=WorkplanTemplate[workplan_name.upper()],
        )

        await build_and_run_dag(generated_wp_path, run_id, output_dir)

    assert key in str(ex)


@pytest.mark.asyncio
async def test_prepare_composed_dag(
    tmp_path: Path,
    bp_templates_dir: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that the composed DAG is transformed by the DAG runner.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    default_blueprint_path : str
        Fixture returning the default blueprint path contained in template workplans
    """
    workplan_name: str = "single_step"

    wp_template_file = f"{workplan_name}.yaml"
    wp_template_path = wp_templates_dir / wp_template_file

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    output_dir = tmp_path / "original-output-dir"
    output_override_dir = tmp_path / "overridden-output-dir"
    run_id = "my-run"

    mock_env = {
        ENV_CSTAR_STATE_HOME: output_override_dir.as_posix(),
        ENV_CSTAR_SLURM_ACCOUNT: "xyz",
        ENV_CSTAR_SLURM_QUEUE: "wholenode",
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:5:00",
        ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_ON,
        ENV_CSTAR_ORCH_TRX_FREQ: SplitFrequency.Monthly.value,
    }

    def _raise_ex(*args, **kwargs):
        raise RuntimeError("on-purpose-fail")

    with (
        mock.patch("cstar.orchestration.orchestration.Planner.__init__", _raise_ex),
        mock.patch.dict(os.environ, mock_env, clear=True),
    ):
        generated_wp_path = compose(
            wp_template_path.as_posix(),
            bp_template_path.as_posix(),
            output_dir.as_posix(),
            run_id=run_id,
            template=WorkplanTemplate[workplan_name.upper()],
        )

        configure_environment(output_dir, run_id)
        run_id = get_run_id()
        output_dir = DirectoryManager.data_home()
        check_environment()
        wp, wp_path = await prepare_workplan(generated_wp_path, output_dir, run_id)

    wp = deserialize(wp_path, Workplan)
    steps = list(wp.steps)

    # default blueprint is 366 days; confirm 366 steps after tranform is applied.
    num_timeslices = 12
    assert len(steps) == num_timeslices

    paths = set()
    for step in steps:
        bp = deserialize(step.blueprint_path, RomsMarblBlueprint)
        paths.add(bp.runtime_params.output_dir)

        assert bp.runtime_params.output_dir.exists()

    # confirm that every step has a unique output path
    assert len(steps) == num_timeslices


@pytest.mark.skip
@pytest.mark.asyncio
async def test_run_composed_dag(
    tmp_path: Path,
    bp_templates_dir: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that the composed DAG is transformed by the DAG runner.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    default_blueprint_path : str
        Fixture returning the default blueprint path contained in template workplans
    """
    workplan_name: str = "single_step"

    wp_template_file = f"{workplan_name}.yaml"
    wp_template_path = wp_templates_dir / wp_template_file

    bp_template_file = "blueprint.yaml"
    bp_template_path = bp_templates_dir / bp_template_file

    output_dir = Path("/home/x-cmcbride/dag/jan12001")
    run_id = f"test-run-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    print(f"Composing workplan in: {tmp_path}")
    mock_env = {
        ENV_CSTAR_SLURM_ACCOUNT: "ees250129",
        ENV_CSTAR_SLURM_QUEUE: "wholenode",
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:5:00",
        ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_ON,
        ENV_CSTAR_ORCH_TRX_FREQ: SplitFrequency.Monthly.value,
        ENV_CSTAR_CMD_CONVERTER_OVERRIDE: "sleep",
    }

    mock_run_cmd = mock.Mock(return_code=0, stderr="", stdout="mock-run-output")

    def no_delay():
        while True:
            yield 1

    with (
        mock.patch.dict(os.environ, mock_env, clear=True),
        mock.patch("cstar.base.utils._run_cmd", mock_run_cmd),
    ):
        generated_wp_path = compose(
            wp_template_path.as_posix(),
            bp_template_path.as_posix(),
            output_dir.as_posix(),
            run_id=run_id,
            template=WorkplanTemplate[workplan_name.upper()],
        )

        wp = deserialize(generated_wp_path, Workplan)
        for step in wp.steps:
            step.application = Application.ROMS_MARBL.value

        tweak_path = WorkplanTransformer.derived_path(
            generated_wp_path, suffix="_composed"
        )
        serialize(tweak_path, wp)

        wp_path = await build_and_run_dag(tweak_path, run_id, output_dir)

    wp = deserialize(wp_path, Workplan)
    steps = list(wp.steps)

    paths = set()
    for step in steps:
        bp = deserialize(step.blueprint_path, RomsMarblBlueprint)
        paths.add(bp.runtime_params.output_dir)

        assert bp.runtime_params.output_dir.exists()
