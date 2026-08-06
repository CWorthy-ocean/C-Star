import os
from pathlib import Path

from cstar.applications.cache_demo import (
    APP_NAME,
    CacheDemoBlueprint,
    CacheDemoRunner,
)
from cstar.applications.core import RunnerRequest, get_application
from cstar.base.env import ENV_CSTAR_CACHE_DISABLE
from cstar.entrypoint.config import ServiceConfiguration, get_job_config
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.serialization import serialize


def write_blueprint(tmp_path: Path, working_dir: Path) -> Path:
    blueprint = CacheDemoBlueprint(
        name="cache demo test",
        description="unit test blueprint",
        application=APP_NAME,
        working_dir=working_dir,
        dataset_name="test_basin",
        num_files=2,
        sleep_seconds=0.01,
    )
    bp_path = tmp_path / "cache_demo.yaml"
    serialize(bp_path, blueprint)
    return bp_path


async def run_blueprint(bp_path: Path) -> CacheDemoRunner:
    request = RunnerRequest(str(bp_path), CacheDemoBlueprint)
    runner = CacheDemoRunner(request, ServiceConfiguration(), get_job_config())
    await runner.execute()
    return runner


def test_application_is_registered() -> None:
    definition = get_application(APP_NAME)
    assert definition.blueprint is CacheDemoBlueprint
    assert definition.runner is CacheDemoRunner


async def test_first_run_generates_second_run_hits(tmp_path: Path) -> None:
    working_dir = tmp_path / "work"
    bp_path = write_blueprint(tmp_path, working_dir)

    first = await run_blueprint(bp_path)
    assert first.state.status == ExecutionStatus.COMPLETED

    output_dir = working_dir / "output"
    expected = [
        output_dir / "test_basin_summary.txt",
        output_dir / "tiles" / "test_basin_tile_000.dat",
        output_dir / "tiles" / "test_basin_tile_001.dat",
        output_dir / "test_basin_stats.json",
    ]
    for path in expected:
        assert path.exists(), f"missing output: {path}"
        assert path.is_symlink(), "cached outputs must be symlinks into the cache"

    # a second run against a fresh working dir is served entirely from cache
    working_dir_2 = tmp_path / "work2"
    bp_path_2 = write_blueprint(tmp_path / "bp2", working_dir_2)
    second = await run_blueprint(bp_path_2)
    assert second.state.status == ExecutionStatus.COMPLETED
    assert (working_dir_2 / "output" / "test_basin_summary.txt").is_symlink()


async def test_no_cache_run_produces_real_files(tmp_path: Path) -> None:
    working_dir = tmp_path / "work"
    bp_path = write_blueprint(tmp_path, working_dir)

    os.environ[ENV_CSTAR_CACHE_DISABLE] = "1"
    runner = await run_blueprint(bp_path)
    assert runner.state.status == ExecutionStatus.COMPLETED

    summary = working_dir / "output" / "test_basin_summary.txt"
    assert summary.exists()
    assert not summary.is_symlink()
