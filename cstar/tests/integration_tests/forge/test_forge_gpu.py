"""Tier 1: forge input generation for an experimental ``ucla_roms_gpu`` blueprint.

The ``gpu`` case runs ``process_forge_blueprint`` once against real roms-tools (no
ROMS build, no run) and the tests inspect the emitted ``ucla_roms_gpu`` blueprint, the
rendered namelist and the absence of a ``cppdefs.opt``.

Regenerate the goldens after an intentional change with
``UPDATE_GOLDEN=1 pytest cstar/tests/integration_tests/forge/test_forge_gpu.py``
(the namelist and the blueprint golden), review the diff, and commit it.
"""

import os
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import f90nml
import pytest
import yaml
from typer.testing import CliRunner

from cstar.applications.ucla_roms_gpu.models import UclaRomsGpuBlueprint
from cstar.cli.blueprint.check import app as check_app
from cstar.orchestration.serialization import load_blueprint
from cstar.roms.namelist import gpu_namelist_schema_for_ref, namelist_schema_for_ref
from cstar.roms.precheck import (
    check_gpu_array_sizes,
    check_gpu_features,
    check_gpu_ignored_settings,
)
from cstar.tests.integration_tests.forge.test_forge_inputs import (
    Run,
    _generate,
    _normalize,
)

if TYPE_CHECKING:
    from cstar.applications.forge.blueprint import ForgeBlueprint

pytestmark = pytest.mark.requires_tools("nccopy")  # converts the input files to CDF-5

FIXTURES = Path(__file__).parent / "fixtures"
GPU_REF = "v0.1.3-beta"
NAMELIST_REF = "0.11.0"
"""The ucla-roms namelist tier roms-marbl-gpu v0.1.3-beta reads."""


@pytest.fixture(scope="module")
def run(
    tmp_path_factory: pytest.TempPathFactory,
    forge_blueprint_factory: Callable[..., tuple["ForgeBlueprint", Path]],
) -> Run:
    """One generation of the ``gpu`` case, shared by every test of this module."""
    return _generate("gpu", tmp_path_factory.mktemp("gpu"), forge_blueprint_factory)


def _namelist_path(run: Run) -> Path:
    return run.working_dir / "builds" / "run-time" / "namelist.nml"


def _check_golden(actual: str, golden: Path, regen_hint: str) -> None:
    """Compare ``actual`` with a golden file; ``UPDATE_GOLDEN=1`` rewrites it and fails."""
    if os.environ.get("UPDATE_GOLDEN"):
        golden.write_text(actual)
        pytest.fail(
            f"UPDATE_GOLDEN=1: wrote {golden}. Review the diff and commit it, "
            "then rerun without UPDATE_GOLDEN to confirm the test passes."
        )
    assert actual == golden.read_text(), (
        f"Rendered output drifted from {golden}. If intentional, regenerate with "
        f"{regen_hint}, review the diff and commit it."
    )


class TestGpuBlueprint:
    """The emitted ``ucla_roms_gpu`` blueprint."""

    def test_blueprint_loads(self, run: Run) -> None:
        bp = load_blueprint(run.blueprint_path, UclaRomsGpuBlueprint)
        assert bp.application == "ucla_roms_gpu"
        assert (bp.tiling.n_procs_x, bp.tiling.n_procs_y) == (2, 2)
        assert bp.tiling.nodes == 1
        assert bp.tiling.writers_per_node == 1
        assert bp.code.roms.commit == GPU_REF

    def test_code_has_only_roms_and_run_time(self, run: Run) -> None:
        raw = yaml.safe_load(run.blueprint_path.read_text())
        assert set(raw["code"]) == {"roms", "run_time"}

    def test_compute_needs(self, run: Run) -> None:
        needs = load_blueprint(run.blueprint_path, UclaRomsGpuBlueprint).compute_needs
        assert (needs.tasks, needs.gpu_tasks, needs.nodes) == (5, 4, 1)

    def test_blueprint_check_cli(self, run: Run) -> None:
        result = CliRunner().invoke(check_app, [str(run.blueprint_path)])
        assert result.exit_code == 0, result.output

    def test_matches_golden(self, run: Run) -> None:
        raw = yaml.safe_load(run.blueprint_path.read_text())
        raw.pop("provenance")  # timestamps and run ids
        text = _normalize(
            yaml.safe_dump(raw, sort_keys=False, default_flow_style=False),
            run.working_dir,
        )
        _check_golden(
            text,
            FIXTURES / "golden_blueprint_gpu.yaml",
            "UPDATE_GOLDEN=1 pytest cstar/tests/integration_tests/forge/"
            "test_forge_gpu.py",
        )


class TestGpuRenderedFiles:
    """What the executor wrote beside the blueprint."""

    def test_no_cppdefs_rendered(self, run: Run) -> None:
        compile_time = run.working_dir / "builds" / "compile-time"
        assert not compile_time.exists() or not list(compile_time.iterdir())

    def test_namelist_and_marbl_in_rendered(self, run: Run) -> None:
        run_time = run.working_dir / "builds" / "run-time"
        assert (run_time / "namelist.nml").is_file()
        assert (run_time / "marbl_in").is_file()


class TestGpuNamelist:
    """The rendered ``namelist.nml`` on the ucla-roms 0.11.0 tier."""

    def test_schema_and_checks(self, run: Run) -> None:
        assert gpu_namelist_schema_for_ref(GPU_REF) is namelist_schema_for_ref(
            NAMELIST_REF
        )
        nml = gpu_namelist_schema_for_ref(GPU_REF).read(_namelist_path(run))
        assert nml.time_stepping.dt > 0
        check_gpu_features(nml)
        check_gpu_ignored_settings(nml)
        check_gpu_array_sizes(nml)

    def test_raw_namelist_parses(self, run: Run) -> None:
        raw = f90nml.read(str(_namelist_path(run)))
        assert raw["param_settings"]["np_xi"] == 2
        assert raw["param_settings"]["np_eta"] == 2

    def test_matches_golden(self, run: Run) -> None:
        normalized = _normalize(_namelist_path(run).read_text(), run.working_dir)
        _check_golden(
            normalized,
            FIXTURES / "golden_namelist_gpu.nml",
            "UPDATE_GOLDEN=1 pytest cstar/tests/integration_tests/forge/"
            "test_forge_gpu.py",
        )
