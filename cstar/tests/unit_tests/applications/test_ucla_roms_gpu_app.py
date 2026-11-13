"""The `ucla_roms_gpu` application: its registration, runner hooks, declared
artifacts and resume.
"""

import sys
import types
import typing as t
from contextlib import AbstractContextManager
from pathlib import Path
from unittest import mock

import pytest
import xarray as xr
import yaml

from cstar.applications.core import (
    ArtifactKind,
    RunnerRequest,
    get_application,
)
from cstar.applications.ucla_roms.app import (
    RomsSimulationRunner,
    UclaRomsApplication,
    UclaRomsRunner,
)
from cstar.applications.ucla_roms.transforms import (
    CarbonateSensitivityDirective,
    ContinuanceDirective,
    NestingDirective,
)
from cstar.applications.ucla_roms_gpu.app import (
    UclaRomsGpuApplication,
    UclaRomsGpuRunner,
)
from cstar.applications.ucla_roms_gpu.models import (
    APP_NAME,
    TOOLCHAIN,
    UclaRomsGpuBlueprint,
)
from cstar.applications.ucla_roms_gpu.resume import prepare_resume_blueprint
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.catalog.domain_catalog import DomainCatalog
from cstar.entrypoint.config import ServiceConfiguration
from cstar.orchestration.manifest import expand_artifacts
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import allowed_directives
from cstar.roms.file_system import RomsFileSystemManager
from cstar.roms.gpu_external_codebase import ROMSGpuExternalCodeBase
from cstar.roms.gpu_simulation import GPU_BUILD_DIR_NAME, GPU_EXECUTABLE_NAME
from cstar.roms.outputs import OUTPUT_STREAMS

TEMPLATE_NAME = "blueprint.1.0.0.yaml"
"""The file name of the template for schema 1.0.0 of the blueprint."""

STAMP = "20200601000000"
"""The timestamp of the restart a resumed run continues from."""

RESUME_LOGGER_NAME = "cstar.applications.ucla_roms.resume"
"""The logger that reports a resume search, which every ROMS application shares."""


class TestRegistration:
    def test_application_is_discovered_by_name(self) -> None:
        app = get_application(APP_NAME)

        assert isinstance(app, UclaRomsGpuApplication)
        assert app.name == "ucla_roms_gpu"
        assert app.long_name == "UCLA-ROMS GPU simulation runner"
        assert app.runner is UclaRomsGpuRunner
        assert app.blueprint is UclaRomsGpuBlueprint

    def test_application_declares_the_gpu_toolchain(self) -> None:
        app = get_application(APP_NAME)

        assert app.toolchain == "gpu"
        assert app.toolchain == TOOLCHAIN

    def test_application_declares_its_schema_and_capabilities(self) -> None:
        app = get_application(APP_NAME)

        assert app.schema_version == "1.0.1"
        assert app.resumable
        assert app.pre_runnable
        assert app.migrations is None

    def test_directives_are_the_roms_directives(self) -> None:
        app = get_application(APP_NAME)

        assert set(app.directives) == {
            ContinuanceDirective,
            NestingDirective,
            CarbonateSensitivityDirective,
        }

    def test_no_transform_is_applicable(self) -> None:
        """`RomsTimeSplitter` rebuilds a ucla_roms blueprint, so it is not listed."""
        assert tuple(get_application(APP_NAME).applicable_transforms) == ()

    def test_directive_keys_are_allowed_for_its_steps(self) -> None:
        allowed = allowed_directives(APP_NAME)

        assert set(allowed) == {
            "apply-overrides",
            ContinuanceDirective.key(),
            NestingDirective.key(),
            CarbonateSensitivityDirective.key(),
        }

    def test_ucla_roms_is_not_replaced(self) -> None:
        gpu = get_application(APP_NAME)
        cpu = get_application("ucla_roms")

        assert isinstance(cpu, UclaRomsApplication)
        assert not isinstance(gpu, UclaRomsApplication)
        assert cpu.runner is UclaRomsRunner
        assert cpu.toolchain == "cpu"
        assert gpu.runner is not cpu.runner

    def test_earlier_ucla_roms_name_still_resolves_to_ucla_roms(self) -> None:
        assert isinstance(get_application("roms_marbl"), UclaRomsApplication)


class TestArtifacts:
    def test_declared_specs(self) -> None:
        app = get_application(APP_NAME)

        specs = app.artifacts(app.blueprint.model_construct())

        assert [(s.kind.value, s.pattern, s.metadata) for s in specs] == [
            *(
                (kind.value, f"output/{stem}.*.nc", dict(metadata))
                for stem, kind, metadata in OUTPUT_STREAMS
            ),
            ("output", "output/*.nc", {}),
            ("config", "work/cstar_generated_roms.nml", {"role": "namelist"}),
            ("config", "input/runtime_code/marbl_in", {"role": "marbl_in"}),
            ("build", "input/codebases/roms/build_gpu/roms_gpu_cuda", {}),
            ("log", "logs/*.out", {}),
        ]

    def test_executable_is_the_gpu_build(self) -> None:
        app = get_application(APP_NAME)

        specs = app.artifacts(app.blueprint.model_construct())

        (build,) = (s for s in specs if s.kind is ArtifactKind.build)
        assert build.pattern == "input/codebases/roms/build_gpu/roms_gpu_cuda"

    def test_no_cppdefs_is_declared(self) -> None:
        app = get_application(APP_NAME)

        specs = app.artifacts(app.blueprint.model_construct())

        assert not any("cppdefs" in s.pattern for s in specs)
        assert not any(s.metadata.get("role") == "cppdefs" for s in specs)

    def test_declares_each_file_the_run_leaves(self, tmp_path: Path) -> None:
        for name in (
            f"output/output_rst.{STAMP}.nc",
            f"output/output_his.{STAMP}.nc",
            f"output/output_bgc_his.{STAMP}.nc",
            f"output/output_cdr.{STAMP}.nc",
            "work/cstar_generated_roms.nml",
            "input/runtime_code/marbl_in",
            "input/codebases/roms/build_gpu/roms_gpu_cuda",
            "logs/run.out",
            # not products: partitioned pieces, inputs and a ucla_roms build
            f"temp_output/output_rst.{STAMP}.000.nc",
            "input/input_datasets/x.nc",
            "input/compile_time_code/roms",
            "input/compile_time_code/cppdefs.opt",
        ):
            path = tmp_path / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"x")
        app = get_application(APP_NAME)

        found = {
            p.path: (p.kind, p.metadata)
            for p in expand_artifacts(
                tmp_path, app.artifacts(app.blueprint.model_construct())
            )
        }

        assert found == {
            f"output/output_rst.{STAMP}.nc": ("restart", {}),
            f"output/output_his.{STAMP}.nc": ("history", {}),
            f"output/output_bgc_his.{STAMP}.nc": ("output", {}),
            f"output/output_cdr.{STAMP}.nc": ("cdr_output", {}),
            "work/cstar_generated_roms.nml": ("config", {"role": "namelist"}),
            "input/runtime_code/marbl_in": ("config", {"role": "marbl_in"}),
            "input/codebases/roms/build_gpu/roms_gpu_cuda": ("build", {}),
            "logs/run.out": ("log", {}),
        }

    def test_executable_is_in_the_clone_the_simulation_builds(self) -> None:
        """The declared path is in the layout's directory for the GPU codebase, at
        the build directory and executable name the simulation uses.
        """
        root = Path("/step")
        key = ROMSGpuExternalCodeBase().key
        executable = (
            RomsFileSystemManager(root).codebase_subdir(key)
            / GPU_BUILD_DIR_NAME
            / GPU_EXECUTABLE_NAME
        )

        app = get_application(APP_NAME)
        (build,) = (
            s
            for s in app.artifacts(app.blueprint.model_construct())
            if s.kind is ArtifactKind.build
        )
        assert root / build.pattern == executable


def _stub_gpu_simulation_module(
    simulation_class: mock.Mock,
) -> AbstractContextManager[t.Any]:
    """Stand in for `cstar.roms.gpu_simulation`, naming `simulation_class`."""
    module = types.ModuleType("cstar.roms.gpu_simulation")
    module.ROMSGpuSimulation = simulation_class  # type: ignore[attr-defined]
    return mock.patch.dict(sys.modules, {module.__name__: module})


@pytest.fixture
def gpu_blueprint_dict(bp_templates_dir: Path, tmp_path: Path) -> dict[str, t.Any]:
    """The template blueprint, its working directory in the test's temp directory."""
    data = yaml.safe_load((bp_templates_dir / APP_NAME / TEMPLATE_NAME).read_text())
    data["working_dir"] = str(tmp_path / "run")
    return data


@pytest.fixture
def gpu_blueprint_path(gpu_blueprint_dict: dict[str, t.Any], tmp_path: Path) -> Path:
    """The path of the template blueprint written to the test's temp directory."""
    path = tmp_path / "blueprint.yaml"
    path.write_text(yaml.safe_dump(gpu_blueprint_dict))
    return path


def _runner_arguments(
    blueprint_path: Path, *, resume: bool = False, pre_run: bool = False
) -> tuple[RunnerRequest[UclaRomsGpuBlueprint], ServiceConfiguration]:
    """Build the arguments of a `UclaRomsGpuRunner` for a blueprint."""
    request = RunnerRequest(
        str(blueprint_path), UclaRomsGpuBlueprint, resume=resume, pre_run=pre_run
    )
    service_config = ServiceConfiguration(
        as_service=False, loop_delay=0, health_check_frequency=0, name="gpu-runner"
    )
    return request, service_config


class TestRunner:
    def test_runner_is_a_roms_simulation_runner(self) -> None:
        assert issubclass(UclaRomsGpuRunner, RomsSimulationRunner)
        assert not issubclass(UclaRomsGpuRunner, UclaRomsRunner)

    def test_simulation_class_is_resolved_when_asked(self) -> None:
        """The runner names `ROMSGpuSimulation` of `cstar.roms.gpu_simulation`."""
        simulation_class = mock.Mock()

        with _stub_gpu_simulation_module(simulation_class):
            assert UclaRomsGpuRunner._simulation_class() is simulation_class

    def test_simulation_class_is_the_gpu_simulation(self) -> None:
        gpu_simulation = pytest.importorskip("cstar.roms.gpu_simulation")
        from cstar.roms.simulation import ROMSSimulation

        simulation_class = UclaRomsGpuRunner._simulation_class()

        assert simulation_class is gpu_simulation.ROMSGpuSimulation
        assert issubclass(simulation_class, ROMSSimulation)
        assert simulation_class is not ROMSSimulation

    @pytest.mark.parametrize("resume", [False, True])
    def test_init_builds_the_simulation_from_the_blueprint(
        self, gpu_blueprint_path: Path, resume: bool
    ) -> None:
        """A resume request derives the blueprint through this package's
        `prepare_resume_blueprint` and builds from the derived path.
        """
        resumed_uri = str(
            gpu_blueprint_path.with_stem(f"{gpu_blueprint_path.stem}.resume")
        )
        simulation_class = mock.Mock()
        simulation_class.from_blueprint.return_value.name = "My GPU Simulation"
        request, service_config = _runner_arguments(gpu_blueprint_path, resume=resume)

        with (
            mock.patch(
                "cstar.applications.ucla_roms_gpu.app.prepare_resume_blueprint",
                return_value=resumed_uri,
            ) as mock_prepare,
            _stub_gpu_simulation_module(simulation_class),
        ):
            runner = UclaRomsGpuRunner(request, service_config)

        simulation_class.check_launch_location.assert_called_once_with()
        expected_uri = resumed_uri if resume else str(gpu_blueprint_path)
        simulation_class.from_blueprint.assert_called_once_with(expected_uri)
        if resume:
            mock_prepare.assert_called_once_with(str(gpu_blueprint_path))
        else:
            mock_prepare.assert_not_called()
        assert runner.simulation is simulation_class.from_blueprint.return_value
        assert runner.simulation.name == "my-gpu-simulation"
        assert request.blueprint_uri == str(gpu_blueprint_path)

    def test_resume_hook_derives_the_blueprint_with_this_packages_search(
        self, gpu_blueprint_path: Path
    ) -> None:
        with mock.patch(
            "cstar.applications.ucla_roms_gpu.app.prepare_resume_blueprint",
            return_value="derived.yaml",
        ) as mock_prepare:
            derived = UclaRomsGpuRunner._resume_blueprint(str(gpu_blueprint_path))

        assert derived == "derived.yaml"
        mock_prepare.assert_called_once_with(str(gpu_blueprint_path))

    def test_pre_run_skips_the_launch_check(self, gpu_blueprint_path: Path) -> None:
        simulation_class = mock.Mock()
        simulation_class.from_blueprint.return_value.name = "sim"
        request, service_config = _runner_arguments(gpu_blueprint_path, pre_run=True)

        with _stub_gpu_simulation_module(simulation_class):
            UclaRomsGpuRunner(request, service_config)

        simulation_class.check_launch_location.assert_not_called()
        simulation_class.from_blueprint.assert_called_once_with(str(gpu_blueprint_path))

    def test_launch_check_refusal_stops_the_runner_before_it_builds(
        self, gpu_blueprint_path: Path
    ) -> None:
        simulation_class = mock.Mock()
        simulation_class.check_launch_location.side_effect = CstarError("refused")
        request, service_config = _runner_arguments(gpu_blueprint_path, resume=True)

        with (
            mock.patch(
                "cstar.applications.ucla_roms_gpu.app.prepare_resume_blueprint"
            ) as mock_prepare,
            _stub_gpu_simulation_module(simulation_class),
            pytest.raises(CstarError, match="refused"),
        ):
            UclaRomsGpuRunner(request, service_config)

        mock_prepare.assert_not_called()
        simulation_class.from_blueprint.assert_not_called()


def _write_netcdf(path: Path) -> None:
    """Write a tiny, real netCDF file at `path`, creating parents as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    xr.Dataset({"ocean_time": ("time", [0.0])}).to_netcdf(path)


def _restart_name(stamp: str, piece: str = "") -> str:
    """The name of a restart file: whole, or one piece when `piece` is given."""
    return f"output_rst.{stamp}.{piece}.nc" if piece else f"output_rst.{stamp}.nc"


@pytest.fixture
def run_fs(
    gpu_blueprint_path: Path,
) -> RomsFileSystemManager:
    """The file layout of the template blueprint's working directory."""
    bp = deserialize(gpu_blueprint_path, UclaRomsGpuBlueprint)
    return RomsFileSystemManager(bp.effective_working_dir)


class TestPrepareResumeBlueprint:
    def test_continues_from_a_whole_restart_in_output(
        self, gpu_blueprint_path: Path, run_fs: RomsFileSystemManager
    ) -> None:
        whole = run_fs.output_dir / _restart_name(STAMP)
        _write_netcdf(whole)
        before = gpu_blueprint_path.read_bytes()

        result = prepare_resume_blueprint(str(gpu_blueprint_path))

        assert Path(result) == run_fs.run_dir / f"{gpu_blueprint_path.stem}.resume.yaml"
        resumed = deserialize(result, UclaRomsGpuBlueprint)
        ic = resumed.initial_conditions.data[0]
        assert Path(str(ic.location)) == whole.resolve()
        assert ic.partitioned is False
        assert resumed.runtime_params.start_date.strftime("%Y%m%d%H%M%S") == STAMP
        assert (
            resumed.tiling
            == deserialize(gpu_blueprint_path, UclaRomsGpuBlueprint).tiling
        )
        assert gpu_blueprint_path.read_bytes() == before

    def test_newest_readable_restart_wins(
        self, gpu_blueprint_path: Path, run_fs: RomsFileSystemManager
    ) -> None:
        """A restart a killed run left unreadable is skipped for an older one."""
        older, newest = "20200501000000", "20200701000000"
        _write_netcdf(run_fs.output_dir / _restart_name(older))
        (run_fs.output_dir / _restart_name(newest)).write_bytes(b"not netcdf")

        result = prepare_resume_blueprint(str(gpu_blueprint_path))

        resumed = deserialize(result, UclaRomsGpuBlueprint)
        assert resumed.runtime_params.start_date.strftime("%Y%m%d%H%M%S") == older

    def test_partitioned_pieces_in_temp_output_are_not_a_restart(
        self,
        gpu_blueprint_path: Path,
        run_fs: RomsFileSystemManager,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The GPU run never writes pieces, so a resume does not look for them."""
        for piece in ("000", "001", "002", "003"):
            _write_netcdf(run_fs.temp_output_dir / _restart_name(STAMP, piece))

        with caplog.at_level("WARNING", logger=RESUME_LOGGER_NAME):
            result = prepare_resume_blueprint(str(gpu_blueprint_path))

        assert result == str(gpu_blueprint_path)
        assert any("No usable restart found" in r.message for r in caplog.records)

    def test_no_restart_returns_the_blueprint_unchanged(
        self, gpu_blueprint_path: Path
    ) -> None:
        assert prepare_resume_blueprint(str(gpu_blueprint_path)) == str(
            gpu_blueprint_path
        )

    def test_restart_at_the_end_date_raises(
        self, gpu_blueprint_path: Path, run_fs: RomsFileSystemManager
    ) -> None:
        """The template ends at 2021-01-01: a restart there means the run finished."""
        _write_netcdf(run_fs.output_dir / _restart_name("20210101000000"))

        with pytest.raises(CstarExpectationFailed, match="appears to have finished"):
            prepare_resume_blueprint(str(gpu_blueprint_path))


class TestBundledCatalog:
    @pytest.fixture
    def bundled(self) -> DomainCatalog:
        return DomainCatalog(read_only=True)

    def test_lists_the_gpu_example_under_its_application(
        self, bundled: DomainCatalog
    ) -> None:
        assert bundled.blueprint_names(APP_NAME) == ["wales-toy-gpu"]
        path = bundled.blueprint_path(APP_NAME, "wales-toy-gpu")
        assert path.name == "wales-toy-gpu.yaml" and path.is_file()
        assert APP_NAME in bundled.blueprint_applications

    def test_ucla_roms_example_is_not_listed_under_the_gpu_application(
        self, bundled: DomainCatalog
    ) -> None:
        assert "wales-toy" in bundled.blueprint_names("ucla_roms")
        assert "wales-toy" not in bundled.blueprint_names(APP_NAME)

    def test_reports_no_legacy_blueprints(self, bundled: DomainCatalog) -> None:
        assert bundled.legacy_blueprints == []

    def test_the_example_names_the_application_it_is_filed_under(
        self, bundled: DomainCatalog
    ) -> None:
        path = bundled.blueprint_path(APP_NAME, "wales-toy-gpu")

        data = yaml.safe_load(path.read_text())

        assert data["application"] == APP_NAME
        assert data["schema_version"] == get_application(APP_NAME).schema_version
