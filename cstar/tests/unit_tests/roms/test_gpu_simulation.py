"""Tests for `cstar.roms.gpu_simulation`: the GPU build key and `make gpu` command,
and `ROMSGpuSimulation`, which runs gpu-ucla-roms.

No network and no compiler: the system manager, the shell commands and the
linkage checks are replaced where `gpu_simulation` looks them up. The runtime
namelist and the input datasets are real files in a temporary directory, so
the namelist schema, the overrides and the file layout are the real ones.
"""

import dataclasses
import json
import logging
import math
import shlex
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import xarray as xr

import cstar
from cstar.base.additional_code import AdditionalCode
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.file_system import BUILD_RECORD_NAME
from cstar.roms.gpu_external_codebase import ROMSGpuExternalCodeBase
from cstar.roms.gpu_layout import GpuRankLayout
from cstar.roms.gpu_simulation import (
    GPU_BUILD_DIR_NAME,
    GPU_EXECUTABLE_NAME,
    GPU_WRITERS_ENV_VAR,
    GpuBuildKey,
    ROMSGpuSimulation,
    fortran_double_literal,
    gpu_make_command,
)
from cstar.roms.input_dataset import ROMSInitialConditions, ROMSModelGrid
from cstar.roms.namelist import RomsNamelistV0_11_0
from cstar.roms.precheck import (
    MAX_NAMELIST_FILE_NAME_LENGTH,
    NamelistConsistencyError,
    check_output_streams_divide_rst,
)
from cstar.roms.simulation import IOLayout, ROMSSimulation
from cstar.system.toolchain import Toolchain

V0_11_0_NAMELIST = Path(__file__).parent / "fixtures" / "example_namelist_v0_11_0.nml"
PACKAGE_ROOT = Path(cstar.__file__).parent
TEMPLATE_BLUEPRINT = (
    PACKAGE_ROOT / "additional_files/templates/bp/ucla_roms_gpu/blueprint.1.0.0.yaml"
)
BUNDLED_BLUEPRINT = (
    PACKAGE_ROOT / "catalog/bundled/blueprints/ucla_roms_gpu/wales-toy-gpu.yaml"
)

GPU_REPO = "https://github.com/CWorthy-ocean/gpu-ucla-roms.git"
MPI_PREFIX = "srun --kill-on-bad-exit=1"
MPI_HOME = "/opt/mpi/"  # with the trailing slash perlmutter.gpu.env gives it
NETCDF_HOME = "/opt/netcdf/"  # with the trailing slash perlmutter.gpu.env gives it
LINK_FLAGS_TEMPLATE = "${PE_MPICH_GTL_DIR_nvidia80} ${PE_MPICH_GTL_LIBS_nvidia80}"
LAUNCH_NAMELIST = "cstar_generated_roms.nml"


class TestFortranDoubleLiteral:
    @pytest.mark.parametrize(
        ("value", "literal"),
        [
            (1025.3, "1025.3d0"),
            (1000.0, "1000.0d0"),
            (1000, "1000.0d0"),
            (1e20, "1d+20"),
            (1e-3, "0.001d0"),
            (1.5e-7, "1.5d-07"),
            (-2.5, "-2.5d0"),
        ],
    )
    def test_writes_a_double_precision_literal(
        self, value: float, literal: str
    ) -> None:
        assert fortran_double_literal(value) == literal

    @pytest.mark.parametrize("value", [1025.3, 1e20, 1.5e-7, 1027.123456789])
    def test_reads_back_as_the_same_double(self, value: float) -> None:
        """Fortran reads `d` as the exponent letter of a double: `float` reads the
        same text with `e`.
        """
        assert float(fortran_double_literal(value).replace("d", "e")) == value

    def test_a_plain_literal_would_not_be_the_double(self) -> None:
        """Why the `d0`: single precision cannot hold 1025.3, and the model stops
        when the build's `rho0` differs from the namelist's by more than 1e-6.
        """
        import numpy as np

        assert abs(float(np.float32(1025.3)) - 1025.3) > 1e-6

    @pytest.mark.parametrize("value", [math.inf, -math.inf, math.nan])
    def test_refuses_a_value_that_is_not_finite(self, value: float) -> None:
        with pytest.raises(ValueError, match="Fortran literal"):
            fortran_double_literal(value)


class TestGpuBuildKey:
    def test_fflags_with_passive_fp64(self) -> None:
        key = GpuBuildKey("nvfortran", "cc80", 1025.3, passive_fp64=True)

        assert key.fflags() == (
            "-O2 -g -cuda -gpu=cc80 -DUSE_CUDA -DPASSIVE_FP64 "
            "-DROMS_RHO0=1025.3d0 -Mpreprocess -Mfree -Mnofma -Isrc/shared"
        )

    def test_fflags_without_passive_fp64(self) -> None:
        key = GpuBuildKey("nvfortran", "cc90", 1000.0, passive_fp64=False)

        assert key.fflags() == (
            "-O2 -g -cuda -gpu=cc90 -DUSE_CUDA "
            "-DROMS_RHO0=1000.0d0 -Mpreprocess -Mfree -Mnofma -Isrc/shared"
        )

    def test_is_frozen_and_comparable(self) -> None:
        key = GpuBuildKey("nvfortran", "cc80", 1000.0, passive_fp64=True)

        assert key == GpuBuildKey("nvfortran", "cc80", 1000.0, passive_fp64=True)
        assert key != dataclasses.replace(key, rho0=1025.3)
        with pytest.raises(dataclasses.FrozenInstanceError):
            key.rho0 = 1025.3  # type: ignore[misc]


def make_variables(command: str) -> dict[str, str]:
    """The `VAR=value` arguments of a `make` command, as the shell passes them."""
    name_and_value = (arg.split("=", 1) for arg in shlex.split(command)[2:])
    return {name: value for name, value in name_and_value}


class TestGpuMakeCommand:
    KEY = GpuBuildKey("nvfortran", "cc80", 1025.3, passive_fp64=True)

    def test_exact_command(self) -> None:
        """Each `VAR=value` is quoted for the shell; the NetCDF include directory
        is passed as `NETCDF_FFLAGS`, and the libraries name the NetCDF and MPI
        libraries, the link flags, and an rpath for every library directory.
        """
        command = gpu_make_command(
            self.KEY,
            Path(MPI_HOME),
            Path(NETCDF_HOME),
            Path(NETCDF_HOME),
            "-L/opt/gtl/lib -lmpi_gtl_cuda",
        )

        assert command == (
            "make gpu FC_GPU=nvfortran "
            "FFLAGS_GPU='-O2 -g -cuda -gpu=cc80 -DUSE_CUDA -DPASSIVE_FP64 "
            "-DROMS_RHO0=1025.3d0 -Mpreprocess -Mfree -Mnofma -Isrc/shared' "
            "FFLAGS_MPI='-DUSE_MPI -I/opt/mpi/include' "
            "NETCDF_FFLAGS=-I/opt/netcdf/include "
            "LIBS='-L/opt/netcdf/lib -lnetcdff -lnetcdf "
            "-L/opt/mpi/lib -lmpifort -lmpi -L/opt/gtl/lib -lmpi_gtl_cuda "
            "-Wl,-rpath,/opt/netcdf/lib -Wl,-rpath,/opt/mpi/lib "
            "-Wl,-rpath,/opt/gtl/lib'"
        )

    def test_netcdf_fortran_apart_from_netcdf_c(self) -> None:
        """`netcdf.mod` comes from the NetCDF-Fortran installation, and both
        library directories are searched and embedded.
        """
        variables = make_variables(
            gpu_make_command(
                self.KEY, Path("/opt/mpi"), Path("/opt/nff"), Path("/opt/nc"), ""
            )
        )

        assert variables["NETCDF_FFLAGS"] == "-I/opt/nff/include"
        assert variables["LIBS"] == (
            "-L/opt/nff/lib -L/opt/nc/lib -lnetcdff -lnetcdf "
            "-L/opt/mpi/lib -lmpifort -lmpi "
            "-Wl,-rpath,/opt/nff/lib -Wl,-rpath,/opt/nc/lib -Wl,-rpath,/opt/mpi/lib"
        )

    def test_rpaths_are_deduplicated_and_keep_their_order(self) -> None:
        command = gpu_make_command(
            self.KEY,
            Path("/opt/mpi"),
            Path("/a"),
            Path("/a"),
            "-L/b/lib -ly -L/a/lib -L/opt/mpi/lib",
        )

        libs = make_variables(command)["LIBS"]
        rpaths = [t for t in libs.split() if t.startswith("-Wl,-rpath,")]
        assert rpaths == [
            "-Wl,-rpath,/a/lib",
            "-Wl,-rpath,/opt/mpi/lib",
            "-Wl,-rpath,/b/lib",
        ]

    def test_a_separated_library_directory_is_found(self) -> None:
        """`-L <dir>` names a directory as `-L<dir>` does."""
        command = gpu_make_command(
            self.KEY, Path("/opt/mpi"), Path("/n"), Path("/n"), "-L /a/lib -lx"
        )

        assert "-Wl,-rpath,/a/lib" in command

    def test_no_link_flags_leave_no_gap(self) -> None:
        command = gpu_make_command(
            self.KEY, Path("/opt/mpi"), Path(NETCDF_HOME), Path(NETCDF_HOME), "  "
        )

        assert "  " not in command
        assert (
            "LIBS='-L/opt/netcdf/lib -lnetcdff -lnetcdf -L/opt/mpi/lib "
            "-lmpifort -lmpi -Wl,-rpath,/opt/netcdf/lib -Wl,-rpath,/opt/mpi/lib'"
        ) in command

    def test_values_with_spaces_are_quoted(self) -> None:
        key = GpuBuildKey("nv fortran", "cc80", 1000.0, passive_fp64=False)

        command = gpu_make_command(
            key, Path("/opt/my mpi"), Path("/opt/nc"), Path("/opt/nc"), ""
        )

        assert "FC_GPU='nv fortran'" in command
        variables = make_variables(command)
        assert variables["FC_GPU"] == "nv fortran"
        assert variables["FFLAGS_MPI"] == "-DUSE_MPI -I/opt/my mpi/include"


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def gpu_toolchain() -> Toolchain:
    """The GPU toolchain the mocked system offers (Perlmutter's, in shape)."""
    return Toolchain(
        compiler="nvfortran",
        mpi_prefix=MPI_PREFIX,
        profile="gpu",
        gpu_arch="cc80",
        link_flags=LINK_FLAGS_TEMPLATE,
    )


@pytest.fixture
def gpu_system(
    gpu_toolchain: Toolchain, monkeypatch: pytest.MonkeyPatch
) -> Generator[mock.Mock]:
    """A system manager with a GPU toolchain, active, in place where
    `gpu_simulation` and the build helpers it calls look it up. The modules of
    the toolchain "set" the link-flag variables.
    """
    monkeypatch.setenv("PE_MPICH_GTL_DIR_nvidia80", "-L/opt/cray/gtl/lib")
    monkeypatch.setenv("PE_MPICH_GTL_LIBS_nvidia80", "-lmpi_gtl_cuda")
    # a login shell may export these (e.g. on Anvil); the profile must decide
    monkeypatch.delenv("NETCDFHOME", raising=False)
    monkeypatch.delenv("NETCDFFHOME", raising=False)
    environment = mock.Mock(
        mpi_exec_prefix=MPI_PREFIX,
        environment_variables={"MPIHOME": MPI_HOME, "NETCDFHOME": NETCDF_HOME},
    )
    sysmgr = mock.Mock()
    sysmgr.name = "perlmutter"
    sysmgr.environment_for.return_value = environment
    sysmgr.environment = environment
    sysmgr.toolchain.return_value = gpu_toolchain
    with (
        mock.patch("cstar.roms.gpu_simulation.get_sysmgr", return_value=sysmgr),
        mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
    ):
        yield sysmgr


@pytest.fixture
def runtime_source(tmp_path: Path) -> Path:
    """A directory holding the runtime code: a 0.11.0 namelist and MARBL settings."""
    source = tmp_path / "runtime_source"
    source.mkdir()
    # the fixture's one warn switch (see check_gpu_features), off
    namelist = V0_11_0_NAMELIST.read_text().replace(
        "calc_pflx = .true.", "calc_pflx = .false."
    )
    (source / "namelist.nml").write_text(namelist)
    (source / "marbl_in").write_text("! marbl settings\n")
    return source


SimulationFactory = Callable[..., ROMSGpuSimulation]


GPU_AKT_BAK: dict[str, dict[str, Any]] = {
    "vertical_mixing_settings": {"akt_bak": [0.0, 0.0]}
}
"""The override a GPU blueprint needs over a CPU namelist: the GPU model reads
two `akt_bak` values (Rule G), the fixture namelist gives one per tracer."""


@pytest.fixture
def make_simulation(tmp_path: Path, runtime_source: Path) -> SimulationFactory:
    """Build a `ROMSGpuSimulation` with its runtime code staged.

    By default 8 tiles (4 x 2) on 2 nodes with one writer per node, and the
    two-value `akt_bak` override (`GPU_AKT_BAK`) under any `namelist_overrides`
    given; pass `vertical_mixing_settings` to replace it.
    """

    def _make(
        *,
        n_procs: tuple[int, int] = (4, 2),
        nodes: int = 2,
        writers_per_node: int = 1,
        namelist_overrides: dict[str, dict[str, Any]] | None = None,
        stage_runtime_code: bool = True,
        **kwargs: Any,
    ) -> ROMSGpuSimulation:
        sim = ROMSGpuSimulation(
            name="gpu-test",
            directory=tmp_path / "simulation",
            discretization=ROMSDiscretization(
                n_procs_x=n_procs[0], n_procs_y=n_procs[1]
            ),
            runtime_code=AdditionalCode(
                location=str(runtime_source), files=["namelist.nml", "marbl_in"]
            ),
            codebase=ROMSGpuExternalCodeBase(),
            start_date="2012-01-01",
            end_date="2012-01-02",
            valid_start_date="2012-01-01",
            valid_end_date="2012-02-01",
            namelist_overrides=GPU_AKT_BAK | (namelist_overrides or {}),
            nodes=nodes,
            writers_per_node=writers_per_node,
            **kwargs,
        )
        if stage_runtime_code:
            sim.runtime_code.get(sim.fs_manager.runtime_code_dir)
        return sim

    return _make


@pytest.fixture
def sim(make_simulation: SimulationFactory) -> ROMSGpuSimulation:
    """A GPU simulation: 8 tiles on 2 nodes, 1 writer per node, runtime code staged."""
    return make_simulation()


def stage_codebase(simulation: ROMSGpuSimulation) -> Path:
    """Give the simulation a staged gpu-ucla-roms clone (an empty directory)."""
    clone = simulation.fs_manager.codebase_subdir(simulation.codebase.key)
    clone.mkdir(parents=True, exist_ok=True)
    simulation.codebase._working_copy = mock.Mock(  # type: ignore[assignment]
        path=clone, checkout_hash="abc123"
    )
    return clone


@dataclasses.dataclass
class FakeBuildTools:
    """What the fake `make` and friends did."""

    commands: list[tuple[str, Path | None]]
    assert_single_toolchain_stack: mock.Mock
    verify_roms_linkage: mock.Mock

    @property
    def make_commands(self) -> list[tuple[str, Path | None]]:
        return [c for c in self.commands if c[0].startswith("make gpu ")]


@pytest.fixture
def build_tools() -> Generator[FakeBuildTools]:
    """Replace what `build()` runs: `make gpu` (which writes an
    executable), the toolchain assertion and the linkage check.
    """
    commands: list[tuple[str, Path | None]] = []

    def run_cmd(cmd: str, cwd: Path | None = None, **kwargs: Any) -> str:
        commands.append((cmd, cwd))
        assert cmd.startswith("make gpu ") and cwd is not None, cmd
        build_dir = cwd / GPU_BUILD_DIR_NAME
        build_dir.mkdir(exist_ok=True)
        (build_dir / GPU_EXECUTABLE_NAME).write_text(f"binary built by {cmd}")
        return ""

    with (
        mock.patch("cstar.roms.gpu_simulation._run_cmd", side_effect=run_cmd),
        mock.patch(
            "cstar.roms.gpu_simulation.assert_single_toolchain_stack"
        ) as assert_stack,
        mock.patch("cstar.roms.gpu_simulation.verify_roms_linkage") as verify,
    ):
        yield FakeBuildTools(commands, assert_stack, verify)


# ---------------------------------------------------------------------------
# construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_rank_layout_comes_from_the_tiling_and_the_job(
        self, make_simulation: SimulationFactory
    ) -> None:
        sim = make_simulation(n_procs=(4, 2), nodes=2, writers_per_node=3)

        assert sim.rank_layout == GpuRankLayout(tiles=8, nodes=2, writers_per_node=3)
        assert sim.rank_layout.tasks == 14

    def test_a_job_on_one_node_with_one_writer_by_default(
        self, make_simulation: SimulationFactory
    ) -> None:
        sim = ROMSGpuSimulation(
            name="gpu-test",
            directory=Path("unused"),
            discretization=ROMSDiscretization(n_procs_x=2, n_procs_y=2),
            runtime_code=make_simulation().runtime_code,
            codebase=ROMSGpuExternalCodeBase(),
            start_date="2012-01-01",
            end_date="2012-01-02",
        )

        assert sim.rank_layout == GpuRankLayout(tiles=4, nodes=1, writers_per_node=1)

    @pytest.mark.parametrize("missing", ["n_procs_x", "n_procs_y"])
    def test_requires_both_tile_counts(
        self, missing: str, make_simulation: SimulationFactory, tmp_path: Path
    ) -> None:
        discretization = ROMSDiscretization(n_procs_x=2, n_procs_y=2)
        setattr(discretization, missing, None)

        with pytest.raises(
            ValueError, match="n_procs_x and n_procs_y must both be set"
        ):
            ROMSGpuSimulation(
                name="gpu-test",
                directory=tmp_path / "simulation",
                discretization=discretization,
                runtime_code=make_simulation().runtime_code,
                start_date="2012-01-01",
                end_date="2012-01-02",
            )

    def test_refuses_tiles_that_do_not_divide_over_the_nodes(
        self, make_simulation: SimulationFactory
    ) -> None:
        with pytest.raises(ValueError, match="cannot be spread evenly over 2 nodes"):
            make_simulation(n_procs=(3, 1), nodes=2)

    def test_has_no_marbl_pio_or_compile_time_code(
        self, sim: ROMSGpuSimulation
    ) -> None:
        assert sim.compile_time_code is None
        assert sim.marbl_codebase is None
        assert sim.pio_codebase is None
        assert sim.use_pio is False
        assert sim.auto_tiling is False

    def test_codebases_are_the_gpu_codebase_alone(self, sim: ROMSGpuSimulation) -> None:
        assert sim.codebases == [sim.codebase]
        assert isinstance(sim.codebase, ROMSGpuExternalCodeBase)

    def test_the_default_codebase_is_the_gpu_codebase(
        self, make_simulation: SimulationFactory, tmp_path: Path
    ) -> None:
        sim = ROMSGpuSimulation(
            name="gpu-test",
            directory=tmp_path / "simulation",
            discretization=ROMSDiscretization(n_procs_x=2, n_procs_y=2),
            runtime_code=make_simulation().runtime_code,
            start_date="2012-01-01",
            end_date="2012-01-02",
        )

        assert isinstance(sim.default_codebase, ROMSGpuExternalCodeBase)
        assert isinstance(sim.codebase, ROMSGpuExternalCodeBase)
        assert sim.codebase.source.location == GPU_REPO

    def test_io_layout_is_joined(self, sim: ROMSGpuSimulation) -> None:
        assert sim.io_layout is IOLayout.JOINED

    def test_names_and_directory_of_the_build(self, sim: ROMSGpuSimulation) -> None:
        assert sim._executable_name == "roms_gpu_cuda"
        assert sim._build_dir is None  # the clone is not staged yet

        clone = stage_codebase(sim)

        assert sim._build_dir == clone / "build_gpu"


class TestCompileTimeCode:
    def test_refuses_compile_time_code(self, sim: ROMSGpuSimulation) -> None:
        with pytest.raises(ValueError, match="takes no compile-time code"):
            ROMSGpuSimulation._check_compile_time_code(sim.runtime_code)

    def test_accepts_none(self) -> None:
        ROMSGpuSimulation._check_compile_time_code(None)

    def test_the_constructor_has_no_compile_time_code_parameter(
        self, make_simulation: SimulationFactory
    ) -> None:
        with pytest.raises(TypeError, match="compile_time_code"):
            make_simulation(compile_time_code=make_simulation().runtime_code)


class TestSelectNamelistSchema:
    def test_every_release_reads_the_0_11_0_vocabulary(
        self, sim: ROMSGpuSimulation
    ) -> None:
        assert sim._select_namelist_schema() is RomsNamelistV0_11_0

    def test_a_commit_is_resolved_in_the_staged_clone(
        self, sim: ROMSGpuSimulation
    ) -> None:
        clone = stage_codebase(sim)
        sim.codebase._source = mock.Mock(checkout_target="a" * 40)  # type: ignore[assignment]

        with mock.patch(
            "cstar.roms.gpu_simulation.gpu_namelist_schema_for_ref",
            return_value=RomsNamelistV0_11_0,
        ) as mock_select:
            assert sim._select_namelist_schema() is RomsNamelistV0_11_0

        mock_select.assert_called_once_with("a" * 40, repo_path=clone)

    def test_without_a_clone_there_is_no_repository_to_ask(
        self, sim: ROMSGpuSimulation
    ) -> None:
        with mock.patch(
            "cstar.roms.gpu_simulation.gpu_namelist_schema_for_ref",
            return_value=RomsNamelistV0_11_0,
        ) as mock_select:
            sim._select_namelist_schema()

        mock_select.assert_called_once_with("v0.1.2-beta", repo_path=None)


# ---------------------------------------------------------------------------
# the GPU toolchain
# ---------------------------------------------------------------------------


class TestGpuEnvironment:
    def test_asks_the_system_for_the_gpu_toolchain(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        environment = sim._gpu_environment()

        gpu_system.environment_for.assert_called_once_with("gpu")
        assert environment is gpu_system.environment_for.return_value

    def test_setup_switches_to_it_before_the_base_class_stages_anything(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        calls = mock.Mock()
        calls.attach_mock(gpu_system.environment_for, "environment_for")

        with mock.patch.object(ROMSSimulation, "setup") as mock_base_setup:
            calls.attach_mock(mock_base_setup, "base_setup")
            sim.setup(clobber=True)

        assert calls.mock_calls == [
            mock.call.environment_for("gpu"),
            mock.call.base_setup(clobber=True),
        ]

    def test_attach_switches_to_it_before_the_base_class_adopts_anything(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        calls = mock.Mock()
        calls.attach_mock(gpu_system.environment_for, "environment_for")

        with mock.patch.object(ROMSSimulation, "attach") as mock_base_attach:
            calls.attach_mock(mock_base_attach, "base_attach")
            sim.attach()

        assert calls.mock_calls == [
            mock.call.environment_for("gpu"),
            mock.call.base_attach(),
        ]

    def test_a_system_without_gpus_stops_setup_before_anything_is_staged(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        gpu_system.environment_for.side_effect = CstarError(
            "perlmutter has no GPU toolchain registered with C-Star"
        )

        with (
            mock.patch.object(ROMSGpuExternalCodeBase, "setup") as mock_codebase_setup,
            mock.patch.object(AdditionalCode, "get") as mock_code_get,
            pytest.raises(CstarError, match="no GPU toolchain"),
        ):
            sim.setup()

        mock_codebase_setup.assert_not_called()
        mock_code_get.assert_not_called()
        assert not sim.fs_manager.input_datasets_dir.exists()

    def test_a_system_without_gpus_stops_attach_before_anything_is_adopted(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        gpu_system.environment_for.side_effect = CstarError("no GPU toolchain")

        with (
            mock.patch.object(ROMSGpuExternalCodeBase, "attach") as mock_attach,
            pytest.raises(CstarError, match="no GPU toolchain"),
        ):
            sim.attach()

        mock_attach.assert_not_called()


# ---------------------------------------------------------------------------
# the runtime namelist
# ---------------------------------------------------------------------------


class TestRuntimeSettings:
    def test_the_schema_is_the_0_11_0_tier(
        self, sim: ROMSGpuSimulation, tmp_path: Path
    ) -> None:
        nml = sim.roms_runtime_settings

        assert type(nml) is RomsNamelistV0_11_0
        assert nml.param_settings.np_xi == 4
        assert nml.param_settings.np_eta == 2

        written = tmp_path / "written.nml"
        nml.write(written)
        # a key of the later tiers, which the GPU model rejects
        assert "v_sponge_trc" not in written.read_text()

    @pytest.mark.filterwarnings("ignore:The GPU model does not honour")
    def test_a_stop_switch_of_the_gpu_model_is_refused(
        self, make_simulation: SimulationFactory
    ) -> None:
        sim = make_simulation(
            namelist_overrides={"frc_output_settings": {"wrt_frc": True}}
        )

        with pytest.raises(NamelistConsistencyError, match="forcing output") as raised:
            _ = sim.roms_runtime_settings

        assert raised.value.rule == "gpu_features"

    def test_more_akt_bak_values_than_the_gpu_reads_are_refused(
        self, make_simulation: SimulationFactory
    ) -> None:
        """The fixture's CPU namelist gives one `akt_bak` per tracer (Rule G)."""
        sim = make_simulation(
            namelist_overrides={"vertical_mixing_settings": {"akt_bak": [0.0] * 3}}
        )

        with pytest.raises(
            NamelistConsistencyError, match="akt_bak: 3 values"
        ) as raised:
            _ = sim.roms_runtime_settings

        assert raised.value.rule == "gpu_array_sizes"

    def test_a_file_name_too_long_to_read_is_refused(
        self, make_simulation: SimulationFactory
    ) -> None:
        too_long = "/" + "a" * MAX_NAMELIST_FILE_NAME_LENGTH + ".nc"
        sim = make_simulation(
            namelist_overrides={"forcing_files": {"frcfiles": [too_long]}}
        )

        with pytest.raises(NamelistConsistencyError, match="Shorten these file names"):
            _ = sim.roms_runtime_settings

    def test_the_ucla_roms_stream_rollover_rule_is_enforced(
        self, make_simulation: SimulationFactory
    ) -> None:
        """A history file that rolls over every 7 records does not divide the daily
        restart period. The GPU model does not check that (ucla-roms does, rule A),
        but it names and splits files as ucla-roms does, so a restart would leave
        a partial history file for resume and continue-from to start from.
        """
        overrides = {"basic_output_settings": {"wrt_file_his": True}}
        sim = make_simulation(namelist_overrides=overrides)

        with pytest.raises(NamelistConsistencyError, match="evenly divide"):
            _ = sim.roms_runtime_settings

    def test_a_rollover_that_divides_the_restart_period_passes(
        self, make_simulation: SimulationFactory
    ) -> None:
        overrides = {"basic_output_settings": {"wrt_file_his": True, "nrpf_his": 1}}
        sim = make_simulation(namelist_overrides=overrides)

        nml = sim.roms_runtime_settings  # no error

        check_output_streams_divide_rst(nml)

    def test_a_joined_layout_names_whole_files_and_writes_into_output(
        self, make_simulation: SimulationFactory, tmp_path: Path
    ) -> None:
        data = tmp_path / "data"
        data.mkdir()
        xr.Dataset({"h": ("xi_rho", [1.0])}).to_netcdf(data / "grid.nc")
        xr.Dataset({"temp": ("xi_rho", [1.0])}).to_netcdf(data / "ini.nc")
        grid = ROMSModelGrid(location=str(data / "grid.nc"))
        initial_conditions = ROMSInitialConditions(location=str(data / "ini.nc"))
        sim = make_simulation(model_grid=grid, initial_conditions=initial_conditions)
        for dataset in (grid, initial_conditions):
            dataset.get(sim.fs_manager.input_datasets_dir)

        nml = sim.roms_runtime_settings

        assert nml.grid_settings.grdname == "../input/input_datasets/grid.nc"
        assert nml.initial_conditions.inifile == "../input/input_datasets/ini.nc"
        assert nml.simulation_name_settings.output_root_name == "../output/output"
        assert nml.extract_data_settings.extract_root_name == "../output/child"

        with mock.patch.object(ROMSModelGrid, "partition") as mock_partition:
            sim.pre_run()
        mock_partition.assert_not_called()

    def test_no_warning_when_the_writers_suffice(
        self, sim: ROMSGpuSimulation, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.WARNING, logger=sim.log.name)

        _ = sim.roms_runtime_settings

        assert not [r for r in caplog.records if "writers_per_node" in r.message]

    def test_only_informs_when_a_small_record_has_fewer_writers_than_suggested(
        self, make_simulation: SimulationFactory, caplog: pytest.LogCaptureFixture
    ) -> None:
        sim = make_simulation(writers_per_node=0)
        caplog.set_level(logging.INFO, logger=sim.log.name)

        _ = sim.roms_runtime_settings

        advice = [r for r in caplog.records if "writers_per_node is 0" in r.message]
        assert [r.levelno for r in advice] == [logging.INFO]
        assert "suggests 1 per node" in advice[0].message

    def test_warns_when_the_writers_cannot_keep_up_with_the_record(
        self, make_simulation: SimulationFactory, caplog: pytest.LogCaptureFixture
    ) -> None:
        # ~61 GB per history record with MARBL: two writers need ~51 s per record
        sim = make_simulation(
            namelist_overrides={
                "param_settings": {"llm": 2000, "mmm": 2000, "nz": 100, "nt_bgc": 32}
            }
        )
        caplog.set_level(logging.WARNING, logger=sim.log.name)

        with mock.patch(
            "cstar.roms.gpu_simulation.suggested_writers_per_node", return_value=3
        ) as mock_suggested:
            _ = sim.roms_runtime_settings

        assert mock_suggested.call_args.args[1] == 2  # the nodes
        [message] = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert "tiling.writers_per_node is 1" in message
        assert "suggests 3 per node on 2 node(s)" in message
        assert "Raise tiling.writers_per_node" in message

    def test_sizes_the_write_time_by_one_files_writers(
        self, make_simulation: SimulationFactory, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Eight writers form groups of four (io_server's `writer_groups`), one
        file per group: a ~64 GB record takes four writers ~27 s, though all
        eight together would take ~13 s, under the 15 s target.
        """
        sim = make_simulation(
            n_procs=(2, 2),
            nodes=1,
            writers_per_node=8,
            namelist_overrides={
                "param_settings": {"llm": 2000, "mmm": 2000, "nz": 100, "nt_bgc": 32}
            },
        )
        caplog.set_level(logging.WARNING, logger=sim.log.name)

        with mock.patch(
            "cstar.roms.gpu_simulation.suggested_writers_per_node", return_value=9
        ):
            _ = sim.roms_runtime_settings

        [message] = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert "tiling.writers_per_node is 8" in message
        assert "may stall the GPUs" in message


class TestCdrLiteEnabled:
    def test_off_without_cdr_lite_tracers(self, sim: ROMSGpuSimulation) -> None:
        assert sim._cdr_lite_enabled() is False

    @pytest.mark.parametrize("key", ["nt_cdr_oae", "nt_cdr_dor"])
    def test_on_with_either_kind_of_tracer(
        self, key: str, make_simulation: SimulationFactory
    ) -> None:
        sim = make_simulation(namelist_overrides={"param_settings": {key: 2}})

        assert sim._cdr_lite_enabled() is True

    def test_a_schema_without_the_keys_has_no_cdr_lite(
        self, sim: ROMSGpuSimulation
    ) -> None:
        namelist = mock.Mock(param_settings=mock.Mock(spec=["nt_bgc"]))

        with mock.patch.object(sim, "_composed_namelist", return_value=namelist):
            assert sim._cdr_lite_enabled() is False

    def test_the_setup_check_asks_it(self, make_simulation: SimulationFactory) -> None:
        """The carbonate-sensitivity check of the base class goes by this answer:
        a GPU simulation has no `cppdefs.opt` for it to read.
        """
        sim = make_simulation(namelist_overrides={"param_settings": {"nt_cdr_oae": 2}})

        with pytest.raises(ValueError, match="no forcing file provides ddic_dco2"):
            sim._validate_carbonate_sensitivity_inputs()

        sim.namelist_overrides = {}
        sim._validate_carbonate_sensitivity_inputs()  # CDR_LITE is off: nothing to check


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


class TestBuildKey:
    def test_from_the_toolchain_and_the_namelist(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        assert sim._gpu_build_key() == GpuBuildKey(
            compiler="nvfortran", gpu_arch="cc80", rho0=1000.0, passive_fp64=True
        )
        gpu_system.toolchain.assert_called_with("gpu")

    def test_overrides_are_part_of_the_namelist(
        self, make_simulation: SimulationFactory, gpu_system: mock.Mock
    ) -> None:
        sim = make_simulation(
            namelist_overrides={
                "rho0_settings": {"rho0": 1025.3},
                "param_settings": {"nt_bgc": 0},
            }
        )

        key = sim._gpu_build_key()

        assert key.rho0 == 1025.3
        assert key.passive_fp64 is False

    def test_a_toolchain_without_an_architecture_is_an_error(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock, gpu_toolchain: Toolchain
    ) -> None:
        gpu_system.toolchain.return_value = dataclasses.replace(
            gpu_toolchain, gpu_arch=""
        )

        with pytest.raises(CstarError, match="names no GPU architecture"):
            sim._gpu_build_key()


class TestBuild:
    EXPECTED_COMMAND = (
        "make gpu FC_GPU=nvfortran "
        "FFLAGS_GPU='-O2 -g -cuda -gpu=cc80 -DUSE_CUDA -DPASSIVE_FP64 "
        "-DROMS_RHO0=1025.3d0 -Mpreprocess -Mfree -Mnofma -Isrc/shared' "
        "FFLAGS_MPI='-DUSE_MPI -I/opt/mpi/include' "
        "NETCDF_FFLAGS=-I/opt/netcdf/include "
        "LIBS='-L/opt/netcdf/lib -lnetcdff -lnetcdf "
        "-L/opt/mpi/lib -lmpifort -lmpi -L/opt/cray/gtl/lib -lmpi_gtl_cuda "
        "-Wl,-rpath,/opt/netcdf/lib -Wl,-rpath,/opt/mpi/lib "
        "-Wl,-rpath,/opt/cray/gtl/lib'"
    )

    @pytest.fixture
    def rho0_sim(self, make_simulation: SimulationFactory) -> ROMSGpuSimulation:
        """A staged simulation whose namelist `rho0` is not exact in float32."""
        sim = make_simulation(namelist_overrides={"rho0_settings": {"rho0": 1025.3}})
        stage_codebase(sim)
        return sim

    def test_composes_the_make_command_and_runs_it_in_the_clone(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
    ) -> None:
        clone = rho0_sim.fs_manager.codebase_subdir(rho0_sim.codebase.key)

        rho0_sim.build()

        assert build_tools.commands == [(self.EXPECTED_COMMAND, clone)]
        gpu_system.environment_for.assert_called_with("gpu")
        build_tools.assert_single_toolchain_stack.assert_called_once_with(None)

    def test_embeds_the_run_path_the_profile_declares(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
    ) -> None:
        """Like the CPU build, the GPU build embeds the env file's `LD_RUN_PATH`."""
        with mock.patch(
            "cstar.roms.gpu_simulation.rpath_link_flags",
            return_value="-Wl,-rpath,/opt/hdf5/lib",
        ):
            rho0_sim.build()

        [(command, _)] = build_tools.make_commands
        assert "-lmpi_gtl_cuda -Wl,-rpath,/opt/hdf5/lib -Wl,-rpath," in command

    def test_records_the_executable_and_the_build(
        self,
        rho0_sim: ROMSGpuSimulation,
        build_tools: FakeBuildTools,
        gpu_system: mock.Mock,
    ) -> None:
        clone = rho0_sim.fs_manager.codebase_subdir(rho0_sim.codebase.key)
        executable = clone / "build_gpu" / "roms_gpu_cuda"

        rho0_sim.build()

        assert rho0_sim.exe_path == executable
        assert rho0_sim._exe_hash is not None
        assert rho0_sim._namelist_schema is RomsNamelistV0_11_0
        build_tools.verify_roms_linkage.assert_called_once_with(executable)
        record = json.loads((clone / "build_gpu" / BUILD_RECORD_NAME).read_text())
        assert record == {
            "namelist_schema": "RomsNamelistV0_11_0",
            "ucla_roms_ref": "v0.1.2-beta",
            "ucla_roms_commit": "abc123",
            "gpu_build": {
                "compiler": "nvfortran",
                "gpu_arch": "cc80",
                "rho0": 1025.3,
                "passive_fp64": True,
            },
        }

    @pytest.mark.parametrize(
        ("nt_bgc", "passive_fp64"), [(32, True), (0, False)], ids=["marbl", "no-marbl"]
    )
    def test_marbl_tracers_are_stored_in_double_precision(
        self,
        nt_bgc: int,
        passive_fp64: bool,
        make_simulation: SimulationFactory,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
    ) -> None:
        sim = make_simulation(namelist_overrides={"param_settings": {"nt_bgc": nt_bgc}})
        stage_codebase(sim)

        sim.build()

        [(command, _)] = build_tools.make_commands
        assert ("-DPASSIVE_FP64" in command) is passive_fp64
        assert "-DROMS_RHO0=1000.0d0" in command

    def test_does_not_build_again_for_the_same_build(
        self,
        rho0_sim: ROMSGpuSimulation,
        build_tools: FakeBuildTools,
        gpu_system: mock.Mock,
    ) -> None:
        rho0_sim.build()
        executable_hash = rho0_sim._exe_hash
        build_tools.commands.clear()

        rho0_sim.build()

        assert build_tools.commands == []
        assert rho0_sim._exe_hash == executable_hash

    def test_builds_again_on_request(
        self,
        rho0_sim: ROMSGpuSimulation,
        build_tools: FakeBuildTools,
        gpu_system: mock.Mock,
    ) -> None:
        rho0_sim.build()

        rho0_sim.build(rebuild=True)

        assert len(build_tools.make_commands) == 2

    def test_builds_again_when_rho0_changes(
        self,
        rho0_sim: ROMSGpuSimulation,
        build_tools: FakeBuildTools,
        gpu_system: mock.Mock,
    ) -> None:
        """The model stops at start-up when its `rho0` is not the namelist's, so an
        executable built for another one is not reused.
        """
        rho0_sim.build()
        rho0_sim.namelist_overrides = GPU_AKT_BAK | {"rho0_settings": {"rho0": 1027.5}}

        rho0_sim.build()

        commands = [c for c, _ in build_tools.make_commands]
        assert len(commands) == 2
        assert "-DROMS_RHO0=1025.3d0" in commands[0]
        assert "-DROMS_RHO0=1027.5d0" in commands[1]
        clone = rho0_sim.fs_manager.codebase_subdir(rho0_sim.codebase.key)
        record = json.loads((clone / "build_gpu" / BUILD_RECORD_NAME).read_text())
        assert record["gpu_build"]["rho0"] == 1027.5

    def test_builds_again_when_the_executable_changed(
        self,
        rho0_sim: ROMSGpuSimulation,
        build_tools: FakeBuildTools,
        gpu_system: mock.Mock,
    ) -> None:
        rho0_sim.build()
        assert rho0_sim.exe_path is not None
        rho0_sim.exe_path.write_text("something else")

        rho0_sim.build()

        assert len(build_tools.make_commands) == 2

    def test_a_replaced_build_takes_its_record_with_it(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        gpu_toolchain: Toolchain,
    ) -> None:
        """The record of a build that fails midway is gone, not left to describe
        an executable that was replaced.
        """
        clone = rho0_sim.fs_manager.codebase_subdir(rho0_sim.codebase.key)
        record = clone / "build_gpu" / BUILD_RECORD_NAME
        record.parent.mkdir()
        record.write_text("{}")
        rho0_sim._namelist_schema = RomsNamelistV0_11_0

        def fail_to_compile(cmd: str, **kwargs: Any) -> str:
            if cmd.startswith("make gpu"):
                raise RuntimeError("Error when compiling gpu-ucla-roms.")
            return ""

        with (
            mock.patch(
                "cstar.roms.gpu_simulation._run_cmd", side_effect=fail_to_compile
            ),
            mock.patch("cstar.roms.gpu_simulation.assert_single_toolchain_stack"),
            pytest.raises(RuntimeError, match="Error when compiling"),
        ):
            rho0_sim.build()

        assert not record.exists()
        assert rho0_sim._namelist_schema is None
        assert rho0_sim.exe_path is None

    def test_refuses_more_akt_bak_values_than_the_gpu_reads_before_compiling(
        self,
        make_simulation: SimulationFactory,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
    ) -> None:
        """Rule G fails the build, not the launch after it."""
        sim = make_simulation(
            namelist_overrides={"vertical_mixing_settings": {"akt_bak": [0.0] * 3}}
        )
        stage_codebase(sim)

        with pytest.raises(NamelistConsistencyError, match="akt_bak: 3 values"):
            sim.build()

        assert build_tools.make_commands == []

    def test_needs_the_clone(
        self,
        make_simulation: SimulationFactory,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
    ) -> None:
        sim = make_simulation()

        with pytest.raises(ValueError, match="codebase is not staged locally"):
            sim.build()

        gpu_system.environment_for.assert_not_called()
        assert build_tools.commands == []

    def test_needs_mpihome(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        gpu_system.environment_for.return_value.environment_variables = {}
        monkeypatch.delenv("MPIHOME", raising=False)

        with pytest.raises(CstarError, match="GPU toolchain profile.*must set MPIHOME"):
            rho0_sim.build()

        assert build_tools.make_commands == []

    def test_mpihome_may_come_from_the_process_environment(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        gpu_system.environment_for.return_value.environment_variables = {
            "NETCDFHOME": NETCDF_HOME
        }
        monkeypatch.setenv("MPIHOME", "/from/the/environment")

        rho0_sim.build()

        [(command, _)] = build_tools.make_commands
        assert "-I/from/the/environment/include" in command
        assert "-Wl,-rpath,/from/the/environment/lib" in command

    def test_needs_netcdfhome(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        gpu_system.environment_for.return_value.environment_variables = {
            "MPIHOME": MPI_HOME
        }
        monkeypatch.delenv("NETCDFHOME", raising=False)
        monkeypatch.delenv("NETCDFFHOME", raising=False)

        with pytest.raises(
            CstarError, match="GPU toolchain profile.*must set NETCDFHOME"
        ):
            rho0_sim.build()

        assert build_tools.make_commands == []

    def test_netcdffhome_names_netcdf_fortran(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """As build verification reads them: `NETCDFFHOME` is NetCDF-Fortran's
        installation, `NETCDFHOME` NetCDF-C's.
        """
        gpu_system.environment_for.return_value.environment_variables = {
            "MPIHOME": MPI_HOME,
            "NETCDFHOME": "/opt/nc",
            "NETCDFFHOME": "/opt/nff",
        }
        monkeypatch.delenv("NETCDFHOME", raising=False)
        monkeypatch.delenv("NETCDFFHOME", raising=False)

        rho0_sim.build()

        [(command, _)] = build_tools.make_commands
        variables = make_variables(command)
        assert variables["NETCDF_FFLAGS"] == "-I/opt/nff/include"
        assert variables["LIBS"].startswith(
            "-L/opt/nff/lib -L/opt/nc/lib -lnetcdff -lnetcdf "
        )

    def test_netcdffhome_alone_names_both(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
    ) -> None:
        """With only `NETCDFFHOME`, NetCDF-C is taken to be installed with it."""
        gpu_system.environment_for.return_value.environment_variables = {
            "MPIHOME": MPI_HOME,
            "NETCDFFHOME": "/opt/nff/",
        }

        rho0_sim.build()

        [(command, _)] = build_tools.make_commands
        variables = make_variables(command)
        assert variables["NETCDF_FFLAGS"] == "-I/opt/nff/include"
        assert variables["LIBS"].startswith("-L/opt/nff/lib -lnetcdff -lnetcdf ")
        assert "-Wl,-rpath,/opt/nff/lib" in variables["LIBS"]

    def test_a_shell_netcdffhome_does_not_mix_with_the_profile(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The profile names `NETCDFHOME` only; a `NETCDFFHOME` exported in the
        shell (another toolchain's) must not supply `netcdf.mod`.
        """
        monkeypatch.setenv("NETCDFFHOME", "/ambient/nff")

        rho0_sim.build()

        [(command, _)] = build_tools.make_commands
        assert "/ambient" not in command
        assert make_variables(command)["NETCDF_FFLAGS"] == "-I/opt/netcdf/include"

    def test_the_process_environment_is_used_when_the_profile_names_neither(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        gpu_system.environment_for.return_value.environment_variables = {
            "MPIHOME": MPI_HOME
        }
        monkeypatch.setenv("NETCDFHOME", "/from/the/environment")

        rho0_sim.build()

        [(command, _)] = build_tools.make_commands
        assert "NETCDF_FFLAGS=-I/from/the/environment/include" in command

    def test_never_asks_nf_config(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
    ) -> None:
        """Cray's `nf-config` prints no flags, and a conda one may come first on
        PATH, so the build takes the NetCDF flags from the profile only.
        """
        rho0_sim.build()

        assert [c for c, _ in build_tools.commands if "nf-config" in c] == []

    def test_a_link_flag_variable_the_modules_did_not_set_is_an_error(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("PE_MPICH_GTL_LIBS_nvidia80")

        with pytest.raises(
            CstarError, match=r"not set: \$\{PE_MPICH_GTL_LIBS_nvidia80\}"
        ):
            rho0_sim.build()

        assert build_tools.make_commands == []

    def test_a_toolchain_without_link_flags_builds_without_them(
        self,
        rho0_sim: ROMSGpuSimulation,
        gpu_system: mock.Mock,
        gpu_toolchain: Toolchain,
        build_tools: FakeBuildTools,
    ) -> None:
        gpu_system.toolchain.return_value = dataclasses.replace(
            gpu_toolchain, link_flags=""
        )

        rho0_sim.build()

        [(command, _)] = build_tools.make_commands
        assert "gtl" not in command


# ---------------------------------------------------------------------------
# attach
# ---------------------------------------------------------------------------


class TestAttach:
    """`attach` adopts a working directory a previous `build` left behind."""

    @pytest.fixture
    def built(
        self,
        make_simulation: SimulationFactory,
        gpu_system: mock.Mock,
        build_tools: FakeBuildTools,
    ) -> Path:
        """Build a simulation with `rho0` 1025.3; return its build directory."""
        sim = make_simulation(namelist_overrides={"rho0_settings": {"rho0": 1025.3}})
        clone = stage_codebase(sim)
        sim.build()
        return clone / GPU_BUILD_DIR_NAME

    def attach(
        self, make_simulation: SimulationFactory, **kwargs: Any
    ) -> ROMSGpuSimulation:
        """Attach a new simulation of the same directory, as a resumed run does."""
        sim = make_simulation(**kwargs)
        clone = stage_codebase(sim)
        with (
            mock.patch.object(ROMSGpuExternalCodeBase, "attach"),
            mock.patch("cstar.roms.simulation.verify_roms_linkage") as mock_linkage,
        ):
            sim.attach()
        mock_linkage.assert_called_once_with(
            clone / GPU_BUILD_DIR_NAME / GPU_EXECUTABLE_NAME
        )
        return sim

    def test_adopts_an_executable_built_for_the_same_namelist(
        self, built: Path, make_simulation: SimulationFactory, gpu_system: mock.Mock
    ) -> None:
        sim = self.attach(
            make_simulation, namelist_overrides={"rho0_settings": {"rho0": 1025.3}}
        )

        assert sim.exe_path == built / GPU_EXECUTABLE_NAME
        assert sim._exe_hash is not None
        assert sim._namelist_schema is RomsNamelistV0_11_0
        gpu_system.environment_for.assert_called_with("gpu")

    def test_reports_an_executable_built_for_another_rho0(
        self, built: Path, make_simulation: SimulationFactory, gpu_system: mock.Mock
    ) -> None:
        sim = make_simulation(namelist_overrides={"rho0_settings": {"rho0": 1027.5}})
        stage_codebase(sim)

        with (
            mock.patch.object(ROMSGpuExternalCodeBase, "attach"),
            mock.patch("cstar.roms.simulation.verify_roms_linkage"),
            pytest.raises(CstarExpectationFailed) as raised,
        ):
            sim.attach()

        message = str(raised.value)
        assert f"compiled ROMS executable {built / GPU_EXECUTABLE_NAME}" in message
        assert "rho0 was 1025.3 and is now 1027.5" in message
        assert "--clobber" in message
        assert sim.exe_path is None

    def test_reports_an_executable_built_for_another_tracer_precision(
        self, built: Path, make_simulation: SimulationFactory, gpu_system: mock.Mock
    ) -> None:
        sim = make_simulation(
            namelist_overrides={
                "rho0_settings": {"rho0": 1025.3},
                "param_settings": {"nt_bgc": 0},
            }
        )
        stage_codebase(sim)

        with (
            mock.patch.object(ROMSGpuExternalCodeBase, "attach"),
            mock.patch("cstar.roms.simulation.verify_roms_linkage"),
            pytest.raises(CstarExpectationFailed, match="passive_fp64 was True"),
        ):
            sim.attach()

    @staticmethod
    def _refused(make_simulation: SimulationFactory) -> str:
        sim = make_simulation(namelist_overrides={"rho0_settings": {"rho0": 1025.3}})
        stage_codebase(sim)
        with (
            mock.patch.object(ROMSGpuExternalCodeBase, "attach"),
            mock.patch("cstar.roms.simulation.verify_roms_linkage"),
            pytest.raises(CstarExpectationFailed) as raised,
        ):
            sim.attach()
        assert sim.exe_path is None
        return str(raised.value)

    def test_refuses_an_executable_without_a_build_record(
        self, built: Path, make_simulation: SimulationFactory, gpu_system: mock.Mock
    ) -> None:
        """A failed rebuild (the record is removed before compiling) or a build by
        hand leaves no record, and the model checks only `rho0` at start-up.
        """
        (built / BUILD_RECORD_NAME).unlink()

        message = self._refused(make_simulation)

        assert "has no GPU build record" in message
        assert "--clobber" in message

    def test_refuses_a_record_that_holds_no_gpu_build(
        self, built: Path, make_simulation: SimulationFactory, gpu_system: mock.Mock
    ) -> None:
        record = built / BUILD_RECORD_NAME
        contents = json.loads(record.read_text())
        del contents["gpu_build"]
        record.write_text(json.dumps(contents))

        assert "has no GPU build record" in self._refused(make_simulation)


# ---------------------------------------------------------------------------
# launch
# ---------------------------------------------------------------------------


class TestLaunch:
    @staticmethod
    def stage_executable(simulation: ROMSGpuSimulation) -> Path:
        clone = stage_codebase(simulation)
        executable = clone / GPU_BUILD_DIR_NAME / GPU_EXECUTABLE_NAME
        executable.parent.mkdir(parents=True)
        executable.write_text("binary")
        simulation.exe_path = executable
        return executable

    def test_launch_command(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        """8 compute ranks and 1 writer on each of 2 nodes: 10 tasks."""
        assert sim._launch_command(LAUNCH_NAMELIST) == (
            "env ROMS_GPU_NWRITERS=1 srun --kill-on-bad-exit=1 -n 10 "
            "./roms_gpu_cuda run cstar_generated_roms.nml"
        )
        gpu_system.environment_for.assert_called_with("gpu")

    def test_without_writers_the_tasks_are_the_tiles(
        self, make_simulation: SimulationFactory, gpu_system: mock.Mock
    ) -> None:
        sim = make_simulation(writers_per_node=0)

        assert sim._launch_command(LAUNCH_NAMELIST) == (
            "env ROMS_GPU_NWRITERS=0 srun --kill-on-bad-exit=1 -n 8 "
            "./roms_gpu_cuda run cstar_generated_roms.nml"
        )

    def test_the_gpu_profile_sets_gpu_support_not_the_command(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        assert "MPICH_GPU_SUPPORT_ENABLED" not in sim._launch_command(LAUNCH_NAMELIST)

    def test_the_writer_variable_is_the_models(self) -> None:
        assert GPU_WRITERS_ENV_VAR == "ROMS_GPU_NWRITERS"

    def test_prepare_launch_plans_the_gpu_command_in_a_joined_layout(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        executable = self.stage_executable(sim)

        plan = sim.prepare_launch()

        assert plan.command == (
            "env ROMS_GPU_NWRITERS=1 srun --kill-on-bad-exit=1 -n 10 "
            "./roms_gpu_cuda run cstar_generated_roms.nml"
        )
        assert plan.run_path == sim.fs_manager.run_dir
        assert plan.runtime_settings_file == sim.fs_manager.run_dir / LAUNCH_NAMELIST
        assert plan.runtime_settings_file.exists()
        link = sim.fs_manager.run_dir / GPU_EXECUTABLE_NAME
        assert link.is_symlink()
        assert link.resolve() == executable.resolve()
        assert sim.fs_manager.output_dir.is_dir()
        assert not sim.fs_manager.temp_output_dir.exists()
        gpu_system.environment_for.assert_called_with("gpu")

    def test_the_written_namelist_is_the_0_11_0_vocabulary(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        self.stage_executable(sim)

        plan = sim.prepare_launch()

        written = plan.runtime_settings_file.read_text()
        assert "np_xi = 4" in written
        assert "np_eta = 2" in written
        assert "output_root_name = '../output/output'" in written
        assert "v_sponge_trc" not in written

    def test_prepare_launch_needs_the_executable(
        self, sim: ROMSGpuSimulation, gpu_system: mock.Mock
    ) -> None:
        with pytest.raises(ValueError, match="exe_path is None"):
            sim.prepare_launch()


# ---------------------------------------------------------------------------
# post-run, dicts and blueprints
# ---------------------------------------------------------------------------


class TestPostRun:
    def test_the_outputs_are_not_joined(
        self, sim: ROMSGpuSimulation, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The model wrote whole files into `output`; there is nothing to join."""
        from cstar.execution.handler import ExecutionStatus

        sim.fs_manager.output_dir.mkdir(parents=True)
        (sim.fs_manager.output_dir / "output_his.20120101000000.nc").write_text("x")
        sim._execution_handler = mock.Mock(status=ExecutionStatus.COMPLETED)
        caplog.set_level(logging.INFO, logger=sim.log.name)

        with mock.patch("cstar.roms.simulation._run_cmd") as mock_run_cmd:
            sim.post_run()

        mock_run_cmd.assert_not_called()
        assert any("1 output files" in r.message for r in caplog.records)


class TestFromDict:
    def test_is_not_supported(self, sim: ROMSGpuSimulation, tmp_path: Path) -> None:
        with pytest.raises(NotImplementedError, match="from_blueprint"):
            ROMSGpuSimulation.from_dict(sim.to_dict(), directory=tmp_path)


@pytest.mark.usefixtures("offline_remote_files")
class TestFromBlueprint:
    @pytest.fixture(autouse=True)
    def _in_a_scratch_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A blueprint's working directory is relative to the working directory."""
        monkeypatch.chdir(tmp_path)

    def test_template_blueprint(self) -> None:
        sim = ROMSGpuSimulation.from_blueprint(str(TEMPLATE_BLUEPRINT))

        assert isinstance(sim, ROMSGpuSimulation)
        assert sim.name == "Test GPU Blueprint"
        assert sim.codebase.source.location == GPU_REPO
        assert sim.codebase.source.checkout_target == "v0.1.2-beta"
        # 8 tiles on 2 nodes, one writer per node
        assert sim.discretization.n_procs_x == 4
        assert sim.discretization.n_procs_y == 2
        assert sim.rank_layout == GpuRankLayout(tiles=8, nodes=2, writers_per_node=1)
        assert sim.namelist_overrides == {
            "time_stepping": {"dt": 1},
            "vertical_mixing_settings": {"akt_bak": [0.0, 0.0]},
        }
        self.assert_a_gpu_simulation(sim)

    def test_bundled_blueprint(self) -> None:
        sim = ROMSGpuSimulation.from_blueprint(str(BUNDLED_BLUEPRINT))

        assert sim.name == "wales-toy-gpu"
        assert sim.codebase.source.location == GPU_REPO
        assert sim.codebase.source.checkout_target == "v0.1.2-beta"
        assert sim.rank_layout == GpuRankLayout(tiles=4, nodes=1, writers_per_node=1)
        assert sim.discretization.n_procs_tot == 4
        assert sim.namelist_overrides == {
            "time_stepping": {"dt": 360},
            "vertical_mixing_settings": {"akt_bak": [0.0, 0.0]},
        }
        assert sim.cdr_forcing is not None
        assert sim.model_grid is not None
        assert sim.initial_conditions is not None
        assert len(sim.boundary_forcing) == 2
        assert len(sim.surface_forcing) == 2
        assert sim.tidal_forcing is not None
        self.assert_a_gpu_simulation(sim)

    @staticmethod
    def assert_a_gpu_simulation(sim: ROMSGpuSimulation) -> None:
        assert sim.io_layout is IOLayout.JOINED
        assert sim.use_pio is False
        assert sim.compile_time_code is None
        assert sim.marbl_codebase is None
        assert sim.codebases == [sim.codebase]
        assert isinstance(sim.codebase, ROMSGpuExternalCodeBase)
        # the namelist and MARBL settings come from the blueprint's run_time code
        assert sorted(f.basename for f in sim.runtime_code.source) == [
            "marbl_in",
            "namelist.nml",
        ]

    def test_dates_come_from_the_blueprint(self) -> None:
        sim = ROMSGpuSimulation.from_blueprint(str(BUNDLED_BLUEPRINT))

        assert sim.start_date.isoformat() == "2012-01-01T00:00:00"
        assert sim.end_date.isoformat() == "2012-01-01T00:30:00"
        assert sim.valid_start_date is not None
        assert sim.valid_start_date.isoformat() == "2012-01-01T00:00:00"
        assert sim.valid_end_date is not None
        assert sim.valid_end_date.isoformat() == "2012-01-31T12:00:00"
