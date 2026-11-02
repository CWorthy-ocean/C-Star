import os
from collections.abc import Generator
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest
from dotenv import dotenv_values

from cstar.base.exceptions import CstarError
from cstar.system.environment import CStarEnvironment, LmodEnvSettings
from cstar.system.manager import (
    CTX_REGISTRY,
    AnvilSystemContext,
    BouchetSystemContext,
    CStarSystemManager,
    LinuxSystemContext,
    PerlmutterSystemContext,
    SystemContext,
    SystemSelection,
    SystemSource,
)
from cstar.system.toolchain import GpuNodes, Toolchain

PACKAGE_ROOT = Path(CStarEnvironment._find_package_root())
GPU_SYSTEMS = ("anvil", "perlmutter")


@pytest.mark.parametrize("ctx", CTX_REGISTRY.values(), ids=list(CTX_REGISTRY))
def test_cpu_toolchain_is_derived_from_class_vars(ctx: type[SystemContext]) -> None:
    """Verify every context's CPU toolchain restates nothing beyond its class vars."""
    assert ctx.toolchain("cpu") == Toolchain(
        compiler=ctx.compiler, mpi_prefix=ctx.mpi_prefix
    )


@pytest.mark.parametrize("ctx", [PerlmutterSystemContext, AnvilSystemContext])
def test_gpu_toolchain(ctx: type[SystemContext]) -> None:
    """Verify the GPU toolchains of the systems that register one."""
    assert ctx.toolchain("gpu") == Toolchain(
        compiler="nvfortran",
        mpi_prefix="srun --kill-on-bad-exit=1",
        profile="gpu",
        gpu_arch="cc80",
    )


@pytest.mark.parametrize("ctx", [BouchetSystemContext, LinuxSystemContext])
def test_gpu_toolchain_missing_names_system(ctx: type[SystemContext]) -> None:
    """Verify asking a system without GPU nodes for a GPU toolchain fails by name."""
    with pytest.raises(CstarError, match=ctx.name):
        ctx.toolchain("gpu")


def test_perlmutter_gpu_nodes() -> None:
    """Verify the GPU node facts of Perlmutter, and that CPU jobs keep their directives."""
    scheduler = PerlmutterSystemContext.create_scheduler()
    assert scheduler is not None

    assert scheduler.gpu == GpuNodes(
        gpus_per_node=4,
        queue="regular",
        directives={"-C": "gpu", "--gpu-bind": "none"},
        cores_per_node=64,
    )
    assert scheduler.other_scheduler_directives == {"-C": "cpu"}


def test_anvil_gpu_nodes() -> None:
    """Verify the GPU node facts of Anvil, and that its primary queue is unchanged."""
    scheduler = AnvilSystemContext.create_scheduler()
    assert scheduler is not None

    assert scheduler.gpu == GpuNodes(
        gpus_per_node=4,
        queue="gpu",
        directives={"--gpu-bind": "none"},
        cores_per_node=128,
    )
    assert scheduler.primary_queue_name == "wholenode"
    assert scheduler.gpu.queue in scheduler.queue_names


def test_no_gpu_nodes_by_default() -> None:
    """Verify a system without GPU nodes registered reports none."""
    scheduler = BouchetSystemContext.create_scheduler()
    assert scheduler is not None
    assert scheduler.gpu is None


class TestEnvironmentFor:
    """Tests for the per-toolchain environments of the `CStarSystemManager`."""

    @pytest.fixture
    def manager(self) -> Generator[CStarSystemManager, None, None]:
        """A Perlmutter manager whose environments build without Lmod or env files."""
        selection = SystemSelection(PerlmutterSystemContext, SystemSource.DETECTED)
        env = {LmodEnvSettings.variable("SYSTEM_NAME"): "Perlmutter"}
        with (
            patch.dict(os.environ, env, clear=True),
            mock.patch("cstar.system.manager.select_system", return_value=selection),
            mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
            mock.patch.object(CStarEnvironment, "uses_lmod", False),
        ):
            yield CStarSystemManager()

    def test_environment_is_the_cpu_environment(
        self, manager: CStarSystemManager
    ) -> None:
        """Verify `environment` is the cached CPU profile, built with the old files."""
        assert manager.environment is manager.environment_for("cpu")
        assert manager.environment.profile == ""
        assert manager.environment.compiler == "gnu"
        assert manager.environment.lmod_path.name == "perlmutter.lmod"

    def test_environment_for_gpu_is_cached(self, manager: CStarSystemManager) -> None:
        """Verify the GPU environment uses the GPU toolchain and is built once."""
        first = manager.environment_for("gpu")

        assert manager.environment_for("gpu") is first
        # `environment` follows the active profile without switching it
        assert manager.environment is first
        assert manager.environment_for("cpu") is not first
        assert manager.environment is not first
        assert first.profile == "gpu"
        assert first.compiler == "nvfortran"
        assert first.lmod_path.name == "perlmutter.gpu.lmod"
        assert first.system_env_path.name == "perlmutter.gpu.env"

    def test_environment_for_gpu_without_gpu_names_system(self) -> None:
        """Verify requesting a GPU environment from a CPU-only system fails by name."""
        selection = SystemSelection(LinuxSystemContext, SystemSource.DETECTED)
        with (
            patch.dict(os.environ, {}, clear=True),
            mock.patch("cstar.system.manager.select_system", return_value=selection),
            mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
            mock.patch.object(CStarEnvironment, "uses_lmod", False),
        ):
            manager = CStarSystemManager()
            with pytest.raises(CstarError, match=LinuxSystemContext.name):
                manager.environment_for("gpu")


def test_profile_in_str_only_when_set() -> None:
    """Verify the CPU environment's summary is unchanged and a profile adds one line."""
    with (
        patch.dict(os.environ, {}, clear=True),
        mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
        mock.patch.object(CStarEnvironment, "uses_lmod", False),
    ):
        cpu = CStarEnvironment("mock", "mpirun", "gnu")
        gpu = CStarEnvironment("mock", "mpirun", "nvfortran", profile="gpu")

    assert "Profile" not in str(cpu)
    assert "\nProfile: gpu\n" in str(gpu)


@pytest.mark.parametrize("system", GPU_SYSTEMS)
def test_gpu_data_files_exist_and_parse(system: str) -> None:
    """Verify the GPU module and env files ship and load through the existing parsers."""
    lmod_file = PACKAGE_ROOT / f"additional_files/lmod_lists/{system}.gpu.lmod"
    env_file = PACKAGE_ROOT / f"additional_files/env_files/{system}.gpu.env"
    assert lmod_file.is_file()
    assert env_file.is_file()

    env = dotenv_values(env_file)
    assert env["GIT_DISCOVERY_ACROSS_FILESYSTEM"] == "1"
    assert "PNETCDFHOME" not in env
    assert all(value is not None for value in env.values())

    with (
        patch.dict(os.environ, {}, clear=True),
        mock.patch.object(CStarEnvironment, "uses_lmod", True),
        mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
        mock.patch.object(CStarEnvironment, "_call_lmod") as call_lmod,
    ):
        CStarEnvironment(system, "mpirun", "nvfortran", profile="gpu")

    loaded = [c.args[0] for c in call_lmod.call_args_list]
    assert loaded[0] == "reset"
    assert len(loaded) > 1
    for command in loaded[1:]:
        assert command.startswith("load ")
        module = command.removeprefix("load ")
        assert module
        assert not any(ch in module for ch in "# ")


def test_perlmutter_gpu_env_keys() -> None:
    """Verify the Perlmutter GPU env file's keys differ from the CPU file as designed."""
    cpu = dotenv_values(
        PACKAGE_ROOT / "additional_files/env_files/perlmutter.env",
        interpolate=False,
    )
    gpu = dotenv_values(
        PACKAGE_ROOT / "additional_files/env_files/perlmutter.gpu.env",
        interpolate=False,
    )

    assert set(gpu) == (set(cpu) - {"PNETCDFHOME"}) | {"MPICH_GPU_SUPPORT_ENABLED"}
    assert gpu["MPICH_GPU_SUPPORT_ENABLED"] == "1"
    assert gpu["NETCDFHOME"] == "${CRAY_NETCDF_HDF5PARALLEL_PREFIX}/"


def test_lmod_parser_skips_comments_and_blank_lines(tmp_path: Path) -> None:
    """Verify comment-only, blank and inline-commented lines never reach `module load`."""
    modules_file = tmp_path / "x.lmod"
    modules_file.write_text("# header\n\ngpu  # meta-module\nnvhpc\n")

    with (
        patch.dict(os.environ, {}, clear=True),
        mock.patch.object(CStarEnvironment, "lmod_path", modules_file),
        mock.patch.object(CStarEnvironment, "uses_lmod", True),
        mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
        mock.patch.object(CStarEnvironment, "_call_lmod") as call_lmod,
    ):
        CStarEnvironment("mock", "mpirun", "gnu")

    assert call_lmod.call_args_list == [
        mock.call("reset"),
        mock.call("load gpu"),
        mock.call("load nvhpc"),
    ]
