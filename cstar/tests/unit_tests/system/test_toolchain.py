import contextlib
import os
from collections.abc import Generator
from unittest import mock
from unittest.mock import patch

import pytest

from cstar.base.exceptions import CstarError
from cstar.system.environment import CStarEnvironment, LmodEnvSettings
from cstar.system.manager import (
    CStarSystemManager,
    SystemContext,
    SystemSelection,
    SystemSource,
    registered_context,
    registered_names,
)
from cstar.system.spec import SystemFile
from cstar.system.toolchain import GpuNodes, Toolchain

GPU_SYSTEMS = ("perlmutter",)

PERLMUTTER_GPU_MODULES = (
    "gpu/1.0",
    "PrgEnv-nvidia/8.7.0",
    "nvidia/26.5",
    "cudatoolkit/13.2",
    "craype-accel-nvidia80",
    "cray-hdf5-parallel/1.14.3.9",
    "cray-netcdf-hdf5parallel/4.9.0.13",
)
PERLMUTTER_GPU_ENV = {
    "MPIHOME": "${CRAY_MPICH_PREFIX}/",
    "NETCDFHOME": "${CRAY_NETCDF_HDF5PARALLEL_PREFIX}/",
    "PATH": "${PATH}:${NETCDFHOME}bin",
    "LIBRARY_PATH": "${LIBRARY_PATH}:${NETCDFHOME}lib",
    "MPICH_GPU_SUPPORT_ENABLED": "1",
    "GIT_DISCOVERY_ACROSS_FILESYSTEM": "1",
}


def _system_file(name: str) -> SystemFile:
    """Return the system file the named bundled context was read from."""
    system_file = registered_context(name).system_file
    assert system_file is not None
    return system_file


@pytest.mark.parametrize("name", registered_names())
def test_cpu_toolchain_is_derived_from_class_vars(name: str) -> None:
    """Verify every context's CPU toolchain restates nothing beyond its class vars,
    and carries the modules and env of its system file's cpu entry.
    """
    ctx = registered_context(name)
    cpu = _system_file(name).spec.toolchains["cpu"]
    assert ctx.toolchain("cpu") == Toolchain(
        compiler=ctx.compiler,
        mpi_prefix=ctx.mpi_prefix,
        modules=tuple(cpu.modules),
        env=dict(cpu.env),
    )


def test_gpu_toolchain() -> None:
    """Verify the GPU toolchain of Perlmutter, the system that registers one."""
    assert registered_context("perlmutter").toolchain("gpu") == Toolchain(
        compiler="nvfortran",
        mpi_prefix="srun --kill-on-bad-exit=1",
        profile="gpu",
        gpu_arch="cc80",
        link_flags="${PE_MPICH_GTL_DIR_nvidia80} ${PE_MPICH_GTL_LIBS_nvidia80}",
        modules=PERLMUTTER_GPU_MODULES,
        env=PERLMUTTER_GPU_ENV,
    )


def test_link_flags_default_to_none() -> None:
    """Verify a toolchain adds no linker flags unless its system names some."""
    assert Toolchain(compiler="gnu", mpi_prefix="mpirun").link_flags == ""
    assert registered_context("perlmutter").toolchain("cpu").link_flags == ""


@pytest.mark.parametrize("name", ["anvil", "bouchet", "linux_x86_64"])
def test_gpu_toolchain_missing_names_system(name: str) -> None:
    """Verify asking a system without GPU nodes for a GPU toolchain fails by name."""
    ctx = registered_context(name)
    with pytest.raises(CstarError, match=ctx.name):
        ctx.toolchain("gpu")


def test_perlmutter_gpu_nodes() -> None:
    """Verify the GPU node facts of Perlmutter, and that CPU jobs keep their directives."""
    scheduler = registered_context("perlmutter").create_scheduler()
    assert scheduler is not None

    assert scheduler.gpu == GpuNodes(
        gpus_per_node=4,
        queue="regular",
        directives={"-C": "gpu", "--gpu-bind": "none"},
        cores_per_node=64,
        account_required=True,
    )
    assert scheduler.other_scheduler_directives == {"-C": "cpu"}


def test_anvil_gpu_nodes() -> None:
    """Verify the GPU node facts of Anvil, and that its primary queue is unchanged."""
    scheduler = registered_context("anvil").create_scheduler()
    assert scheduler is not None

    assert scheduler.gpu == GpuNodes(
        gpus_per_node=4,
        queue="gpu",
        directives={"--gpu-bind": "none"},
        cores_per_node=128,
    )
    assert scheduler.primary_queue_name == "wholenode"
    assert scheduler.gpu.queue in scheduler.queue_names
    assert not scheduler.gpu.account_required


def test_anvil_registers_no_gpu_toolchain() -> None:
    """Verify Anvil's system file lists no GPU toolchain (modules or env)."""
    assert "gpu" not in _system_file("anvil").spec.toolchains


def test_no_gpu_nodes_by_default() -> None:
    """Verify a system without GPU nodes registered reports none."""
    scheduler = registered_context("bouchet").create_scheduler()
    assert scheduler is not None
    assert scheduler.gpu is None


class TestEnvironmentFor:
    """Tests for the per-toolchain environments of the `CStarSystemManager`."""

    @pytest.fixture
    def manager(self) -> Generator[CStarSystemManager, None, None]:
        """A Perlmutter manager whose environments build without Lmod or env."""
        selection = SystemSelection(
            registered_context("perlmutter"), SystemSource.DETECTED
        )
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
        """Verify `environment` is the cached CPU profile, built from the cpu entry of its system file."""
        assert manager.environment is manager.environment_for("cpu")
        assert manager.environment.profile == ""
        assert manager.environment.compiler == "gnu"
        assert "cpu toolchain" in manager.environment.env_origin
        assert "perlmutter.yaml" in manager.environment.env_origin

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
        assert "gpu toolchain" in first.env_origin
        assert "perlmutter.yaml" in first.env_origin

    def test_environment_for_gpu_without_gpu_names_system(self) -> None:
        """Verify requesting a GPU environment from a CPU-only system fails by name."""
        selection = SystemSelection(
            registered_context("linux_x86_64"), SystemSource.DETECTED
        )
        with (
            patch.dict(os.environ, {}, clear=True),
            mock.patch("cstar.system.manager.select_system", return_value=selection),
            mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
            mock.patch.object(CStarEnvironment, "uses_lmod", False),
        ):
            manager = CStarSystemManager()
            with pytest.raises(
                CstarError, match=registered_context("linux_x86_64").name
            ):
                manager.environment_for("gpu")


class TestManagerToolchain:
    """Tests for `CStarSystemManager.toolchain`."""

    @staticmethod
    @contextlib.contextmanager
    def _manager(
        context: type[SystemContext], system_name: str
    ) -> Generator[CStarSystemManager, None, None]:
        """A manager for `context` whose environments build without Lmod or env."""
        selection = SystemSelection(context, SystemSource.DETECTED)
        env = {LmodEnvSettings.variable("SYSTEM_NAME"): system_name}
        with (
            patch.dict(os.environ, env, clear=True),
            mock.patch("cstar.system.manager.select_system", return_value=selection),
            mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
            mock.patch.object(CStarEnvironment, "uses_lmod", False),
        ):
            yield CStarSystemManager()

    def test_gpu_toolchain_on_perlmutter(self) -> None:
        """Verify the manager reports the system's GPU toolchain, link flags included,
        without switching the active profile.
        """
        with self._manager(registered_context("perlmutter"), "Perlmutter") as manager:
            toolchain = manager.toolchain("gpu")

            assert toolchain == registered_context("perlmutter").toolchain("gpu")
            assert toolchain.link_flags
            assert manager.toolchain("cpu").link_flags == ""
            # describing a toolchain does not load it
            assert manager.environment.profile == ""

    def test_missing_gpu_toolchain_names_system(self) -> None:
        """Verify a system without a GPU toolchain refuses by name."""
        with self._manager(registered_context("linux_x86_64"), "") as manager:
            with pytest.raises(
                CstarError, match=registered_context("linux_x86_64").name
            ):
                manager.toolchain("gpu")
            assert manager.toolchain("cpu") == registered_context(
                "linux_x86_64"
            ).toolchain("cpu")


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
def test_gpu_toolchain_data_exists_and_loads(system: str) -> None:
    """Verify the GPU modules and env are in the system file and load through
    `CStarEnvironment`.
    """
    gpu = _system_file(system).spec.toolchains["gpu"]
    assert gpu.env["GIT_DISCOVERY_ACROSS_FILESYSTEM"] == "1"
    assert "PNETCDFHOME" not in gpu.env
    assert gpu.modules
    assert all(value is not None for value in gpu.env.values())

    with (
        patch.dict(os.environ, {}, clear=True),
        mock.patch.object(CStarEnvironment, "uses_lmod", True),
        mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
        mock.patch.object(CStarEnvironment, "_call_lmod") as call_lmod,
    ):
        CStarEnvironment(
            system, "mpirun", "nvfortran", profile="gpu", modules=gpu.modules
        )

    loaded = [c.args[0] for c in call_lmod.call_args_list]
    assert loaded[0] == "reset"
    assert len(loaded) > 1
    for command in loaded[1:]:
        assert command.startswith("load ")
        module = command.removeprefix("load ")
        assert module
        assert not any(ch in module for ch in "# ")


def test_perlmutter_gpu_env_keys() -> None:
    """Verify the Perlmutter GPU env's keys differ from the CPU env as designed."""
    toolchains = _system_file("perlmutter").spec.toolchains
    cpu = toolchains["cpu"].env
    gpu = toolchains["gpu"].env

    assert set(gpu) == (set(cpu) - {"PNETCDFHOME"}) | {"MPICH_GPU_SUPPORT_ENABLED"}
    assert gpu["MPICH_GPU_SUPPORT_ENABLED"] == "1"
    assert gpu["NETCDFHOME"] == "${CRAY_NETCDF_HDF5PARALLEL_PREFIX}/"


def test_lmod_modules_load_in_order_after_reset() -> None:
    """Verify the toolchain's modules reach `module load` one each, in order, after reset."""
    with (
        patch.dict(os.environ, {}, clear=True),
        mock.patch.object(CStarEnvironment, "uses_lmod", True),
        mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
        mock.patch.object(CStarEnvironment, "_call_lmod") as call_lmod,
    ):
        CStarEnvironment("mock", "mpirun", "gnu", modules=["gpu", "nvhpc"])

    assert call_lmod.call_args_list == [
        mock.call("reset"),
        mock.call("load gpu"),
        mock.call("load nvhpc"),
    ]
