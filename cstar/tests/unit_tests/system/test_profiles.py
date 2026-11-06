import os
from collections.abc import Generator
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest

from cstar.base.exceptions import CstarError
from cstar.system.environment import CStarEnvironment, LmodEnvSettings
from cstar.system.manager import (
    CStarSystemManager,
    LinuxSystemContext,
    PerlmutterSystemContext,
    SystemSelection,
    SystemSource,
)

CPU_ENV = (
    "NETCDFHOME=/cpu/netcdf/\n"
    "PNETCDFHOME=/cpu/pnetcdf/\n"
    "PATH=${PATH}:/cpu/netcdf/bin\n"
    "LIBRARY_PATH=/cpu/netcdf/lib\n"
)
GPU_ENV = (
    "NETCDFHOME=/gpu/netcdf/\n"
    "PATH=${PATH}:/gpu/netcdf/bin\n"
    "LIBRARY_PATH=/gpu/netcdf/lib\n"
    "MPICH_GPU_SUPPORT_ENABLED=1\n"
)
BASE_PATH = "/usr/bin:/bin"


@pytest.fixture
def package_root(tmp_path: Path) -> Path:
    """A package root holding CPU and GPU module lists and env files."""
    (tmp_path / "additional_files/env_files").mkdir(parents=True)
    (tmp_path / "additional_files/lmod_lists").mkdir(parents=True)
    for stem, text in (("perlmutter", CPU_ENV), ("perlmutter.gpu", GPU_ENV)):
        (tmp_path / f"additional_files/env_files/{stem}.env").write_text(text)
        (tmp_path / f"additional_files/lmod_lists/{stem}.lmod").write_text("gcc\n")
    return tmp_path


@pytest.fixture
def lmod_calls() -> Generator[mock.Mock, None, None]:
    """Mock the call into Lmod, and the process environment around it."""
    env = {
        LmodEnvSettings.variable("SYSTEM_NAME"): "Perlmutter",
        "PATH": BASE_PATH,
        "LIBRARY_PATH": "/base/lib",
    }
    with (
        patch.dict(os.environ, env, clear=True),
        mock.patch.object(CStarEnvironment, "uses_lmod", True),
        mock.patch.object(CStarEnvironment, "_call_lmod") as call_lmod,
    ):
        yield call_lmod


@pytest.fixture
def manager(
    package_root: Path, lmod_calls: mock.Mock
) -> Generator[CStarSystemManager, None, None]:
    """A Perlmutter manager over the temporary files, with the CPU profile active."""
    selection = SystemSelection(PerlmutterSystemContext, SystemSource.DETECTED)
    with (
        mock.patch("cstar.system.manager.select_system", return_value=selection),
        patch.object(CStarEnvironment, "package_root", new=package_root),
    ):
        yield CStarSystemManager()


def make_environment(profile: str = "") -> CStarEnvironment:
    """Build an environment that reads the files of the patched package root."""
    return CStarEnvironment("perlmutter", "mpirun", "gnu", profile=profile)


class TestUnload:
    """Tests for `CStarEnvironment.unload`."""

    def test_restores_prior_value_and_removes_new_keys(
        self, package_root: Path, lmod_calls: mock.Mock
    ) -> None:
        """Verify unload restores what existed and removes what the env file added."""
        os.environ["NETCDFHOME"] = "/before/"
        with patch.object(CStarEnvironment, "package_root", new=package_root):
            env = make_environment()

        assert os.environ["NETCDFHOME"] == "/cpu/netcdf/"
        assert "PNETCDFHOME" in os.environ

        env.unload()

        assert os.environ["NETCDFHOME"] == "/before/"
        assert os.environ["PATH"] == BASE_PATH
        assert os.environ["LIBRARY_PATH"] == "/base/lib"
        assert "PNETCDFHOME" not in os.environ

    def test_is_safe_to_call_twice(
        self, package_root: Path, lmod_calls: mock.Mock
    ) -> None:
        """Verify a second unload neither fails nor undoes later changes."""
        with patch.object(CStarEnvironment, "package_root", new=package_root):
            env = make_environment()
        env.unload()

        os.environ["PNETCDFHOME"] = "/set/later/"
        env.unload()

        assert os.environ["PNETCDFHOME"] == "/set/later/"
        assert os.environ["PATH"] == BASE_PATH

    def test_keeps_loaded_variables_after_unload(
        self, package_root: Path, lmod_calls: mock.Mock
    ) -> None:
        """Verify the record of what was loaded outlives the unload."""
        with patch.object(CStarEnvironment, "package_root", new=package_root):
            env = make_environment()
        env.unload()

        assert env.environment_variables["NETCDFHOME"] == "/cpu/netcdf/"

    def test_load_after_unload_reapplies_the_env_file(
        self, package_root: Path, lmod_calls: mock.Mock
    ) -> None:
        """Verify a cached profile can be loaded again, modules included."""
        with patch.object(CStarEnvironment, "package_root", new=package_root):
            env = make_environment()
            env.unload()
            lmod_calls.reset_mock()
            env.load()

        assert lmod_calls.call_args_list == [
            mock.call("reset"),
            mock.call("load gcc"),
        ]
        assert os.environ["NETCDFHOME"] == "/cpu/netcdf/"
        assert os.environ["PATH"] == f"{BASE_PATH}:/cpu/netcdf/bin"

    def test_load_twice_restores_the_original(
        self, package_root: Path, lmod_calls: mock.Mock
    ) -> None:
        """Verify loading an already loaded profile does not record its own values."""
        with patch.object(CStarEnvironment, "package_root", new=package_root):
            env = make_environment()
            env.load()
        env.unload()

        assert os.environ["PATH"] == BASE_PATH
        assert "PNETCDFHOME" not in os.environ


class TestProfileSwitching:
    """Tests for switching the active profile of the `CStarSystemManager`."""

    def test_gpu_after_cpu_inherits_nothing_from_cpu(
        self, manager: CStarSystemManager
    ) -> None:
        """Verify the GPU profile leaves no CPU-only variable and builds its own PATH."""
        assert os.environ["PNETCDFHOME"] == "/cpu/pnetcdf/"

        gpu = manager.environment_for("gpu")

        assert gpu.profile == "gpu"
        assert "PNETCDFHOME" not in os.environ
        assert os.environ["NETCDFHOME"] == "/gpu/netcdf/"
        assert os.environ["PATH"] == f"{BASE_PATH}:/gpu/netcdf/bin"
        assert os.environ["LIBRARY_PATH"] == "/gpu/netcdf/lib"
        assert os.environ["MPICH_GPU_SUPPORT_ENABLED"] == "1"
        assert "/cpu/" not in os.environ["PATH"]

    def test_switching_back_restores_the_cpu_profile(
        self, manager: CStarSystemManager
    ) -> None:
        """Verify returning to CPU drops the GPU variables and restores the CPU ones."""
        cpu = manager.environment
        manager.environment_for("gpu")

        assert manager.environment_for("cpu") is cpu

        assert "MPICH_GPU_SUPPORT_ENABLED" not in os.environ
        assert os.environ["PNETCDFHOME"] == "/cpu/pnetcdf/"
        assert os.environ["NETCDFHOME"] == "/cpu/netcdf/"
        assert os.environ["PATH"] == f"{BASE_PATH}:/cpu/netcdf/bin"
        assert os.environ["LIBRARY_PATH"] == "/cpu/netcdf/lib"

    def test_repeated_switching_does_not_accumulate_paths(
        self, manager: CStarSystemManager
    ) -> None:
        """Verify many round trips leave the same PATH as one."""
        for _ in range(3):
            manager.environment_for("gpu")
            manager.environment_for("cpu")

        assert os.environ["PATH"] == f"{BASE_PATH}:/cpu/netcdf/bin"

    def test_active_profile_is_returned_without_reloading(
        self, manager: CStarSystemManager, lmod_calls: mock.Mock
    ) -> None:
        """Verify asking for the active profile returns the same object and loads nothing."""
        cpu = manager.environment_for("cpu")
        gpu = manager.environment_for("gpu")
        lmod_calls.reset_mock()

        assert manager.environment_for("gpu") is gpu
        assert manager.environment_for("gpu") is gpu
        assert manager.environment_for("cpu") is cpu

        # only the switch back to cpu loaded modules
        assert lmod_calls.call_args_list == [
            mock.call("reset"),
            mock.call("load gcc"),
        ]
        assert os.environ["PATH"] == f"{BASE_PATH}:/cpu/netcdf/bin"

    def test_unregistered_toolchain_leaves_the_active_profile(
        self, package_root: Path, lmod_calls: mock.Mock
    ) -> None:
        """Verify a refused request does not unload the profile in use."""
        selection = SystemSelection(LinuxSystemContext, SystemSource.DETECTED)
        with (
            mock.patch("cstar.system.manager.select_system", return_value=selection),
            patch.object(CStarEnvironment, "package_root", new=package_root),
            mock.patch.object(CStarEnvironment, "_load_env", return_value={}),
            mock.patch.object(CStarEnvironment, "uses_lmod", False),
        ):
            manager = CStarSystemManager()
            cpu = manager.environment
            with pytest.raises(CstarError):
                manager.environment_for("gpu")

            assert manager.environment_for("cpu") is cpu
