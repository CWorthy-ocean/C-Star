import os
import subprocess
import unittest.mock as mock
from pathlib import Path

import pytest

from cstar.roms.gpu_external_codebase import (
    MARBL_SOURCE_SENTINEL,
    ROMSGpuExternalCodeBase,
)

GPU_REPO = "https://github.com/CWorthy-ocean/gpu-ucla-roms.git"


@pytest.fixture
def clone(tmp_path: Path) -> Path:
    """A directory standing in for a clone of gpu-ucla-roms."""
    path = tmp_path / "gpu-ucla-roms"
    path.mkdir()
    return path


@pytest.fixture
def staged_codebase(clone: Path) -> ROMSGpuExternalCodeBase:
    """A GPU codebase whose clone is staged at `clone` (no network)."""
    codebase = ROMSGpuExternalCodeBase()
    codebase._working_copy = mock.Mock(path=clone)
    return codebase


class TestROMSGpuExternalCodeBaseProperties:
    def test_defaults(self) -> None:
        """The defaults pin the gpu-ucla-roms repository at its tested release."""
        codebase = ROMSGpuExternalCodeBase()

        assert codebase._default_source_repo == GPU_REPO
        assert codebase._default_checkout_target == "v0.1.2-beta"
        assert codebase.root_env_var == "ROMS_GPU_ROOT"
        assert codebase.source.location == GPU_REPO
        assert codebase.source.checkout_target == "v0.1.2-beta"

    def test_init_with_args(self) -> None:
        """A source repository and checkout target replace the defaults."""
        codebase = ROMSGpuExternalCodeBase(
            source_repo="https://github.com/example/fork.git",
            checkout_target="a-branch",
        )

        assert codebase.source.location == "https://github.com/example/fork.git"
        assert codebase.source.checkout_target == "a-branch"
        assert codebase.to_dict() == {
            "source_repo": "https://github.com/example/fork.git",
            "checkout_target": "a-branch",
        }

    def test_key_names_the_codebase_subdirectory(self) -> None:
        """The GPU model is the simulation's only ROMS codebase, so it takes the
        `roms` subdirectory of the codebase directory, as ucla-roms does.
        """
        assert ROMSGpuExternalCodeBase().key == "roms"


class TestROMSGpuExternalCodeBaseConfigure:
    def test_configure_fetches_the_marbl_submodule_in_the_clone(
        self,
        staged_codebase: ROMSGpuExternalCodeBase,
        clone: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A plain clone leaves MARBL's directory empty, so `_configure` fetches
        the submodule, recursively, in the clone, and exports `ROMS_GPU_ROOT`.
        """
        monkeypatch.setenv("ROMS_GPU_ROOT", "set by the test, replaced by _configure")

        with mock.patch("cstar.roms.gpu_external_codebase._run_cmd") as mock_run_cmd:
            staged_codebase._configure()

        mock_run_cmd.assert_called_once_with(
            "git submodule update --init --recursive",
            cwd=clone,
            msg_pre=mock.ANY,
            msg_post=mock.ANY,
            msg_err=mock.ANY,
            raise_on_error=True,
        )
        assert os.environ["ROMS_GPU_ROOT"] == str(clone)

    @mock.patch("cstar.base.utils.subprocess.run")
    def test_configure_raises_when_the_submodule_cannot_be_fetched(
        self,
        mock_subprocess: mock.Mock,
        staged_codebase: ROMSGpuExternalCodeBase,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A failed submodule update stops the setup, saying what failed."""
        monkeypatch.setenv("ROMS_GPU_ROOT", "set by the test, replaced by _configure")
        mock_subprocess.return_value = subprocess.CompletedProcess(
            args=["git submodule update"],
            returncode=128,
            stdout="",
            stderr="fatal: could not read Username",
        )

        with pytest.raises(
            RuntimeError,
            match=(
                "Error when fetching the MARBL submodule of gpu-ucla-roms. "
                "Return Code: `128`. STDERR:\nfatal: could not read Username"
            ),
        ):
            staged_codebase._configure()

        assert mock_subprocess.call_count == 1


class TestROMSGpuExternalCodeBaseIsBuiltAt:
    def test_true_when_the_marbl_source_is_present(self, clone: Path) -> None:
        sentinel = clone / MARBL_SOURCE_SENTINEL
        sentinel.parent.mkdir(parents=True)
        sentinel.write_text("module marbl_interface\n")

        assert ROMSGpuExternalCodeBase()._is_built_at(clone) is True

    def test_false_for_a_clone_without_the_submodule(self, clone: Path) -> None:
        """The submodule's directory is empty until it is fetched."""
        (clone / "MARBL").mkdir()

        assert ROMSGpuExternalCodeBase()._is_built_at(clone) is False

    def test_false_when_the_named_path_is_not_a_file(self, clone: Path) -> None:
        (clone / MARBL_SOURCE_SENTINEL).mkdir(parents=True)

        assert ROMSGpuExternalCodeBase()._is_built_at(clone) is False

    def test_the_sentinel_is_a_file_the_gpu_build_compiles(self) -> None:
        assert MARBL_SOURCE_SENTINEL == "MARBL/src/marbl_interface.F90"
