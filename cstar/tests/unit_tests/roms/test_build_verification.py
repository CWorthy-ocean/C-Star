"""Unit tests for `cstar.roms.build_verification`."""

from pathlib import Path
from unittest import mock

import pytest

from cstar.roms.build_verification import (
    assert_single_toolchain_stack,
    explicit_mpi_wrapper,
    verify_roms_linkage,
)


def _fake_sysmgr(
    environment_variables: dict[str, str] | None = None,
    compiler: str = "gnu",
    uses_lmod: bool = False,
    system_env_path: Path = Path("/fake/env/system.env"),
) -> mock.MagicMock:
    """Build a fake `CStarSystemManager` for use with `get_sysmgr` patches."""
    sysmgr = mock.MagicMock()
    sysmgr.environment.environment_variables = environment_variables or {}
    sysmgr.environment.compiler = compiler
    sysmgr.environment.uses_lmod = uses_lmod
    sysmgr.environment.system_env_path = system_env_path
    return sysmgr


class TestExplicitMpiWrapper:
    """Tests for `explicit_mpi_wrapper`."""

    def test_returns_path_when_gnu_and_file_exists(self, tmp_path: Path):
        mpi_home = tmp_path / "mpi"
        (mpi_home / "bin").mkdir(parents=True)
        mpifort = mpi_home / "bin" / "mpifort"
        mpifort.touch()

        sysmgr = _fake_sysmgr(
            environment_variables={"MPIHOME": str(mpi_home)}, compiler="gnu"
        )
        with mock.patch(
            "cstar.roms.build_verification.get_sysmgr", return_value=sysmgr
        ):
            assert explicit_mpi_wrapper() == str(mpifort)

    def test_returns_none_when_file_missing(self, tmp_path: Path):
        mpi_home = tmp_path / "mpi"
        mpi_home.mkdir()

        sysmgr = _fake_sysmgr(
            environment_variables={"MPIHOME": str(mpi_home)}, compiler="gnu"
        )
        with mock.patch(
            "cstar.roms.build_verification.get_sysmgr", return_value=sysmgr
        ):
            assert explicit_mpi_wrapper() is None

    def test_returns_none_when_not_gnu(self, tmp_path: Path):
        mpi_home = tmp_path / "mpi"
        (mpi_home / "bin").mkdir(parents=True)
        (mpi_home / "bin" / "mpifort").touch()

        sysmgr = _fake_sysmgr(
            environment_variables={"MPIHOME": str(mpi_home)}, compiler="intel"
        )
        with mock.patch(
            "cstar.roms.build_verification.get_sysmgr", return_value=sysmgr
        ):
            assert explicit_mpi_wrapper() is None

    def test_returns_none_when_mpihome_unset(self):
        sysmgr = _fake_sysmgr(environment_variables={}, compiler="gnu")
        with mock.patch(
            "cstar.roms.build_verification.get_sysmgr", return_value=sysmgr
        ):
            assert explicit_mpi_wrapper() is None


class TestAssertSingleToolchainStack:
    """Tests for `assert_single_toolchain_stack`."""

    @pytest.mark.parametrize(
        "var", ["MPIHOME", "NETCDFHOME", "NETCDFFHOME", "PNETCDFHOME"]
    )
    def test_empty_declared_var_raises(self, var: str):
        env_vars = {var: "   "}
        sysmgr = _fake_sysmgr(environment_variables=env_vars)
        with (
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            pytest.raises(OSError, match=var),
        ):
            assert_single_toolchain_stack(None)

    def test_missing_nf_config_raises(self):
        sysmgr = _fake_sysmgr()

        def which_side_effect(name: str) -> str | None:
            return "/usr/bin/nc-config" if name == "nc-config" else None

        with (
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                side_effect=which_side_effect,
            ),
            pytest.raises(OSError, match="nf-config"),
        ):
            assert_single_toolchain_stack(None)

    def test_explicit_wrapper_that_does_not_exist_raises(self, tmp_path: Path):
        sysmgr = _fake_sysmgr()
        missing_wrapper = str(tmp_path / "does-not-exist" / "mpifort")

        with (
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/nc-config",
            ),
            pytest.raises(OSError, match="does not exist"),
        ):
            assert_single_toolchain_stack(missing_wrapper)

    def test_lmod_conda_shadowing_raises(self, tmp_path: Path, monkeypatch):
        conda_prefix = tmp_path / "conda"
        (conda_prefix / "bin").mkdir(parents=True)
        shadowed_tool = conda_prefix / "bin" / "nf-config"
        shadowed_tool.touch()

        monkeypatch.setenv("CONDA_PREFIX", str(conda_prefix))

        sysmgr = _fake_sysmgr(uses_lmod=True)

        def which_side_effect(name: str) -> str | None:
            if name == "nf-config":
                return str(shadowed_tool)
            if name == "nc-config":
                return "/opt/modules/netcdf/bin/nc-config"
            return "/opt/modules/mpi/bin/mpifort"

        with (
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                side_effect=which_side_effect,
            ),
            pytest.raises(OSError, match="conda deactivate"),
        ):
            assert_single_toolchain_stack(None)

    def test_lmod_happy_path_passes(self, monkeypatch):
        conda_prefix = "/some/conda/env"
        monkeypatch.setenv("CONDA_PREFIX", conda_prefix)

        sysmgr = _fake_sysmgr(uses_lmod=True)

        def which_side_effect(name: str) -> str | None:
            return f"/opt/modules/bin/{name}"

        with (
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                side_effect=which_side_effect,
            ),
        ):
            assert_single_toolchain_stack(None)

    def test_conda_system_tool_outside_conda_raises(self, tmp_path: Path, monkeypatch):
        conda_prefix = tmp_path / "conda"
        (conda_prefix / "bin").mkdir(parents=True)
        monkeypatch.setenv("CONDA_PREFIX", str(conda_prefix))

        sysmgr = _fake_sysmgr(uses_lmod=False)

        def which_side_effect(name: str) -> str | None:
            if name == "nf-config":
                return "/usr/bin/nf-config"  # outside conda
            if name == "nc-config":
                return str(conda_prefix / "bin" / "nc-config")
            return str(conda_prefix / "bin" / "mpifort")

        with (
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                side_effect=which_side_effect,
            ),
            pytest.raises(OSError, match="mixed-linkage"),
        ):
            assert_single_toolchain_stack(None)

    def test_conda_system_happy_path_passes(self, tmp_path: Path, monkeypatch):
        conda_prefix = tmp_path / "conda"
        (conda_prefix / "bin").mkdir(parents=True)
        monkeypatch.setenv("CONDA_PREFIX", str(conda_prefix))

        sysmgr = _fake_sysmgr(uses_lmod=False)

        def which_side_effect(name: str) -> str | None:
            return str(conda_prefix / "bin" / name)

        with (
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                side_effect=which_side_effect,
            ),
        ):
            assert_single_toolchain_stack(None)

    def test_no_conda_prefix_non_lmod_passes(self, monkeypatch):
        monkeypatch.delenv("CONDA_PREFIX", raising=False)
        sysmgr = _fake_sysmgr(uses_lmod=False)

        def which_side_effect(name: str) -> str | None:
            return f"/usr/local/bin/{name}"

        with (
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                side_effect=which_side_effect,
            ),
        ):
            assert_single_toolchain_stack(None)


class TestVerifyRomsLinkageLinux:
    """Tests for `verify_roms_linkage` on Linux."""

    def test_ldd_unavailable_warns_and_skips(self, tmp_path: Path, caplog):
        exe = tmp_path / "roms"
        exe.touch()
        sysmgr = _fake_sysmgr(environment_variables={})

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "linux"),
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch("cstar.roms.build_verification.shutil.which", return_value=None),
            mock.patch("cstar.roms.build_verification._run_cmd") as mock_run_cmd,
        ):
            verify_roms_linkage(exe)

        mock_run_cmd.assert_not_called()

    def test_ldd_not_found_raises(self, tmp_path: Path):
        exe = tmp_path / "roms"
        exe.touch()
        sysmgr = _fake_sysmgr(environment_variables={})

        ldd_output = "\tlibnetcdf.so.19 => not found"

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "linux"),
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/ldd",
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd", return_value=ldd_output
            ),
            pytest.raises(RuntimeError, match="unresolved linkage"),
        ):
            verify_roms_linkage(exe)

    def test_ldd_conda_path_raises(self, tmp_path: Path, monkeypatch):
        exe = tmp_path / "roms"
        exe.touch()
        conda_prefix = tmp_path / "conda"
        (conda_prefix / "lib").mkdir(parents=True)
        conda_lib = conda_prefix / "lib" / "libnetcdf.so.19"
        conda_lib.touch()
        monkeypatch.setenv("CONDA_PREFIX", str(conda_prefix))

        sysmgr = _fake_sysmgr(environment_variables={}, uses_lmod=True)
        ldd_output = f"\tlibnetcdf.so.19 => {conda_lib} (0x00007f0000000000)"

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "linux"),
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/ldd",
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd", return_value=ldd_output
            ),
            pytest.raises(RuntimeError, match="active conda environment"),
        ):
            verify_roms_linkage(exe)

    def test_ldd_conda_path_passes_on_conda_system(self, tmp_path: Path, monkeypatch):
        """On a conda-based (non-Lmod) system the build should link the active
        env's libraries, so conda-resolved libraries must not raise.
        """
        exe = tmp_path / "roms"
        exe.touch()
        conda_prefix = tmp_path / "conda"
        (conda_prefix / "lib").mkdir(parents=True)
        conda_lib = conda_prefix / "lib" / "libnetcdf.so.19"
        conda_lib.touch()
        monkeypatch.setenv("CONDA_PREFIX", str(conda_prefix))

        sysmgr = _fake_sysmgr(environment_variables={}, uses_lmod=False)
        ldd_output = f"\tlibnetcdf.so.19 => {conda_lib} (0x00007f0000000000)"

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "linux"),
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/ldd",
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd", return_value=ldd_output
            ),
        ):
            verify_roms_linkage(exe)

    def test_ldd_module_stack_path_passes(self, tmp_path: Path, monkeypatch):
        """A library resolving outside CONDA_PREFIX (the module-stack case)
        should not raise.
        """
        exe = tmp_path / "roms"
        exe.touch()
        conda_prefix = tmp_path / "conda"
        conda_prefix.mkdir()
        module_lib_dir = tmp_path / "modules" / "netcdf" / "lib"
        module_lib_dir.mkdir(parents=True)
        module_lib = module_lib_dir / "libnetcdf.so.19"
        module_lib.touch()
        monkeypatch.setenv("CONDA_PREFIX", str(conda_prefix))

        sysmgr = _fake_sysmgr(environment_variables={})
        ldd_output = f"\tlibnetcdf.so.19 => {module_lib} (0x00007f0000000000)"

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "linux"),
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/ldd",
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd", return_value=ldd_output
            ),
        ):
            verify_roms_linkage(exe)

    def test_runpath_present_passes(self, tmp_path: Path):
        exe = tmp_path / "roms"
        exe.touch()
        netcdff_home = tmp_path / "netcdff"

        env_vars = {"LD_RUN_PATH": "somepath", "NETCDFFHOME": str(netcdff_home)}
        sysmgr = _fake_sysmgr(environment_variables=env_vars)

        readelf_output = (
            " 0x000000000000001d (RUNPATH)            "
            f"Library runpath: [{netcdff_home}/lib:/other/lib]"
        )

        def run_cmd_side_effect(cmd: str, **kwargs):
            if cmd.startswith("ldd"):
                return ""
            return readelf_output

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "linux"),
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/tool",
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd",
                side_effect=run_cmd_side_effect,
            ),
        ):
            verify_roms_linkage(exe)

    def test_runpath_missing_raises(self, tmp_path: Path):
        exe = tmp_path / "roms"
        exe.touch()
        netcdff_home = tmp_path / "netcdff"

        env_vars = {"LD_RUN_PATH": "somepath", "NETCDFFHOME": str(netcdff_home)}
        sysmgr = _fake_sysmgr(environment_variables=env_vars)

        readelf_output = (
            " 0x000000000000001d (RUNPATH)            Library runpath: [/other/lib]"
        )

        def run_cmd_side_effect(cmd: str, **kwargs):
            if cmd.startswith("ldd"):
                return ""
            return readelf_output

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "linux"),
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/tool",
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd",
                side_effect=run_cmd_side_effect,
            ),
            pytest.raises(RuntimeError, match="not self-locating"),
        ):
            verify_roms_linkage(exe)

    def test_runpath_check_skipped_when_ld_run_path_not_declared(self, tmp_path: Path):
        exe = tmp_path / "roms"
        exe.touch()
        sysmgr = _fake_sysmgr(environment_variables={})

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "linux"),
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/ldd",
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd", return_value=""
            ) as mock_run_cmd,
        ):
            verify_roms_linkage(exe)

        # Only the ldd call should have happened; no readelf call.
        assert mock_run_cmd.call_count == 1
        assert mock_run_cmd.call_args_list[0].args[0].startswith("ldd")

    def test_readelf_unavailable_warns_and_skips(self, tmp_path: Path):
        exe = tmp_path / "roms"
        exe.touch()
        env_vars = {"LD_RUN_PATH": "somepath", "NETCDFFHOME": "/some/netcdff"}
        sysmgr = _fake_sysmgr(environment_variables=env_vars)

        def which_side_effect(name: str) -> str | None:
            return "/usr/bin/ldd" if name == "ldd" else None

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "linux"),
            mock.patch("cstar.roms.build_verification.get_sysmgr", return_value=sysmgr),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                side_effect=which_side_effect,
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd", return_value=""
            ) as mock_run_cmd,
        ):
            verify_roms_linkage(exe)

        # Only the ldd call should have happened; readelf was skipped.
        assert mock_run_cmd.call_count == 1
        assert mock_run_cmd.call_args_list[0].args[0].startswith("ldd")


class TestVerifyRomsLinkageDarwin:
    """Tests for `verify_roms_linkage` on macOS."""

    def test_no_conda_prefix_skips(self, tmp_path: Path, monkeypatch):
        exe = tmp_path / "roms"
        exe.touch()
        monkeypatch.delenv("CONDA_PREFIX", raising=False)

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "darwin"),
            mock.patch("cstar.roms.build_verification._run_cmd") as mock_run_cmd,
        ):
            verify_roms_linkage(exe)

        mock_run_cmd.assert_not_called()

    def test_otool_unavailable_skips(self, tmp_path: Path, monkeypatch):
        exe = tmp_path / "roms"
        exe.touch()
        monkeypatch.setenv("CONDA_PREFIX", str(tmp_path))

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "darwin"),
            mock.patch("cstar.roms.build_verification.shutil.which", return_value=None),
            mock.patch("cstar.roms.build_verification._run_cmd") as mock_run_cmd,
        ):
            verify_roms_linkage(exe)

        mock_run_cmd.assert_not_called()

    def test_offender_raises(self, tmp_path: Path, monkeypatch):
        exe = tmp_path / "roms"
        exe.touch()
        conda_prefix = tmp_path / "conda"
        conda_prefix.mkdir()
        monkeypatch.setenv("CONDA_PREFIX", str(conda_prefix))

        otool_output = (
            f"{exe}:\n"
            f"\t{conda_prefix}/lib/libnetcdf.dylib (compatibility version 1.0.0)\n"
            "\t/opt/homebrew/lib/libmpi.40.dylib (compatibility version 1.0.0)\n"
        )

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "darwin"),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/otool",
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd", return_value=otool_output
            ),
            pytest.raises(RuntimeError, match="libmpi.40.dylib"),
        ):
            verify_roms_linkage(exe)

    def test_happy_path_passes(self, tmp_path: Path, monkeypatch):
        exe = tmp_path / "roms"
        exe.touch()
        conda_prefix = tmp_path / "conda"
        conda_prefix.mkdir()
        monkeypatch.setenv("CONDA_PREFIX", str(conda_prefix))

        otool_output = (
            f"{exe}:\n"
            f"\t{conda_prefix}/lib/libnetcdf.dylib (compatibility version 1.0.0)\n"
            "\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0)\n"
            "\t@rpath/libsomething.dylib (compatibility version 1.0.0)\n"
        )

        with (
            mock.patch("cstar.roms.build_verification.sys.platform", "darwin"),
            mock.patch(
                "cstar.roms.build_verification.shutil.which",
                return_value="/usr/bin/otool",
            ),
            mock.patch(
                "cstar.roms.build_verification._run_cmd", return_value=otool_output
            ),
        ):
            verify_roms_linkage(exe)
