"""Tests for cstar_forge/register_kernel.py (`cstar forge register-kernel`)."""

import json
import os
import stat
from pathlib import Path
from unittest.mock import patch

import pytest

from cstar_forge import register_kernel as rk


class TestDetectPackageManager:
    def test_explicit_values_pass_through(self):
        assert rk.detect_package_manager("micromamba") == "micromamba"
        assert rk.detect_package_manager("conda") == "conda"

    def test_mamba_maps_to_conda(self):
        # mamba envs activate through conda.sh; the wrapper has no mamba branch.
        assert rk.detect_package_manager("mamba") == "conda"

    def test_auto_prefers_micromamba(self):
        with patch.object(rk.shutil, "which", return_value="/usr/bin/micromamba"):
            assert rk.detect_package_manager("auto") == "micromamba"

    def test_auto_falls_back_to_conda(self):
        with patch.object(
            rk.shutil,
            "which",
            side_effect=lambda cmd: "/opt/conda/bin/conda" if cmd == "conda" else None,
        ):
            assert rk.detect_package_manager("auto") == "conda"

    def test_auto_with_neither_raises(self):
        with (
            patch.object(rk.shutil, "which", return_value=None),
            patch.dict(os.environ, {}, clear=False) as env,
        ):
            env.pop("CONDA_EXE", None)
            with pytest.raises(rk.RegisterKernelError, match="neither micromamba"):
                rk.detect_package_manager("auto")

    def test_unknown_value_raises(self):
        with pytest.raises(rk.RegisterKernelError, match="unknown package manager"):
            rk.detect_package_manager("pixi")


class TestBuildWrapperContent:
    def test_micromamba_wrapper(self):
        with patch.object(rk.shutil, "which", return_value="/abs/bin/micromamba"):
            content = rk.build_wrapper_content(
                "micromamba", Path("/envs/my-env"), micromamba_bin="micromamba"
            )
        assert '"$(/abs/bin/micromamba shell hook --shell bash)"' in content
        assert 'micromamba activate "/envs/my-env"' in content
        assert "export PYTHONNOUSERSITE=1" in content
        assert 'export FI_PROVIDER="${FI_PROVIDER:-tcp}"' in content
        assert "unset PYTHONPATH" in content
        assert 'exec python -m ipykernel_launcher "$@"' in content

    def test_micromamba_bin_falls_back_to_given_name_off_path(self):
        with patch.object(rk.shutil, "which", return_value=None):
            content = rk.build_wrapper_content(
                "micromamba", Path("/envs/e"), micromamba_bin="/repo/bin/micromamba"
            )
        assert "/repo/bin/micromamba shell hook" in content

    def test_conda_wrapper(self):
        content = rk.build_wrapper_content(
            "conda", Path("/envs/my-env"), conda_base="/opt/conda"
        )
        assert 'source "/opt/conda/etc/profile.d/conda.sh"' in content
        assert 'conda activate "/envs/my-env"' in content
        assert "export PYTHONNOUSERSITE=1" in content
        assert 'export FI_PROVIDER="${FI_PROVIDER:-tcp}"' in content
        assert "unset PYTHONPATH" in content
        assert 'exec python -m ipykernel_launcher "$@"' in content


class TestRewriteKernelJson:
    def test_argv_replaced_and_metadata_preserved(self, tmp_path):
        spec = {
            "argv": ["/envs/e/bin/python", "-m", "ipykernel_launcher"],
            "display_name": "my-env",
            "language": "python",
            "metadata": {"debugger": True},
        }
        (tmp_path / "kernel.json").write_text(json.dumps(spec))
        wrapper = tmp_path / rk.WRAPPER_FILENAME

        rk.rewrite_kernel_json(tmp_path, wrapper)

        rewritten = json.loads((tmp_path / "kernel.json").read_text())
        # {connection_file} must stay a literal Jupyter template placeholder.
        assert rewritten["argv"] == [str(wrapper), "-f", "{connection_file}"]
        assert rewritten["display_name"] == "my-env"
        assert rewritten["metadata"] == {"debugger": True}


class TestRegisterKernel:
    @pytest.fixture
    def fake_kernelspec(self, tmp_path):
        """A pre-existing kernelspec dir, as ipykernel install would leave it."""
        (tmp_path / "kernel.json").write_text(
            json.dumps(
                {
                    "argv": ["/envs/e/bin/python", "-m", "ipykernel_launcher"],
                    "display_name": "x",
                    "language": "python",
                }
            )
        )
        return tmp_path

    def test_full_flow_writes_executable_wrapper_and_rewrites_json(
        self, fake_kernelspec
    ):
        with (
            patch.object(rk, "_env_prefix", return_value=Path("/envs/my-env")),
            patch.object(
                rk, "_install_kernelspec", return_value=fake_kernelspec
            ) as install,
            patch.object(rk.shutil, "which", return_value="/abs/micromamba"),
        ):
            kernel_dir = rk.register_kernel(
                package_manager="micromamba", log=lambda m: None
            )

        install.assert_called_once_with("my-env", "my-env")
        wrapper = kernel_dir / rk.WRAPPER_FILENAME
        assert wrapper.is_file()
        assert wrapper.stat().st_mode & stat.S_IXUSR
        assert 'micromamba activate "/envs/my-env"' in wrapper.read_text()
        spec = json.loads((kernel_dir / "kernel.json").read_text())
        assert spec["argv"][0] == str(wrapper)

    def test_name_and_display_name_overrides(self, fake_kernelspec):
        with (
            patch.object(rk, "_env_prefix", return_value=Path("/envs/my-env")),
            patch.object(
                rk, "_install_kernelspec", return_value=fake_kernelspec
            ) as install,
            patch.object(rk.shutil, "which", return_value="/abs/micromamba"),
        ):
            rk.register_kernel(
                name="custom",
                display_name="Custom (my-env)",
                package_manager="micromamba",
                log=lambda m: None,
            )
        install.assert_called_once_with("custom", "Custom (my-env)")

    def test_clean_removes_existing_kernelspec_first(self, fake_kernelspec):
        with (
            patch.object(rk, "_env_prefix", return_value=Path("/envs/my-env")),
            patch.object(rk, "_install_kernelspec", return_value=fake_kernelspec),
            patch.object(rk, "_remove_kernelspec", return_value=True) as remove,
            patch.object(rk.shutil, "which", return_value="/abs/micromamba"),
        ):
            rk.register_kernel(
                clean=True, package_manager="micromamba", log=lambda m: None
            )
        remove.assert_called_once_with("my-env")

    def test_wrapper_failure_precedes_kernelspec_changes(self):
        # A missing conda base must fail before install/clean touch anything.
        with (
            patch.object(rk, "_env_prefix", return_value=Path("/envs/my-env")),
            patch.object(rk, "_install_kernelspec") as install,
            patch.object(rk, "_remove_kernelspec") as remove,
            patch.object(
                rk, "_conda_base", side_effect=rk.RegisterKernelError("no base")
            ),
        ):
            with pytest.raises(rk.RegisterKernelError, match="no base"):
                rk.register_kernel(
                    clean=True, package_manager="conda", log=lambda m: None
                )
        install.assert_not_called()
        remove.assert_not_called()

    def test_non_conda_prefix_raises(self, tmp_path):
        with patch.object(rk.sys, "prefix", str(tmp_path)):
            with pytest.raises(rk.RegisterKernelError, match="not inside a"):
                rk.register_kernel(log=lambda m: None)
