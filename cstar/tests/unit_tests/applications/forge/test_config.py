"""
Tests for the config.py module.

Tests cover:
- DataPaths dataclass
- user_catalog_root (the catalog half of the data paths)
- resolve_host (working directory used as written, system named by HostNameEvaluator)
- get_data_paths / ensure_data_dirs (source-data location itself is tested with
  DirectoryManager.source_data_home)
- format_paths
"""

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

import cstar.applications.forge.config as config_module
from cstar.applications.forge.config import DataPaths, ensure_data_dirs, get_data_paths
from cstar.catalog.domain_catalog import user_catalog_root
from cstar.execution.file_system import DirectoryManager


class TestDataPaths:
    """Tests for the DataPaths dataclass (now just source_data + catalog)."""

    def test_datapaths_creation(self, tmp_path):
        cat = tmp_path / "catalog"
        paths = DataPaths(source_data=tmp_path / "source-data", catalog=cat)

        assert paths.source_data == tmp_path / "source-data"
        assert paths.catalog == cat

    def test_datapaths_frozen(self, tmp_path):
        cat = tmp_path / "catalog"
        paths = DataPaths(source_data=tmp_path / "source-data", catalog=cat)

        with pytest.raises(FrozenInstanceError):
            paths.catalog = tmp_path / "new"


# NB: catalog_root anchoring (resolve_catalog_dir) was removed with the executor's
# config/catalog decoupling — the forge app writes under the injected host.working_dir.
# with_catalog (config.py's own catalog-relocation helper) was removed for the same
# reason: nothing outside config.py and its tests read the field it moved.


class TestUserCatalogRoot:
    """Tests for domain_catalog.user_catalog_root (the writable catalog layer's
    root), imported here because get_data_paths().catalog is just
    ``user_catalog_root()``.
    """

    def test_env_override_uses_first_pathsep_entry(self, monkeypatch, tmp_path):
        import os

        first = tmp_path / "first-catalog"
        second = tmp_path / "second-catalog"
        monkeypatch.setenv("CSTAR_CATALOG", os.pathsep.join([str(first), str(second)]))
        assert user_catalog_root() == first.expanduser().resolve()

    def test_env_override_single_entry(self, monkeypatch, tmp_path):
        entry = tmp_path / "only-catalog"
        monkeypatch.setenv("CSTAR_CATALOG", str(entry))
        assert user_catalog_root() == entry.expanduser().resolve()

    def test_default_is_home_anchored_when_env_unset(self, monkeypatch, tmp_path):
        # conftest.py forces CSTAR_CATALOG globally for test isolation, so
        # this test must monkeypatch (auto-undone), never delete it globally.
        # The default is computed via Path("~/cstar/catalog").expanduser(), which
        # resolves through os.path.expanduser (the HOME env var), not Path.home().
        monkeypatch.delenv("CSTAR_CATALOG", raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        assert user_catalog_root() == tmp_path / "cstar" / "catalog"

    def test_does_not_create_the_directory(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CSTAR_CATALOG", str(tmp_path / "not-yet-created"))
        result = user_catalog_root()
        assert not result.exists()


def _fake_evaluator(name: str) -> type:
    """Stand in for C-Star's HostNameEvaluator, whose heuristics are tested elsewhere."""

    class _FakeEvaluator:
        pass

    _FakeEvaluator.name = name  # type: ignore[attr-defined]
    return _FakeEvaluator


class TestResolveHost:
    """resolve_host uses the working directory as written: expanded, never relocated."""

    def test_tilde_is_expanded(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HOME", str(tmp_path))

        host = config_module.resolve_host("~/runs/my-run")

        assert host.working_dir == tmp_path / "runs" / "my-run"

    def test_path_is_otherwise_verbatim(self, tmp_path):
        """Not resolved or normalised: a ``..`` segment and a relative path survive."""
        odd = tmp_path / "a" / ".." / "b"

        assert config_module.resolve_host(odd).working_dir == odd
        assert config_module.resolve_host("rel/dir").working_dir == Path("rel/dir")

    def test_no_relocation_onto_scratch_on_hpc(self, monkeypatch, tmp_path):
        """A home-rooted path stays in home even on an HPC system with $SCRATCH set."""
        home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
        monkeypatch.setattr(
            config_module, "HostNameEvaluator", _fake_evaluator("anvil")
        )

        host = config_module.resolve_host(home / "runs" / "my-run")

        assert host.working_dir == home / "runs" / "my-run"
        assert host.system == "anvil"

    def test_source_data_cache_comes_from_the_data_paths(self, monkeypatch, tmp_path):
        dp = DataPaths(source_data=tmp_path / "src", catalog=tmp_path / "cat")
        monkeypatch.setattr(config_module, "get_data_paths", lambda: dp)

        host = config_module.resolve_host(tmp_path / "wd")

        assert host.source_data_cache == dp.source_data

    def test_system_is_named_by_host_name_evaluator(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            config_module, "HostNameEvaluator", _fake_evaluator("perlmutter")
        )

        assert config_module.resolve_host(tmp_path / "wd").system == "perlmutter"

    def test_real_evaluator_returns_a_nonempty_system(self, tmp_path):
        # No mocking: exercises the actual import wiring end to end (the dev box
        # is never a registered HPC system, so this only asserts C-Star named
        # *something*).
        assert config_module.resolve_host(tmp_path / "wd").system


class TestGetDataPaths:
    """Tests for get_data_paths and ensure_data_dirs."""

    @pytest.fixture(autouse=True)
    def _isolated_roots(self, monkeypatch, tmp_path):
        """Point the source-data root and the catalog at not-yet-created paths.

        conftest.py forces CSTAR_CATALOG to an already-created temp dir (for global
        test isolation), which would make the "not exists()" assertions meaningless.
        """
        for var in (
            "CSTAR_PROJECT_HOME",
            "PROJECT",
            "SCRATCH",
            "SCRATCH_DIR",
            "LOCAL_SCRATCH",
        ):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("CSTAR_PROJECT_HOME", str(tmp_path / "project"))
        monkeypatch.setenv("CSTAR_CATALOG", str(tmp_path / "not-yet-created"))

    def test_get_data_paths_creates_nothing(self, tmp_path):
        """Importing config must have no filesystem side effects, so building the
        paths must not create directories.
        """
        paths = get_data_paths()

        assert isinstance(paths, DataPaths)
        assert paths.source_data == DirectoryManager.source_data_home()
        assert paths.catalog == user_catalog_root()
        assert not paths.source_data.exists()
        assert not paths.catalog.exists()

    def test_ensure_data_dirs_creates_both(self):
        paths = ensure_data_dirs()

        assert paths == get_data_paths()
        assert paths.source_data.is_dir()
        assert paths.catalog.is_dir()


class TestFormatPaths:
    """format_paths backs `cstar forge show-paths` in both text and JSON forms."""

    @pytest.fixture
    def fake_paths(self, monkeypatch, tmp_path):
        dp = DataPaths(source_data=tmp_path / "src", catalog=tmp_path / "cat")
        monkeypatch.setattr(config_module, "get_data_paths", lambda: dp)
        monkeypatch.setattr(
            config_module, "HostNameEvaluator", _fake_evaluator("anvil")
        )
        monkeypatch.setattr(config_module, "_hostname", lambda: "node01")
        return dp

    def test_text_output(self, fake_paths):
        out = config_module.format_paths()
        assert "System tag : anvil" in out
        assert "Hostname   : node01" in out
        assert str(fake_paths.source_data) in out and str(fake_paths.catalog) in out

    def test_json_output(self, fake_paths):
        import json

        payload = json.loads(config_module.format_paths(as_json=True))
        assert payload["system"] == "anvil"
        assert payload["hostname"] == "node01"
        assert payload["paths"] == {
            "source_data": str(fake_paths.source_data),
            "catalog": str(fake_paths.catalog),
        }

    def test_hostname_falls_back_when_socket_is_empty(self, monkeypatch):
        monkeypatch.setattr(config_module.socket, "gethostname", lambda: "")
        monkeypatch.setattr(config_module.platform, "node", lambda: "")
        monkeypatch.setenv("HOSTNAME", "from-env")
        assert config_module._hostname() == "from-env"
