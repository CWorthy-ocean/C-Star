"""
Tests for the config.py module.

Tests cover:
- DataPaths dataclass
- detect_system (the seam onto C-Star's HostNameEvaluator)
- System layout registry / source-data path resolution
- scratch_data_home / default_working_dir / resolve_host (working_dir used as written)
- get_data_paths / ensure_data_dirs
"""

import logging
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

import cstar.applications.forge.config as config_module
from cstar.applications.forge.config import (
    SYSTEM_LAYOUT_REGISTRY,
    DataPaths,
    get_data_paths,
    register_system,
)
from cstar.catalog.domain_catalog import user_catalog_root


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
    root), imported here because config.paths.catalog is now just
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


class TestDetectSystem:
    """detect_system() is a one-line seam onto C-Star's HostNameEvaluator.

    C-Star's own hostname/LMOD/is_match matching heuristics belong to C-Star's
    test suite, not forge's — these tests only check that the seam delegates,
    not how HostNameEvaluator itself decides a name.
    """

    def test_delegates_to_host_name_evaluator(self, monkeypatch):
        class _FakeEvaluator:
            name = "anvil"

        monkeypatch.setattr(config_module, "HostNameEvaluator", _FakeEvaluator)
        assert config_module.detect_system() == "anvil"

    def test_real_evaluator_returns_a_nonempty_name(self):
        # No mocking: exercises the actual import wiring end to end (the dev
        # box this runs on is never one of the registered HPC systems, so this
        # only asserts C-Star could name *something*, not which name).
        assert config_module.detect_system()


class TestSystemLayoutRegistry:
    """Tests for the system layout registry, keyed by C-Star's system names."""

    def test_system_layout_registry_has_defaults(self):
        assert "anvil" in SYSTEM_LAYOUT_REGISTRY
        assert "perlmutter" in SYSTEM_LAYOUT_REGISTRY
        assert "bouchet" in SYSTEM_LAYOUT_REGISTRY
        assert "darwin_arm64" in SYSTEM_LAYOUT_REGISTRY
        assert "linux_x86_64" in SYSTEM_LAYOUT_REGISTRY
        assert "linux_aarch64" in SYSTEM_LAYOUT_REGISTRY

    def test_register_system_decorator(self):
        """Test registering a custom system layout."""

        @register_system("test_system")
        def test_layout(home: Path, env: dict) -> Path:
            return home / "test-source"

        assert "test_system" in SYSTEM_LAYOUT_REGISTRY
        assert SYSTEM_LAYOUT_REGISTRY["test_system"] == test_layout

        # Clean up
        del SYSTEM_LAYOUT_REGISTRY["test_system"]

    def test_home_anchored_layout_registered_under_each_local_dev_name(self, tmp_path):
        """darwin_arm64/linux_x86_64/linux_aarch64 all share one function."""
        for tag in ("darwin_arm64", "linux_x86_64", "linux_aarch64"):
            layout_fn = SYSTEM_LAYOUT_REGISTRY[tag]
            assert layout_fn is config_module._layout_home_anchored
            source_data = layout_fn(tmp_path, {})
            assert source_data == tmp_path / "cstar-forge-data" / "source-data"

    def test_unregistered_system_name_falls_back_to_home_anchored_layout(self):
        """.get(name, fallback): any C-Star name with no dedicated HPC layout below
        (e.g. "derecho", which forge doesn't special-case) gets the home-anchored
        default -- a deliberate fallback, not a second detection heuristic.
        """
        fallback = SYSTEM_LAYOUT_REGISTRY.get(
            "derecho", config_module._layout_home_anchored
        )
        assert fallback is config_module._layout_home_anchored

    def test_anvil_layout(self, tmp_path):
        """Test Anvil layout function."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["anvil"]
        env = {"PROJECT": str(tmp_path / "proj")}
        source_data = layout_fn(tmp_path, env)

        assert source_data == tmp_path / "proj" / "cstar-forge-data" / "source-data"

    def test_perlmutter_layout(self, tmp_path):
        """Test Perlmutter layout function."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["perlmutter"]
        env = {"SCRATCH": str(tmp_path / "scratch")}
        source_data = layout_fn(tmp_path, env)

        assert source_data == tmp_path / "scratch" / "cstar-forge-data" / "source-data"

    def test_bouchet_layout(self, tmp_path, monkeypatch):
        """Test Bouchet layout function using the discovered scratch_pi_* dir."""
        monkeypatch.setattr(config_module, "USER", "testuser")
        (tmp_path / "scratch_pi_abc" / "testuser").mkdir(parents=True)

        layout_fn = SYSTEM_LAYOUT_REGISTRY["bouchet"]
        source_data = layout_fn(tmp_path, {})

        scratch_root = tmp_path / "scratch_pi_abc" / "testuser"
        assert source_data == scratch_root / "cstar-forge-data" / "source-data"

    def test_bouchet_layout_scratch_env_override_wins(self, tmp_path, monkeypatch):
        """An explicit $SCRATCH override in the layout's env dict wins over the
        scratch_pi_* glob.
        """
        monkeypatch.setattr(config_module, "USER", "testuser")
        (tmp_path / "scratch_pi_abc" / "testuser").mkdir(parents=True)

        layout_fn = SYSTEM_LAYOUT_REGISTRY["bouchet"]
        env = {"SCRATCH": str(tmp_path / "explicit-scratch")}
        source_data = layout_fn(tmp_path, env)

        assert (
            source_data
            == tmp_path / "explicit-scratch" / "cstar-forge-data" / "source-data"
        )

    def test_bouchet_layout_falls_back_to_home_anchored_without_scratch_pi(
        self, tmp_path, monkeypatch
    ):
        """No scratch_pi_* dir and no $SCRATCH falls back to the home-anchored layout."""
        monkeypatch.setattr(config_module, "USER", "testuser")

        bouchet_fn = SYSTEM_LAYOUT_REGISTRY["bouchet"]
        home_fn = config_module._layout_home_anchored
        assert bouchet_fn(tmp_path, {}) == home_fn(tmp_path, {})

    # ---- $PROJECT: standard env var for the (shared) data-base parent dir ----

    def test_anvil_project_drives_source_data_work_ignored(self, tmp_path):
        """$PROJECT drives the data base; $WORK is never consulted, so a
        user-overridden $PROJECT moves everything with it.
        """
        layout_fn = SYSTEM_LAYOUT_REGISTRY["anvil"]
        env = {"PROJECT": str(tmp_path / "proj"), "WORK": str(tmp_path / "work")}
        source_data = layout_fn(tmp_path, env)

        assert source_data == tmp_path / "proj" / "cstar-forge-data" / "source-data"

    def test_anvil_without_project_uses_home_even_if_work_set(self, tmp_path):
        """No $PROJECT falls back to home/work; a lone $WORK is ignored."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["anvil"]
        env = {"WORK": str(tmp_path / "elsewhere")}
        source_data = layout_fn(tmp_path, env)
        assert source_data == tmp_path / "work" / "cstar-forge-data" / "source-data"

    def test_perlmutter_project_moves_data_base(self, tmp_path):
        """$PROJECT relocates the data base, overriding the $SCRATCH-based default."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["perlmutter"]
        env = {"PROJECT": str(tmp_path / "proj"), "SCRATCH": str(tmp_path / "scratch")}
        source_data = layout_fn(tmp_path, env)

        assert source_data == tmp_path / "proj" / "cstar-forge-data" / "source-data"

    def test_bouchet_project_moves_data_base(self, tmp_path, monkeypatch):
        """$PROJECT relocates the data base; the discovered scratch_pi_* root is
        only consulted to decide whether the home-anchored fallback applies.
        """
        monkeypatch.setattr(config_module, "USER", "testuser")
        (tmp_path / "scratch_pi_abc" / "testuser").mkdir(parents=True)

        layout_fn = SYSTEM_LAYOUT_REGISTRY["bouchet"]
        env = {"PROJECT": str(tmp_path / "proj")}
        source_data = layout_fn(tmp_path, env)

        assert source_data == tmp_path / "proj" / "cstar-forge-data" / "source-data"

    def test_bouchet_project_ignored_without_scratch_root(self, tmp_path, monkeypatch):
        """Documented edge: with no discoverable scratch root, the home-anchored
        fallback ignores $PROJECT entirely.
        """
        monkeypatch.setattr(config_module, "USER", "testuser")

        bouchet_fn = SYSTEM_LAYOUT_REGISTRY["bouchet"]
        home_fn = config_module._layout_home_anchored
        env = {"PROJECT": str(tmp_path / "proj")}
        assert bouchet_fn(tmp_path, env) == home_fn(tmp_path, {})


class TestGetDataPaths:
    """Tests for get_data_paths function."""

    def test_get_data_paths(self, monkeypatch, tmp_path):
        """Test get_data_paths returns DataPaths object without creating directories.

        Importing cstar.applications.forge.config must not have filesystem side effects, so the
        default (``create=False``) only builds Path objects.
        """
        monkeypatch.setattr(config_module, "detect_system", lambda: "darwin_arm64")

        # conftest.py forces CSTAR_CATALOG to an already-created temp dir
        # (for global test isolation), which would make the "not exists()"
        # assertion below meaningless -- point it at a not-yet-created path
        # instead so this test still checks that get_data_paths() itself
        # creates nothing.
        monkeypatch.setenv("CSTAR_CATALOG", str(tmp_path / "not-yet-created"))
        # Use a real home directory that exists for the test
        monkeypatch.setenv("HOME", str(tmp_path))
        paths = get_data_paths()

        assert isinstance(paths, DataPaths)
        # No directories are created by default
        assert not paths.source_data.exists()
        assert not paths.catalog.exists()
        assert paths.catalog == user_catalog_root()

    def test_get_data_paths_creates_directories(self, monkeypatch, tmp_path):
        """Test that get_data_paths(create=True) creates necessary directories."""
        monkeypatch.setattr(config_module, "detect_system", lambda: "darwin_arm64")

        # See test_get_data_paths above: repoint the catalog at a not-yet-created
        # path so this test actually exercises directory creation for it too.
        monkeypatch.setenv("CSTAR_CATALOG", str(tmp_path / "not-yet-created"))
        monkeypatch.setenv("HOME", str(tmp_path))
        paths = get_data_paths(create=True)

        # Verify directories were created (they should exist after get_data_paths)
        assert paths.source_data.exists()
        assert paths.catalog.exists()


class TestFormatPaths:
    """format_paths backs `cstar forge show-paths` in both text and JSON forms."""

    @pytest.fixture
    def fake_paths(self, monkeypatch, tmp_path):
        dp = DataPaths(source_data=tmp_path / "src", catalog=tmp_path / "cat")
        monkeypatch.setattr(config_module, "paths", dp)
        monkeypatch.setattr(config_module, "detect_system", lambda: "anvil")
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


@pytest.fixture
def fake_home(monkeypatch, tmp_path):
    """Point ``$HOME`` (what ``Path.home()`` and ``expanduser`` read) at a temp dir."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    return home


def _patch_data_home(monkeypatch, path: Path) -> None:
    monkeypatch.setattr(
        config_module.DirectoryManager, "data_home", classmethod(lambda cls: path)
    )


class TestScratchDataHome:
    """scratch_data_home: C-Star's data home when it is off $HOME, else None."""

    def test_returns_data_home_when_off_home(self, monkeypatch, tmp_path, fake_home):
        scratch = (tmp_path / "scratch" / "cstar").resolve()
        _patch_data_home(monkeypatch, scratch)
        assert config_module.scratch_data_home() == scratch

    def test_returns_none_when_data_home_is_under_home(
        self, monkeypatch, tmp_path, fake_home
    ):
        _patch_data_home(monkeypatch, (fake_home / "cstar" / "cstar").resolve())
        assert config_module.scratch_data_home() is None


class TestDefaultWorkingDir:
    """default_working_dir: what the wizard writes for a new blueprint."""

    def test_uses_scratch_data_home_when_available(self, monkeypatch, tmp_path):
        scratch = tmp_path / "scratch" / "cstar"
        monkeypatch.setattr(config_module, "scratch_data_home", lambda: scratch)
        assert (
            config_module.default_working_dir("run1")
            == (scratch / "_forge_bp_runs" / "run1").as_posix()
        )

    def test_falls_back_to_portable_default(self, monkeypatch):
        monkeypatch.setattr(config_module, "scratch_data_home", lambda: None)
        assert (
            config_module.default_working_dir("run1") == "~/cstar/_forge_bp_runs/run1"
        )


class TestResolveHost:
    """resolve_host uses the blueprint's working_dir as written (after ~ expansion)."""

    def test_expands_tilde_and_keeps_path(self, monkeypatch, fake_home):
        monkeypatch.setattr(config_module, "scratch_data_home", lambda: None)
        host = config_module.resolve_host("~/cstar/_forge_bp_runs/run1")
        assert host.working_dir == fake_home / "cstar" / "_forge_bp_runs" / "run1"
        assert host.source_data_cache == config_module.paths.source_data
        assert host.system == config_module.system

    def test_default_form_path_is_not_relocated(self, monkeypatch, tmp_path, fake_home):
        """No scratch rebase any more: even with a scratch data home, the stored
        path is honoured (with a warning, tested below).
        """
        scratch = tmp_path / "scratch" / "cstar"
        monkeypatch.setattr(config_module, "scratch_data_home", lambda: scratch)
        host = config_module.resolve_host("~/cstar/_forge_bp_runs/run1")
        assert host.working_dir == fake_home / "cstar" / "_forge_bp_runs" / "run1"

    def test_warns_when_home_rooted_on_scratch_host(
        self, monkeypatch, tmp_path, fake_home, caplog
    ):
        scratch = tmp_path / "scratch" / "cstar"
        monkeypatch.setattr(config_module, "scratch_data_home", lambda: scratch)
        with caplog.at_level(logging.WARNING, logger="cstar.applications.forge.config"):
            config_module.resolve_host("~/cstar/_forge_bp_runs/run1")
        assert "under $HOME" in caplog.text
        assert str(scratch / "_forge_bp_runs") in caplog.text

    def test_no_warning_without_scratch_data_home(self, monkeypatch, fake_home, caplog):
        monkeypatch.setattr(config_module, "scratch_data_home", lambda: None)
        with caplog.at_level(logging.WARNING, logger="cstar.applications.forge.config"):
            config_module.resolve_host("~/cstar/_forge_bp_runs/run1")
        assert caplog.text == ""

    def test_no_warning_for_off_home_path(
        self, monkeypatch, tmp_path, fake_home, caplog
    ):
        scratch = tmp_path / "scratch" / "cstar"
        monkeypatch.setattr(config_module, "scratch_data_home", lambda: scratch)
        with caplog.at_level(logging.WARNING, logger="cstar.applications.forge.config"):
            host = config_module.resolve_host(tmp_path / "elsewhere" / "run1")
        assert host.working_dir == tmp_path / "elsewhere" / "run1"
        assert caplog.text == ""
