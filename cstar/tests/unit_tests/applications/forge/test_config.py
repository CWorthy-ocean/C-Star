"""
Tests for the config.py module.

Tests cover:
- DataPaths dataclass
- detect_system (the seam onto C-Star's HostNameEvaluator)
- System layout registry / source-data path resolution
- Bouchet scratch-root heuristic
- _hpc_scratch_root / relocate_working_dir
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

# The env vars C-Star's hpc_data_directory() searches (CSTAR_SCRATCH_DIRS' default),
# plus CSTAR_SCRATCH_DIRS itself. Cleared in tests that exercise _hpc_scratch_root /
# relocate_working_dir so the result doesn't depend on the real host's environment.
_SCRATCH_ENV_VARS = ("SCRATCH", "SCRATCH_DIR", "LOCAL_SCRATCH", "CSTAR_SCRATCH_DIRS")


@pytest.fixture
def clean_scratch_env(monkeypatch):
    """Clear the env vars hpc_data_directory() searches, for determinism."""
    for var in _SCRATCH_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


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
        """An explicit $SCRATCH override (in the layout's own env dict, distinct from
        the real-process lookup _hpc_scratch_root does) wins over the scratch_pi_*
        glob.
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


class TestBouchetScratchRoot:
    """Tests for _bouchet_scratch_root (the scratch_pi_* glob heuristic)."""

    def test_picks_sorted_first_scratch_pi_dir(self, tmp_path, monkeypatch):
        from cstar.applications.forge.config import _bouchet_scratch_root

        monkeypatch.setattr(config_module, "USER", "testuser")
        (tmp_path / "scratch_pi_zeta").mkdir()
        (tmp_path / "scratch_pi_alpha").mkdir()
        (tmp_path / "scratch_pi_mid").mkdir()

        result = _bouchet_scratch_root(tmp_path)
        assert result == tmp_path / "scratch_pi_alpha" / "testuser"

    def test_skips_non_directory_matches(self, tmp_path, monkeypatch):
        from cstar.applications.forge.config import _bouchet_scratch_root

        monkeypatch.setattr(config_module, "USER", "testuser")
        (tmp_path / "scratch_pi_notadir").write_text("not a directory")
        (tmp_path / "scratch_pi_real").mkdir()

        result = _bouchet_scratch_root(tmp_path)
        assert result == tmp_path / "scratch_pi_real" / "testuser"

    def test_returns_none_when_no_matches(self, tmp_path, monkeypatch):
        from cstar.applications.forge.config import _bouchet_scratch_root

        monkeypatch.setattr(config_module, "USER", "testuser")
        result = _bouchet_scratch_root(tmp_path)
        assert result is None

    def test_returns_none_on_oserror(self, tmp_path, monkeypatch, caplog):
        """A failed scan (e.g. stale mount) degrades to None instead of raising."""
        from cstar.applications.forge.config import _bouchet_scratch_root

        monkeypatch.setattr(config_module, "USER", "testuser")

        def _boom(self, pattern):
            raise OSError("stale NFS handle")

        monkeypatch.setattr(Path, "glob", _boom)
        with caplog.at_level("WARNING", logger="cstar.applications.forge.config"):
            result = _bouchet_scratch_root(tmp_path)
        assert result is None
        assert "scratch_pi_*" in caplog.text


class TestHpcScratchRoot:
    """_hpc_scratch_root's primary mechanism is C-Star's own
    ``find_scratch_dir``/``CSTAR_SCRATCH_DIRS`` search (``$SCRATCH``,
    ``$SCRATCH_DIR``, ``$LOCAL_SCRATCH`` by default, in that order) against the
    passed environment; only when none of those variables is set does Forge's
    own per-system fallback apply. None for non-HPC names even if one of the
    listed variables is set.

    All tests use ``clean_scratch_env`` so the search's own list
    (``CSTAR_SCRATCH_DIRS``, read from the real process environment by
    ``find_scratch_dir``) can't pick up a leftover value from the host running
    the tests.
    """

    def test_scratch_env_wins_on_every_hpc_system(self, tmp_path, clean_scratch_env):
        env = {"SCRATCH": str(tmp_path / "scratch"), "PROJECT": str(tmp_path / "proj")}
        for tag in ("perlmutter", "anvil", "bouchet"):
            assert config_module._hpc_scratch_root(tag, env, tmp_path / "home") == (
                tmp_path / "scratch"
            ), tag

    def test_scratch_dir_wins_when_scratch_unset(self, tmp_path, clean_scratch_env):
        """$SCRATCH_DIR (second in CSTAR_SCRATCH_DIRS) is honoured when $SCRATCH
        is not set -- new precedence the old $SCRATCH-only code ignored.
        """
        env = {"SCRATCH_DIR": str(tmp_path / "sdir"), "PROJECT": str(tmp_path / "proj")}
        for tag in ("perlmutter", "anvil", "bouchet"):
            assert config_module._hpc_scratch_root(tag, env, tmp_path / "home") == (
                tmp_path / "sdir"
            ), tag

    def test_local_scratch_wins_when_scratch_and_scratch_dir_unset(
        self, tmp_path, clean_scratch_env
    ):
        """$LOCAL_SCRATCH (third/last in CSTAR_SCRATCH_DIRS) is honoured when
        neither $SCRATCH nor $SCRATCH_DIR is set.
        """
        env = {"LOCAL_SCRATCH": str(tmp_path / "ljob")}
        for tag in ("perlmutter", "anvil", "bouchet"):
            assert config_module._hpc_scratch_root(tag, env, tmp_path / "home") == (
                tmp_path / "ljob"
            ), tag

    def test_scratch_wins_over_scratch_dir_and_local_scratch(
        self, tmp_path, clean_scratch_env
    ):
        """CSTAR_SCRATCH_DIRS search order: $SCRATCH first even when the others
        are also set.
        """
        env = {
            "SCRATCH": str(tmp_path / "scratch"),
            "SCRATCH_DIR": str(tmp_path / "sdir"),
            "LOCAL_SCRATCH": str(tmp_path / "ljob"),
        }
        assert config_module._hpc_scratch_root(
            "perlmutter", env, tmp_path / "home"
        ) == (tmp_path / "scratch")

    def test_perlmutter_falls_back_to_home_scratch(self, tmp_path, clean_scratch_env):
        home = tmp_path / "home"
        assert (
            config_module._hpc_scratch_root("perlmutter", {}, home) == home / "scratch"
        )

    def test_anvil_fallback_when_scratch_unset(self, tmp_path, clean_scratch_env):
        home = tmp_path / "home"
        env = {"PROJECT": str(tmp_path / "proj")}
        assert (
            config_module._hpc_scratch_root("anvil", env, home)
            == tmp_path / "proj" / "scratch"
        )

    def test_anvil_fallback_without_project(self, tmp_path, clean_scratch_env):
        home = tmp_path / "home"
        assert (
            config_module._hpc_scratch_root("anvil", {}, home)
            == home / "work" / "scratch"
        )

    def test_anvil_project_fallback_only_applies_when_no_scratch_dirs_var_set(
        self, tmp_path, clean_scratch_env
    ):
        """A bare $SCRATCH_DIR pre-empts the $PROJECT/scratch fallback -- Forge's
        own convention only kicks in once the CSTAR_SCRATCH_DIRS search comes up
        empty.
        """
        home = tmp_path / "home"
        env = {"SCRATCH_DIR": str(tmp_path / "sdir"), "PROJECT": str(tmp_path / "proj")}
        assert config_module._hpc_scratch_root("anvil", env, home) == tmp_path / "sdir"

    def test_bouchet_fallback_uses_scratch_pi_glob(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        home = tmp_path / "home"
        (home / "scratch_pi_abc" / "testuser").mkdir(parents=True)
        monkeypatch.setattr(config_module, "USER", "testuser")
        assert (
            config_module._hpc_scratch_root("bouchet", {}, home)
            == home / "scratch_pi_abc" / "testuser"
        )

    def test_bouchet_returns_none_without_scratch_pi(self, tmp_path, clean_scratch_env):
        home = tmp_path / "home"
        home.mkdir()
        assert config_module._hpc_scratch_root("bouchet", {}, home) is None

    def test_bouchet_glob_only_applies_when_no_scratch_dirs_var_set(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        """A bare $LOCAL_SCRATCH pre-empts the scratch_pi_* glob fallback."""
        home = tmp_path / "home"
        (home / "scratch_pi_abc" / "testuser").mkdir(parents=True)
        monkeypatch.setattr(config_module, "USER", "testuser")
        env = {"LOCAL_SCRATCH": str(tmp_path / "ljob")}
        assert (
            config_module._hpc_scratch_root("bouchet", env, home) == tmp_path / "ljob"
        )

    def test_non_hpc_name_returns_none_even_if_scratch_is_set(
        self, tmp_path, clean_scratch_env
    ):
        env = {"SCRATCH": str(tmp_path / "scratch")}
        for tag in ("darwin_arm64", "linux_x86_64", "derecho"):
            assert (
                config_module._hpc_scratch_root(tag, env, tmp_path / "home") is None
            ), tag

    def test_non_hpc_name_returns_none_even_if_scratch_dir_is_set(
        self, tmp_path, clean_scratch_env
    ):
        env = {"SCRATCH_DIR": str(tmp_path / "sdir")}
        for tag in ("darwin_arm64", "linux_x86_64", "derecho"):
            assert (
                config_module._hpc_scratch_root(tag, env, tmp_path / "home") is None
            ), tag

    def test_uses_cstar_find_scratch_dir_not_a_reimplemented_loop(
        self, tmp_path, clean_scratch_env, monkeypatch
    ):
        """_hpc_scratch_root defers to cstar.base.env.find_scratch_dir (the
        single definition of the CSTAR_SCRATCH_DIRS search) rather than
        re-implementing the loop -- assert the seam is actually called.
        """
        calls = []

        def _fake_find_scratch_dir(env):
            calls.append(dict(env))
            return None

        monkeypatch.setattr(config_module, "find_scratch_dir", _fake_find_scratch_dir)
        home = tmp_path / "home"
        env = {"SCRATCH": str(tmp_path / "scratch")}
        config_module._hpc_scratch_root("perlmutter", env, home)
        assert calls == [env]


class TestRelocateWorkingDir:
    """Tests for relocate_working_dir (default-form paths rebase onto HPC scratch)."""

    def test_default_path_rebases_to_scratch_on_perlmutter(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        wd = relocate_working_dir(
            home / "cstar" / "_forge_bp_runs" / "my-run",
            system_tag="perlmutter",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"

    def test_default_path_rebases_to_scratch_on_anvil(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        wd = relocate_working_dir(
            home / "cstar" / "_forge_bp_runs" / "my-run",
            system_tag="anvil",
            env={
                "SCRATCH": str(tmp_path / "scratch"),
                "PROJECT": str(tmp_path / "proj"),
            },
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"

    def test_anvil_falls_back_to_project_scratch(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        """No $SCRATCH: the fallback derives from $PROJECT; $WORK is ignored."""
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        env = {"PROJECT": str(tmp_path / "proj"), "WORK": str(tmp_path / "work")}
        wd = relocate_working_dir(
            home / "cstar" / "_forge_bp_runs" / "my-run",
            system_tag="anvil",
            env=env,
            home=home,
        )
        assert (
            wd == tmp_path / "proj" / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"
        )

    def test_legacy_cstar_forge_run_root_rebases_to_scratch(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        """The legacy sentinel (``~/cstar-forge-run``, the default before this
        rename) rebases onto the *current* scratch working root, so old
        blueprints no longer write into the old sibling location on HPC.
        """
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        wd = relocate_working_dir(
            home / "cstar-forge-run" / "my-run",
            system_tag="perlmutter",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"

    def test_non_hpc_leaves_path_alone(self, tmp_path, monkeypatch, clean_scratch_env):
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        wd = relocate_working_dir(
            home / "cstar-forge-run" / "my-run",
            system_tag="darwin_arm64",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == home / "cstar-forge-run" / "my-run"

    def test_custom_path_passes_through_on_hpc(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
        custom = tmp_path / "elsewhere" / "my-run"
        wd = relocate_working_dir(
            custom,
            system_tag="perlmutter",
            env={},
            home=home,
        )
        assert wd == custom

    def test_legacy_default_root_rebases_to_scratch(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        """The legacy sentinel (``~/cstar-forge-data/cstar-forge-run``, from blueprints
        authored before the default was renamed) rebases onto the *current* scratch
        working root, so old blueprints no longer write into home on HPC.
        """
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        wd = relocate_working_dir(
            home / "cstar-forge-data" / "cstar-forge-run" / "my-run",
            system_tag="perlmutter",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"

    def test_bare_cstar_forge_data_path_passes_through(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        """The legacy match is deliberately narrow: only the nested
        ``cstar-forge-data/cstar-forge-run`` sentinel rebases. A bare path under
        ``~/cstar-forge-data`` (which is also the mac/dev source_data cache base)
        is a user choice and passes through untouched.
        """
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
        custom = home / "cstar-forge-data" / "my-hand-picked-run"
        wd = relocate_working_dir(
            custom,
            system_tag="perlmutter",
            env={},
            home=home,
        )
        assert wd == custom

    def test_home_rooted_nondefault_warns_on_hpc(
        self, tmp_path, monkeypatch, clean_scratch_env, caplog
    ):
        """A home-rooted path that matches no default root is left in home on HPC;
        warn so an unrelocated (e.g. very old default) run doesn't go unnoticed.
        """
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
        custom = home / "cstar-forge-data" / "my-hand-picked-run"
        with caplog.at_level(logging.WARNING, logger="cstar.applications.forge.config"):
            wd = relocate_working_dir(
                custom,
                system_tag="perlmutter",
                env={},
                home=home,
            )
        assert wd == custom
        assert "was not relocated to scratch" in caplog.text

    def test_off_home_custom_path_does_not_warn(
        self, tmp_path, monkeypatch, clean_scratch_env, caplog
    ):
        """A deliberate path outside home is normal and must not warn."""
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
        custom = tmp_path / "elsewhere" / "my-run"
        with caplog.at_level(logging.WARNING, logger="cstar.applications.forge.config"):
            wd = relocate_working_dir(
                custom,
                system_tag="perlmutter",
                env={},
                home=home,
            )
        assert wd == custom
        assert caplog.text == ""

    def test_default_path_rebases_to_scratch_on_bouchet(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        from cstar.applications.forge.config import relocate_working_dir

        monkeypatch.setattr(config_module, "USER", "testuser")
        home = tmp_path / "home"
        (home / "scratch_pi_abc" / "testuser").mkdir(parents=True)
        wd = relocate_working_dir(
            home / "cstar" / "_forge_bp_runs" / "my-run",
            system_tag="bouchet",
            env={},
            home=home,
        )
        assert (
            wd
            == home
            / "scratch_pi_abc"
            / "testuser"
            / "cstar"
            / "_forge_bp_runs"
            / "my-run"
        )

    def test_bouchet_without_scratch_pi_leaves_path_alone(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        """With no scratch_pi_* dir discoverable, _hpc_scratch_root returns None,
        so relocate_working_dir returns the path unchanged (same as any other
        HPC system with no resolvable scratch root -- no warning in this branch,
        since the function returns before the home-rooted-warning check).
        """
        from cstar.applications.forge.config import relocate_working_dir

        monkeypatch.setattr(config_module, "USER", "testuser")
        home = tmp_path / "home"
        home.mkdir(parents=True)
        custom = home / "cstar" / "_forge_bp_runs" / "my-run"
        wd = relocate_working_dir(
            custom,
            system_tag="bouchet",
            env={},
            home=home,
        )
        assert wd == custom

    def test_tilde_default_expands_then_rebases(
        self, tmp_path, monkeypatch, clean_scratch_env
    ):
        from cstar.applications.forge.config import relocate_working_dir

        home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(home))
        wd = relocate_working_dir(
            "~/cstar/_forge_bp_runs/my-run",
            system_tag="perlmutter",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"


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
