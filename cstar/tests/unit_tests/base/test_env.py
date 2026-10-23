from pathlib import Path

import pytest

from cstar.base.env import (
    ENV_CSTAR_CATALOG,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_LOG_LEVEL,
    ENV_CSTAR_RUNID,
    ENV_CSTAR_SCRATCH_DIRS,
    GROUP_INTERNAL,
    REMOVED_SETTINGS,
    discover_env_vars,
    find_scratch_dir,
    get_env_item,
    hpc_data_directory,
)


def test_catalog_defaults_under_home_not_data_home(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The catalog default is home-anchored, independent of CSTAR_DATA_HOME.

    Catalog entries are durable, user-registered content that must survive HPC
    scratch purges, so the default must not follow CSTAR_DATA_HOME (which
    resolves onto scratch on HPC systems).
    """
    monkeypatch.delenv(ENV_CSTAR_CATALOG, raising=False)
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, "/data/home")
    expected = (Path.home() / "cstar" / "catalog").as_posix()
    assert get_env_item(ENV_CSTAR_CATALOG).value == expected


def test_catalog_explicit_value_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_CSTAR_CATALOG, "/mine:https://github.com/org/repo/catalog")
    assert (
        get_env_item(ENV_CSTAR_CATALOG).value
        == "/mine:https://github.com/org/repo/catalog"
    )


def test_catalog_is_discoverable() -> None:
    assert ENV_CSTAR_CATALOG in discover_env_vars()


def test_removed_settings_are_not_registered() -> None:
    """Verify no removed setting is still a registered environment variable."""
    assert REMOVED_SETTINGS
    assert not REMOVED_SETTINGS.keys() & discover_env_vars().keys()


def test_runid_is_internal_and_not_persistable() -> None:
    """Verify the run id is in the internal group and cannot be persisted."""
    item = get_env_item(ENV_CSTAR_RUNID)

    assert item.group == GROUP_INTERNAL
    assert item.persistable is False


def test_only_runid_and_config_home_are_not_persistable() -> None:
    """Verify every registered setting but the run id and config home is persistable."""
    assert get_env_item(ENV_CSTAR_CONFIG_HOME).persistable is False

    not_persistable = {
        name for name, item in discover_env_vars().items() if not item.persistable
    }

    assert not_persistable == {ENV_CSTAR_RUNID, ENV_CSTAR_CONFIG_HOME}


def test_env_item_keeps_name_and_reads_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an `EnvItem` built from an annotation carries its name and reads
    the value of that variable from the environment.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture used to set the variable.
    """
    monkeypatch.setenv(ENV_CSTAR_LOG_LEVEL, "DEBUG")

    item = get_env_item(ENV_CSTAR_LOG_LEVEL)

    assert item.name == ENV_CSTAR_LOG_LEVEL
    assert item.value == "DEBUG"


class TestFindScratchDir:
    """Tests for find_scratch_dir, the CSTAR_SCRATCH_DIRS search behind
    hpc_data_directory, run against an explicit environment mapping.
    """

    def test_default_search_order_is_scratch_then_scratch_dir_then_local_scratch(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(ENV_CSTAR_SCRATCH_DIRS, raising=False)
        assert (
            find_scratch_dir(
                {"SCRATCH": "/scr", "SCRATCH_DIR": "/sdir", "LOCAL_SCRATCH": "/lscr"}
            )
            == "/scr"
        )
        assert find_scratch_dir({"SCRATCH_DIR": "/sdir", "LOCAL_SCRATCH": "/lscr"}) == (
            "/sdir"
        )
        assert find_scratch_dir({"LOCAL_SCRATCH": "/lscr"}) == "/lscr"

    def test_returns_none_when_nothing_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(ENV_CSTAR_SCRATCH_DIRS, raising=False)
        assert find_scratch_dir({}) is None
        assert find_scratch_dir({"SOME_OTHER_VAR": "/x"}) is None

    def test_searches_the_passed_mapping_not_the_real_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A value set in the real process environment must not leak into a
        search against an explicit mapping that doesn't carry it.
        """
        monkeypatch.delenv(ENV_CSTAR_SCRATCH_DIRS, raising=False)
        monkeypatch.setenv("SCRATCH", "/real-process-scratch")
        assert find_scratch_dir({}) is None
        monkeypatch.delenv("SCRATCH", raising=False)

    def test_respects_a_custom_cstar_scratch_dirs_list(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENV_CSTAR_SCRATCH_DIRS, "MY_SCRATCH_VAR")
        assert find_scratch_dir({"SCRATCH": "/scr", "MY_SCRATCH_VAR": "/mine"}) == (
            "/mine"
        )

    def test_hpc_data_directory_delegates_to_find_scratch_dir_on_os_environ(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(ENV_CSTAR_SCRATCH_DIRS, raising=False)
        monkeypatch.delenv("SCRATCH_DIR", raising=False)
        monkeypatch.delenv("LOCAL_SCRATCH", raising=False)
        monkeypatch.setenv("SCRATCH", "/real-scratch")
        assert hpc_data_directory() == "/real-scratch"
        monkeypatch.delenv("SCRATCH", raising=False)
        assert hpc_data_directory() is None


class TestHpcDataDirectorySystemFallback:
    """hpc_data_directory consults the SystemContext's scratch_root() hook only
    when none of the CSTAR_SCRATCH_DIRS variables is set.
    """

    @pytest.fixture(autouse=True)
    def _no_scratch_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in ("CSTAR_SCRATCH_DIRS", "SCRATCH", "SCRATCH_DIR", "LOCAL_SCRATCH"):
            monkeypatch.delenv(var, raising=False)

    @staticmethod
    def _context_with(root: Path | None):
        class _Ctx:
            @classmethod
            def scratch_root(cls) -> Path | None:
                return root

        return lambda: _Ctx

    def test_uses_system_hook_when_no_variable_is_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "cstar.system.manager.get_system_context",
            self._context_with(Path("/sys-scratch/user")),
        )
        assert hpc_data_directory() == "/sys-scratch/user"

    def test_variable_wins_over_system_hook(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SCRATCH", "/env-scratch")
        monkeypatch.setattr(
            "cstar.system.manager.get_system_context",
            self._context_with(Path("/sys-scratch/user")),
        )
        assert hpc_data_directory() == "/env-scratch"

    def test_none_when_hook_has_no_convention(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "cstar.system.manager.get_system_context", self._context_with(None)
        )
        assert hpc_data_directory() is None

    def test_none_when_system_is_unknown(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from cstar.base.exceptions import CstarError

        def _unknown():
            raise CstarError("Unknown system requested: nowhere")

        monkeypatch.setattr("cstar.system.manager.get_system_context", _unknown)
        assert hpc_data_directory() is None

    def test_none_when_system_name_cannot_be_determined(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """HostNameEvaluator raises OSError when no name can be determined at all."""

        def _nameless():
            raise OSError("C-Star cannot determine your system name")

        monkeypatch.setattr("cstar.system.manager.get_system_context", _nameless)
        assert hpc_data_directory() is None
