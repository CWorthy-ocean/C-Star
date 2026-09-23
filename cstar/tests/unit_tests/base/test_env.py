import logging
from pathlib import Path

import pytest

from cstar.base.env import (
    ENV_CSTAR_CATALOG,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_ORCH_MAX_CONC,
    ENV_CSTAR_SCRATCH_DIRS,
    discover_env_vars,
    find_scratch_dir,
    get_env_item,
    hpc_data_directory,
    max_concurrency,
)


def test_max_concurrency_default_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the documented default (10) is returned when the env var is unset."""
    monkeypatch.delenv(ENV_CSTAR_ORCH_MAX_CONC, raising=False)

    assert max_concurrency() == 10


def test_max_concurrency_parses_valid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify a valid integer value is parsed and returned as-is."""
    monkeypatch.setenv(ENV_CSTAR_ORCH_MAX_CONC, "42")

    assert max_concurrency() == 42


def test_max_concurrency_falls_back_on_non_integer(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a non-integer value falls back to the default with a logged warning."""
    monkeypatch.setenv(ENV_CSTAR_ORCH_MAX_CONC, "not-a-number")

    with caplog.at_level(logging.WARNING):
        value = max_concurrency()

    assert value == 10
    assert "Unable to parse" in caplog.text


@pytest.mark.parametrize("bad_value", ["0", "-1", "-100"])
def test_max_concurrency_falls_back_on_non_positive(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    bad_value: str,
) -> None:
    """Verify a zero or negative value falls back to the default with a logged warning.

    Parameters
    ----------
    bad_value : str
        A parseable but invalid (non-positive) configured value.
    """
    monkeypatch.setenv(ENV_CSTAR_ORCH_MAX_CONC, bad_value)

    with caplog.at_level(logging.WARNING):
        value = max_concurrency()

    assert value == 10
    assert "invalid" in caplog.text


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


class TestFindScratchDir:
    """Tests for find_scratch_dir, the CSTAR_SCRATCH_DIRS search shared by
    hpc_data_directory and callers (e.g. Forge's own scratch-root resolution)
    that need to run the same search against an explicit environment mapping.
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
