"""SystemContext.scratch_root: the per-system scratch convention behind
``CSTAR_DATA_HOME`` when no SCRATCH-style variable is exported.
"""

import logging
from pathlib import Path

import pytest

from cstar.system.manager import (
    CTX_REGISTRY,
    BouchetSystemContext,
    SystemContext,
    find_bouchet_scratch_root,
)


def test_protocol_default_is_none() -> None:
    assert SystemContext.scratch_root() is None


@pytest.mark.parametrize(
    "context",
    [ctx for name, ctx in sorted(CTX_REGISTRY.items()) if name != "bouchet"],
    ids=lambda ctx: ctx.name,
)
def test_only_bouchet_declares_a_convention(context: type[SystemContext]) -> None:
    """Anvil, Perlmutter and the rest export SCRATCH (or have no scratch at all), so
    they rely on CSTAR_SCRATCH_DIRS and contribute nothing here.
    """
    assert context.scratch_root() is None


class TestFindBouchetScratchRoot:
    def test_picks_sorted_first_scratch_pi_dir(self, tmp_path: Path) -> None:
        for name in ("scratch_pi_zeta", "scratch_pi_alpha", "scratch_pi_mid"):
            (tmp_path / name).mkdir()
        assert find_bouchet_scratch_root(tmp_path, "testuser") == (
            tmp_path / "scratch_pi_alpha" / "testuser"
        )

    def test_skips_non_directory_matches(self, tmp_path: Path) -> None:
        (tmp_path / "scratch_pi_notadir").write_text("not a directory")
        (tmp_path / "scratch_pi_real").mkdir()
        assert find_bouchet_scratch_root(tmp_path, "testuser") == (
            tmp_path / "scratch_pi_real" / "testuser"
        )

    def test_returns_none_when_no_matches(self, tmp_path: Path) -> None:
        assert find_bouchet_scratch_root(tmp_path, "testuser") is None

    def test_returns_none_on_oserror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
    ) -> None:
        """A failed scan (a stale mount, say) degrades to None instead of raising."""

        def _boom(self, pattern):
            raise OSError("stale NFS handle")

        monkeypatch.setattr(Path, "glob", _boom)
        with caplog.at_level(logging.WARNING, logger="cstar.system.manager"):
            assert find_bouchet_scratch_root(tmp_path, "testuser") is None
        assert "scratch_pi_*" in caplog.text


class TestBouchetContext:
    def test_scratch_root_reads_home_and_user(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        home = tmp_path / "home"
        (home / "scratch_pi_abc").mkdir(parents=True)
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("USER", "testuser")
        assert BouchetSystemContext.scratch_root() == (
            home / "scratch_pi_abc" / "testuser"
        )

    def test_scratch_root_none_without_scratch_pi(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        home = tmp_path / "home"
        home.mkdir()
        monkeypatch.setenv("HOME", str(home))
        assert BouchetSystemContext.scratch_root() is None
