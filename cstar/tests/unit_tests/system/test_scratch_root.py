"""SystemContext.scratch_root: the per-system scratch convention behind
``CSTAR_DATA_HOME`` when no SCRATCH-style variable is exported.
"""

import logging
from pathlib import Path

import pytest

from cstar.system.manager import SystemContext, registered_context, registered_names


def test_protocol_default_is_none() -> None:
    assert SystemContext.scratch_root() is None


@pytest.mark.parametrize(
    "name", [name for name in registered_names() if name not in {"bouchet", "oscar"}]
)
def test_contexts_without_a_convention_return_none(name: str) -> None:
    """Apart from Bouchet and Oscar, systems export SCRATCH (or have no scratch at
    all), so they rely on CSTAR_SCRATCH_DIRS and contribute nothing here.
    """
    assert registered_context(name).scratch_root() is None


class TestBouchetContext:
    """Bouchet's convention: ``$HOME/scratch_pi_*/<user>``, first in sorted order."""

    @pytest.fixture
    def home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        home = tmp_path / "home"
        home.mkdir()
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("USER", "testuser")
        return home

    def test_scratch_root_reads_home_and_user(self, home: Path) -> None:
        (home / "scratch_pi_abc").mkdir()
        assert registered_context("bouchet").scratch_root() == (
            home / "scratch_pi_abc" / "testuser"
        )

    def test_picks_sorted_first_scratch_pi_dir(self, home: Path) -> None:
        for name in ("scratch_pi_zeta", "scratch_pi_alpha", "scratch_pi_mid"):
            (home / name).mkdir()
        assert registered_context("bouchet").scratch_root() == (
            home / "scratch_pi_alpha" / "testuser"
        )

    def test_skips_non_directory_matches(self, home: Path) -> None:
        (home / "scratch_pi_notadir").write_text("not a directory")
        (home / "scratch_pi_real").mkdir()
        assert registered_context("bouchet").scratch_root() == (
            home / "scratch_pi_real" / "testuser"
        )

    def test_scratch_root_none_without_scratch_pi(self, home: Path) -> None:
        assert registered_context("bouchet").scratch_root() is None

    def test_returns_none_on_oserror(
        self, home: Path, monkeypatch: pytest.MonkeyPatch, caplog
    ) -> None:
        """A failed scan (a stale mount, say) degrades to None instead of raising."""

        def _boom(pattern: str) -> list[str]:
            raise OSError("stale NFS handle")

        monkeypatch.setattr("cstar.system.spec.glob.glob", _boom)
        with caplog.at_level(logging.WARNING, logger="cstar.system.spec"):
            assert registered_context("bouchet").scratch_root() is None
        assert "scratch_pi_*" in caplog.text


class TestOscarContext:
    """Oscar's convention: the ``$HOME/scratch`` directory."""

    @pytest.fixture
    def home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        home = tmp_path / "home"
        home.mkdir()
        monkeypatch.setenv("HOME", str(home))
        return home

    def test_scratch_directory_is_the_root(self, home: Path) -> None:
        (home / "scratch").mkdir()
        assert registered_context("oscar").scratch_root() == home / "scratch"

    def test_none_without_scratch(self, home: Path) -> None:
        assert registered_context("oscar").scratch_root() is None

    def test_none_when_scratch_is_a_file(self, home: Path) -> None:
        (home / "scratch").write_text("not a directory")
        assert registered_context("oscar").scratch_root() is None

    def test_returns_none_on_oserror(
        self, home: Path, monkeypatch: pytest.MonkeyPatch, caplog
    ) -> None:
        """A failed check (a stale mount, say) degrades to None instead of raising."""
        (home / "scratch").mkdir()

        def _boom(self: Path) -> bool:
            raise OSError("stale NFS handle")

        monkeypatch.setattr(Path, "is_dir", _boom)
        with caplog.at_level(logging.WARNING, logger="cstar.system.spec"):
            assert registered_context("oscar").scratch_root() is None
        assert "SCRATCH" in caplog.text
