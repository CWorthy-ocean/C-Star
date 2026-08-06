import typing as t
from pathlib import Path

from typer.testing import CliRunner

from cstar.caching import CacheManager
from cstar.caching.store import function_slug
from cstar.cli.cache.commands import app

if t.TYPE_CHECKING:
    from cstar.tests.unit_tests.caching.conftest import CountingArtifact

runner = CliRunner()


def seed(counting_artifact: "CountingArtifact", tmp_path: Path) -> str:
    handle = counting_artifact("demo", 2, True, output_dir=tmp_path / "seed")
    return handle.key


def test_list_shows_entries(
    counting_artifact: "CountingArtifact",
    tmp_path: Path,
) -> None:
    key = seed(counting_artifact, tmp_path)

    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert key[:12] in result.output
    assert "counting" in result.output
    assert "personal" in result.output


def test_list_empty(tmp_path: Path) -> None:
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "No cache entries found" in result.output


def test_list_function_filter(
    counting_artifact: "CountingArtifact",
    tmp_path: Path,
) -> None:
    seed(counting_artifact, tmp_path)

    result = runner.invoke(app, ["list", "--function", "no-such-function"])
    assert result.exit_code == 0
    assert "No cache entries found" in result.output


def test_show_prints_manifest(
    counting_artifact: "CountingArtifact",
    tmp_path: Path,
) -> None:
    key = seed(counting_artifact, tmp_path)

    result = runner.invoke(app, ["show", key[:12]])
    assert result.exit_code == 0
    assert key in result.output
    assert "key_material" in result.output
    assert "provenance" in result.output


def test_show_unknown_reference_fails(tmp_path: Path) -> None:
    result = runner.invoke(app, ["show", "deadbeef1234"])
    assert result.exit_code == 1
    assert "No cache entry matches" in result.output


def test_promote_and_clear_roundtrip(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    key = seed(counting_artifact, tmp_path)
    slug = function_slug(
        counting_artifact("demo", 2, True, output_dir=tmp_path / "s2").function
    )

    result = runner.invoke(app, ["promote", key[:12], "--yes"])
    assert result.exit_code == 0, result.output
    assert manager.group is not None
    assert manager.group.find(slug, key) is not None

    # default clear targets the personal tier
    result = runner.invoke(app, ["clear", key[:12], "--yes"])
    assert result.exit_code == 0, result.output
    assert manager.personal.find(slug, key) is None
    assert manager.group.find(slug, key) is not None

    # group clear requires the explicit tier
    result = runner.invoke(app, ["clear", key[:12], "--tier", "group", "--yes"])
    assert result.exit_code == 0, result.output
    assert manager.group.find(slug, key) is None


def test_clear_dry_run_removes_nothing(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    key = seed(counting_artifact, tmp_path)

    result = runner.invoke(app, ["clear", key[:12], "--dry-run"])
    assert result.exit_code == 0
    assert "would remove" in result.output
    assert len(list(manager.personal.iter_entries())) == 1


def test_clear_all_requires_no_reference(tmp_path: Path) -> None:
    result = runner.invoke(app, ["clear", "abcdef123456", "--all"])
    assert result.exit_code != 0

    result = runner.invoke(app, ["clear"])
    assert result.exit_code != 0


def test_clear_all_personal(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    counting_artifact("demo", 1, True, output_dir=tmp_path / "a")
    counting_artifact("demo", 2, True, output_dir=tmp_path / "b")
    assert len(list(manager.personal.iter_entries())) == 2

    result = runner.invoke(app, ["clear", "--all", "--yes"])
    assert result.exit_code == 0
    assert list(manager.personal.iter_entries()) == []


def test_clear_all_reaps_staging_leftovers(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    """Crashed runs strand staging dirs; `clear --all` must reap them."""
    counting_artifact("demo", 1, True, output_dir=tmp_path / "a")
    leftover = manager.personal.begin_staging("f" * 64)
    assert leftover.exists()

    result = runner.invoke(app, ["clear", "--all", "--yes"])
    assert result.exit_code == 0
    assert not leftover.exists()
    assert list(manager.personal.iter_entries()) == []


def test_invalid_tier_rejected(tmp_path: Path) -> None:
    result = runner.invoke(app, ["list", "--tier", "cosmic"])
    assert result.exit_code != 0
