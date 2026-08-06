import typing as t
from pathlib import Path

import pytest

from cstar.caching import CacheManager, CacheTier
from cstar.caching.store import CacheConfigurationError, function_slug

if t.TYPE_CHECKING:
    from cstar.tests.unit_tests.caching.conftest import CountingArtifact


def seed_entry(
    counting_artifact: "CountingArtifact", manager: CacheManager, tmp_path: Path
):
    handle = counting_artifact("demo", 2, True, output_dir=tmp_path / "seed")
    entry = manager.personal.find(function_slug(handle.function), handle.key)
    assert entry is not None
    return entry


def test_promote_copies_payload_and_stamps_provenance(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    entry = seed_entry(counting_artifact, manager, tmp_path)
    assert entry.manifest.provenance.promoted_at is None

    promoted = manager.promote(entry)

    assert promoted.tier == CacheTier.group
    assert promoted.manifest.provenance.promoted_at is not None
    assert promoted.manifest.provenance.promoted_by
    assert (
        promoted.manifest.provenance.created_at == entry.manifest.provenance.created_at
    )

    for source, copy in zip(entry.payload_paths, promoted.payload_paths, strict=True):
        assert copy.read_text() == source.read_text()

    # default promote keeps the personal copy (symlinks from prior runs survive)
    assert (
        manager.personal.find(
            function_slug(entry.manifest.function), entry.manifest.key
        )
        is not None
    )


def test_promote_delete_source(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    entry = seed_entry(counting_artifact, manager, tmp_path)
    slug = function_slug(entry.manifest.function)

    manager.promote(entry, delete_source=True)

    assert manager.personal.find(slug, entry.manifest.key) is None
    assert manager.group is not None
    assert manager.group.find(slug, entry.manifest.key) is not None


def test_promote_is_idempotent(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    entry = seed_entry(counting_artifact, manager, tmp_path)

    first = manager.promote(entry)
    second = manager.promote(entry)
    assert second.entry_dir == first.entry_dir

    # promoting an already-group entry is a no-op
    third = manager.promote(first)
    assert third.entry_dir == first.entry_dir


def test_promote_without_group_configured_raises(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    entry = seed_entry(counting_artifact, manager, tmp_path)
    ungrouped = CacheManager(manager.personal, group=None)

    with pytest.raises(CacheConfigurationError, match="CSTAR_CACHE_GROUP_ROOT"):
        ungrouped.promote(entry)


def test_promoted_entry_serves_after_personal_cleared(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    entry = seed_entry(counting_artifact, manager, tmp_path)
    manager.promote(entry)
    manager.personal.remove(entry)

    served = counting_artifact("demo", 2, True, output_dir=tmp_path / "later")
    assert served.hit is True
    assert served.tier == CacheTier.group
    assert counting_artifact.calls == 1
