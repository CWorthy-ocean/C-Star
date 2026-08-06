from pathlib import Path

import pytest

from cstar.caching.models import (
    MANIFEST_FILENAME,
    MANIFEST_SCHEMA_VERSION,
    CacheFileRecord,
    CacheManifest,
    CacheProvenance,
    CacheTier,
)
from cstar.caching.store import (
    AmbiguousCacheKeyError,
    CacheEntryNotFoundError,
    CacheError,
    CacheManager,
    CacheStore,
    function_slug,
)

KEY_A = "a" * 64
KEY_B = "b" * 64
FUNCTION = "tests.caching.producer"
SLUG = function_slug(FUNCTION)


def build_manifest(
    key: str = KEY_A, label: str = "", files: dict[str, str] | None = None
) -> CacheManifest:
    """Create a manifest for the supplied payload contents."""
    files = files if files is not None else {"a.dat": "aaa"}
    return CacheManifest(
        schema_version=MANIFEST_SCHEMA_VERSION,
        key=key,
        function=FUNCTION,
        function_version="1",
        label=label,
        key_material={"args": {"name": "demo"}},
        files=[
            CacheFileRecord(relpath=relpath, size_bytes=len(content))
            for relpath, content in files.items()
        ],
        provenance=CacheProvenance.capture(),
    )


def stage_entry(
    store: CacheStore,
    key: str = KEY_A,
    label: str = "",
    files: dict[str, str] | None = None,
) -> tuple[Path, CacheManifest]:
    """Stage a payload and return the staging dir and its manifest."""
    files = files if files is not None else {"a.dat": "aaa"}
    staging = store.begin_staging(key)
    for relpath, content in files.items():
        target = staging / "payload" / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    return staging, build_manifest(key=key, label=label, files=files)


@pytest.fixture
def store(tmp_path: Path) -> CacheStore:
    return CacheStore(tmp_path / "store", CacheTier.personal)


def test_commit_and_find_roundtrip(store: CacheStore) -> None:
    staging, manifest = stage_entry(store, files={"a.dat": "aaa", "sub/b.dat": "bb"})
    entry = store.commit(staging, manifest)

    assert not staging.exists(), "staging dir must be renamed away"
    assert entry.tier == CacheTier.personal
    assert (entry.payload_dir / "sub" / "b.dat").read_text() == "bb"

    found = store.find(SLUG, KEY_A)
    assert found is not None
    assert found.manifest.key == KEY_A
    assert found.manifest.provenance.created_by == manifest.provenance.created_by
    assert [p.name for p in found.payload_paths] == ["a.dat", "b.dat"]


def test_find_missing_returns_none(store: CacheStore) -> None:
    assert store.find(SLUG, KEY_A) is None


def test_find_corrupt_manifest_returns_none(store: CacheStore) -> None:
    staging, manifest = stage_entry(store)
    entry = store.commit(staging, manifest)

    (entry.entry_dir / MANIFEST_FILENAME).write_text(":::: not yaml {{{{")
    assert store.find(SLUG, KEY_A) is None


def test_find_missing_payload_file_returns_none(store: CacheStore) -> None:
    staging, manifest = stage_entry(store)
    entry = store.commit(staging, manifest)

    (entry.payload_dir / "a.dat").unlink()
    assert store.find(SLUG, KEY_A) is None


def test_find_size_mismatch_returns_none(store: CacheStore) -> None:
    staging, manifest = stage_entry(store)
    entry = store.commit(staging, manifest)

    (entry.payload_dir / "a.dat").write_text("wrong size content")
    assert store.find(SLUG, KEY_A) is None


def test_commit_race_loser_adopts_winner(store: CacheStore) -> None:
    staging_winner, manifest_winner = stage_entry(store, files={"a.dat": "won"})
    staging_loser, manifest_loser = stage_entry(store, files={"a.dat": "lst"})

    winner = store.commit(staging_winner, manifest_winner)
    adopted = store.commit(staging_loser, manifest_loser)

    assert not staging_loser.exists(), "loser staging must be discarded"
    assert adopted.entry_dir == winner.entry_dir
    assert (adopted.payload_dir / "a.dat").read_text() == "won"


def test_commit_replaces_invalid_occupant(store: CacheStore) -> None:
    occupant = store.entry_dir(SLUG, KEY_A)
    occupant.mkdir(parents=True)
    (occupant / MANIFEST_FILENAME).write_text(":::: garbage {{{{")

    staging, manifest = stage_entry(store)
    entry = store.commit(staging, manifest)

    assert (entry.payload_dir / "a.dat").read_text() == "aaa"
    assert store.find(SLUG, KEY_A) is not None


def test_iter_entries_yields_only_valid(store: CacheStore) -> None:
    staging_a, manifest_a = stage_entry(store, key=KEY_A)
    store.commit(staging_a, manifest_a)

    staging_b, manifest_b = stage_entry(store, key=KEY_B)
    broken = store.commit(staging_b, manifest_b)
    (broken.payload_dir / "a.dat").unlink()

    keys = [entry.manifest.key for entry in store.iter_entries()]
    assert keys == [KEY_A]


def test_remove_wrong_tier_raises(store: CacheStore) -> None:
    staging, manifest = stage_entry(store)
    entry = store.commit(staging, manifest)
    imposter = entry.model_copy(update={"tier": CacheTier.group})

    with pytest.raises(CacheError, match="Refusing"):
        store.remove(imposter)

    store.remove(entry)
    assert store.find(SLUG, KEY_A) is None


def test_purge_removes_everything(store: CacheStore) -> None:
    staging, manifest = stage_entry(store)
    store.commit(staging, manifest)
    leftover = store.begin_staging(KEY_B)
    assert leftover.exists()

    store.purge()
    assert not store.entries_dir.exists()
    assert not store.staging_dir.exists()


def test_place_symlinks_refuses_output_inside_entry(store: CacheStore) -> None:
    """Linking into the entry itself would destroy the payload files."""
    from cstar.caching.store import place_symlinks

    staging, manifest = stage_entry(store)
    entry = store.commit(staging, manifest)

    with pytest.raises(CacheError, match="inside the cache entry"):
        place_symlinks(entry, entry.payload_dir)

    assert (entry.payload_dir / "a.dat").read_text() == "aaa", "payload intact"


def test_manager_lookup_prefers_group(manager: CacheManager) -> None:
    assert manager.group is not None

    staging_p, manifest_p = stage_entry(manager.personal, files={"a.dat": "prs"})
    manager.personal.commit(staging_p, manifest_p)
    staging_g, manifest_g = stage_entry(manager.group, files={"a.dat": "grp"})
    manager.group.commit(staging_g, manifest_g)

    entry = manager.lookup(SLUG, KEY_A)
    assert entry is not None
    assert entry.tier == CacheTier.group


def test_manager_lookup_falls_back_to_personal(manager: CacheManager) -> None:
    staging, manifest = stage_entry(manager.personal)
    manager.personal.commit(staging, manifest)

    entry = manager.lookup(SLUG, KEY_A)
    assert entry is not None
    assert entry.tier == CacheTier.personal


def test_resolve_by_prefix_and_label(manager: CacheManager) -> None:
    staging, manifest = stage_entry(manager.personal, key=KEY_A, label="my-run")
    manager.personal.commit(staging, manifest)

    assert manager.resolve(KEY_A[:8]).manifest.key == KEY_A
    assert manager.resolve("my-run").manifest.key == KEY_A


def test_resolve_rejects_short_prefix(manager: CacheManager) -> None:
    staging, manifest = stage_entry(manager.personal, key=KEY_A)
    manager.personal.commit(staging, manifest)

    with pytest.raises(CacheEntryNotFoundError):
        manager.resolve(KEY_A[:4])


def test_resolve_ambiguous_raises(manager: CacheManager) -> None:
    key_similar = "a" * 63 + "f"
    staging_a, manifest_a = stage_entry(manager.personal, key=KEY_A)
    manager.personal.commit(staging_a, manifest_a)
    staging_f, manifest_f = stage_entry(manager.personal, key=key_similar)
    manager.personal.commit(staging_f, manifest_f)

    with pytest.raises(AmbiguousCacheKeyError, match="candidates"):
        manager.resolve(KEY_A[:8])


def test_resolve_not_found(manager: CacheManager) -> None:
    with pytest.raises(CacheEntryNotFoundError):
        manager.resolve("deadbeef1234")


def test_resolve_tier_filter(manager: CacheManager) -> None:
    assert manager.group is not None
    staging_p, manifest_p = stage_entry(manager.personal)
    manager.personal.commit(staging_p, manifest_p)
    staging_g, manifest_g = stage_entry(manager.group)
    manager.group.commit(staging_g, manifest_g)

    assert manager.resolve(KEY_A[:8]).tier == CacheTier.group
    resolved = manager.resolve(KEY_A[:8], tier=CacheTier.personal)
    assert resolved.tier == CacheTier.personal
