"""Unit tests for set-valued artifacts in :mod:`cstar.orchestration.artifact_cache`.

A partition is many files that are only useful together, so the cache treats
the collection as one artifact: an expanded container in the user tier, an
archive in the shared tier.
"""

import tarfile
from pathlib import Path

import pytest

from cstar.orchestration.artifact_cache import (
    SET_MANIFEST_NAME,
    ArtifactCache,
    ArtifactCacheError,
    ArtifactExistsError,
    ArtifactKind,
    ArtifactNotFoundError,
    OnConflict,
    SetManifest,
    Tier,
    UnsafePathError,
)
from cstar.orchestration.fingerprinting import FullFingerprinter, NullFingerprinter

RUN_ID = "run-abc-123"
NAME = "partitioned.set"


@pytest.fixture
def cache(tmp_path: Path) -> ArtifactCache:
    """Return a cache that fingerprints members fully.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    ArtifactCache
        Cache under test.
    """
    return ArtifactCache(
        tmp_path / "user", tmp_path / "shared", fingerprinter=FullFingerprinter()
    )


@pytest.fixture
def partition(tmp_path: Path) -> Path:
    """Return a directory of eight ranks plus one nested member.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Path
        Directory holding the members.
    """
    work = tmp_path / "work"
    (work / "checkpoint-0042").mkdir(parents=True)
    for rank in range(8):
        (work / f"rank{rank:03d}.nc").write_bytes(b"rank-%03d" % rank)
    (work / "checkpoint-0042" / "rank000.nc").write_bytes(b"nested")
    return work


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


def test_ingest_commits_a_container(cache: ArtifactCache, partition: Path) -> None:
    """A directory of members becomes one artifact."""
    location = cache.ingest_aggregate(partition, NAME, RUN_ID)

    assert location.path.is_dir()
    assert location.is_container
    assert location.kind is ArtifactKind.SET
    assert location.exists


def test_manifest_is_hidden_from_member_globs(
    cache: ArtifactCache, partition: Path
) -> None:
    """A caller globbing for data must not pick up bookkeeping."""
    location = cache.ingest_aggregate(partition, NAME, RUN_ID)

    assert (location.path / SET_MANIFEST_NAME).is_file()
    assert len(sorted(location.path.glob("*.nc"))) == 8


def test_nested_members_are_preserved(cache: ArtifactCache, partition: Path) -> None:
    """Members are relative paths, so a container may hold subdirectories."""
    location = cache.ingest_aggregate(partition, NAME, RUN_ID)

    assert (location.path / "checkpoint-0042" / "rank000.nc").read_bytes() == b"nested"


def test_record_describes_a_set(cache: ArtifactCache, partition: Path) -> None:
    """The record carries the kind and the container's manifest digest."""
    cache.ingest_aggregate(partition, NAME, RUN_ID)

    record = cache.read_manifest(RUN_ID).artifacts[NAME]
    assert record.kind is ArtifactKind.SET
    assert record.checksum
    assert record.size_bytes == 8 * len(b"rank-000") + len(b"nested")


def test_selected_members_only(cache: ArtifactCache, partition: Path) -> None:
    """A caller may restrict which files are taken."""
    location = cache.ingest_aggregate(
        partition, NAME, RUN_ID, members=["rank000.nc", "rank001.nc"]
    )

    assert sorted(p.name for p in location.path.glob("*.nc")) == [
        "rank000.nc",
        "rank001.nc",
    ]


def test_member_escaping_the_container_is_refused(
    cache: ArtifactCache, partition: Path
) -> None:
    """A relative path must not reach outside the container."""
    with pytest.raises(UnsafePathError, match="escapes"):
        cache.ingest_aggregate(partition, NAME, RUN_ID, members=["../outside.nc"])


def test_empty_source_is_refused(cache: ArtifactCache, tmp_path: Path) -> None:
    """An empty container is meaningless and is caught at ingest."""
    empty = tmp_path / "empty"
    empty.mkdir()

    with pytest.raises(ArtifactCacheError, match="no members"):
        cache.ingest_aggregate(empty, NAME, RUN_ID)


def test_missing_source_directory(cache: ArtifactCache, tmp_path: Path) -> None:
    """Ingesting a directory that is not there fails before anything is written."""
    with pytest.raises(FileNotFoundError):
        cache.ingest_aggregate(tmp_path / "absent", NAME, RUN_ID)


def test_ingest_can_refuse_to_overwrite(cache: ArtifactCache, partition: Path) -> None:
    """``overwrite=False`` protects an existing container."""
    cache.ingest_aggregate(partition, NAME, RUN_ID)

    with pytest.raises(ArtifactExistsError):
        cache.ingest_aggregate(partition, NAME, RUN_ID, overwrite=False)


def test_reingest_replaces_the_container(
    cache: ArtifactCache, partition: Path, tmp_path: Path
) -> None:
    """Committing over an existing container leaves no trace of the old one.

    ``os.replace`` cannot overwrite a non-empty directory, so this exercises
    the rename-aside path.
    """
    cache.ingest_aggregate(partition, NAME, RUN_ID)

    smaller = tmp_path / "smaller"
    smaller.mkdir()
    (smaller / "only.nc").write_bytes(b"one")
    location = cache.ingest_aggregate(smaller, NAME, RUN_ID)

    assert sorted(p.name for p in location.path.glob("*.nc")) == ["only.nc"]
    assert not list(location.path.parent.glob("*.old"))
    assert not list(location.path.parent.glob("*.tmp"))


def test_failed_ingest_leaves_nothing_behind(
    cache: ArtifactCache, partition: Path
) -> None:
    """A container is built aside and only swapped in once verified."""
    with pytest.raises(FileNotFoundError):
        cache.ingest_aggregate(partition, NAME, RUN_ID, members=["absent.nc"])

    assert cache.resolve(NAME, RUN_ID) is None
    assert not list((cache.user_root / RUN_ID).glob("*.tmp"))


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------


def test_truncated_container_fails_verification(
    cache: ArtifactCache, partition: Path
) -> None:
    """A job killed partway leaves a directory that looks plausible.

    The declared member count is what makes that detectable; a single file's
    "did you write something non-empty" check has no analogue for a set.
    """
    location = cache.ingest_aggregate(partition, NAME, RUN_ID)
    (location.path / "rank003.nc").unlink()

    assert cache.verify(NAME, RUN_ID) is False


def test_modified_member_fails_verification(
    cache: ArtifactCache, partition: Path
) -> None:
    """Editing one member invalidates the whole set."""
    location = cache.ingest_aggregate(partition, NAME, RUN_ID)
    (location.path / "rank003.nc").write_bytes(b"tampered")

    assert cache.verify(NAME, RUN_ID) is False


def test_intact_container_verifies(cache: ArtifactCache, partition: Path) -> None:
    """An untouched container matches its recorded digest."""
    cache.ingest_aggregate(partition, NAME, RUN_ID)

    assert cache.verify(NAME, RUN_ID) is True


def test_directory_without_a_manifest_is_not_an_artifact(cache: ArtifactCache) -> None:
    """Ordinary subdirectories must not be mistaken for containers."""
    stray = cache.user_root / RUN_ID / "scratch"
    stray.mkdir(parents=True)
    (stray / "note.txt").write_bytes(b"x")

    assert cache.resolve("scratch", RUN_ID) is None
    assert cache.list_user_artifacts(RUN_ID) == []


# ---------------------------------------------------------------------------
# Promotion and expansion
# ---------------------------------------------------------------------------


def test_promotion_packs_one_archive(cache: ArtifactCache, partition: Path) -> None:
    """The shared tier holds a single file, so it keeps every file guarantee."""
    cache.ingest_aggregate(partition, NAME, RUN_ID)

    shared = cache.promote(NAME, RUN_ID)

    assert shared.path.is_file()
    assert tarfile.is_tarfile(shared.path)
    assert cache.read_shared_record(NAME) is not None


def test_archives_of_identical_members_match(
    cache: ArtifactCache, partition: Path, tmp_path: Path
) -> None:
    """Normalised entry metadata makes the archive a content identity.

    Without it, two runs producing byte-identical members would still produce
    different archives, because tar records each member's mtime.
    """
    cache.ingest_aggregate(partition, NAME, RUN_ID)
    first = tmp_path / "first.tar"
    cache._pack(cache.locate(NAME, Tier.USER, RUN_ID).path, first)

    second = tmp_path / "second.tar"
    cache._pack(cache.locate(NAME, Tier.USER, RUN_ID).path, second)

    assert first.read_bytes() == second.read_bytes()


def test_repromoting_an_identical_set_is_a_no_op(
    cache: ArtifactCache, partition: Path
) -> None:
    """Two runs producing the same set must not be treated as a conflict."""
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.ingest_aggregate(partition, NAME, "run-B")
    cache.promote(NAME, "run-A")

    assert cache.promote(NAME, "run-B").path == cache.locate(NAME, Tier.SHARED).path
    record = cache.read_shared_record(NAME)
    assert record is not None
    assert record.promoted_from_run_id == "run-A"


def test_repromoting_a_different_set_conflicts(
    cache: ArtifactCache, partition: Path, tmp_path: Path
) -> None:
    """Divergent members are a genuine conflict, not tar framing noise."""
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")

    other = tmp_path / "other"
    other.mkdir()
    (other / "rank000.nc").write_bytes(b"different")
    cache.ingest_aggregate(other, NAME, "run-B")

    with pytest.raises(ArtifactExistsError):
        cache.promote(NAME, "run-B")


def test_materialize_expands_the_shared_archive(
    cache: ArtifactCache, partition: Path
) -> None:
    """A consuming run gets a directory it can glob."""
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")

    got = cache.materialize(NAME, "run-B")

    assert got is not None
    assert got.is_container
    assert len(sorted(got.path.glob("*.nc"))) == 8
    assert (got.path / "checkpoint-0042" / "rank000.nc").read_bytes() == b"nested"


def test_materialize_is_idempotent(cache: ArtifactCache, partition: Path) -> None:
    """A container already present is returned untouched."""
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")

    first = cache.materialize(NAME, "run-B")
    second = cache.materialize(NAME, "run-B")

    assert first is not None and second is not None
    assert first.path == second.path


def test_materialize_returns_none_when_absent(cache: ArtifactCache) -> None:
    """Nothing to expand is not an error."""
    assert cache.materialize("never.set", RUN_ID) is None


def test_resolve_does_not_expand(cache: ArtifactCache, partition: Path) -> None:
    """``resolve`` runs on every lookup and must not acquire the right to write."""
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")

    found = cache.resolve(NAME, "run-B")

    assert found is not None
    assert found.tier is Tier.SHARED
    assert not cache.locate(NAME, Tier.USER, "run-B").exists


def test_expansion_is_recorded_in_the_expanding_run(
    cache: ArtifactCache, partition: Path
) -> None:
    """An expanded container is a user-tier artifact and carries a record.

    Without one it is invisible to ``read_manifest`` and left behind by
    ``delete_user``, which prunes the run manifest.
    """
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")
    cache.materialize(NAME, "run-B")

    shared = cache.read_shared_record(NAME)
    record = cache.read_manifest("run-B").artifacts[NAME]
    assert shared is not None
    assert record.kind is ArtifactKind.SET
    assert record.checksum == shared.checksum
    assert record.source == str(cache.locate(NAME, Tier.SHARED).path)


def test_expansion_record_survives_repeat_materialize(
    cache: ArtifactCache, partition: Path
) -> None:
    """Materializing twice neither duplicates nor rewrites the record."""
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")
    cache.materialize(NAME, "run-B")
    first = cache.read_manifest("run-B").artifacts[NAME]
    cache.materialize(NAME, "run-B")

    assert cache.read_manifest("run-B").artifacts[NAME] == first


def test_deleting_an_expansion_prunes_its_record(
    cache: ArtifactCache, partition: Path
) -> None:
    """A run may drop an expanded set without losing the run's other entries."""
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")
    cache.materialize(NAME, "run-B")

    assert cache.delete_user(NAME, "run-B") is True
    assert NAME not in cache.read_manifest("run-B").artifacts
    assert not cache.locate(NAME, Tier.USER, "run-B").exists


def test_expanded_set_verifies_against_the_shared_record(
    cache: ArtifactCache, partition: Path
) -> None:
    """A round trip through the archive preserves identity."""
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")
    cache.materialize(NAME, "run-B")

    assert cache.verify(NAME, "run-B", prefer_local=True) is True


def test_shared_archive_verifies(cache: ArtifactCache, partition: Path) -> None:
    """A shared set is checked against the manifest it carries."""
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")

    assert cache.verify(NAME) is True


# ---------------------------------------------------------------------------
# Node-local expansion tier
# ---------------------------------------------------------------------------


def test_node_tier_receives_the_expansion(tmp_path: Path, partition: Path) -> None:
    """Several runs on one node share a single expanded copy."""
    cache = ArtifactCache(
        tmp_path / "user",
        tmp_path / "shared",
        fingerprinter=FullFingerprinter(),
        node_cache_root=tmp_path / "node",
    )
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")

    got = cache.materialize(NAME, "run-B")

    assert got is not None
    assert (tmp_path / "node") in got.path.parents
    again = cache.materialize(NAME, "run-C")
    assert again is not None and again.path == got.path


def test_node_tier_containers_are_read_only(tmp_path: Path, partition: Path) -> None:
    """A run writing into a shared copy would corrupt its neighbours."""
    cache = ArtifactCache(
        tmp_path / "user",
        tmp_path / "shared",
        fingerprinter=FullFingerprinter(),
        node_cache_root=tmp_path / "node",
    )
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.promote(NAME, "run-A")

    got = cache.materialize(NAME, "run-B")

    assert got is not None
    assert not (got.path / "rank000.nc").stat().st_mode & 0o222


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------


def test_delete_user_removes_a_container(cache: ArtifactCache, partition: Path) -> None:
    """Retaining only the newest of a series must not require deleting the run."""
    cache.ingest_aggregate(partition, NAME, RUN_ID)

    assert cache.delete_user(NAME, RUN_ID) is True
    assert cache.resolve(NAME, RUN_ID) is None
    assert RUN_ID in cache.list_runs()


def test_delete_user_removes_a_plain_file(cache: ArtifactCache) -> None:
    """The same call serves single-file artifacts."""
    with cache.stage("single.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")

    assert cache.delete_user("single.nc", RUN_ID) is True
    assert cache.resolve("single.nc", RUN_ID) is None


def test_delete_user_prunes_the_record(cache: ArtifactCache, partition: Path) -> None:
    """A deleted artifact must not linger in the run manifest."""
    cache.ingest_aggregate(partition, NAME, RUN_ID)
    cache.delete_user(NAME, RUN_ID)

    assert NAME not in cache.read_manifest(RUN_ID).artifacts


def test_delete_user_keeps_siblings(cache: ArtifactCache, partition: Path) -> None:
    """Only the named artifact goes."""
    cache.ingest_aggregate(partition, "first.set", RUN_ID)
    cache.ingest_aggregate(partition, "second.set", RUN_ID)

    cache.delete_user("first.set", RUN_ID)

    assert [loc.name for loc in cache.list_user_artifacts(RUN_ID)] == ["second.set"]


def test_delete_user_tolerates_absence(cache: ArtifactCache) -> None:
    """Deleting what is not there is a no-op by default."""
    assert cache.delete_user("never.set", RUN_ID) is False


def test_delete_user_can_require_presence(cache: ArtifactCache) -> None:
    """``missing_ok=False`` turns absence into an error."""
    with pytest.raises(ArtifactNotFoundError):
        cache.delete_user("never.set", RUN_ID, missing_ok=False)


def test_delete_user_refuses_a_symlink(cache: ArtifactCache, tmp_path: Path) -> None:
    """A recursive delete could otherwise follow a link out of managed storage."""
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "precious.nc").write_bytes(b"keep")
    link = cache.locate("linked.set", Tier.USER, RUN_ID)
    link.path.parent.mkdir(parents=True, exist_ok=True)
    link.path.symlink_to(outside)

    with pytest.raises(UnsafePathError, match="symlink"):
        cache.delete_user("linked.set", RUN_ID)
    assert (outside / "precious.nc").exists()


# ---------------------------------------------------------------------------
# Manifest model
# ---------------------------------------------------------------------------


def test_manifest_digest_is_order_independent() -> None:
    """Members are sorted, so discovery order cannot change identity."""
    from cstar.orchestration.artifact_cache import SetMember

    forward = SetManifest.build(
        [
            SetMember(path="a.nc", size_bytes=1, checksum="x"),
            SetMember(path="b.nc", size_bytes=2, checksum="y"),
        ],
        None,
    )
    reverse = SetManifest.build(
        [
            SetMember(path="b.nc", size_bytes=2, checksum="y"),
            SetMember(path="a.nc", size_bytes=1, checksum="x"),
        ],
        None,
    )

    assert forward.manifest_digest == reverse.manifest_digest


def test_manifest_digest_tracks_members() -> None:
    """A different member set is a different identity."""
    from cstar.orchestration.artifact_cache import SetMember

    one = SetManifest.build([SetMember(path="a.nc", size_bytes=1, checksum="x")], None)
    two = SetManifest.build([SetMember(path="a.nc", size_bytes=1, checksum="z")], None)

    assert one.manifest_digest != two.manifest_digest


def test_set_without_fingerprints_cannot_prove_sameness(
    tmp_path: Path, partition: Path
) -> None:
    """With no member digests there is no evidence two sets match.

    Consistent with single files: absent a digest the cache reports a conflict
    rather than assuming equality.
    """
    cache = ArtifactCache(
        tmp_path / "user", tmp_path / "shared", fingerprinter=NullFingerprinter()
    )
    cache.ingest_aggregate(partition, NAME, "run-A")
    cache.ingest_aggregate(partition, NAME, "run-B")
    cache.promote(NAME, "run-A")

    with pytest.raises(ArtifactExistsError):
        cache.promote(NAME, "run-B")
    assert cache.promote(NAME, "run-B", on_conflict=OnConflict.SKIP) is not None
