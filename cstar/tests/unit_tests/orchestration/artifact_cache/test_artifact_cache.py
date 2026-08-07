"""Unit tests for :class:`cstar.orchestration.artifact_cache.ArtifactCache`."""

import json
import os
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from cstar.orchestration.artifact_cache import (
    MANIFEST_NAME,
    ArtifactCache,
    ArtifactCacheError,
    ArtifactExistsError,
    ArtifactNotFoundError,
    Tier,
    UnsafePathError,
)
from cstar.tests.unit_tests.orchestration.artifact_cache.conftest import RUN_ID

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_roots_are_created_by_default(user_root: Path, shared_root: Path) -> None:
    """Both tier roots exist after construction."""
    ArtifactCache(user_root, shared_root)
    assert user_root.is_dir()
    assert shared_root.is_dir()


def test_roots_can_be_left_uncreated(user_root: Path, shared_root: Path) -> None:
    """Construction is side-effect free when ``create_roots`` is disabled."""
    ArtifactCache(user_root, shared_root, create_roots=False)
    assert not user_root.exists()
    assert not shared_root.exists()


def test_user_paths_are_expanded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``~``-relative root is expanded against the user's home directory."""
    monkeypatch.setenv("HOME", str(tmp_path))
    cache = ArtifactCache("~/cache", tmp_path / "shared")
    assert cache.user_root == (tmp_path / "cache").resolve()


def test_identical_roots_rejected(user_root: Path) -> None:
    """Collapsing both tiers onto one directory is a configuration error."""
    with pytest.raises(ValueError, match="must differ"):
        ArtifactCache(user_root, user_root)


def test_view_root_is_optional(user_root: Path, shared_root: Path) -> None:
    """Views are opt-in; the cache is usable without one."""
    assert ArtifactCache(user_root, shared_root).view_root is None


def test_repr_names_both_roots(cache: ArtifactCache) -> None:
    """The representation is useful when debugging misconfigured roots."""
    text = repr(cache)
    assert "ArtifactCache" in text
    assert str(cache.user_root) in text
    assert str(cache.shared_root) in text


# ---------------------------------------------------------------------------
# Location
# ---------------------------------------------------------------------------


def test_root_for_maps_tiers(cache: ArtifactCache) -> None:
    """Each tier resolves to its configured root."""
    assert cache.root_for(Tier.USER) == cache.user_root
    assert cache.root_for(Tier.SHARED) == cache.shared_root


def test_locate_builds_run_scoped_path(cache: ArtifactCache) -> None:
    """Artifacts live at ``<root>/<run_id>/<name>``."""
    location = cache.locate("foo.nc", RUN_ID, Tier.USER)
    assert location.path == cache.user_root / RUN_ID / "foo.nc"
    assert location.tier is Tier.USER
    assert location.name == "foo.nc"
    assert location.run_id == RUN_ID


def test_locate_does_not_touch_the_filesystem(cache: ArtifactCache) -> None:
    """Computing a location never creates directories."""
    location = cache.locate("foo.nc", RUN_ID, Tier.SHARED)
    assert not location.path.parent.exists()
    assert location.exists is False


def test_uri_matches_located_path(cache: ArtifactCache) -> None:
    """The asset URI is derived from the same path used for writes."""
    location = cache.locate("foo.nc", RUN_ID, Tier.SHARED)
    assert location.uri == location.path.as_uri()


@pytest.mark.parametrize("bad", ["../escape", "a/b", "", ".", "..", os.sep])
def test_locate_rejects_traversal_in_run_id(cache: ArtifactCache, bad: str) -> None:
    """Run identifiers cannot escape the managed root."""
    with pytest.raises(UnsafePathError):
        cache.locate("foo.nc", bad, Tier.USER)


@pytest.mark.parametrize("bad", ["../escape", "a/b", "", ".", ".."])
def test_locate_rejects_traversal_in_name(cache: ArtifactCache, bad: str) -> None:
    """Artifact names cannot escape the managed root."""
    with pytest.raises(UnsafePathError):
        cache.locate(bad, RUN_ID, Tier.USER)


def test_candidates_are_shared_then_user(cache: ArtifactCache) -> None:
    """Candidate order encodes the default resolution precedence."""
    shared, user = cache.candidates("foo.nc", RUN_ID)
    assert shared.tier is Tier.SHARED
    assert user.tier is Tier.USER


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def test_resolve_returns_none_when_absent(cache: ArtifactCache) -> None:
    """A miss is reported as ``None`` rather than raising."""
    assert cache.resolve("nope.nc", RUN_ID) is None


def test_resolve_finds_user_tier(cache: ArtifactCache, staged_artifact: str) -> None:
    """An unpromoted artifact resolves to the user tier."""
    resolved = cache.resolve(staged_artifact, RUN_ID)
    assert resolved is not None
    assert resolved.tier is Tier.USER


def test_resolve_prefers_shared(cache: ArtifactCache, staged_artifact: str) -> None:
    """Once promoted, the durable copy wins."""
    cache.promote(staged_artifact, RUN_ID)
    resolved = cache.resolve(staged_artifact, RUN_ID)
    assert resolved is not None
    assert resolved.tier is Tier.SHARED


def test_prefer_local_reverses_precedence(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """The escape hatch lets a user work against their own copy."""
    cache.promote(staged_artifact, RUN_ID)
    resolved = cache.resolve(staged_artifact, RUN_ID, prefer_local=True)
    assert resolved is not None
    assert resolved.tier is Tier.USER


def test_resolve_falls_back_when_local_deleted(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Promoted data survives the user deleting their scratch copy."""
    cache.promote(staged_artifact, RUN_ID)
    cache.locate(staged_artifact, RUN_ID, Tier.USER).path.unlink()
    resolved = cache.resolve(staged_artifact, RUN_ID, prefer_local=True)
    assert resolved is not None
    assert resolved.tier is Tier.SHARED


def test_resolve_is_never_memoized(cache: ArtifactCache, staged_artifact: str) -> None:
    """Deleting a file between calls turns a hit into a miss."""
    assert cache.resolve(staged_artifact, RUN_ID) is not None
    cache.locate(staged_artifact, RUN_ID, Tier.USER).path.unlink()
    assert cache.resolve(staged_artifact, RUN_ID) is None


def test_require_returns_location(cache: ArtifactCache, staged_artifact: str) -> None:
    """``require`` is ``resolve`` with a failure mode."""
    assert cache.require(staged_artifact, RUN_ID).name == staged_artifact


def test_require_raises_when_absent(cache: ArtifactCache) -> None:
    """A missing artifact raises a typed error naming the run."""
    with pytest.raises(ArtifactNotFoundError, match=RUN_ID):
        cache.require("nope.nc", RUN_ID)


# ---------------------------------------------------------------------------
# Staging
# ---------------------------------------------------------------------------


def test_stage_commits_atomically(cache: ArtifactCache) -> None:
    """Content written to the staged path lands at the canonical path."""
    with cache.stage("foo.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
        assert not cache.locate("foo.nc", RUN_ID, Tier.USER).exists
    assert cache.locate("foo.nc", RUN_ID, Tier.USER).path.read_bytes() == b"payload"


def test_stage_records_manifest_entry(cache: ArtifactCache) -> None:
    """A commit writes a manifest record describing the artifact."""
    with cache.stage("foo.nc", RUN_ID, source="raw.nc", metadata={"k": "v"}) as tmp:
        tmp.write_bytes(b"payload")
    record = cache.read_manifest(RUN_ID, Tier.USER).artifacts["foo.nc"]
    assert record.size_bytes == len(b"payload")
    assert record.source == "raw.nc"
    assert record.metadata == {"k": "v"}
    assert record.asset_uri == cache.locate("foo.nc", RUN_ID, Tier.USER).uri
    assert record.checksum is None


def test_stage_can_checksum(cache: ArtifactCache) -> None:
    """Checksumming is opt-in and produces a SHA-256 digest."""
    with cache.stage("foo.nc", RUN_ID, compute_checksum=True) as tmp:
        tmp.write_bytes(b"payload")
    checksum = cache.read_manifest(RUN_ID, Tier.USER).artifacts["foo.nc"].checksum
    assert checksum is not None
    assert len(checksum) == 64


def test_stage_accepts_explicit_asset_uri(cache: ArtifactCache) -> None:
    """A caller-supplied asset key overrides the derived default."""
    with cache.stage("foo.nc", RUN_ID, asset_uri="s3://bucket/foo.nc") as tmp:
        tmp.write_bytes(b"payload")
    record = cache.read_manifest(RUN_ID, Tier.USER).artifacts["foo.nc"]
    assert record.asset_uri == "s3://bucket/foo.nc"


def test_stage_cleans_up_after_exception(cache: ArtifactCache) -> None:
    """A failed body leaves neither an artifact nor a temporary file behind."""
    with (
        pytest.raises(RuntimeError, match="simulated"),
        cache.stage("foo.nc", RUN_ID) as tmp,
    ):
        tmp.write_bytes(b"partial")
        raise RuntimeError("simulated failure")
    assert cache.resolve("foo.nc", RUN_ID) is None
    assert not any(
        p.name.endswith(".tmp") for p in (cache.user_root / RUN_ID).iterdir()
    )


def test_stage_rejects_body_that_never_writes(cache: ArtifactCache) -> None:
    """Forgetting to write is an error, not a silent empty commit."""
    with (
        pytest.raises(ArtifactCacheError, match="never written"),
        cache.stage("foo.nc", RUN_ID),
    ):
        pass
    assert cache.resolve("foo.nc", RUN_ID) is None


def test_stage_rejects_empty_file(cache: ArtifactCache) -> None:
    """A zero-byte artifact is treated as a failed write."""
    with (
        pytest.raises(ArtifactCacheError, match="empty"),
        cache.stage("foo.nc", RUN_ID) as tmp,
    ):
        tmp.write_bytes(b"")
    assert cache.resolve("foo.nc", RUN_ID) is None


def test_stage_overwrites_by_default(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Re-staging replaces the previous content."""
    with cache.stage(staged_artifact, RUN_ID) as tmp:
        tmp.write_bytes(b"replacement")
    path = cache.locate(staged_artifact, RUN_ID, Tier.USER).path
    assert path.read_bytes() == b"replacement"


def test_stage_can_refuse_to_overwrite(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """``overwrite=False`` protects an existing artifact."""
    with (
        pytest.raises(ArtifactExistsError),
        cache.stage(staged_artifact, RUN_ID, overwrite=False),
    ):
        pass


def test_stage_can_target_shared_tier(cache: ArtifactCache) -> None:
    """Writing directly to the shared tier is possible though rarely correct."""
    with cache.stage("foo.nc", RUN_ID, tier=Tier.SHARED) as tmp:
        tmp.write_bytes(b"payload")
    assert cache.locate("foo.nc", RUN_ID, Tier.SHARED).exists


def test_concurrent_stages_do_not_lose_records(cache: ArtifactCache) -> None:
    """Manifest updates from parallel writers are serialised by the lock."""

    def write(index: int) -> None:
        with cache.stage(f"file-{index}.nc", RUN_ID) as tmp:
            tmp.write_bytes(b"payload")

    threads = [threading.Thread(target=write, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(cache.read_manifest(RUN_ID, Tier.USER).artifacts) == 8


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


def test_ingest_copies_into_user_tier(cache: ArtifactCache, tmp_path: Path) -> None:
    """An externally produced file is copied into the cache."""
    source = tmp_path / "transient.nc"
    source.write_bytes(b"external")
    location = cache.ingest(source, "foo.nc", RUN_ID)
    assert location.tier is Tier.USER
    assert location.path.read_bytes() == b"external"
    assert source.exists()


def test_ingest_records_provenance(cache: ArtifactCache, tmp_path: Path) -> None:
    """The original location is retained in the manifest."""
    source = tmp_path / "transient.nc"
    source.write_bytes(b"external")
    cache.ingest(source, "foo.nc", RUN_ID, metadata={"origin": "model"})
    record = cache.read_manifest(RUN_ID, Tier.USER).artifacts["foo.nc"]
    assert record.source == str(source)
    assert record.metadata == {"origin": "model"}


def test_ingest_can_move(cache: ArtifactCache, tmp_path: Path) -> None:
    """``move=True`` removes the source after a successful copy."""
    source = tmp_path / "transient.nc"
    source.write_bytes(b"external")
    cache.ingest(source, "foo.nc", RUN_ID, move=True)
    assert not source.exists()
    assert cache.locate("foo.nc", RUN_ID, Tier.USER).exists


def test_ingest_can_checksum(cache: ArtifactCache, tmp_path: Path) -> None:
    """Checksumming is available on the ingestion path too."""
    source = tmp_path / "transient.nc"
    source.write_bytes(b"external")
    cache.ingest(source, "foo.nc", RUN_ID, compute_checksum=True)
    assert cache.read_manifest(RUN_ID, Tier.USER).artifacts["foo.nc"].checksum


def test_ingest_rejects_missing_source(cache: ArtifactCache, tmp_path: Path) -> None:
    """A nonexistent source fails before anything is written."""
    with pytest.raises(FileNotFoundError):
        cache.ingest(tmp_path / "absent.nc", "foo.nc", RUN_ID)


def test_ingest_rejects_directory_source(cache: ArtifactCache, tmp_path: Path) -> None:
    """A directory is not a valid artifact source."""
    directory = tmp_path / "a-directory"
    directory.mkdir()
    with pytest.raises(FileNotFoundError):
        cache.ingest(directory, "foo.nc", RUN_ID)


def test_ingest_respects_overwrite_flag(cache: ArtifactCache, tmp_path: Path) -> None:
    """Ingestion honours the same overwrite protection as staging."""
    source = tmp_path / "transient.nc"
    source.write_bytes(b"external")
    cache.ingest(source, "foo.nc", RUN_ID)
    with pytest.raises(ArtifactExistsError):
        cache.ingest(source, "foo.nc", RUN_ID, overwrite=False)


# ---------------------------------------------------------------------------
# Promotion
# ---------------------------------------------------------------------------


def test_promote_copies_without_moving(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Promotion leaves the user's copy intact."""
    shared = cache.promote(staged_artifact, RUN_ID)
    assert shared.tier is Tier.SHARED
    assert shared.exists
    assert cache.locate(staged_artifact, RUN_ID, Tier.USER).exists


def test_promote_preserves_content(cache: ArtifactCache, staged_artifact: str) -> None:
    """The promoted bytes match the user tier copy."""
    user = cache.locate(staged_artifact, RUN_ID, Tier.USER)
    shared = cache.promote(staged_artifact, RUN_ID)
    assert shared.path.read_bytes() == user.path.read_bytes()


def test_promote_carries_metadata(cache: ArtifactCache, staged_artifact: str) -> None:
    """Descriptive metadata follows the artifact into the shared tier."""
    cache.promote(staged_artifact, RUN_ID)
    record = cache.read_manifest(RUN_ID, Tier.SHARED).artifacts[staged_artifact]
    assert record.metadata == {"vars": ["x"]}
    assert record.asset_uri == cache.locate(staged_artifact, RUN_ID, Tier.SHARED).uri


def test_promote_stamps_timestamp(cache: ArtifactCache, staged_artifact: str) -> None:
    """The shared manifest records when the run was promoted."""
    cache.promote(staged_artifact, RUN_ID)
    assert cache.read_manifest(RUN_ID, Tier.SHARED).promoted_at is not None


def test_promote_requires_user_copy(cache: ArtifactCache) -> None:
    """Promotion of an artifact that was never created fails clearly."""
    with pytest.raises(ArtifactNotFoundError, match="cannot promote"):
        cache.promote("absent.nc", RUN_ID)


def test_promote_treats_shared_as_immutable(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Re-promotion is refused by default to protect published data."""
    cache.promote(staged_artifact, RUN_ID)
    with pytest.raises(ArtifactExistsError, match="overwrite=True"):
        cache.promote(staged_artifact, RUN_ID)


def test_promote_can_overwrite_explicitly(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """An explicit flag allows republishing."""
    cache.promote(staged_artifact, RUN_ID)
    with cache.stage(staged_artifact, RUN_ID) as tmp:
        tmp.write_bytes(b"revised")
    shared = cache.promote(staged_artifact, RUN_ID, overwrite=True)
    assert shared.path.read_bytes() == b"revised"


def test_promote_without_prior_manifest(cache: ArtifactCache) -> None:
    """An artifact placed on disk out of band can still be promoted."""
    location = cache.locate("orphan.nc", RUN_ID, Tier.USER)
    location.path.parent.mkdir(parents=True)
    location.path.write_bytes(b"orphan")
    assert cache.promote("orphan.nc", RUN_ID).exists


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------


def test_manifest_path_is_run_scoped(cache: ArtifactCache) -> None:
    """The sidecar sits inside the run directory it describes."""
    path = cache.manifest_path(RUN_ID, Tier.USER)
    assert path == cache.user_root / RUN_ID / MANIFEST_NAME


def test_manifest_path_validates_run_id(cache: ArtifactCache) -> None:
    """Manifest lookups are guarded against traversal too."""
    with pytest.raises(UnsafePathError):
        cache.manifest_path("../escape", Tier.USER)


def test_read_manifest_returns_empty_when_absent(cache: ArtifactCache) -> None:
    """A run with no manifest reads as empty rather than raising."""
    manifest = cache.read_manifest("never-run", Tier.USER)
    assert manifest.artifacts == {}
    assert manifest.run_id == "never-run"


def test_read_manifest_survives_corruption(cache: ArtifactCache) -> None:
    """A truncated manifest degrades to empty instead of breaking listing."""
    path = cache.manifest_path(RUN_ID, Tier.USER)
    path.parent.mkdir(parents=True)
    path.write_text("{not valid json")
    assert cache.read_manifest(RUN_ID, Tier.USER).artifacts == {}


def test_write_manifest_is_atomic_and_readable(cache: ArtifactCache) -> None:
    """A written manifest round-trips and leaves no temporary file."""
    manifest = cache.read_manifest(RUN_ID, Tier.USER)
    path = cache.write_manifest(manifest)
    assert json.loads(path.read_text())["run_id"] == RUN_ID
    assert not any(p.name.endswith(".tmp") for p in path.parent.iterdir())


# ---------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------


def test_list_runs_reads_from_disk(cache: ArtifactCache, staged_artifact: str) -> None:
    """Run listing derives from directories, not from an index."""
    assert cache.list_runs(Tier.USER) == [RUN_ID]
    assert cache.list_runs(Tier.SHARED) == []


def test_list_runs_handles_absent_root(user_root: Path, shared_root: Path) -> None:
    """An uncreated root lists as empty."""
    cache = ArtifactCache(user_root, shared_root, create_roots=False)
    assert cache.list_runs(Tier.USER) == []


def test_list_artifacts_excludes_bookkeeping(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """The manifest, its lock, and dotfiles are not artifacts."""
    (cache.user_root / RUN_ID / ".hidden").write_bytes(b"x")
    names = [location.name for location in cache.list_artifacts(RUN_ID, Tier.USER)]
    assert names == [staged_artifact]


def test_list_artifacts_excludes_in_flight_temporaries(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """A concurrent writer's staging file is not reported as an artifact."""
    (cache.user_root / RUN_ID / "other.nc.999.tmp").write_bytes(b"partial")
    names = [location.name for location in cache.list_artifacts(RUN_ID, Tier.USER)]
    assert names == [staged_artifact]


def test_list_artifacts_handles_absent_run(cache: ArtifactCache) -> None:
    """An unknown run lists as empty."""
    assert cache.list_artifacts("never-run", Tier.USER) == []


def test_describe_reconciles_with_disk(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Records for purged files are dropped from the description."""
    assert set(cache.describe(RUN_ID, Tier.USER)) == {staged_artifact}
    cache.locate(staged_artifact, RUN_ID, Tier.USER).path.unlink()
    assert cache.describe(RUN_ID, Tier.USER) == {}


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def test_refresh_view_links_artifacts(
    cache: ArtifactCache, staged_artifact: str, view_root: Path
) -> None:
    """The view directory contains a symlink per resolvable artifact."""
    linked = cache.refresh_view(RUN_ID)
    link = view_root / RUN_ID / staged_artifact
    assert link.is_symlink()
    assert link.resolve() == linked[staged_artifact].resolve()


def test_refresh_view_prefers_shared_target(
    cache: ArtifactCache, staged_artifact: str, view_root: Path
) -> None:
    """Links point at the durable copy so they survive a scratch purge."""
    cache.promote(staged_artifact, RUN_ID)
    linked = cache.refresh_view(RUN_ID)
    assert cache.shared_root in linked[staged_artifact].parents


def test_refresh_view_honours_prefer_local(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """The precedence override applies to view construction too."""
    cache.promote(staged_artifact, RUN_ID)
    linked = cache.refresh_view(RUN_ID, prefer_local=True)
    assert cache.user_root in linked[staged_artifact].parents


def test_refresh_view_self_heals_after_deletion(
    cache: ArtifactCache, staged_artifact: str, view_root: Path
) -> None:
    """Rebuilding drops links whose targets no longer exist."""
    cache.refresh_view(RUN_ID)
    cache.locate(staged_artifact, RUN_ID, Tier.USER).path.unlink()
    assert cache.refresh_view(RUN_ID) == {}
    assert list((view_root / RUN_ID).iterdir()) == []


def test_refresh_view_repoints_after_promotion(
    cache: ArtifactCache, staged_artifact: str, view_root: Path
) -> None:
    """A stale link into scratch is repointed at the shared copy."""
    cache.refresh_view(RUN_ID)
    cache.promote(staged_artifact, RUN_ID)
    cache.locate(staged_artifact, RUN_ID, Tier.USER).path.unlink()
    cache.refresh_view(RUN_ID)
    link = view_root / RUN_ID / staged_artifact
    assert link.is_symlink()
    assert link.resolve().is_file()


def test_refresh_view_is_idempotent(cache: ArtifactCache, staged_artifact: str) -> None:
    """Rebuilding twice produces the same result without erroring."""
    assert cache.refresh_view(RUN_ID) == cache.refresh_view(RUN_ID)


def test_refresh_view_unions_both_tiers(cache: ArtifactCache) -> None:
    """Artifacts present in only one tier still appear in the view."""
    with cache.stage("user-only.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"u")
    with cache.stage("shared-only.nc", RUN_ID, tier=Tier.SHARED) as tmp:
        tmp.write_bytes(b"s")
    assert set(cache.refresh_view(RUN_ID)) == {"user-only.nc", "shared-only.nc"}


def test_refresh_view_accepts_explicit_directory(
    cache: ArtifactCache, staged_artifact: str, tmp_path: Path
) -> None:
    """A caller-chosen destination overrides the configured view root."""
    destination = tmp_path / "elsewhere"
    cache.refresh_view(RUN_ID, view_dir=destination)
    assert (destination / staged_artifact).is_symlink()


def test_refresh_view_preserves_non_symlinks(
    cache: ArtifactCache, staged_artifact: str, view_root: Path
) -> None:
    """A user's own notes in the view directory are not deleted."""
    directory = view_root / RUN_ID
    directory.mkdir(parents=True)
    (directory / "NOTES.txt").write_text("mine")
    cache.refresh_view(RUN_ID)
    assert (directory / "NOTES.txt").read_text() == "mine"


def test_refresh_view_requires_a_destination(
    user_root: Path, shared_root: Path
) -> None:
    """Building a view without any configured root is a usage error."""
    cache = ArtifactCache(user_root, shared_root)
    with pytest.raises(ValueError, match="view_root"):
        cache.refresh_view(RUN_ID)


def test_refresh_view_validates_run_id(cache: ArtifactCache) -> None:
    """View construction is guarded against traversal."""
    with pytest.raises(UnsafePathError):
        cache.refresh_view("../escape")


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------


def test_delete_user_run_removes_directory(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Deleting a user run clears its directory."""
    assert cache.delete_user_run(RUN_ID) is True
    assert cache.list_runs(Tier.USER) == []


def test_delete_user_run_leaves_shared_intact(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Clearing scratch does not touch published data."""
    cache.promote(staged_artifact, RUN_ID)
    cache.delete_user_run(RUN_ID)
    assert cache.resolve(staged_artifact, RUN_ID) is not None


def test_delete_user_run_tolerates_absence(cache: ArtifactCache) -> None:
    """Deleting an unknown run is a no-op by default."""
    assert cache.delete_user_run("never-run") is False


def test_delete_user_run_can_require_presence(cache: ArtifactCache) -> None:
    """``missing_ok=False`` turns absence into an error."""
    with pytest.raises(ArtifactNotFoundError):
        cache.delete_user_run("never-run", missing_ok=False)


def test_delete_shared_run_requires_confirmation(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Shared deletion is guarded because other users may depend on it."""
    cache.promote(staged_artifact, RUN_ID)
    with pytest.raises(PermissionError, match="confirm=True"):
        cache.delete_shared_run(RUN_ID)
    assert cache.list_runs(Tier.SHARED) == [RUN_ID]


def test_delete_shared_run_with_confirmation(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """An explicit confirmation removes the shared run."""
    cache.promote(staged_artifact, RUN_ID)
    assert cache.delete_shared_run(RUN_ID, confirm=True) is True
    assert cache.list_runs(Tier.SHARED) == []


def test_delete_shared_run_tolerates_absence(cache: ArtifactCache) -> None:
    """Confirmed deletion of an unknown run is a no-op."""
    assert cache.delete_shared_run("never-run", confirm=True) is False


def test_delete_refuses_symlinked_run_directory(cache: ArtifactCache) -> None:
    """A symlinked run directory could let a recursive delete escape."""
    outside = cache.user_root.parent / "outside"
    outside.mkdir()
    (outside / "precious.nc").write_bytes(b"keep")
    (cache.user_root / RUN_ID).symlink_to(outside)
    with pytest.raises(UnsafePathError, match="symlink"):
        cache.delete_user_run(RUN_ID)
    assert (outside / "precious.nc").exists()


def test_delete_refuses_non_directory(cache: ArtifactCache) -> None:
    """A file where a run directory was expected is not deleted."""
    (cache.user_root / RUN_ID).write_bytes(b"not a directory")
    with pytest.raises(UnsafePathError, match="not a directory"):
        cache.delete_user_run(RUN_ID)


def test_delete_validates_run_id(cache: ArtifactCache) -> None:
    """Deletion is guarded against traversal."""
    with pytest.raises(UnsafePathError):
        cache.delete_user_run("../escape")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename",
    [MANIFEST_NAME, "manifest.lock", ".hidden", "foo.nc.123.tmp"],
)
def test_reserved_names_are_not_artifacts(filename: str) -> None:
    """Bookkeeping files are classified as reserved."""
    assert ArtifactCache._is_reserved(filename) is True


def test_regular_names_are_not_reserved() -> None:
    """Ordinary data files are not classified as reserved."""
    assert ArtifactCache._is_reserved("foo.nc") is False


def test_assert_contained_rejects_escape(cache: ArtifactCache, tmp_path: Path) -> None:
    """Containment checking rejects paths outside the managed root."""
    with pytest.raises(UnsafePathError, match="escapes"):
        ArtifactCache._assert_contained(tmp_path / "elsewhere", cache.user_root)


def test_assert_contained_accepts_child(cache: ArtifactCache) -> None:
    """A path beneath the root passes containment checking."""
    ArtifactCache._assert_contained(cache.user_root / "a" / "b.nc", cache.user_root)


def test_sha256_matches_hashlib(tmp_path: Path) -> None:
    """The streaming digest agrees with a one-shot hash."""
    import hashlib

    path = tmp_path / "payload.bin"
    payload = b"x" * (1024 * 1024 + 7)
    path.write_bytes(payload)
    assert ArtifactCache._sha256(path) == hashlib.sha256(payload).hexdigest()


def test_utcnow_is_iso_utc() -> None:
    """Timestamps are timezone-aware ISO-8601 in UTC."""
    from datetime import datetime

    stamp = ArtifactCache._utcnow()
    assert datetime.fromisoformat(stamp).tzinfo is not None
    assert stamp.endswith("+00:00")


def test_username_falls_back_when_unavailable() -> None:
    """An unresolvable username degrades rather than failing the write."""
    with patch("getpass.getuser", side_effect=OSError("no passwd entry")):
        assert ArtifactCache._username() == "unknown"


def test_username_reports_current_user() -> None:
    """The recorded writer identity comes from the operating system."""
    with patch("getpass.getuser", return_value="chris"):
        assert ArtifactCache._username() == "chris"


def test_manifest_lock_is_reentrant_across_sequential_uses(
    cache: ArtifactCache,
) -> None:
    """The lock is released on exit so later writers are not blocked."""
    for _ in range(3):
        with cache._manifest_lock(RUN_ID, Tier.USER):
            pass
    assert cache.manifest_path(RUN_ID, Tier.USER).with_suffix(".lock").exists()
