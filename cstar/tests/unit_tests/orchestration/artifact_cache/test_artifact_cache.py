"""Unit tests for :class:`cstar.orchestration.artifact_cache.ArtifactCache`."""

import json
import os
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from cstar.base.env import ENV_CSTAR_ARTIFACT_CACHE_BYPASS, FLAG_ON
from cstar.orchestration.artifact_cache import (
    MANIFEST_NAME,
    MAX_REFERENCES,
    SHARED_RECORD_DIR,
    ArtifactCache,
    ArtifactCacheError,
    ArtifactExistsError,
    ArtifactNotFoundError,
    Tier,
    UnsafePathError,
)
from cstar.orchestration.fingerprinting import (
    ChecksumMode,
    Fingerprinter,
    FullFingerprinter,
    NullFingerprinter,
    QuickFingerprinter,
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
    location = cache.locate("foo.nc", Tier.USER, RUN_ID)
    assert location.path == cache.user_root / RUN_ID / "foo.nc"
    assert location.tier is Tier.USER
    assert location.name == "foo.nc"
    assert location.run_id == RUN_ID


def test_locate_does_not_touch_the_filesystem(cache: ArtifactCache) -> None:
    """Computing a location never creates anything."""
    location = cache.locate("foo.nc", Tier.USER, RUN_ID)
    assert not location.path.parent.exists()
    assert location.exists is False


def test_uri_matches_located_path(cache: ArtifactCache) -> None:
    """The asset URI is derived from the same path used for writes."""
    location = cache.locate("foo.nc", Tier.SHARED)
    assert location.uri == location.path.as_uri()


@pytest.mark.parametrize("bad", ["../escape", "a/b", "", ".", "..", os.sep])
def test_locate_rejects_traversal_in_run_id(cache: ArtifactCache, bad: str) -> None:
    """Run identifiers cannot escape the managed root."""
    with pytest.raises(UnsafePathError):
        cache.locate("foo.nc", Tier.USER, bad)


@pytest.mark.parametrize("bad", ["../escape", "a/b", "", ".", ".."])
def test_locate_rejects_traversal_in_name(cache: ArtifactCache, bad: str) -> None:
    """Artifact names cannot escape the managed root."""
    with pytest.raises(UnsafePathError):
        cache.locate(bad, Tier.USER, RUN_ID)


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
    cache.locate(staged_artifact, Tier.USER, RUN_ID).path.unlink()
    resolved = cache.resolve(staged_artifact, RUN_ID, prefer_local=True)
    assert resolved is not None
    assert resolved.tier is Tier.SHARED


def test_resolve_is_never_memoized(cache: ArtifactCache, staged_artifact: str) -> None:
    """Deleting a file between calls turns a hit into a miss."""
    assert cache.resolve(staged_artifact, RUN_ID) is not None
    cache.locate(staged_artifact, Tier.USER, RUN_ID).path.unlink()
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
        assert not cache.locate("foo.nc", Tier.USER, RUN_ID).exists
    assert cache.locate("foo.nc", Tier.USER, RUN_ID).path.read_bytes() == b"payload"


def test_stage_records_manifest_entry(cache: ArtifactCache) -> None:
    """A commit writes a manifest record describing the artifact."""
    with cache.stage("foo.nc", RUN_ID, source="raw.nc", metadata={"k": "v"}) as tmp:
        tmp.write_bytes(b"payload")
    record = cache.read_manifest(RUN_ID).artifacts["foo.nc"]
    assert record.size_bytes == len(b"payload")
    assert record.source == "raw.nc"
    assert record.metadata == {"k": "v"}
    assert record.asset_uri == cache.locate("foo.nc", Tier.USER, RUN_ID).uri
    assert record.checksum is None


def test_stage_can_checksum(cache: ArtifactCache) -> None:
    """Full checksumming is opt-in and produces a SHA-256 digest."""
    with cache.stage("foo.nc", RUN_ID, fingerprinter=FullFingerprinter()) as tmp:
        tmp.write_bytes(b"payload")
    record = cache.read_manifest(RUN_ID).artifacts["foo.nc"]
    assert record.checksum is not None
    assert len(record.checksum) == 64
    assert record.checksum_mode is ChecksumMode.FULL


def test_stage_can_quick_checksum(cache: ArtifactCache) -> None:
    """Quick mode records a digest and labels it as such."""
    with cache.stage("foo.nc", RUN_ID, fingerprinter=QuickFingerprinter()) as tmp:
        tmp.write_bytes(b"payload")
    record = cache.read_manifest(RUN_ID).artifacts["foo.nc"]
    assert record.checksum is not None
    assert record.checksum_mode is ChecksumMode.QUICK


def test_stage_quick_and_full_digests_differ(cache: ArtifactCache) -> None:
    """The two strategies are not comparable, hence the recorded mode."""
    with cache.stage("q.nc", RUN_ID, fingerprinter=QuickFingerprinter()) as tmp:
        tmp.write_bytes(b"payload")
    with cache.stage("f.nc", RUN_ID, fingerprinter=FullFingerprinter()) as tmp:
        tmp.write_bytes(b"payload")
    records = cache.read_manifest(RUN_ID).artifacts
    assert records["q.nc"].checksum != records["f.nc"].checksum


def test_stage_records_no_mode_without_checksum(cache: ArtifactCache) -> None:
    """Skipping the digest leaves both checksum fields unset."""
    with cache.stage("foo.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
    record = cache.read_manifest(RUN_ID).artifacts["foo.nc"]
    assert record.checksum is None
    assert record.checksum_mode is None


def test_stage_accepts_explicit_asset_uri(cache: ArtifactCache) -> None:
    """A caller-supplied asset key overrides the derived default."""
    with cache.stage("foo.nc", RUN_ID, asset_uri="s3://bucket/foo.nc") as tmp:
        tmp.write_bytes(b"payload")
    record = cache.read_manifest(RUN_ID).artifacts["foo.nc"]
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
    path = cache.locate(staged_artifact, Tier.USER, RUN_ID).path
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
    with cache.stage("foo.nc", tier=Tier.SHARED) as tmp:
        tmp.write_bytes(b"payload")
    assert cache.locate("foo.nc", Tier.SHARED).exists


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

    assert len(cache.read_manifest(RUN_ID).artifacts) == 8


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
    record = cache.read_manifest(RUN_ID).artifacts["foo.nc"]
    assert record.source == str(source)
    assert record.metadata == {"origin": "model"}


def test_ingest_can_move(cache: ArtifactCache, tmp_path: Path) -> None:
    """``move=True`` removes the source after a successful copy."""
    source = tmp_path / "transient.nc"
    source.write_bytes(b"external")
    cache.ingest(source, "foo.nc", RUN_ID, move=True)
    assert not source.exists()
    assert cache.locate("foo.nc", Tier.USER, RUN_ID).exists


@pytest.mark.parametrize("strategy", [QuickFingerprinter(), FullFingerprinter()])
def test_ingest_can_checksum(
    cache: ArtifactCache, tmp_path: Path, strategy: Fingerprinter
) -> None:
    """Both fingerprinting strategies are available on the ingestion path."""
    source = tmp_path / "transient.nc"
    source.write_bytes(b"external")
    cache.ingest(source, "foo.nc", RUN_ID, fingerprinter=strategy)
    record = cache.read_manifest(RUN_ID).artifacts["foo.nc"]
    assert record.checksum is not None
    assert record.checksum_mode is strategy.mode


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
    assert cache.locate(staged_artifact, Tier.USER, RUN_ID).exists


def test_promote_preserves_content(cache: ArtifactCache, staged_artifact: str) -> None:
    """The promoted bytes match the user tier copy."""
    user = cache.locate(staged_artifact, Tier.USER, RUN_ID)
    shared = cache.promote(staged_artifact, RUN_ID)
    assert shared.path.read_bytes() == user.path.read_bytes()


def test_promote_carries_metadata(cache: ArtifactCache, staged_artifact: str) -> None:
    """Descriptive metadata follows the artifact into the shared tier."""
    cache.promote(staged_artifact, RUN_ID)
    record = cache.read_shared_record(staged_artifact)
    assert record is not None
    assert record.metadata == {"vars": ["x"]}
    assert record.asset_uri == cache.locate(staged_artifact, Tier.SHARED).uri


def test_promote_stamps_timestamp(cache: ArtifactCache, staged_artifact: str) -> None:
    """The shared manifest records when the run was promoted."""
    cache.promote(staged_artifact, RUN_ID)
    record = cache.read_shared_record(staged_artifact)
    assert record is not None
    assert record.promoted_at is not None


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
    location = cache.locate("orphan.nc", Tier.USER, RUN_ID)
    location.path.parent.mkdir(parents=True)
    location.path.write_bytes(b"orphan")
    assert cache.promote("orphan.nc", RUN_ID).exists


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------


def test_manifest_path_is_run_scoped(cache: ArtifactCache) -> None:
    """The sidecar sits inside the run directory it describes."""
    path = cache.manifest_path(RUN_ID)
    assert path == cache.user_root / RUN_ID / MANIFEST_NAME


def test_manifest_path_validates_run_id(cache: ArtifactCache) -> None:
    """Manifest lookups are guarded against traversal too."""
    with pytest.raises(UnsafePathError):
        cache.manifest_path("../escape")


def test_read_manifest_returns_empty_when_absent(cache: ArtifactCache) -> None:
    """A run with no manifest reads as empty rather than raising."""
    manifest = cache.read_manifest("never-run")
    assert manifest.artifacts == {}
    assert manifest.run_id == "never-run"


def test_read_manifest_survives_corruption(cache: ArtifactCache) -> None:
    """A truncated manifest degrades to empty instead of breaking listing."""
    path = cache.manifest_path(RUN_ID)
    path.parent.mkdir(parents=True)
    path.write_text("{not valid json")
    assert cache.read_manifest(RUN_ID).artifacts == {}


def test_write_manifest_is_atomic_and_readable(cache: ArtifactCache) -> None:
    """A written manifest round-trips and leaves no temporary file."""
    manifest = cache.read_manifest(RUN_ID)
    path = cache.write_manifest(manifest)
    assert json.loads(path.read_text())["run_id"] == RUN_ID
    assert not any(p.name.endswith(".tmp") for p in path.parent.iterdir())


# ---------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------


def test_list_runs_reads_from_disk(cache: ArtifactCache, staged_artifact: str) -> None:
    """Run listing derives from directories, not from an index."""
    assert cache.list_runs() == [RUN_ID]
    assert [loc.name for loc in cache.list_shared_artifacts()] == []


def test_list_runs_handles_absent_root(user_root: Path, shared_root: Path) -> None:
    """An uncreated root lists as empty."""
    cache = ArtifactCache(user_root, shared_root, create_roots=False)
    assert cache.list_runs() == []


def test_list_artifacts_excludes_bookkeeping(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """The manifest, its lock, and dotfiles are not artifacts."""
    (cache.user_root / RUN_ID / ".hidden").write_bytes(b"x")
    names = [location.name for location in cache.list_user_artifacts(RUN_ID)]
    assert names == [staged_artifact]


def test_list_artifacts_excludes_in_flight_temporaries(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """A concurrent writer's staging file is not reported as an artifact."""
    (cache.user_root / RUN_ID / "other.nc.999.tmp").write_bytes(b"partial")
    names = [location.name for location in cache.list_user_artifacts(RUN_ID)]
    assert names == [staged_artifact]


def test_list_artifacts_handles_absent_run(cache: ArtifactCache) -> None:
    """An unknown run lists as empty."""
    assert cache.list_user_artifacts("never-run") == []


def test_describe_reconciles_with_disk(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Records for purged files are dropped from the description."""
    assert set(cache.describe(RUN_ID)) == {staged_artifact}
    cache.locate(staged_artifact, Tier.USER, RUN_ID).path.unlink()
    assert cache.describe(RUN_ID) == {}


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
    cache.locate(staged_artifact, Tier.USER, RUN_ID).path.unlink()
    assert cache.refresh_view(RUN_ID) == {}
    assert list((view_root / RUN_ID).iterdir()) == []


def test_refresh_view_repoints_after_promotion(
    cache: ArtifactCache, staged_artifact: str, view_root: Path
) -> None:
    """A stale link into scratch is repointed at the shared copy."""
    cache.refresh_view(RUN_ID)
    cache.promote(staged_artifact, RUN_ID)
    cache.locate(staged_artifact, Tier.USER, RUN_ID).path.unlink()

    linked = cache.refresh_view(RUN_ID, names=[staged_artifact])
    link = view_root / RUN_ID / staged_artifact
    assert link.is_symlink()
    assert link.resolve().is_file()
    assert cache.shared_root in linked[staged_artifact].parents


def test_refresh_view_is_idempotent(cache: ArtifactCache, staged_artifact: str) -> None:
    """Rebuilding twice produces the same result without erroring."""
    assert cache.refresh_view(RUN_ID) == cache.refresh_view(RUN_ID)


def test_refresh_view_covers_the_run_by_default(cache: ArtifactCache) -> None:
    """The flat shared tier is not swept wholesale into every run's view."""
    with cache.stage("user-only.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"u")
    with cache.stage("shared-only.nc", tier=Tier.SHARED) as tmp:
        tmp.write_bytes(b"s")

    assert set(cache.refresh_view(RUN_ID)) == {"user-only.nc"}


def test_refresh_view_includes_named_shared_artifacts(cache: ArtifactCache) -> None:
    """A run opts into the shared artifacts it consumes, by name."""
    with cache.stage("user-only.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"u")
    with cache.stage("shared-only.nc", tier=Tier.SHARED) as tmp:
        tmp.write_bytes(b"s")

    linked = cache.refresh_view(RUN_ID, names=["shared-only.nc"])
    assert set(linked) == {"user-only.nc", "shared-only.nc"}
    assert cache.shared_root in linked["shared-only.nc"].parents


def test_refresh_view_ignores_named_artifacts_that_are_absent(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Naming an artifact that does not exist yields no dangling link."""
    linked = cache.refresh_view(RUN_ID, names=["never-promoted.nc"])
    assert "never-promoted.nc" not in linked


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
    assert cache.list_runs() == []


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


def test_delete_shared_requires_confirmation(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Shared deletion is guarded because other users may depend on it."""
    cache.promote(staged_artifact, RUN_ID)
    with pytest.raises(PermissionError, match="confirm=True"):
        cache.delete_shared(staged_artifact)
    assert [loc.name for loc in cache.list_shared_artifacts()] == [staged_artifact]


def test_delete_shared_with_confirmation(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """An explicit confirmation removes the artifact and its sidecar."""
    cache.promote(staged_artifact, RUN_ID)
    assert cache.delete_shared(staged_artifact, confirm=True) is True
    assert cache.list_shared_artifacts() == []
    assert not cache.shared_record_path(staged_artifact).exists()


def test_delete_shared_tolerates_absence(cache: ArtifactCache) -> None:
    """Confirmed deletion of an unknown artifact is a no-op."""
    assert cache.delete_shared("never-promoted.nc", confirm=True) is False


def test_delete_shared_can_require_presence(cache: ArtifactCache) -> None:
    """``missing_ok=False`` turns absence into an error."""
    with pytest.raises(ArtifactNotFoundError):
        cache.delete_shared("never-promoted.nc", confirm=True, missing_ok=False)


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
        with cache._lock(cache.manifest_path(RUN_ID)):
            pass
    assert cache.manifest_path(RUN_ID).with_name(MANIFEST_NAME + ".lock").exists()


# ---------------------------------------------------------------------------
# Fingerprinter injection
# ---------------------------------------------------------------------------


def test_default_fingerprinter_takes_no_digest(cache: ArtifactCache) -> None:
    """The cache defaults to the strategy that costs a single pass over data."""
    assert isinstance(cache.fingerprinter, NullFingerprinter)


def test_injected_fingerprinter_applies_to_every_write(
    user_root: Path, shared_root: Path
) -> None:
    """A cache-level strategy is used when a write does not supply one."""
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("foo.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
    record = cache.read_manifest(RUN_ID).artifacts["foo.nc"]
    assert record.checksum_mode is ChecksumMode.FULL


def test_per_write_fingerprinter_overrides_the_default(
    user_root: Path, shared_root: Path
) -> None:
    """An individual write may choose a different strategy."""
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("foo.nc", RUN_ID, fingerprinter=QuickFingerprinter()) as tmp:
        tmp.write_bytes(b"payload")
    record = cache.read_manifest(RUN_ID).artifacts["foo.nc"]
    assert record.checksum_mode is ChecksumMode.QUICK


def test_promotion_uses_the_cache_default(user_root: Path, shared_root: Path) -> None:
    """Copying into the shared tier fingerprints with the cache's strategy."""
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("foo.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
    cache.promote("foo.nc", RUN_ID)
    record = cache.read_shared_record("foo.nc")
    assert record is not None
    assert record.checksum_mode is ChecksumMode.FULL


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def test_verify_passes_for_untouched_artifact(
    user_root: Path, shared_root: Path
) -> None:
    """An artifact that has not changed verifies against its record."""
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("foo.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
    assert cache.verify("foo.nc", RUN_ID) is True


def test_verify_fails_after_modification(user_root: Path, shared_root: Path) -> None:
    """Editing an artifact behind the cache's back is detected."""
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("foo.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
    cache.locate("foo.nc", Tier.USER, RUN_ID).path.write_bytes(b"tampered")
    assert cache.verify("foo.nc", RUN_ID) is False


def test_verify_uses_the_recorded_strategy(user_root: Path, shared_root: Path) -> None:
    """A quick digest is checked with a quick strategy, not a full one."""
    cache = ArtifactCache(user_root, shared_root, fingerprinter=QuickFingerprinter())
    with cache.stage("foo.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
    assert cache.verify("foo.nc", RUN_ID) is True


def test_verify_returns_none_without_a_digest(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """With no digest recorded there is nothing to check against."""
    assert cache.verify(staged_artifact, RUN_ID) is None


def test_verify_raises_for_missing_artifact(cache: ArtifactCache) -> None:
    """Verification of an absent artifact fails like any other lookup."""
    with pytest.raises(ArtifactNotFoundError):
        cache.verify("nope.nc", RUN_ID)


# ---------------------------------------------------------------------------
# Shared tier addressing
# ---------------------------------------------------------------------------


def test_shared_path_carries_no_run_id(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """A promoted artifact sits directly under the shared root.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    staged_artifact : str
        Name of a committed user-tier artifact.
    """
    shared = cache.promote(staged_artifact, RUN_ID)
    assert shared.path == cache.shared_root / staged_artifact
    assert shared.run_id is None
    assert RUN_ID not in str(shared.path)


def test_shared_locate_rejects_a_run_id(cache: ArtifactCache) -> None:
    """Passing a run id for a shared location is a usage error, not ignored.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    """
    with pytest.raises(ValueError, match="addressed by name alone"):
        cache.locate("foo.nc", Tier.SHARED, RUN_ID)


def test_user_locate_requires_a_run_id(cache: ArtifactCache) -> None:
    """The user tier is run-scoped, so a run id is mandatory there.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    """
    with pytest.raises(ValueError, match="run_id is required"):
        cache.locate("foo.nc", Tier.USER)


def test_consumer_finds_shared_artifact_without_the_producing_run(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """The point of the flat layout: lookup needs no knowledge of the producer.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    staged_artifact : str
        Name of a committed user-tier artifact.
    """
    cache.promote(staged_artifact, RUN_ID)

    found = cache.resolve(staged_artifact)
    assert found is not None
    assert found.tier is Tier.SHARED


def test_a_different_run_resolves_to_the_shared_copy(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """A consumer passing its own run id still finds another run's promotion.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    staged_artifact : str
        Name of a committed user-tier artifact.
    """
    cache.promote(staged_artifact, RUN_ID)

    found = cache.resolve(staged_artifact, run_id="some-other-run")
    assert found is not None
    assert found.tier is Tier.SHARED


def test_resolve_without_run_id_ignores_user_copies(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Omitting the run id asks only whether the shared tier has it.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    staged_artifact : str
        Name of a committed, unpromoted user-tier artifact.
    """
    assert cache.resolve(staged_artifact) is None
    assert cache.resolve(staged_artifact, RUN_ID) is not None


def test_require_names_the_scope_it_searched(cache: ArtifactCache) -> None:
    """The error distinguishes a shared-only lookup from a run-scoped one.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    """
    with pytest.raises(ArtifactNotFoundError, match="shared tier"):
        cache.require("nope.nc")


def test_candidates_are_shared_only_without_a_run_id(cache: ArtifactCache) -> None:
    """With no run id there is exactly one place to look.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    """
    assert len(cache.candidates("foo.nc")) == 1
    assert len(cache.candidates("foo.nc", RUN_ID)) == 2


# ---------------------------------------------------------------------------
# Promotion provenance and collisions
# ---------------------------------------------------------------------------


def test_promotion_records_provenance(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """The path no longer carries the producer, so the record must.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    staged_artifact : str
        Name of a committed user-tier artifact.
    """
    cache.promote(staged_artifact, RUN_ID)

    record = cache.read_shared_record(staged_artifact)
    assert record is not None
    assert record.promoted_from_run_id == RUN_ID
    assert record.promoted_by
    assert record.promoted_at


def test_shared_records_live_in_a_dot_directory(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Sidecars are namespaced away so they cannot be mistaken for artifacts.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    staged_artifact : str
        Name of a committed user-tier artifact.
    """
    cache.promote(staged_artifact, RUN_ID)

    sidecar = cache.shared_record_path(staged_artifact)
    assert sidecar.parent.name == SHARED_RECORD_DIR
    assert sidecar.is_file()
    assert [loc.name for loc in cache.list_shared_artifacts()] == [staged_artifact]


def test_repromoting_identical_content_is_a_no_op(
    user_root: Path, shared_root: Path
) -> None:
    """Two runs producing the same bytes under one name is not a conflict.

    Parameters
    ----------
    user_root : Path
        User tier root.
    shared_root : Path
        Shared tier root.
    """
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    for run in ("run-A", "run-B"):
        with cache.stage("shared.nc", run) as tmp:
            tmp.write_bytes(b"identical")

    first = cache.promote("shared.nc", "run-A")
    second = cache.promote("shared.nc", "run-B")

    assert first.path == second.path
    record = cache.read_shared_record("shared.nc")
    assert record is not None
    assert record.promoted_from_run_id == "run-A"


def test_repromoting_different_content_raises(
    user_root: Path, shared_root: Path
) -> None:
    """Divergence under one shared name is surfaced rather than silently won.

    Parameters
    ----------
    user_root : Path
        User tier root.
    shared_root : Path
        Shared tier root.
    """
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("shared.nc", "run-A") as tmp:
        tmp.write_bytes(b"original")
    with cache.stage("shared.nc", "run-B") as tmp:
        tmp.write_bytes(b"divergent")

    cache.promote("shared.nc", "run-A")
    with pytest.raises(ArtifactExistsError, match="different content"):
        cache.promote("shared.nc", "run-B")


def test_repromotion_without_a_digest_is_treated_as_a_conflict(
    cache: ArtifactCache,
) -> None:
    """Absent a fingerprint there is no evidence of sameness, so it raises.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test, using the default no-digest strategy.
    """
    for run in ("run-A", "run-B"):
        with cache.stage("shared.nc", run) as tmp:
            tmp.write_bytes(b"identical")

    cache.promote("shared.nc", "run-A")
    with pytest.raises(ArtifactExistsError):
        cache.promote("shared.nc", "run-B")


def test_overwrite_forces_republication(user_root: Path, shared_root: Path) -> None:
    """An explicit flag still allows replacing shared content.

    Parameters
    ----------
    user_root : Path
        User tier root.
    shared_root : Path
        Shared tier root.
    """
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("shared.nc", "run-A") as tmp:
        tmp.write_bytes(b"original")
    with cache.stage("shared.nc", "run-B") as tmp:
        tmp.write_bytes(b"divergent")

    cache.promote("shared.nc", "run-A")
    cache.promote("shared.nc", "run-B", overwrite=True)

    assert cache.locate("shared.nc", Tier.SHARED).path.read_bytes() == b"divergent"
    record = cache.read_shared_record("shared.nc")
    assert record is not None
    assert record.promoted_from_run_id == "run-B"


def test_describe_shared_reconciles_with_disk(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """Records for artifacts removed from disk drop out of the description.

    Parameters
    ----------
    cache : ArtifactCache
        Cache under test.
    staged_artifact : str
        Name of a committed user-tier artifact.
    """
    cache.promote(staged_artifact, RUN_ID)
    assert set(cache.describe_shared()) == {staged_artifact}

    cache.locate(staged_artifact, Tier.SHARED).path.unlink()
    assert cache.describe_shared() == {}


def test_verify_works_without_a_run_id(user_root: Path, shared_root: Path) -> None:
    """A consumer can check integrity of shared data it did not produce.

    Parameters
    ----------
    user_root : Path
        User tier root.
    shared_root : Path
        Shared tier root.
    """
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("shared.nc", "run-A") as tmp:
        tmp.write_bytes(b"payload")
    cache.promote("shared.nc", "run-A")

    assert cache.verify("shared.nc") is True
    cache.locate("shared.nc", Tier.SHARED).path.write_bytes(b"tampered")
    assert cache.verify("shared.nc") is False


# ---------------------------------------------------------------------------
# Reference log
# ---------------------------------------------------------------------------


def _shared_cache(user_root: Path, shared_root: Path) -> ArtifactCache:
    """Build a cache whose fingerprints allow idempotent re-promotion.

    Parameters
    ----------
    user_root : Path
        User tier root.
    shared_root : Path
        Shared tier root.

    Returns
    -------
    ArtifactCache
        Cache with a full-digest strategy.
    """
    return ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())


def _promoted(cache: ArtifactCache, name: str = "shared.nc") -> str:
    """Commit and promote an artifact, returning its name.

    Parameters
    ----------
    cache : ArtifactCache
        Cache to write into.
    name : str, optional
        Artifact filename.

    Returns
    -------
    str
        The promoted artifact's name.
    """
    with cache.stage(name, RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
    cache.promote(name, RUN_ID)
    return name


def test_shared_record_starts_with_no_references(
    cache: ArtifactCache, staged_artifact: str
) -> None:
    """A freshly promoted artifact has been used by nobody."""
    cache.promote(staged_artifact, RUN_ID)
    record = cache.read_shared_record(staged_artifact)
    assert record is not None
    assert record.references == []
    assert record.reference_total == 0
    assert record.first_referenced_at is None


def test_record_use_registers_a_consumer(user_root: Path, shared_root: Path) -> None:
    """A run recording a use appears in the log with a timestamp."""
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)

    assert cache.record_use(name, "run-B") is True

    (entry,) = cache.references_for(name)
    assert entry.run_id == "run-B"
    assert entry.used_by
    assert entry.last_used_at


def test_resolve_records_use_on_the_read_path(
    user_root: Path, shared_root: Path
) -> None:
    """Registration is automatic for consumers going through the cache.

    This is what keeps the log honest: a consumer that resolves and then opens
    the path directly would otherwise never be recorded.
    """
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)

    cache.resolve(name, "run-B", record_use=True)
    cache.resolve(name, "run-C", record_use=True)

    assert [ref.run_id for ref in cache.references_for(name)] == ["run-B", "run-C"]


def test_resolve_does_not_record_by_default(user_root: Path, shared_root: Path) -> None:
    """Reads stay read-only unless the caller opts in."""
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)

    cache.resolve(name, "run-B")

    assert cache.references_for(name) == []


def test_resolve_does_not_record_user_tier_hits(
    user_root: Path, shared_root: Path
) -> None:
    """Only shared artifacts carry a reference log."""
    cache = _shared_cache(user_root, shared_root)
    with cache.stage("local.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")

    resolved = cache.resolve("local.nc", RUN_ID, record_use=True)

    assert resolved is not None
    assert resolved.tier is Tier.USER
    assert cache.references_for("local.nc") == []


def test_repeated_use_is_debounced(user_root: Path, shared_root: Path) -> None:
    """A hot artifact does not generate a sidecar write per read."""
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)

    assert cache.record_use(name, "run-B") is True
    assert cache.record_use(name, "run-B") is False
    assert len(cache.references_for(name)) == 1


def test_debounce_can_be_bypassed(user_root: Path, shared_root: Path) -> None:
    """A zero interval forces the timestamp to refresh."""
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)

    cache.record_use(name, "run-B")
    assert cache.record_use(name, "run-B", min_interval_seconds=0) is True
    assert len(cache.references_for(name)) == 1


def test_reference_total_counts_distinct_runs(
    user_root: Path, shared_root: Path
) -> None:
    """Re-use by a known run refreshes it rather than counting again."""
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)

    cache.record_use(name, "run-B")
    cache.record_use(name, "run-C")
    cache.record_use(name, "run-B", min_interval_seconds=0)

    record = cache.read_shared_record(name)
    assert record is not None
    assert record.reference_total == 2


def test_reference_log_is_capped(user_root: Path, shared_root: Path) -> None:
    """A widely used artifact's sidecar stays small.

    The history that falls off the end survives as a count, so the cap loses
    detail rather than the fact that use occurred.
    """
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)

    for index in range(MAX_REFERENCES + 5):
        cache.record_use(name, f"run-{index:03d}")

    record = cache.read_shared_record(name)
    assert record is not None
    assert len(record.references) == MAX_REFERENCES
    assert record.reference_total == MAX_REFERENCES + 5
    assert record.references[-1].run_id == f"run-{MAX_REFERENCES + 4:03d}"


def test_record_use_ignores_absent_artifacts(cache: ArtifactCache) -> None:
    """There is nothing to attach a reference to."""
    assert cache.record_use("never-promoted.nc", "run-B") is False


def test_references_survive_republication(user_root: Path, shared_root: Path) -> None:
    """References belong to the shared name, which re-promotion does not change."""
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)
    cache.record_use(name, "run-B")

    with cache.stage(name, "run-D") as tmp:
        tmp.write_bytes(b"revised")
    cache.promote(name, "run-D", overwrite=True)

    record = cache.read_shared_record(name)
    assert record is not None
    assert [ref.run_id for ref in record.references] == ["run-B"]
    assert record.promoted_from_run_id == "run-D"


def test_last_used_at_falls_back_to_promotion(
    user_root: Path, shared_root: Path
) -> None:
    """An artifact nobody has read still has a meaningful age."""
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)

    record = cache.read_shared_record(name)
    assert record is not None
    assert record.last_used_at == record.promoted_at


# ---------------------------------------------------------------------------
# Garbage-collection reporting
# ---------------------------------------------------------------------------


def test_gc_candidates_excludes_recent_artifacts(
    user_root: Path, shared_root: Path
) -> None:
    """A just-promoted artifact is not a candidate."""
    cache = _shared_cache(user_root, shared_root)
    _promoted(cache)

    assert cache.gc_candidates(idle_days=180) == []


def test_gc_candidates_reports_idle_artifacts(
    user_root: Path, shared_root: Path
) -> None:
    """A zero threshold reports everything, with its usage summary."""
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)
    cache.record_use(name, "run-B")

    (report,) = cache.gc_candidates(idle_days=0.0)
    assert report.name == name
    assert report.reference_total == 1
    assert report.recent_runs == ["run-B"]
    assert report.promoted_from_run_id == RUN_ID
    assert report.idle_days is not None


def test_gc_candidates_never_deletes(user_root: Path, shared_root: Path) -> None:
    """Reporting is not deletion; liveness here is inferred, not known."""
    cache = _shared_cache(user_root, shared_root)
    name = _promoted(cache)

    cache.gc_candidates(idle_days=0.0)

    assert cache.locate(name, Tier.SHARED).exists
    assert cache.shared_record_path(name).exists()


def test_gc_candidates_orders_most_idle_first(
    user_root: Path, shared_root: Path
) -> None:
    """The report is ordered so the strongest candidates read first."""
    cache = _shared_cache(user_root, shared_root)
    for artifact in ("old.nc", "new.nc"):
        with cache.stage(artifact, RUN_ID) as tmp:
            tmp.write_bytes(artifact.encode())
        cache.promote(artifact, RUN_ID)

    stale = cache.read_shared_record("old.nc")
    assert stale is not None
    cache.write_shared_record(
        stale.model_copy(update={"promoted_at": "2000-01-01T00:00:00+00:00"})
    )

    names = [report.name for report in cache.gc_candidates(idle_days=0.0)]
    assert names[0] == "old.nc"


# ---------------------------------------------------------------------------
# Cache bypass
# ---------------------------------------------------------------------------


def test_bypass_defaults_off(cache: ArtifactCache) -> None:
    """Caching is active unless the flag says otherwise."""
    assert cache.bypass is False


def test_bypass_reads_the_environment_flag(
    user_root: Path, shared_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The env var is the ergonomic front door for enabling bypass."""
    monkeypatch.setenv(ENV_CSTAR_ARTIFACT_CACHE_BYPASS, FLAG_ON)
    assert ArtifactCache(user_root, shared_root).bypass is True


def test_explicit_argument_overrides_the_environment(
    user_root: Path, shared_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An explicit value wins, so tests and callers can scope the behaviour."""
    monkeypatch.setenv(ENV_CSTAR_ARTIFACT_CACHE_BYPASS, FLAG_ON)
    assert ArtifactCache(user_root, shared_root, bypass=False).bypass is False


def test_repr_surfaces_bypass(user_root: Path, shared_root: Path) -> None:
    """A bypassed cache says so when printed, so surprise is diagnosable."""
    assert "bypass=True" in repr(ArtifactCache(user_root, shared_root, bypass=True))


def test_bypass_makes_resolve_report_a_miss(user_root: Path, shared_root: Path) -> None:
    """Client code that looks for a cached artifact is told there is none."""
    cache = ArtifactCache(user_root, shared_root)
    with cache.stage("prod.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")

    bypassed = ArtifactCache(user_root, shared_root, bypass=True)
    assert bypassed.resolve("prod.nc", RUN_ID) is None
    assert bypassed.locate("prod.nc", Tier.USER, RUN_ID).exists


def test_bypass_reports_a_miss_for_shared_artifacts(
    user_root: Path, shared_root: Path
) -> None:
    """Promoted data is ignored too, so the run recreates rather than reuses."""
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("prod.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
    cache.promote("prod.nc", RUN_ID)

    bypassed = ArtifactCache(user_root, shared_root, bypass=True)
    assert bypassed.resolve("prod.nc") is None
    assert bypassed.resolve("prod.nc", RUN_ID) is None


def test_bypass_still_writes_to_the_user_cache(
    user_root: Path, shared_root: Path
) -> None:
    """Bypass ignores the cache; it does not disable it."""
    bypassed = ArtifactCache(user_root, shared_root, bypass=True)

    with bypassed.stage("prod.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"fresh")

    assert bypassed.locate("prod.nc", Tier.USER, RUN_ID).path.read_bytes() == b"fresh"
    assert RUN_ID in bypassed.list_runs()


def test_bypass_forces_overwrite_on_write(user_root: Path, shared_root: Path) -> None:
    """Having been told the artifact is missing, the caller must be able to write it.

    Without this, a caller that passes ``overwrite=False`` after a reported
    miss would hit :class:`ArtifactExistsError` on a file it was just told did
    not exist.
    """
    cache = ArtifactCache(user_root, shared_root)
    with cache.stage("prod.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"original")

    bypassed = ArtifactCache(user_root, shared_root, bypass=True)
    with bypassed.stage("prod.nc", RUN_ID, overwrite=False) as tmp:
        tmp.write_bytes(b"recreated")

    assert (
        bypassed.locate("prod.nc", Tier.USER, RUN_ID).path.read_bytes() == b"recreated"
    )


def test_bypass_ingest_recreates_over_an_existing_artifact(
    user_root: Path, shared_root: Path, tmp_path: Path
) -> None:
    """Ingestion follows the same rule, since it writes through ``stage``."""
    cache = ArtifactCache(user_root, shared_root)
    with cache.stage("prod.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"original")

    source = tmp_path / "external.nc"
    source.write_bytes(b"ingested")
    bypassed = ArtifactCache(user_root, shared_root, bypass=True)
    bypassed.ingest(source, "prod.nc", RUN_ID, overwrite=False)

    assert (
        bypassed.locate("prod.nc", Tier.USER, RUN_ID).path.read_bytes() == b"ingested"
    )


def test_require_names_the_flag_when_bypassed(
    user_root: Path, shared_root: Path
) -> None:
    """A miss caused by bypass is diagnosable rather than mystifying."""
    cache = ArtifactCache(user_root, shared_root)
    with cache.stage("prod.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")

    bypassed = ArtifactCache(user_root, shared_root, bypass=True)
    with pytest.raises(ArtifactNotFoundError, match=ENV_CSTAR_ARTIFACT_CACHE_BYPASS):
        bypassed.require("prod.nc", RUN_ID)


def test_verify_is_not_bypassed(user_root: Path, shared_root: Path) -> None:
    """Integrity checking is not a reuse decision, so bypass must not blind it."""
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("prod.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")

    bypassed = ArtifactCache(
        user_root, shared_root, fingerprinter=FullFingerprinter(), bypass=True
    )
    assert bypassed.verify("prod.nc", RUN_ID) is True


def test_refresh_view_is_not_bypassed(
    user_root: Path, shared_root: Path, view_root: Path
) -> None:
    """A view must link the files this run just wrote, bypass or not."""
    bypassed = ArtifactCache(user_root, shared_root, view_root, bypass=True)
    with bypassed.stage("prod.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"fresh")

    linked = bypassed.refresh_view(RUN_ID)

    assert set(linked) == {"prod.nc"}
    assert (view_root / RUN_ID / "prod.nc").is_symlink()


def test_listing_is_not_bypassed(user_root: Path, shared_root: Path) -> None:
    """Inspection reports what is on disk; bypass only affects reuse lookups."""
    cache = ArtifactCache(user_root, shared_root, fingerprinter=FullFingerprinter())
    with cache.stage("prod.nc", RUN_ID) as tmp:
        tmp.write_bytes(b"payload")
    cache.promote("prod.nc", RUN_ID)

    bypassed = ArtifactCache(user_root, shared_root, bypass=True)
    assert [loc.name for loc in bypassed.list_shared_artifacts()] == ["prod.nc"]
    assert set(bypassed.describe(RUN_ID)) == {"prod.nc"}
