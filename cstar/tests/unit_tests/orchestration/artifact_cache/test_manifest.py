"""Unit tests for :class:`cstar.orchestration.artifact_cache.Manifest`."""

import json

import pytest
from pydantic import ValidationError

from cstar.orchestration.artifact_cache import (
    MANIFEST_VERSION,
    ArtifactRecord,
    Manifest,
    Tier,
)


@pytest.fixture
def record() -> ArtifactRecord:
    """Return a minimal artifact record.

    Returns
    -------
    ArtifactRecord
        Record to embed in manifests under test.
    """
    return ArtifactRecord(
        name="foo.nc",
        size_bytes=10,
        created_at="2026-08-07T00:00:00+00:00",
        created_by="chris",
    )


@pytest.fixture
def manifest(record: ArtifactRecord) -> Manifest:
    """Return a manifest containing one record.

    Parameters
    ----------
    record : ArtifactRecord
        Record to embed.

    Returns
    -------
    Manifest
        Manifest under test.
    """
    return Manifest(run_id="run-1", tier=Tier.USER, artifacts={record.name: record})


def test_defaults_to_empty_and_unpromoted() -> None:
    """A fresh manifest carries no artifacts and no promotion stamp."""
    manifest = Manifest(run_id="run-1", tier=Tier.USER)
    assert manifest.artifacts == {}
    assert manifest.promoted_at is None
    assert manifest.version == MANIFEST_VERSION


def test_artifact_defaults_are_not_shared() -> None:
    """Each manifest receives its own artifacts mapping."""
    first = Manifest(run_id="a", tier=Tier.USER)
    second = Manifest(run_id="b", tier=Tier.USER)
    first.artifacts["x"] = ArtifactRecord(
        name="x", size_bytes=1, created_at="t", created_by="u"
    )
    assert second.artifacts == {}


def test_to_dict_serialises_tier_as_value(manifest: Manifest) -> None:
    """The tier is stored as a plain string so the manifest is portable JSON."""
    payload = manifest.to_dict()
    assert payload["tier"] == "user"
    assert payload["run_id"] == "run-1"
    assert payload["version"] == MANIFEST_VERSION
    assert payload["promoted_at"] is None


def test_to_dict_is_json_serialisable(manifest: Manifest) -> None:
    """The whole structure survives :func:`json.dumps` without custom encoders."""
    text = json.dumps(manifest.to_dict())
    assert json.loads(text)["artifacts"]["foo.nc"]["size_bytes"] == 10


def test_round_trip_preserves_records(manifest: Manifest) -> None:
    """A manifest survives serialisation and reconstruction unchanged."""
    assert Manifest.from_dict(manifest.to_dict()) == manifest


def test_round_trip_preserves_promotion_stamp(manifest: Manifest) -> None:
    """The promotion timestamp is carried across serialisation."""
    promoted = manifest.model_copy(update={"promoted_at": "2026-08-07T01:00:00+00:00"})
    assert Manifest.from_dict(promoted.to_dict()).promoted_at == promoted.promoted_at


def test_from_dict_tolerates_missing_optional_sections() -> None:
    """A minimal payload loads into an empty manifest at the current version."""
    manifest = Manifest.from_dict({"run_id": "run-1", "tier": "shared"})
    assert manifest.tier is Tier.SHARED
    assert manifest.artifacts == {}
    assert manifest.version == MANIFEST_VERSION


def test_from_dict_preserves_foreign_version() -> None:
    """An older schema version is retained rather than silently upgraded."""
    assert (
        Manifest.from_dict({"run_id": "r", "tier": "user", "version": 0}).version == 0
    )


def test_from_dict_rejects_unknown_tier() -> None:
    """A corrupt tier value fails loudly rather than defaulting."""
    with pytest.raises(ValueError):
        Manifest.from_dict({"run_id": "r", "tier": "archive"})


def test_is_frozen(manifest: Manifest) -> None:
    """Manifests are immutable; updates go through :meth:`~pydantic.BaseModel.model_copy`."""
    with pytest.raises(ValidationError, match="frozen"):
        manifest.run_id = "other"


def test_model_copy_produces_an_updated_manifest(manifest: Manifest) -> None:
    """Frozen manifests are updated by copying rather than mutating."""
    updated = manifest.model_copy(update={"promoted_at": "2026-08-07T01:00:00+00:00"})
    assert manifest.promoted_at is None
    assert updated.promoted_at == "2026-08-07T01:00:00+00:00"
    assert updated.artifacts == manifest.artifacts
