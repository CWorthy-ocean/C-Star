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
    return Manifest(run_id="run-1", artifacts={record.name: record})


def test_defaults_to_empty_user_tier() -> None:
    """A fresh manifest is empty and belongs to the user tier.

    Only the user tier keeps a manifest; shared artifacts carry per-artifact
    sidecars instead, so no promotion stamp lives here.
    """
    manifest = Manifest(run_id="run-1")
    assert manifest.artifacts == {}
    assert manifest.tier is Tier.USER
    assert manifest.version == MANIFEST_VERSION


def test_artifact_defaults_are_not_shared() -> None:
    """Each manifest receives its own artifacts mapping."""
    first = Manifest(run_id="a")
    second = Manifest(run_id="b")
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


def test_to_dict_is_json_serialisable(manifest: Manifest) -> None:
    """The whole structure survives :func:`json.dumps` without custom encoders."""
    text = json.dumps(manifest.to_dict())
    assert json.loads(text)["artifacts"]["foo.nc"]["size_bytes"] == 10


def test_round_trip_preserves_records(manifest: Manifest) -> None:
    """A manifest survives serialisation and reconstruction unchanged."""
    assert Manifest.from_dict(manifest.to_dict()) == manifest


def test_from_dict_tolerates_missing_optional_sections() -> None:
    """A minimal payload loads into an empty manifest at the current version."""
    manifest = Manifest.from_dict({"run_id": "run-1"})
    assert manifest.tier is Tier.USER
    assert manifest.artifacts == {}
    assert manifest.version == MANIFEST_VERSION


def test_from_dict_preserves_foreign_version() -> None:
    """An older schema version is retained rather than silently upgraded."""
    assert Manifest.from_dict({"run_id": "r", "version": 1}).version == 1


def test_from_dict_rejects_unknown_tier() -> None:
    """A corrupt tier value fails loudly rather than defaulting."""
    with pytest.raises(ValueError):
        Manifest.from_dict({"run_id": "r", "tier": "archive"})


def test_is_frozen(manifest: Manifest) -> None:
    """Manifests are immutable; updates go through :meth:`~pydantic.BaseModel.model_copy`."""
    with pytest.raises(ValidationError, match="frozen"):
        manifest.run_id = "other"


def test_model_copy_produces_an_updated_manifest(
    manifest: Manifest, record: ArtifactRecord
) -> None:
    """Frozen manifests are updated by copying rather than mutating."""
    extra = record.model_copy(update={"name": "second.nc"})
    updated = manifest.model_copy(
        update={"artifacts": {**manifest.artifacts, "second.nc": extra}}
    )
    assert set(manifest.artifacts) == {"foo.nc"}
    assert set(updated.artifacts) == {"foo.nc", "second.nc"}
