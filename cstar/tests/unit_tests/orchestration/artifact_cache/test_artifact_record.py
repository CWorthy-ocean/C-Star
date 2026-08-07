"""Unit tests for :class:`cstar.orchestration.artifact_cache.ArtifactRecord`."""

from typing import Any

import pytest
from pydantic import ValidationError

from cstar.orchestration.artifact_cache import ArtifactRecord, ChecksumMode


@pytest.fixture
def record() -> ArtifactRecord:
    """Return a fully populated record.

    Returns
    -------
    ArtifactRecord
        Record under test.
    """
    return ArtifactRecord(
        name="foo.nc",
        size_bytes=2048,
        created_at="2026-08-07T00:00:00+00:00",
        created_by="chris",
        checksum="deadbeef",
        checksum_mode=ChecksumMode.FULL,
        source="/scratch/raw.nc",
        asset_uri="file:///scratch/app/run-1/foo.nc",
        metadata={"variables": ["x", "y"]},
    )


def test_optional_fields_default_to_none() -> None:
    """Only the four descriptive fields are required."""
    record = ArtifactRecord(
        name="foo.nc",
        size_bytes=1,
        created_at="2026-08-07T00:00:00+00:00",
        created_by="chris",
    )
    assert record.checksum is None
    assert record.checksum_mode is None
    assert record.source is None
    assert record.asset_uri is None
    assert record.metadata == {}


def test_metadata_defaults_are_not_shared() -> None:
    """Each record receives its own metadata mapping."""
    first = ArtifactRecord(name="a", size_bytes=1, created_at="t", created_by="u")
    second = ArtifactRecord(name="b", size_bytes=1, created_at="t", created_by="u")
    first.metadata["only_first"] = True
    assert second.metadata == {}


def test_to_dict_is_json_compatible(record: ArtifactRecord) -> None:
    """Serialisation emits plain types suitable for :func:`json.dump`."""
    payload = record.to_dict()
    assert payload["name"] == "foo.nc"
    assert payload["size_bytes"] == 2048
    assert payload["metadata"] == {"variables": ["x", "y"]}
    assert set(payload) == {
        "name",
        "size_bytes",
        "created_at",
        "created_by",
        "checksum",
        "checksum_mode",
        "source",
        "asset_uri",
        "metadata",
    }


def test_to_dict_copies_metadata(record: ArtifactRecord) -> None:
    """Mutating the serialised payload cannot corrupt the record."""
    payload = record.to_dict()
    payload["metadata"]["injected"] = True
    assert "injected" not in record.metadata


def test_round_trip_preserves_all_fields(record: ArtifactRecord) -> None:
    """A record survives serialisation and reconstruction unchanged."""
    assert ArtifactRecord.from_dict(record.to_dict()) == record


def test_from_dict_ignores_unknown_keys(record: ArtifactRecord) -> None:
    """Forward-compatible manifests written by newer code still load."""
    payload: dict[str, Any] = record.to_dict()
    payload["future_field"] = "ignored"
    assert ArtifactRecord.from_dict(payload) == record


def test_from_dict_coerces_types() -> None:
    """String sizes emitted by hand-edited manifests are coerced."""
    record = ArtifactRecord.from_dict(
        {"name": "a.nc", "size_bytes": "42", "created_at": "t", "created_by": "u"}
    )
    assert record.size_bytes == 42
    assert record.metadata == {}


def test_from_dict_tolerates_null_metadata() -> None:
    """An explicit JSON null for metadata becomes an empty mapping."""
    record = ArtifactRecord.from_dict(
        {
            "name": "a.nc",
            "size_bytes": 1,
            "created_at": "t",
            "created_by": "u",
            "metadata": None,
        }
    )
    assert record.metadata == {}


def test_from_dict_requires_core_fields() -> None:
    """A manifest entry missing required fields fails loudly."""
    with pytest.raises(ValidationError, match="size_bytes"):
        ArtifactRecord.from_dict({"name": "a.nc"})


def test_from_dict_rejects_uncoercible_size() -> None:
    """A non-numeric size is a validation error rather than a silent default."""
    with pytest.raises(ValidationError, match="size_bytes"):
        ArtifactRecord.from_dict(
            {"name": "a.nc", "size_bytes": "huge", "created_at": "t", "created_by": "u"}
        )


def test_is_frozen(record: ArtifactRecord) -> None:
    """Records are immutable once created."""
    with pytest.raises(ValidationError, match="frozen"):
        record.name = "other.nc"


def test_legacy_checksum_without_mode_reads_as_full() -> None:
    """Records predating quick signatures are interpreted as full digests."""
    record = ArtifactRecord.from_dict(
        {
            "name": "a.nc",
            "size_bytes": 1,
            "created_at": "t",
            "created_by": "u",
            "checksum": "deadbeef",
        }
    )
    assert record.checksum_mode is ChecksumMode.FULL


def test_absent_checksum_leaves_mode_unset() -> None:
    """No digest means no inferred mode."""
    record = ArtifactRecord.from_dict(
        {"name": "a.nc", "size_bytes": 1, "created_at": "t", "created_by": "u"}
    )
    assert record.checksum_mode is None


def test_explicit_quick_mode_is_not_overridden() -> None:
    """An explicitly recorded mode survives validation untouched."""
    record = ArtifactRecord.from_dict(
        {
            "name": "a.nc",
            "size_bytes": 1,
            "created_at": "t",
            "created_by": "u",
            "checksum": "deadbeef",
            "checksum_mode": "quick",
        }
    )
    assert record.checksum_mode is ChecksumMode.QUICK
