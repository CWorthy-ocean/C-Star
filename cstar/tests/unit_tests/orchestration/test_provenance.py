"""Tests for the blueprint provenance models."""

import typing as t
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.orchestration.models import (
    BLUEPRINT_METADATA_FIELDS,
    BlueprintIdentity,
    CatalogSpecKind,
    CatalogSpecRef,
    GeneratedBy,
    Provenance,
)
from cstar.orchestration.serialization import (
    PersistenceMode,
    deserialize,
    read_raw,
    serialize,
)


@pytest.fixture
def provenance() -> Provenance:
    """A provenance holding both kinds of reference."""
    return Provenance(
        generated_at=datetime(2026, 10, 6, 17, 40, 12, tzinfo=UTC),
        generated_by=GeneratedBy(
            tool="forge",
            system="perlmutter",
            versions={"cstar-ocean": "0.16.0", "roms-tools": "3.2.1"},
            run_id="chunked-iceland",
            working_dir="/scratch/forge",
        ),
        derived_from=[
            BlueprintIdentity(
                kind="Blueprint",
                application="forge",
                name="wio-toy",
                content_hash="9b1e",
            ),
            CatalogSpecRef(kind="DomainSpec", name="wio-toy", origin="catalog"),
            CatalogSpecRef(
                kind="ModelSpec",
                name="roms-marbl-0.8-default",
                origin="model_default",
                modified=True,
            ),
        ],
    )


def hello_world(**kwargs: t.Any) -> HelloWorldBlueprint:
    """Create the simplest application blueprint."""
    return HelloWorldBlueprint(
        name="hello", description="says hello", target="world", **kwargs
    )


def test_provenance_defaults_to_empty() -> None:
    """Verify a blueprint records no provenance unless it is given some."""
    assert hello_world().provenance == Provenance()
    assert Provenance().derived_from == []


def test_provenance_round_trips(provenance: Provenance) -> None:
    """Verify both kinds of reference survive a dump and a load."""
    dumped = provenance.model_dump(mode="json")
    reloaded = Provenance.model_validate(dumped)

    assert reloaded == provenance
    assert [type(ref) for ref in reloaded.derived_from] == [
        BlueprintIdentity,
        CatalogSpecRef,
        CatalogSpecRef,
    ]


@pytest.mark.parametrize("kind", t.get_args(CatalogSpecKind))
def test_every_catalog_spec_kind_loads_as_a_spec_ref(kind: str) -> None:
    """Verify the discriminated union accepts each kind the alias declares."""
    ref = {"kind": kind, "name": "n", "origin": "catalog"}
    loaded = Provenance.model_validate({"derived_from": [ref]})

    assert loaded.derived_from == [
        CatalogSpecRef(kind=kind, name="n", origin="catalog")
    ]


def test_catalog_spec_ref_origin_is_informational() -> None:
    """Verify a spec reference needs no origin: it reads as empty, however it is
    left blank, and survives a dump that drops defaults.
    """
    ref = CatalogSpecRef.model_validate({"kind": "DomainSpec", "name": "wio-toy"})

    assert ref.origin == ""
    assert CatalogSpecRef(kind="DomainSpec", name="wio-toy", origin="  ") == ref

    dumped = ref.model_dump(exclude_defaults=True)
    assert dumped == {"kind": "DomainSpec", "name": "wio-toy"}
    assert Provenance.model_validate({"derived_from": [dumped]}).derived_from == [ref]


@pytest.mark.parametrize(
    "ref",
    [
        pytest.param({"kind": "Workplan", "name": "n"}, id="unknown kind"),
        pytest.param({"application": "forge", "name": "n"}, id="missing kind"),
        pytest.param(
            {"kind": "Blueprint", "application": "forge", "name": "n", "origin": "x"},
            id="blueprint with a spec field",
        ),
        pytest.param(
            {"kind": "DomainSpec", "name": "n", "origin": "x", "content_hash": "h"},
            id="spec with a blueprint field",
        ),
    ],
)
def test_provenance_rejects_malformed_refs(ref: dict[str, str]) -> None:
    """Verify a reference is told apart by its kind and holds only its own fields."""
    with pytest.raises(ValidationError):
        Provenance.model_validate({"derived_from": [ref]})


def test_provenance_rejects_unknown_fields() -> None:
    """Verify the block is closed: nothing free-form fits in it."""
    with pytest.raises(ValidationError):
        Provenance.model_validate({"notes": "anything"})


class TestGeneratedBy:
    def test_mints_a_uuid4_id(self) -> None:
        generated_by = GeneratedBy(tool="forge")

        parsed = uuid.UUID(generated_by.id)
        assert parsed.version == 4
        assert str(parsed) == generated_by.id

    def test_mints_a_distinct_id_per_event(self) -> None:
        assert GeneratedBy(tool="forge").id != GeneratedBy(tool="forge").id

    def test_loaded_id_is_kept(self) -> None:
        """Verify an id read from a file is never replaced by a new one."""
        original = GeneratedBy(tool="forge")
        reloaded = GeneratedBy.model_validate(original.model_dump(mode="json"))

        assert reloaded.id == original.id
        assert GeneratedBy(tool="wizard", id="from-a-file").id == "from-a-file"

    @pytest.mark.parametrize("field", ["tool", "id"])
    @pytest.mark.parametrize("value", ["", "   "])
    def test_required_strings_are_not_blank(self, field: str, value: str) -> None:
        with pytest.raises(ValidationError):
            GeneratedBy(**{"tool": "forge", field: value})

    def test_optional_fields_default_to_empty(self) -> None:
        generated_by = GeneratedBy(tool="wizard")

        assert generated_by.system == ""
        assert generated_by.versions == {}
        assert generated_by.run_id == ""
        assert generated_by.working_dir == ""


def test_blueprint_ref_dump_keeps_kind_when_defaults_are_excluded() -> None:
    """Verify `kind`, which selects the model on load, survives a dump that drops
    defaults.
    """
    ref = BlueprintIdentity(kind="Blueprint", application="forge", name="wio-toy")

    assert ref.model_dump(exclude_defaults=True) == {
        "kind": "Blueprint",
        "application": "forge",
        "name": "wio-toy",
    }


def test_blueprint_metadata_is_exactly_the_base_blueprint_fields() -> None:
    """Verify an application's own configuration is what is left over."""
    assert BLUEPRINT_METADATA_FIELDS == {
        "name",
        "description",
        "application",
        "state",
        "schema_version",
        "working_dir",
        "provenance",
    }
    assert set(HelloWorldBlueprint.model_fields) - BLUEPRINT_METADATA_FIELDS == {
        "target"
    }


def test_default_provenance_is_not_serialized(tmp_path: Path) -> None:
    """Verify a blueprint with no provenance is written as it was before the field."""
    path = tmp_path / "bp.yaml"

    serialize(path, hello_world())

    assert "provenance" not in read_raw(path)
    assert deserialize(path, HelloWorldBlueprint).provenance == Provenance()


@pytest.mark.parametrize("mode", [PersistenceMode.yaml, PersistenceMode.json])
def test_populated_provenance_survives_serialization(
    tmp_path: Path,
    provenance: Provenance,
    mode: PersistenceMode,
) -> None:
    """Verify a blueprint's provenance loads back equal after it is written.

    C-Star rewrites blueprints it transforms with `exclude_defaults`; a
    reference whose `kind` was dropped could not be loaded again.
    """
    path = tmp_path / f"bp.{mode}"
    blueprint = hello_world(provenance=provenance)

    serialize(path, blueprint, mode=mode)

    written = read_raw(path)["provenance"]
    assert written["derived_from"][0] == {
        "kind": "Blueprint",
        "application": "forge",
        "name": "wio-toy",
        "content_hash": "9b1e",
    }
    assert deserialize(path, HelloWorldBlueprint) == blueprint
