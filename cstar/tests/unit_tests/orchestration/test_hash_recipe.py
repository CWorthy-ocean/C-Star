"""Tests that pin the hash recipes: blueprint content hashes and step definition hashes.

A hash is only worth recording if the same input hashes the same way in every later
release. The golden digests here come from frozen files in
``fixtures/hash_recipe/``, which are never edited; a failure means the recipe moved,
and every hash recorded by earlier runs stopped matching with it.
"""

import shutil
import typing as t
from pathlib import Path

import pytest
from pydantic import BaseModel, Field

from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.applications.ucla_roms.models import UclaRomsBlueprint
from cstar.orchestration.manifest import DEFINITION_HASH_SCHEME, describe_steps
from cstar.orchestration.models import CONTENT_HASH_SCHEME, Workplan
from cstar.orchestration.serialization import deserialize, read_document, serialize

FIXTURES = Path(__file__).parent / "fixtures" / "hash_recipe"
"""Frozen files; never edit them."""

UCLA_ROMS_BLUEPRINT = FIXTURES / "ucla_roms_blueprint.yaml"
HELLO_BLUEPRINT = FIXTURES / "hello.yaml"
WORKPLAN = FIXTURES / "workplan.yaml"

RECIPE_CHANGED = (
    "The hash recipe changed, so every hash recorded by earlier runs stops matching. "
    "If the change is deliberate, bump HASH_RECIPE_VERSION in "
    "cstar/base/provenance.py and re-pin this value; never re-pin without bumping."
)
"""What a failed golden assertion says."""

UCLA_ROMS_CONTENT_HASH = (
    "bp-v2:c9a5c4f172f2085ee38f53a0e38b45e6d9abe91311822e2c3ab0d768c5af8414"
)
"""The content hash of `UCLA_ROMS_BLUEPRINT`."""

DEFINITION_HASHES = {
    "hello": "def-v2:77b7340093abbb67f0f0b7f67e5b1b1f66fd832493caa41f892c94c72da8386c",
    "again": "def-v2:7436eaa9091af21fb79df6a788151c2f47ead89c10ec9305e8cecf6ff784ed63",
}
"""The definition hash of each step of `WORKPLAN`."""


def _definition_hashes(workplan_path: Path) -> dict[str, str]:
    """Describe the steps of a workplan file; return each one's definition hash."""
    plan = deserialize(workplan_path, Workplan)
    facts = describe_steps(plan, workplan_path.parent)
    return {name: step.definition_hash for name, step in facts.items()}


@pytest.fixture
def blueprint() -> UclaRomsBlueprint:
    """The frozen ucla_roms blueprint."""
    return deserialize(UCLA_ROMS_BLUEPRINT, UclaRomsBlueprint)


def _edited(path: tuple[str, ...], value: t.Any) -> UclaRomsBlueprint:
    """The frozen ucla_roms blueprint with the value at `path` replaced."""
    raw = read_document(UCLA_ROMS_BLUEPRINT)
    node = raw
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    return UclaRomsBlueprint.model_validate(raw)


class TestGoldenDigests:
    def test_the_schemes_are_the_ones_pinned_below(self) -> None:
        """Verify the pins below name the schemes the recipes are tagged with, so a
        bump of the recipe version shows up here first.
        """
        assert UCLA_ROMS_CONTENT_HASH.startswith(f"{CONTENT_HASH_SCHEME}:")
        for value in DEFINITION_HASHES.values():
            assert value.startswith(f"{DEFINITION_HASH_SCHEME}:")

    def test_content_hash_of_a_ucla_roms_blueprint(
        self, blueprint: UclaRomsBlueprint
    ) -> None:
        """Verify the frozen ucla_roms blueprint hashes to its pinned digest."""
        assert blueprint.content_hash() == UCLA_ROMS_CONTENT_HASH, RECIPE_CHANGED

    def test_content_hash_does_not_depend_on_where_the_file_lives(
        self, tmp_path: Path
    ) -> None:
        """Verify the same blueprint file in another directory hashes the same."""
        moved = tmp_path / UCLA_ROMS_BLUEPRINT.name
        shutil.copy(UCLA_ROMS_BLUEPRINT, moved)

        loaded = deserialize(moved, UclaRomsBlueprint)

        assert loaded.content_hash() == UCLA_ROMS_CONTENT_HASH, RECIPE_CHANGED

    def test_definition_hashes_of_a_workplan(self) -> None:
        """Verify each step of the frozen workplan, a step over a relative blueprint
        path and a step over the blueprint it produces, hashes to its pinned digest.
        """
        assert _definition_hashes(WORKPLAN) == DEFINITION_HASHES, RECIPE_CHANGED

    def test_definition_hashes_do_not_depend_on_where_the_workplan_lives(
        self, tmp_path: Path
    ) -> None:
        """Verify the workplan and its relative blueprint, copied to another
        directory, hash the same.
        """
        for source in (HELLO_BLUEPRINT, WORKPLAN):
            shutil.copy(source, tmp_path / source.name)

        hashes = _definition_hashes(tmp_path / WORKPLAN.name)

        assert hashes == DEFINITION_HASHES, RECIPE_CHANGED


class TestRecipeProperties:
    """What the content-hash recipe promises, on a blueprint with every section."""

    def test_an_added_field_at_its_default_does_not_move_the_hash(
        self, blueprint: UclaRomsBlueprint
    ) -> None:
        """Verify a defaulted field a blueprint states explicitly, as a schema bump
        that adds the field would write it, leaves the hash as it was.
        """
        raw = read_document(UCLA_ROMS_BLUEPRINT)
        assert "carbonate_sensitivity" not in raw["forcing"]
        raw["forcing"]["carbonate_sensitivity"] = None

        grown = UclaRomsBlueprint.model_validate(raw)

        assert grown.forcing.carbonate_sensitivity is None
        assert grown.content_hash() == blueprint.content_hash()

    def test_writing_and_reading_back_keeps_the_hash(
        self, tmp_path: Path, blueprint: UclaRomsBlueprint
    ) -> None:
        """Verify a blueprint written by C-Star's own writer, which leaves out what
        is at its default, hashes the same when read back.
        """
        path = tmp_path / "written.yaml"
        assert serialize(path, blueprint)

        reread = deserialize(path, UclaRomsBlueprint)

        assert reread.content_hash() == blueprint.content_hash()

    def test_rebuilding_from_a_full_dump_keeps_the_hash(
        self, blueprint: UclaRomsBlueprint
    ) -> None:
        """Verify a blueprint rebuilt from its full dump, as applying overrides
        rebuilds it, hashes the same.
        """
        rebuilt = type(blueprint)(**blueprint.model_dump(exclude={"$schema"}))

        assert rebuilt.content_hash() == blueprint.content_hash()

    @pytest.mark.parametrize(
        ("path", "default"),
        [
            pytest.param(("partitioning", "auto_tiling"), False, id="partitioning"),
            pytest.param(("code", "roms", "commit"), "", id="code"),
        ],
    )
    def test_a_default_written_out_is_not_an_edit(
        self,
        blueprint: UclaRomsBlueprint,
        path: tuple[str, ...],
        default: t.Any,
    ) -> None:
        """Verify a field the blueprint omits, written out at its default, leaves
        the hash as it was.
        """
        assert _edited(path, default).content_hash() == blueprint.content_hash()

    @pytest.mark.parametrize(
        ("path", "value"),
        [
            pytest.param(("code", "roms", "branch"), "dev", id="branch"),
            pytest.param(
                ("runtime_params", "checkpoint_frequency"), "2d", id="checkpoints"
            ),
            pytest.param(
                ("namelist_overrides", "time_stepping", "dt"), 2, id="namelist"
            ),
        ],
    )
    def test_a_changed_value_moves_the_hash(
        self, blueprint: UclaRomsBlueprint, path: tuple[str, ...], value: t.Any
    ) -> None:
        """Verify a field given a value other than its default moves the hash."""
        assert _edited(path, value).content_hash() != blueprint.content_hash()


class _Grid(BaseModel):
    """A nested model as the first schema declares it."""

    nx: int = 10


class _GrownGrid(BaseModel):
    """The nested model after a schema bump added a defaulted field."""

    nx: int = 10
    ny: int = 10


class _Before(HelloWorldBlueprint):
    """An application blueprint as the first schema declares it."""

    grid: _Grid = Field(default_factory=_Grid)


class _After(HelloWorldBlueprint):
    """The blueprint after a schema bump added a defaulted field to it and to its
    nested model.
    """

    grid: _GrownGrid = Field(default_factory=_GrownGrid)
    note: str = ""


BASE: dict[str, t.Any] = {
    "name": "bp",
    "description": "a blueprint",
    "target": "world",
}
"""The data every blueprint of the additive-field tests starts from."""


class TestAdditiveFields:
    """A schema that grows defaulted fields keeps the hashes of existing blueprints."""

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param({}, id="grid-left-out"),
            pytest.param({"grid": {}}, id="grid-at-its-default"),
            pytest.param({"grid": {"nx": 10}}, id="grid-written-out"),
            pytest.param({"grid": {"nx": 20}}, id="grid-edited"),
        ],
    )
    def test_the_grown_schema_hashes_existing_data_as_before(
        self, data: dict[str, t.Any]
    ) -> None:
        """Verify data the first schema accepts hashes the same under the grown
        one, and carries the recipe's tag.
        """
        before = _Before(**{**BASE, **data})
        after = _After(**{**BASE, **data})

        assert after.content_hash() == before.content_hash()
        assert before.content_hash().startswith(f"{CONTENT_HASH_SCHEME}:")

    def test_a_field_with_a_default_factory_is_not_hashed_at_its_default(self) -> None:
        """Verify a nested model equal to what its `default_factory` makes is left
        out of the hash, so a blueprint hashes as it did before the field existed.
        """
        without_grid = HelloWorldBlueprint(**BASE).content_hash()

        cases: list[dict[str, t.Any]] = [{}, {"grid": {}}, {"grid": {"nx": 10}}]
        for data in cases:
            blueprint = _Before(**{**BASE, **data})

            assert "grid" not in blueprint._hashed_content()
            assert blueprint.content_hash() == without_grid

    def test_the_edited_nested_model_moves_the_hash(self) -> None:
        """Verify the cases above are not all the same hash: a nested model away from
        its default is hashed.
        """
        edited = _Before(**BASE, grid={"nx": 20})

        assert "grid" in edited._hashed_content()
        assert edited.content_hash() != _Before(**BASE).content_hash()

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param({"note": "hello"}, id="top-level"),
            pytest.param({"grid": {"ny": 5}}, id="nested"),
        ],
    )
    def test_a_new_field_set_away_from_its_default_moves_the_hash(
        self, data: dict[str, t.Any]
    ) -> None:
        """Verify a field the grown schema added counts once it is given a value."""
        assert _After(**BASE, **data).content_hash() != _After(**BASE).content_hash()
