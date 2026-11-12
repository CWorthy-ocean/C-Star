"""The `ucla_roms_gpu` blueprint: its shipped examples, tiling and namelist overrides."""

import copy
import typing as t
import warnings
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

import cstar
from cstar.applications.ucla_roms_gpu.models import (
    APP_NAME,
    TilingParameterSet,
    UclaRomsGpuBlueprint,
)
from cstar.catalog.domain_catalog import DomainCatalog
from cstar.cli.blueprint.schemas import generate_schema

TEMPLATE_NAME = "blueprint.1.0.0.yaml"
"""The file name of the template for schema 1.0.0 of the blueprint."""

PINNED_TAG = "v0.1.3-beta"
"""The roms-marbl-gpu release the template pins."""


@pytest.fixture
def template_dict(bp_templates_dir: Path) -> dict[str, t.Any]:
    """The template blueprint as a dictionary: 8 tiles on 2 nodes, 1 writer each."""
    path = bp_templates_dir / APP_NAME / TEMPLATE_NAME
    return yaml.safe_load(path.read_text())


def _with_overrides(
    template: dict[str, t.Any], overrides: dict[str, dict[str, t.Any]]
) -> dict[str, t.Any]:
    """Return a copy of the template whose namelist overrides are `overrides`."""
    data = copy.deepcopy(template)
    data["namelist_overrides"] = overrides
    return data


class TestShippedBlueprints:
    def test_template_validates(self, template_dict: dict[str, t.Any]) -> None:
        bp = UclaRomsGpuBlueprint.model_validate(template_dict)

        assert bp.application == APP_NAME
        assert bp.schema_version == "1.0.0"
        assert bp.code.roms.checkout_target == PINNED_TAG

    def test_bundled_example_validates(self) -> None:
        path = DomainCatalog(read_only=True).blueprint_path(APP_NAME, "wales-toy-gpu")

        bp = UclaRomsGpuBlueprint.model_validate(yaml.safe_load(path.read_text()))

        assert bp.name == "wales-toy-gpu"
        assert bp.compute_needs.nodes == 1
        # v0.1.3-beta reads the shared CPU namelist's akt_bak, one per tracer
        assert "vertical_mixing_settings" not in bp.namelist_overrides

    def test_schema_ref_names_a_published_schema(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        bp = UclaRomsGpuBlueprint.model_validate(template_dict)
        published = "ucla_roms_gpu/ucla_roms_gpu_schema.1.0.0.json"

        assert bp.schema_ref.endswith(f"/docs/schemas/bp/{published}")
        assert bp.model_dump(mode="json")["$schema"] == bp.schema_ref
        repo_root = Path(cstar.__file__).resolve().parents[1]
        assert (repo_root / "docs" / "schemas" / "bp" / published).is_file()

    def test_published_schema_is_the_generated_one(self, tmp_path: Path) -> None:
        """The checked-in JSON schema is what the blueprint model generates."""
        generated = generate_schema(UclaRomsGpuBlueprint, tmp_path)

        repo_root = Path(cstar.__file__).resolve().parents[1]
        published = repo_root / "docs" / "schemas" / "bp" / APP_NAME / generated.name
        assert published.read_text() == generated.read_text()


class TestTiling:
    def test_compute_needs_count_tiles_and_writers(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        """8 tiles on 2 nodes, with one writer rank on each, is 10 tasks."""
        bp = UclaRomsGpuBlueprint.model_validate(template_dict)

        needs = bp.compute_needs

        assert (needs.tasks, needs.gpu_tasks, needs.nodes) == (10, 8, 2)
        assert needs.slurm_overrides() == {
            "num_cpus": 10,
            "num_gpus": 8,
            "num_nodes": 2,
        }

    def test_processor_grid_is_the_tiling(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        bp = UclaRomsGpuBlueprint.model_validate(template_dict)

        assert bp.processor_grid == (4, 2)

    def test_more_writers_add_tasks_not_gpus(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        template_dict["tiling"]["writers_per_node"] = 3

        needs = UclaRomsGpuBlueprint.model_validate(template_dict).compute_needs

        assert (needs.tasks, needs.gpu_tasks, needs.nodes) == (14, 8, 2)

    def test_no_writers_is_accepted(self, template_dict: dict[str, t.Any]) -> None:
        template_dict["tiling"]["writers_per_node"] = 0

        needs = UclaRomsGpuBlueprint.model_validate(template_dict).compute_needs

        assert (needs.tasks, needs.gpu_tasks) == (8, 8)

    def test_defaults_are_one_node_and_one_writer(self) -> None:
        tiling = TilingParameterSet(n_procs_x=2, n_procs_y=2)

        assert (tiling.nodes, tiling.writers_per_node) == (1, 1)
        assert tiling.layout.tasks == 5

    def test_tiles_must_divide_over_the_nodes(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        template_dict["tiling"] |= {"n_procs_x": 3, "n_procs_y": 3}

        with pytest.raises(ValidationError, match="cannot be spread evenly"):
            UclaRomsGpuBlueprint.model_validate(template_dict)

    @pytest.mark.parametrize("field", ["n_procs_x", "n_procs_y", "nodes"])
    def test_counts_must_be_positive(
        self, template_dict: dict[str, t.Any], field: str
    ) -> None:
        template_dict["tiling"][field] = 0

        with pytest.raises(ValidationError, match=field):
            UclaRomsGpuBlueprint.model_validate(template_dict)

    def test_a_partitioning_block_is_rejected(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        template_dict["partitioning"] = {"n_procs_x": 4, "n_procs_y": 2}

        with pytest.raises(ValidationError, match="partitioning"):
            UclaRomsGpuBlueprint.model_validate(template_dict)

    def test_compile_time_code_is_rejected(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        template_dict["code"]["compile_time"] = {
            "location": "https://example.com/compile.git",
            "branch": "main",
        }

        with pytest.raises(ValidationError, match="compile_time"):
            UclaRomsGpuBlueprint.model_validate(template_dict)


class TestDatasets:
    @pytest.mark.parametrize(
        ("path", "name"),
        [
            (("grid",), "grid"),
            (("initial_conditions",), "initial_conditions"),
            (("forcing", "surface"), "forcing.surface"),
        ],
    )
    def test_a_partitioned_dataset_is_rejected_by_name(
        self,
        template_dict: dict[str, t.Any],
        path: tuple[str, ...],
        name: str,
    ) -> None:
        section = template_dict
        for key in path:
            section = section[key]
        section["data"][0]["partitioned"] = True

        with pytest.raises(ValidationError, match=f"partitioned: {name}"):
            UclaRomsGpuBlueprint.model_validate(template_dict)

    def test_whole_file_datasets_are_accepted(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        template_dict["grid"]["data"][0]["partitioned"] = False

        UclaRomsGpuBlueprint.model_validate(template_dict)


class TestDates:
    def test_valid_range_must_be_ordered(self, template_dict: dict[str, t.Any]) -> None:
        template_dict["valid_end_date"] = "1999-01-01 00:00:00"

        with pytest.raises(ValidationError, match="must precede"):
            UclaRomsGpuBlueprint.model_validate(template_dict)

    def test_end_date_must_be_in_the_valid_range(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        template_dict["runtime_params"]["end_date"] = "2030-01-01 00:00:00"

        with pytest.raises(ValidationError, match="end_date is outside"):
            UclaRomsGpuBlueprint.model_validate(template_dict)

    def test_start_date_must_be_in_the_valid_range(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        template_dict["runtime_params"]["start_date"] = "1990-01-01 00:00:00"

        with pytest.raises(ValidationError, match="start_date is outside"):
            UclaRomsGpuBlueprint.model_validate(template_dict)


class TestNamelistOverrides:
    def test_a_consistent_processor_grid_is_accepted(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        data = _with_overrides(template_dict, {"param_settings": {"np_xi": 4}})

        UclaRomsGpuBlueprint.model_validate(data)

    @pytest.mark.parametrize(("key", "value"), [("np_xi", 3), ("np_eta", 5)])
    def test_a_conflicting_processor_grid_is_rejected(
        self, template_dict: dict[str, t.Any], key: str, value: int
    ) -> None:
        data = _with_overrides(template_dict, {"param_settings": {key: value}})

        with pytest.raises(ValidationError, match=f"override '{key}'.*conflicts"):
            UclaRomsGpuBlueprint.model_validate(data)

    def test_an_unknown_key_is_rejected_for_a_pinned_release(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        data = _with_overrides(template_dict, {"time_stepping": {"no_such_key": 1}})

        with pytest.raises(ValidationError, match="unknown key 'no_such_key'"):
            UclaRomsGpuBlueprint.model_validate(data)

    def test_an_unknown_key_only_warns_for_a_branch(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        template_dict["code"]["roms"] = {
            "location": "https://github.com/CWorthy-ocean/roms-marbl-gpu.git",
            "branch": "main",
        }
        data = _with_overrides(template_dict, {"time_stepping": {"no_such_key": 1}})

        with pytest.warns(UserWarning, match="unknown key 'no_such_key'"):
            UclaRomsGpuBlueprint.model_validate(data)

    def test_a_known_key_does_not_warn(self, template_dict: dict[str, t.Any]) -> None:
        template_dict["code"]["roms"] = {
            "location": "https://github.com/CWorthy-ocean/roms-marbl-gpu.git",
            "branch": "main",
        }

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            UclaRomsGpuBlueprint.model_validate(template_dict)

    def test_a_key_newer_than_the_gpu_vocabulary_is_rejected(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        """`v_sponge_trc` is a ucla-roms 0.13 key; the GPU release reads 0.11.0."""
        data = _with_overrides(
            template_dict, {"v_sponge_settings": {"v_sponge_trc": 1.0}}
        )

        with pytest.raises(ValidationError, match="unknown key 'v_sponge_trc'"):
            UclaRomsGpuBlueprint.model_validate(data)

    def test_a_switch_for_a_missing_feature_is_rejected(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        data = _with_overrides(
            template_dict, {"frc_output_settings": {"wrt_frc": True}}
        )

        with pytest.raises(ValidationError, match="wrt_frc"):
            UclaRomsGpuBlueprint.model_validate(data)

    def test_the_same_switch_off_is_accepted(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        data = _with_overrides(
            template_dict, {"frc_output_settings": {"wrt_frc": False}}
        )

        UclaRomsGpuBlueprint.model_validate(data)

    def test_more_forcing_files_than_the_gpu_reads_are_rejected(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        data = _with_overrides(
            template_dict,
            {"forcing_files": {"frcfiles": [f"f{i}.nc" for i in range(65)]}},
        )

        with pytest.raises(ValidationError, match="frcfiles: 65 values"):
            UclaRomsGpuBlueprint.model_validate(data)

    def test_one_akt_bak_per_tracer_is_accepted(
        self, template_dict: dict[str, t.Any]
    ) -> None:
        data = _with_overrides(
            template_dict, {"vertical_mixing_settings": {"akt_bak": [1e-5] * 28}}
        )

        UclaRomsGpuBlueprint.model_validate(data)
