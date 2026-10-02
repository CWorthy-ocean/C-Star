"""Test suite to test fixtures defined in conftest.py and fixtures.py files."""

import hashlib
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pooch
import yaml
from _pytest._py.path import LocalPath

from cstar.applications.forge.blueprint import ForgeBlueprint
from cstar.applications.forge.engine import verify_content_hash
from cstar.tests.integration_tests.cases import (
    DT,
    MODEL_SPEC,
    ROMS_REF,
    RUN_END,
    RUN_START,
)
from cstar.tests.integration_tests.data_registry import CACHE_NAME, REGISTRY

if TYPE_CHECKING:
    from cstar.catalog.domain_catalog import LayeredCatalog


def test_modify_template_blueprint(
    modify_template_blueprint: Callable,
    tmp_path: Path,
    tests_path: Path,
) -> None:
    """This test verifies that the modify_template_blueprint fixture correctly reads a
    specified blueprint, performs string replacements, and returns a Simulation instance
    with the correct parameters corresponding to the string replacements.

    Parameters
    ----------
    modify_template_blueprint : Callable
        A fixture that modifies a template blueprint with specific string replacements.
    tmpdir : Path
        Built-in pytest fixture for creating a temporary directory during the test
    tests_path : Path
        Fixture returning the directory containing c-star tests; used to build
        absolute paths for other file located relative to the tests directory.

    Asserts
    -------
    - The returned object is an instance of Path.
    - The additional_source_code location matches that expected after replacement
    """
    working_dir = tmp_path / "test_working_dir"

    test_blueprint = modify_template_blueprint(
        template_blueprint_path=tests_path
        / "integration_tests/blueprints/blueprint_template.yaml",
        strs_to_replace={
            "<additional_code_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git"
        },
        out_dir=working_dir,
    )

    assert isinstance(test_blueprint, LocalPath), (
        f"Expected type LocalPath, but got {type(test_blueprint)}"
    )
    bpyaml = yaml.safe_load(test_blueprint.read())

    assert (
        bpyaml["code"]["compile_time"]["location"]
        == "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git"
    )
    assert bpyaml["working_dir"] == str(working_dir)


def test_registry_files_match_hashes(integration_test_data: dict[str, Path]) -> None:
    """Every registry file is present in the cache and has its pinned sha256."""
    assert set(integration_test_data) == set(REGISTRY)
    for name, path in integration_test_data.items():
        assert path.exists(), name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == REGISTRY[name], name


def _strings(node: object) -> Iterator[str]:
    """Yield every string nested in parsed YAML ``node``, dict keys excluded."""
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for child in node.values():
            yield from _strings(child)
    elif isinstance(node, list):
        for child in node:
            yield from _strings(child)


def test_catalog_root_substitutes_test_data(test_catalog_root: Path) -> None:
    """No ``${TEST_DATA}`` placeholder survives and every substituted path exists."""
    data_dir = str(pooch.os_cache(CACHE_NAME))
    yaml_files = list(test_catalog_root.rglob("*.yaml"))
    assert yaml_files
    substituted = 0
    for yaml_file in yaml_files:
        for value in _strings(yaml.safe_load(yaml_file.read_text())):
            assert "${TEST_DATA}" not in value, yaml_file
            if data_dir in value:
                substituted += 1
                assert Path(value).exists(), value
    assert substituted


def test_catalog_layers(test_catalog: "LayeredCatalog") -> None:
    """The test layer resolves its own specs and defers ModelSpecs to the bundled one."""
    forcing = test_catalog.forcing_data("test-glorys-era5-unified")
    assert set(forcing) >= {"initial_conditions", "forcing"}
    assert set(forcing["forcing"]) == {"surface", "boundary", "tidal", "river"}
    bundled = Path(__file__).parents[2] / "catalog" / "bundled"
    assert bundled in test_catalog.model_dir(MODEL_SPEC).parents


def test_factory_unified(
    forge_blueprint_factory: Callable[..., tuple[ForgeBlueprint, Path]],
    tmp_path: Path,
) -> None:
    """The unified case resolves offline with the expected domain, run and settings."""
    cfg, _ = forge_blueprint_factory("unified", tmp_path)
    assert cfg.datasets == ["ETOPO5"]
    assert set(cfg.forcing.resolved_datasets) == {"ETOPO5"}
    cppdefs = cfg.model_settings["cppdefs"]
    assert cppdefs["use_pio"] is True
    assert cppdefs["marbl"] is True
    assert (cfg.domain.partitioning.n_procs_x, cfg.domain.partitioning.n_procs_y) == (
        2,
        2,
    )
    assert (cfg.run.start_date, cfg.run.end_date) == (RUN_START, RUN_END)
    assert cfg.domain.dt == DT
    assert cfg.model_settings["time_stepping"]["dt"] == DT
    assert cfg.model_settings["ocean_vars"]["output_period_rst"] == 1800
    assert cfg.code.roms.commit == ROMS_REF
    assert cfg.working_dir == tmp_path


def test_factory_constants(
    forge_blueprint_factory: Callable[..., tuple[ForgeBlueprint, Path]],
    tmp_path: Path,
) -> None:
    """The constants case disables the forcings it has no data for."""
    cfg, _ = forge_blueprint_factory("constants", tmp_path)
    cppdefs = cfg.model_settings["cppdefs"]
    assert cppdefs["nhy_forcing"] is False
    assert cppdefs["nox_forcing"] is False
    assert len(cfg.forcing.surface) == 1
    assert cfg.forcing.initial_conditions is not None
    assert cfg.forcing.initial_conditions.bgc_sources[0].source.name == "constants"


def test_blueprint_yaml_round_trip(
    forge_blueprint_factory: Callable[..., tuple[ForgeBlueprint, Path]],
    tmp_path: Path,
) -> None:
    """The written blueprint reloads with the same content hash and no hash warning."""
    cfg, path = forge_blueprint_factory("unified", tmp_path)
    reloaded = ForgeBlueprint.from_yaml(path)
    assert reloaded.content_hash() == cfg.content_hash()
    assert verify_content_hash(reloaded) is None
