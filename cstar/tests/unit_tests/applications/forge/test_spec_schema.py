"""Catalog spec schema versions: every read of a spec file is migrated on read."""

import copy
import logging
import shutil
import typing as t
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from cstar.applications.forge.migration import SPEC_MIGRATIONS
from cstar.applications.forge.models import load_models_yaml
from cstar.applications.forge.resolve import build_forge_blueprint, load_model_spec_data
from cstar.catalog import spec_schema
from cstar.catalog.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog
from cstar.catalog.spec_schema import SPEC_SCHEMA_VERSIONS, migrate_spec, read_spec
from cstar.system.migration import (
    KEY_SV,
    BlueprintMigration,
    CstarSchemaTooNewError,
    version_key,
)

if t.TYPE_CHECKING:
    from cstar.orchestration.models import CatalogSpecKind

_MODEL = "roms-marbl-0.12-default"
_SPEC_LOGGER = "cstar.catalog.spec_schema"
# CDR-lite output needs a CDR tracer, and CDR tracers need CDR_LITE
_TRACERS = {
    "param": {"nt_cdr_oae": 1},
    "cdr_lite": {"cdr_online_carbonate_sensitivity": True},
}
_GRID_KWARGS = dict(
    nx=6,
    ny=2,
    size_x=500,
    size_y=1000,
    center_lon=0,
    center_lat=55,
    rot=10,
    N=3,
    theta_s=5.0,
    theta_b=2.0,
    hc=250.0,
)


@pytest.fixture(autouse=True)
def _clear_warned() -> None:
    spec_schema._WARNED.clear()


@pytest.fixture
def catalog(tmp_path: Path) -> DomainCatalog:
    """A writable copy of the bundled catalog."""
    root = tmp_path / "catalog"
    shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
    return DomainCatalog(catalog_root=root)


def _spec_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and r.name == _SPEC_LOGGER
    ]


def _write(
    catalog: DomainCatalog, kind: str, name: str, data: dict[str, t.Any]
) -> Path:
    path = catalog.specs_dir / kind / f"{name}.yaml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    catalog._scan_specs(t.cast("CatalogSpecKind", kind))
    return path


def _old_forcing_spec(catalog: DomainCatalog) -> dict[str, t.Any]:
    """The bundled unified ForcingSpec in its pre-2.0.0 shape."""
    data = copy.deepcopy(catalog.forcing_data("glorys-era5-unified"))
    ic = data["initial_conditions"]
    ic["bgc_source"] = ic.pop("bgc_sources")[0]["source"]
    boundary = data["forcing"]["boundary"]
    bgc = boundary.pop("bgc_sources")
    data["forcing"]["boundary"] = [
        {"type": "physics", **boundary},
        *({"type": "bgc", "source": b["source"]} for b in bgc),
    ]
    return data


def _old_output_spec(catalog: DomainCatalog) -> dict[str, t.Any]:
    """The bundled standard OutputSpec with the pre-0.9.0 CDR output section name."""
    data = copy.deepcopy(catalog.output_data("standard"))
    section = data.pop("cdr_lite_output")
    section.pop("do_cdr_lite_output")
    section["do_cdr_tracer_output"] = True
    data["cdr_tracer_output"] = section
    return data


def _old_model_spec(catalog: DomainCatalog) -> dict[str, t.Any]:
    """A ModelSpec with the pre-0.9.0 CDR output section name in `model_settings`."""
    data = yaml.safe_load(catalog.model_path(_MODEL).read_text())
    data.pop(KEY_SV, None)
    settings = data["model_settings"]
    settings.pop("cdr_lite_output", None)
    settings["cdr_tracer_output"] = {"do_cdr_tracer_output": True}
    return data


def _build(catalog: DomainCatalog, **over: t.Any) -> t.Any:
    kw: dict[str, t.Any] = dict(
        model_path=catalog.model_path(_MODEL),
        grid_name="test-tiny",
        grid_kwargs=_GRID_KWARGS,
        open_boundaries={"south": False, "east": True, "north": True, "west": False},
        partitioning={"n_procs_x": 1, "n_procs_y": 1},
        start_date=datetime(2012, 1, 1),
        end_date=datetime(2012, 1, 2),
        dt=7200,
        forcing_inputs=catalog.forcing_data("glorys-era5-unified"),
        output_settings=catalog.output_data("standard"),
    )
    kw.update(over)
    return build_forge_blueprint(**kw)


# ---------------------------------------------------------------------------
# migrate_spec / read_spec
# ---------------------------------------------------------------------------


def test_missing_version_reads_as_1_0_0() -> None:
    spec = migrate_spec("OutputSpec", {"cdr_tracer_output": {"x": 1}}, "f.yaml")
    assert spec.found == "1.0.0"
    assert spec.target == SPEC_SCHEMA_VERSIONS["OutputSpec"]
    assert spec.changed
    assert "cdr_lite_output" in spec.content


def test_current_shape_without_version_is_not_changed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    data = {"description": "d", "cdr_lite_output": {"do_cdr_lite_output": True}}
    with caplog.at_level(logging.WARNING, logger=_SPEC_LOGGER):
        spec = migrate_spec("OutputSpec", data, "f.yaml")
        content = read_spec("OutputSpec", data, "f.yaml")
    assert not spec.changed
    assert content == data
    assert not _spec_warnings(caplog)


def test_migrate_catalog_advice_only_for_a_file(
    caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    """The warning points at `migrate-catalog` for a file, not for an argument."""
    data = {"cdr_tracer_output": {"do_cdr_tracer_output": True}}
    with caplog.at_level(logging.WARNING, logger=_SPEC_LOGGER):
        read_spec("OutputSpec", data, "output_settings")
        read_spec("OutputSpec", data, tmp_path / "o.yaml")
    in_memory, on_disk = _spec_warnings(caplog)
    assert "migrate-catalog" not in in_memory
    assert "migrate-catalog" in on_disk


def test_schema_version_is_not_in_the_content() -> None:
    data = {KEY_SV: SPEC_SCHEMA_VERSIONS["DomainSpec"], "grid_name": "g"}
    content = read_spec("DomainSpec", data, "f.yaml")
    assert content == {"grid_name": "g"}
    assert KEY_SV in data  # the caller's dict is not changed


def test_newer_minor_with_known_keys_loads() -> None:
    major = version_key(SPEC_SCHEMA_VERSIONS["DomainSpec"])[0]
    data = {KEY_SV: f"{major}.9.0", "grid_name": "g"}
    assert read_spec("DomainSpec", data, "f.yaml") == {"grid_name": "g"}


def test_newer_minor_with_an_unknown_key_is_refused() -> None:
    major = version_key(SPEC_SCHEMA_VERSIONS["DomainSpec"])[0]
    data = {KEY_SV: f"{major}.9.0", "grid_name": "g", "brand_new_key": 1}
    with pytest.raises(CstarSchemaTooNewError, match="brand_new_key"):
        read_spec("DomainSpec", data, "f.yaml")


def test_newer_major_is_refused() -> None:
    major = version_key(SPEC_SCHEMA_VERSIONS["DomainSpec"])[0]
    with pytest.raises(CstarSchemaTooNewError):
        read_spec("DomainSpec", {KEY_SV: f"{major + 1}.0.0"}, "f.yaml")


def test_non_dotted_version_is_refused() -> None:
    with pytest.raises(ValueError, match="dotted"):
        read_spec("DomainSpec", {KEY_SV: "v2"}, "f.yaml")


@pytest.mark.parametrize("version", [None, "1.0.0", SPEC_SCHEMA_VERSIONS["ModelSpec"]])
def test_model_spec_unknown_top_level_key_is_refused(version: str | None) -> None:
    data: dict[str, t.Any] = {"code": {}, "model_settings": {}, "use_pi0": True}
    if version is not None:
        data[KEY_SV] = version
    with pytest.raises(ValueError, match="use_pi0"):
        read_spec("ModelSpec", data, "m.yaml")


@pytest.mark.parametrize(
    "kind", [k for k, v in SPEC_SCHEMA_VERSIONS.items() if version_key(v)[0] > 1]
)
def test_registered_adapters_reach_the_current_version(kind: str) -> None:
    migration = BlueprintMigration(SPEC_MIGRATIONS, SPEC_SCHEMA_VERSIONS)
    plan = migration.plan({"application": kind, "schema_version": "1.0.0"})
    assert plan.target == SPEC_SCHEMA_VERSIONS[kind]
    assert plan.adapters[-1].target() == SPEC_SCHEMA_VERSIONS[kind]


# ---------------------------------------------------------------------------
# ForcingSpec / OutputSpec through the catalog and the resolver
# ---------------------------------------------------------------------------


def test_old_forcing_spec_builds_from_the_catalog(
    catalog: DomainCatalog, caplog: pytest.LogCaptureFixture
) -> None:
    path = _write(catalog, "ForcingSpec", "old-forcing", _old_forcing_spec(catalog))

    with caplog.at_level(logging.WARNING, logger=_SPEC_LOGGER):
        data = catalog.forcing_data("old-forcing")
    assert data["initial_conditions"]["bgc_sources"] == [
        {"source": {"name": "UNIFIED", "climatology": True}}
    ]
    assert data["forcing"]["boundary"]["source"]["name"] == "GLORYS"
    assert len(_spec_warnings(caplog)) == 1
    assert str(path) in _spec_warnings(caplog)[0]

    cfg = _build(catalog, forcing_inputs=data)
    assert cfg.forcing.initial_conditions.bgc_sources[0].source.name == "UNIFIED"


def test_old_forcing_spec_builds_from_a_raw_dict(catalog: DomainCatalog) -> None:
    """A caller that parsed its own spec file reaches the resolver migrated."""
    raw = _old_forcing_spec(catalog)
    snapshot = copy.deepcopy(raw)

    cfg = _build(catalog, forcing_inputs=raw)

    assert cfg.forcing.initial_conditions.bgc_sources[0].source.name == "UNIFIED"
    assert cfg.forcing.boundary.bgc_sources[0].source.name == "UNIFIED"
    assert raw == snapshot  # the caller's dict is not changed


def test_old_output_spec_warns_once(
    catalog: DomainCatalog, caplog: pytest.LogCaptureFixture
) -> None:
    _write(catalog, "OutputSpec", "old-output", _old_output_spec(catalog))

    with caplog.at_level(logging.WARNING, logger="cstar"):
        first = catalog.output_data("old-output")
        second = catalog.output_data("old-output")
        cfg = _build(catalog, output_settings=second, run_time_overrides=_TRACERS)

    assert first == second
    assert "cdr_tracer_output" not in first
    assert first["cdr_lite_output"]["do_cdr_lite_output"] is True
    assert cfg.model_settings["cdr_lite_output"]["do_cdr_lite_output"] is True
    warned = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert [r.name for r in warned] == [_SPEC_LOGGER]


def test_old_output_spec_builds_from_a_raw_dict(catalog: DomainCatalog) -> None:
    raw = _old_output_spec(catalog)
    snapshot = copy.deepcopy(raw)

    cfg = _build(catalog, output_settings=raw, run_time_overrides=_TRACERS)

    assert cfg.model_settings["cdr_lite_output"]["do_cdr_lite_output"] is True
    assert "cdr_tracer_output" not in cfg.model_settings
    assert raw == snapshot


# ---------------------------------------------------------------------------
# ModelSpec
# ---------------------------------------------------------------------------


def test_old_model_spec_is_renamed_on_every_read(
    catalog: DomainCatalog, tmp_path: Path
) -> None:
    data = _old_model_spec(catalog)
    path = tmp_path / "my-model.yaml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))

    spec = load_models_yaml(path, "my-model")
    assert "cdr_tracer_output" not in spec.model_settings
    assert spec.model_settings["cdr_lite_output"]["do_cdr_lite_output"] is True

    model = load_model_spec_data(path)["model"]
    assert "cdr_tracer_output" not in model["model_settings"]
    assert model["model_settings"]["cdr_lite_output"]["do_cdr_lite_output"] is True
    assert KEY_SV not in model


def test_multi_model_file_unwraps_to_the_block(
    catalog: DomainCatalog, tmp_path: Path
) -> None:
    data = _old_model_spec(catalog)
    path = tmp_path / "models.yaml"
    path.write_text(yaml.safe_dump({"a": data, "b": data}, sort_keys=False))

    assert load_models_yaml(path, "b").name == "b"
    with pytest.raises(KeyError, match="'c' not found"):
        load_models_yaml(path, "c")
    # a one-key file unwraps to its only block, whatever the entry is called
    single = tmp_path / "single.yaml"
    single.write_text(yaml.safe_dump({"a": data}, sort_keys=False))
    assert "cdr_lite_output" in load_model_spec_data(single)["model"]["model_settings"]


def test_unknown_model_spec_key_is_refused_everywhere(
    catalog: DomainCatalog, tmp_path: Path
) -> None:
    data = yaml.safe_load(catalog.model_path(_MODEL).read_text())
    data["use_pi0"] = True
    path = _write(catalog, "ModelSpec", "typo-model", data)

    with pytest.raises(ValueError, match="use_pi0"):
        load_models_yaml(path, "typo-model")
    with pytest.raises(ValueError, match="use_pi0"):
        load_model_spec_data(path)
    with pytest.raises(ValueError, match="use_pi0"):
        catalog.load_model_spec("typo-model")


def test_commit_hash_parsed_as_an_int_is_a_string(
    catalog: DomainCatalog, tmp_path: Path
) -> None:
    data = yaml.safe_load(catalog.model_path(_MODEL).read_text())
    data["code"]["roms"].pop("branch", None)
    data["code"]["roms"]["commit"] = 1234567
    path = tmp_path / "int-commit.yaml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))

    assert load_models_yaml(path, "int-commit").code.roms.commit == "1234567"


# ---------------------------------------------------------------------------
# Writers stamp the kind's current version
# ---------------------------------------------------------------------------


def _first_key_is_version(catalog: DomainCatalog, kind: str, name: str) -> None:
    path = catalog.specs_dir / kind / f"{name}.yaml"
    written = yaml.safe_load(path.read_text())
    assert next(iter(written)) == KEY_SV
    assert written[KEY_SV] == SPEC_SCHEMA_VERSIONS[kind]


def test_writers_stamp_the_current_version(catalog: DomainCatalog) -> None:
    output = catalog.output_data("standard")
    forcing = catalog.forcing_data("glorys-era5-unified")
    forcing.pop("description")
    domain = catalog.domain_data(catalog.domain_names[0])

    catalog.register_output("w-output", output)
    catalog.register_forcing("w-forcing", forcing)
    catalog.register_domain_from_dict("w-domain", domain)
    catalog.register_cdr("w-cdr", mode="none")
    catalog.register_model_from_settings(
        "w-model",
        catalog.model_data(_MODEL)["model_settings"],
        catalog.model_path(_MODEL),
    )

    for kind, name in (
        ("OutputSpec", "w-output"),
        ("ForcingSpec", "w-forcing"),
        ("DomainSpec", "w-domain"),
        ("CdrSpec", "w-cdr"),
        ("ModelSpec", "w-model"),
    ):
        _first_key_is_version(catalog, kind, name)
    assert KEY_SV not in catalog.output_data("w-output")
    assert KEY_SV not in catalog.forcing_data("w-forcing")
    assert KEY_SV not in catalog.domain_data("w-domain")
    assert KEY_SV not in catalog.cdr_data("w-cdr")
    assert KEY_SV not in catalog.model_data("w-model")


def test_register_model_from_settings_migrates_an_old_base(
    catalog: DomainCatalog, tmp_path: Path
) -> None:
    base = tmp_path / "old-base.yaml"
    base.write_text(yaml.safe_dump(_old_model_spec(catalog), sort_keys=False))

    catalog.register_model_from_settings("from-old", {"cppdefs": {}}, base)

    written = yaml.safe_load(
        (catalog.specs_dir / "ModelSpec" / "from-old.yaml").read_text()
    )
    assert written[KEY_SV] == SPEC_SCHEMA_VERSIONS["ModelSpec"]
    assert written["code"] == catalog.model_data(_MODEL)["code"]
