"""Settings a Forge build cannot honor are refused before any input is generated.

Covers FG-13 (R-N5, R-N9, C10), R-N11 and the validator half of FG-09 of the
2026-11 Forge option audit: unknown sections and keys, pipe forcing, tiles with no
grid points, and a model reference date with a time of day. Each is reported by
``validate_run_time_sections`` (what ``process_forge_blueprint`` and the wizard
call) and refused by the resolver; ``test_executor.py`` covers ``configure_build``.
"""

from __future__ import annotations

import copy
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

import cstar.catalog
from cstar.applications.forge.blueprint import ForgeBlueprint, RunWindow
from cstar.applications.forge.engine import blueprint_problems
from cstar.applications.forge.namelist_model import (
    CPPDEFS_KEYS,
    check_tile_sizes,
    run_time_settings_for_ref,
    unknown_settings_keys,
    validate_run_time_sections,
)
from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.catalog.domain_catalog import default_catalog
from cstar.roms.namelist import release_ref_for_remote

_REPO = Path(cstar.__file__).parent.parent
_BUNDLED = Path(cstar.catalog.__file__).parent / "bundled"
_MODEL_SPECS = _BUNDLED / "specs" / "ModelSpec"
_TEMPLATE = (
    Path(cstar.__file__).parent
    / "additional_files"
    / "templates"
    / "forge"
    / "compile-time"
    / "cppdefs.opt.j2"
)
_FORGE_BLUEPRINTS = sorted(
    [
        *(_BUNDLED / "blueprints" / "forge").glob("*.yaml"),
        *(_REPO / "docs").rglob("forge_*.yaml"),
        *(_REPO / "docs").rglob("forge-blueprint-*.yaml"),
        *(Path(__file__).parent / "fixtures").glob("*forge_blueprint*.yaml"),
    ]
)

# (settings edit, a pattern the report must match): the four audit cases.
_UNSUPPORTED: dict[str, tuple[dict[str, Any], str]] = {
    "unknown section": (
        {"botom_drag": {"rdrg2": 1.0e-3}},
        r"botom_drag \(did you mean bottom_drag\?\)",
    ),
    "misspelled output key": (
        {"ocean_vars": {"wrt_fiel_his": True}},
        r"ocean_vars\.wrt_fiel_his \(did you mean ocean_vars\.wrt_file_his\?\)",
    ),
    "misspelled cppdef": (
        {"cppdefs": {"diagnostic": True}},
        r"cppdefs\.diagnostic \(did you mean cppdefs\.diagnostics\?\)",
    ),
    "pipe forcing": (
        {"pipe_frc": {"pipe_source": True}},
        r"pipe_frc\.pipe_source is on, but Forge generates no pipe forcing",
    ),
}


def _merge(base: dict[str, Any], edit: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in edit.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = {**out[key], **value}
        else:
            out[key] = value
    return out


def _roms_ref(bp: ForgeBlueprint) -> str | None:
    ref = bp.code.roms.commit or bp.code.roms.branch
    return release_ref_for_remote(bp.code.roms.location, ref) if ref else None


def _resolve(model: str = "roms-marbl-0.15-default", **over: Any) -> ForgeBlueprint:
    kw: dict[str, Any] = dict(
        model_path=_MODEL_SPECS / f"{model}.yaml",
        grid_name="test-tiny",
        grid_kwargs=dict(
            nx=8,
            ny=8,
            size_x=500,
            size_y=500,
            center_lon=0,
            center_lat=55,
            rot=10,
            N=3,
            theta_s=5.0,
            theta_b=2.0,
            hc=250.0,
        ),
        open_boundaries={"south": False, "east": True, "north": True, "west": False},
        partitioning={"n_procs_x": 2, "n_procs_y": 2},
        start_date=datetime(2012, 1, 1),
        end_date=datetime(2012, 1, 2),
        description="early refusals",
        dt=7200,
        forcing_inputs=default_catalog.forcing_data("glorys-era5-unified"),
        output_settings=default_catalog.output_data("standard"),
    )
    kw.update(over)
    return build_forge_blueprint(**kw)


@pytest.fixture(scope="module")
def resolved() -> ForgeBlueprint:
    return _resolve()


class TestReportedByValidation:
    """``validate_run_time_sections`` reports each case, naming the key."""

    @pytest.mark.parametrize("case", sorted(_UNSUPPORTED))
    def test_reported(self, resolved: ForgeBlueprint, case: str) -> None:
        edit, pattern = _UNSUPPORTED[case]
        settings = _merge(resolved.model_settings, edit)
        problems = validate_run_time_sections(settings, roms_ref=_roms_ref(resolved))
        assert [p for p in problems if re.search(pattern, p)], problems

    def test_untouched_settings_pass(self, resolved: ForgeBlueprint) -> None:
        assert (
            validate_run_time_sections(
                resolved.model_settings, roms_ref=_roms_ref(resolved)
            )
            == []
        )

    def test_every_offender_is_listed_once(self, resolved: ForgeBlueprint) -> None:
        settings = _merge(
            resolved.model_settings,
            {"botom_drag": {}, "ocean_vars": {"wrt_fiel_his": True, "zzz": 1}},
        )
        problems = validate_run_time_sections(settings, roms_ref=_roms_ref(resolved))
        unknown = [p for p in problems if "unknown settings keys" in p]
        assert len(unknown) == 1
        assert "botom_drag" in unknown[0]
        assert "ocean_vars.wrt_fiel_his" in unknown[0]
        assert re.search(r"ocean_vars\.zzz(?! \(did)", unknown[0])

    def test_case_variant_reported_once_as_misspelling(
        self, resolved: ForgeBlueprint
    ) -> None:
        settings = _merge(resolved.model_settings, {"Bottom_drag": {"rdrg": 0.0}})
        problems = validate_run_time_sections(settings, roms_ref=_roms_ref(resolved))
        assert not [p for p in problems if "unknown settings keys" in p]
        assert [p for p in problems if "Bottom_drag (did you mean bottom_drag?)" in p]

    def test_legacy_section_name_is_not_unknown(self) -> None:
        settings_cls = run_time_settings_for_ref("0.15.0")
        assert (
            unknown_settings_keys(
                {"cdr_tracer_output": {"do_cdr_tracer_output": False}}, settings_cls
            )
            == []
        )

    def test_carried_metadata_keys_are_known(self) -> None:
        """Keys Forge writes that no namelist key reads stay accepted, so stored
        blueprints of every C-Star release keep processing.
        """
        settings = {
            "param": {"nsub_x": 1, "nsub_e": 1},
            "bgc": {"nbgc_flx": 2},
            "river_frc": {"rvol_vname": "river_volume", "rtrc_tname": "river_time"},
            "cdr_frc": {"cdrflx_vname": "cdr_trcflx", "cdr_loc_dep": "cdr_dep"},
            "surf_flux": {"sss_vname": "SSS", "sst_tname": "sst_time"},
            "sponge_tune": {"pflx_w_vname": "up_west", "pflx_n_tname": "bry_time"},
            "marbl_bgc": {"marbl_timestep_ratio": 1},
        }
        for ref in ("0.3.0", "0.15.0"):
            assert unknown_settings_keys(settings, run_time_settings_for_ref(ref)) == []


class TestOtherTierKeys:
    """A key another ucla-roms tier models gets its version message, not
    "unknown".
    """

    def test_newer_leaf_key_gets_its_version_message(self) -> None:
        bp = _resolve("roms-marbl-0.13-default")
        settings = _merge(bp.model_settings, {"ts_output": {"wrt_avg_temp": True}})
        problems = validate_run_time_sections(settings, roms_ref=_roms_ref(bp))
        assert [p for p in problems if "requires ucla-roms >= 0.14.0" in p], problems
        assert not [p for p in problems if "unknown settings keys" in p]

    def test_newer_leaf_key_switched_off_passes(self) -> None:
        bp = _resolve("roms-marbl-0.13-default")
        settings = _merge(bp.model_settings, {"ts_output": {"wrt_avg_temp": False}})
        assert validate_run_time_sections(settings, roms_ref=_roms_ref(bp)) == []

    def test_newer_section_gets_its_version_message_at_resolve(self) -> None:
        with pytest.raises(ValueError, match=r"implicit_fs needs ucla-roms >= 0\.15"):
            _resolve(
                "roms-marbl-0.14-default",
                run_time_overrides={"implicit_fs": {"fs_mode": "implicit"}},
            )

    @pytest.mark.parametrize(
        ("ref", "section", "switch", "value", "first"),
        [
            ("0.14.0", "implicit_fs", "fs_mode", "implicit", "0.15.0"),
            ("0.6.0", "cdr_lite_output", "do_cdr_lite_output", True, "0.7.0"),
            ("0.6.0", "cdr_gas_exch_output", "do_cdr_gas_exch_output", True, "0.7.0"),
            ("0.7.0", "cdr_lite", "cdr_online_carbonate_sensitivity", True, "0.9.0"),
        ],
    )
    def test_newer_section_switched_on_gets_its_version_message(
        self, ref: str, section: str, switch: str, value: Any, first: str
    ) -> None:
        settings = {section: {switch: value}}
        problems = validate_run_time_sections(settings, roms_ref=ref)
        assert len(problems) == 1, problems
        assert f"{section}.{switch} enabled" in problems[0]
        assert f"{section} needs ucla-roms >= {first}" in problems[0]
        assert settings == {section: {switch: value}}

    def test_newer_section_switched_off_passes_and_is_kept(self) -> None:
        settings = {"implicit_fs": {"fs_mode": "split"}}
        assert validate_run_time_sections(settings, roms_ref="0.14.0") == []
        assert settings == {"implicit_fs": {"fs_mode": "split"}}

    def test_newer_section_misspelled_switch_reported_once(self) -> None:
        problems = validate_run_time_sections(
            {"implicit_fs": {"Fs_mode": "implicit"}}, roms_ref="0.14.0"
        )
        assert len(problems) == 1, problems
        assert "implicit_fs.Fs_mode" in problems[0]

    def test_newer_section_refused_by_blueprint_problems(self) -> None:
        bp = _resolve("roms-marbl-0.14-default")
        settings = _merge(bp.model_settings, {"implicit_fs": {"fs_mode": "implicit"}})
        problems = blueprint_problems(
            bp.model_copy(update={"model_settings": settings})
        )
        assert [p for p in problems if "implicit_fs needs ucla-roms >= 0.15.0" in p], (
            problems
        )
        assert blueprint_problems(bp) == []

    def test_newer_section_is_not_unknown(self) -> None:
        settings_cls = run_time_settings_for_ref("0.14.0")
        assert (
            unknown_settings_keys({"implicit_fs": {"fs_mode": "split"}}, settings_cls)
            == []
        )


class TestRefusedAtResolve:
    """The resolver refuses each case before a blueprint is written."""

    @pytest.mark.parametrize("case", sorted(_UNSUPPORTED))
    def test_refused(self, case: str) -> None:
        edit, pattern = _UNSUPPORTED[case]
        compile_time = {"cppdefs": edit["cppdefs"]} if "cppdefs" in edit else None
        run_time = {k: v for k, v in edit.items() if k != "cppdefs"} or None
        with pytest.raises(ValueError, match=pattern):
            _resolve(compile_time_overrides=compile_time, run_time_overrides=run_time)

    def test_too_many_tiles(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"param\.np_xi=16 \(partitioning\.n_procs_x\) cannot split the 8 xi",
        ):
            _resolve(partitioning={"n_procs_x": 16, "n_procs_y": 16})

    def test_auto_tiling_is_not_checked(self) -> None:
        bp = _resolve(partitioning={"auto_tiling": True, "n_cores": 256}, use_pio=True)
        assert bp.model_settings["cppdefs"]["auto_tiling"] is True


class TestTileSizes:
    """ucla-roms' ``mpi_setup`` (``roms_mpi.F90``) gives each tile
    ``ceil(points / tiles)`` points and takes the surplus from the first and last
    tile; a split that leaves an edge tile with no points is refused.
    """

    @staticmethod
    def _param(llm: int, np_xi: int, mmm: int = 8, np_eta: int = 1) -> dict:
        return {"llm": llm, "np_xi": np_xi, "mmm": mmm, "np_eta": np_eta}

    def test_sixteen_by_sixteen_on_eight_by_eight(self) -> None:
        with pytest.raises(ValueError) as info:
            check_tile_sizes(
                {"llm": 8, "mmm": 8, "np_xi": 16, "np_eta": 16}, auto_tiling=False
            )
        message = str(info.value)
        assert "param.np_xi=16" in message
        assert "param.np_eta=16" in message
        assert "edge tile with -3" in message
        assert "largest np_xi up to 16 that works is 8" in message

    @pytest.mark.parametrize(
        ("llm", "np_xi"),
        [(10, 9), (10, 7), (8, 6), (8, 16)],
    )
    def test_refused(self, llm: int, np_xi: int) -> None:
        """Each leaves an edge tile with zero or fewer points, although only
        (8, 16) has more tiles than points.
        """
        with pytest.raises(ValueError, match="param.np_xi"):
            check_tile_sizes(self._param(llm, np_xi), auto_tiling=False)

    @pytest.mark.parametrize(
        ("llm", "np_xi"),
        [(8, 8), (10, 3), (10, 4), (8, 5), (10, 1), (20, 2), (208, 16), (224, 16)],
    )
    def test_accepted(self, llm: int, np_xi: int) -> None:
        """Uneven tiles are legal; a one-point tile is too (narrower than the
        two-point halo, but not shown to fail by the ucla-roms source).
        """
        check_tile_sizes(self._param(llm, np_xi), auto_tiling=False)

    def test_auto_tiling_skips(self) -> None:
        check_tile_sizes(self._param(8, 16), auto_tiling=True)

    def test_absent_or_malformed_keys_skip(self) -> None:
        check_tile_sizes({"llm": 8}, auto_tiling=False)
        check_tile_sizes({"llm": "x", "np_xi": 16}, auto_tiling=False)


class TestModelReferenceDate:
    def test_time_of_day_refused(self) -> None:
        with pytest.raises(ValidationError, match="has a time of day"):
            RunWindow(
                start_date=datetime(2012, 1, 1),
                end_date=datetime(2012, 1, 2),
                model_reference_date=datetime(2000, 1, 1, 6),
            )

    def test_midnight_loads(self) -> None:
        window = RunWindow(
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            model_reference_date=datetime(2010, 1, 1),
        )
        assert window.model_reference_date == datetime(2010, 1, 1)

    def test_refused_at_resolve(self) -> None:
        with pytest.raises(ValidationError, match="has a time of day"):
            _resolve(model_reference_date=datetime(2000, 1, 1, 6))


class TestShippedFiles:
    """Every Forge blueprint and ModelSpec shipped with C-Star still passes."""

    @pytest.mark.parametrize(
        "path", _FORGE_BLUEPRINTS, ids=lambda p: str(p.relative_to(_REPO))
    )
    def test_forge_blueprint_validates(self, path: Path) -> None:
        bp = ForgeBlueprint.from_yaml(path)
        assert (
            validate_run_time_sections(bp.model_settings, roms_ref=_roms_ref(bp)) == []
        )

    def test_blueprints_found(self) -> None:
        assert len(_FORGE_BLUEPRINTS) >= 10

    @pytest.mark.parametrize(
        "model", sorted(p.stem for p in _MODEL_SPECS.glob("*.yaml"))
    )
    @pytest.mark.parametrize("output", ["standard", "daily-restarts"])
    def test_model_spec_resolves(self, model: str, output: str) -> None:
        _resolve(model, output_settings=default_catalog.output_data(output))

    def test_cppdefs_keys_cover_the_template(self) -> None:
        """Every ``cppdefs.<key>`` the bundled template reads is a known key."""
        read = set(re.findall(r"cppdefs\.(\w+)", _TEMPLATE.read_text())) - {"opt"}
        assert read <= CPPDEFS_KEYS, sorted(read - CPPDEFS_KEYS)
