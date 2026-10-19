"""What each application declares and reports as the products of its step."""

import typing as t
from pathlib import Path

import pytest

from cstar.applications.core import ArtifactKind, get_application
from cstar.applications.forge.app import INPUT_ARTIFACTS, input_products
from cstar.applications.roms_marbl.app import (
    OUTPUT_FILE_PATTERN,
    OUTPUT_STREAM_KINDS,
    output_products,
)
from cstar.orchestration.manifest import Product, expand_artifacts

_STAMP = "20120101000000"


def _touch(root: Path, *relative: str) -> None:
    for name in relative:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")


def _expanded(app_name: str, root: Path, **fields: t.Any) -> dict[str, Product]:
    app = get_application(app_name)
    bp = app.blueprint.model_construct(**fields)
    return {p.path: p for p in expand_artifacts(root, app.artifacts(bp))}


class TestRomsMarblArtifacts:
    def test_declares_each_file_the_run_leaves(self, tmp_path: Path) -> None:
        _touch(
            tmp_path,
            f"output/output_rst.{_STAMP}.nc",
            f"output/output_his.{_STAMP}.nc",
            f"output/output_avg.{_STAMP}.nc",
            f"output/output_bgc_his.{_STAMP}.nc",
            f"output/child_bry.{_STAMP}.nc",
            "work/cstar_generated_roms.nml",
            "input/compile_time_code/cppdefs.opt",
            "input/compile_time_code/roms",
            "input/runtime_code/marbl_in",
            "logs/run.out",
            # not products
            f"temp_output/output_rst.{_STAMP}.000.nc",
            "input/input_datasets/x.nc",
        )

        found = _expanded("roms_marbl", tmp_path)

        assert {p: (v.kind, v.metadata) for p, v in found.items()} == {
            f"output/output_rst.{_STAMP}.nc": ("restart", {}),
            f"output/output_his.{_STAMP}.nc": ("history", {}),
            f"output/output_avg.{_STAMP}.nc": ("average", {}),
            f"output/output_bgc_his.{_STAMP}.nc": ("output", {}),
            f"output/child_bry.{_STAMP}.nc": ("output", {}),
            "work/cstar_generated_roms.nml": ("config", {"role": "namelist"}),
            "input/compile_time_code/cppdefs.opt": ("config", {"role": "cppdefs"}),
            "input/runtime_code/marbl_in": ("config", {"role": "marbl_in"}),
            "input/compile_time_code/roms": ("build", {}),
            "logs/run.out": ("log", {}),
        }
        assert all(v.size_bytes == 1 for v in found.values())

    def test_every_stream_in_the_table_is_declared(self) -> None:
        """The hook's table and the declaration are one table."""
        app = get_application("roms_marbl")
        specs = app.artifacts(app.blueprint.model_construct())

        declared = {
            s.pattern: s.kind for s in specs if s.pattern.startswith("output/output_")
        }
        assert declared == {
            f"output/output_{stream}.*.nc": kind
            for stream, kind in OUTPUT_STREAM_KINDS.items()
        }


class TestOutputProducts:
    @pytest.mark.parametrize("use_pio", [True, False])
    def test_stream_timestamp_and_format(self, tmp_path: Path, use_pio: bool) -> None:
        out = tmp_path / "output"
        _touch(tmp_path, f"output/output_rst.{_STAMP}.nc")
        _touch(tmp_path, f"output/output_bgc_his.{_STAMP}.nc")

        products = output_products(out, tmp_path, use_pio=use_pio)

        fmt = "pio" if use_pio else "joined"
        assert [(p.kind, p.path, p.metadata) for p in products] == [
            (
                "output",
                f"output/output_bgc_his.{_STAMP}.nc",
                {"stream": "bgc_his", "timestamp": _STAMP, "format": fmt},
            ),
            (
                "restart",
                f"output/output_rst.{_STAMP}.nc",
                {"stream": "rst", "timestamp": _STAMP, "format": fmt},
            ),
        ]
        assert all(p.size_bytes == 0 for p in products)

    def test_names_that_do_not_match_are_skipped(self, tmp_path: Path) -> None:
        _touch(
            tmp_path,
            f"output/child_bry.{_STAMP}.nc",
            f"output/output_rst.{_STAMP}.000.nc",
            "output/output_rst.nc",
        )

        assert not output_products(tmp_path / "output", tmp_path, use_pio=False)

    def test_files_outside_the_root_are_skipped(self, tmp_path: Path) -> None:
        root = tmp_path / "step"
        root.mkdir()
        _touch(tmp_path, f"elsewhere/output_rst.{_STAMP}.nc")

        assert not output_products(tmp_path / "elsewhere", root, use_pio=False)

    def test_pattern_matches_what_roms_names_a_joined_file(self) -> None:
        assert OUTPUT_FILE_PATTERN.match(f"output_rst.{_STAMP}.nc")
        assert not OUTPUT_FILE_PATTERN.match(f"output_rst.{_STAMP}.000.nc")


_FORGE_NAME = "it v0.1 [x]"
_DOMAIN = "it v0_1 [x]"
_FORGE_STEMS = {
    "grid": ("grid", {}),
    "grid_child": ("grid", {"role": "child"}),
    "nesting": ("grid", {"role": "nesting"}),
    "initial_conditions": ("initial_conditions", {}),
    "boundary-physics": ("boundary_forcing", {}),
    "boundary-bgc-x": ("boundary_forcing", {}),
    "surface-physics": ("surface_forcing", {}),
    "surface-bgc-co2": ("surface_forcing", {}),
    "tidal": ("tidal_forcing", {}),
    "river": ("river_forcing", {}),
    "cdr": ("cdr_forcing", {}),
}


class TestForgeArtifacts:
    def test_declares_each_file_the_run_leaves(self, tmp_path: Path) -> None:
        _touch(
            tmp_path,
            *[f"input_data/{_DOMAIN}_{stem}.nc" for stem in _FORGE_STEMS],
            f"output/B_{_FORGE_NAME}.yaml",
            "builds/compile-time/cppdefs.opt",
            "builds/run-time/namelist.nml",
            "builds/run-time/marbl_in",
            "logs/forge_x.log",
            # not products: another domain's file, a sidecar, a stray output
            "input_data/other_grid.nc",
            f"blueprints/B_{_FORGE_NAME}.yaml",
            "output/stray.yaml",
        )

        found = _expanded("forge", tmp_path, name=_FORGE_NAME)

        expected: dict[str, tuple[str, dict[str, str]]] = {
            f"input_data/{_DOMAIN}_{stem}.nc": kind_metadata
            for stem, kind_metadata in _FORGE_STEMS.items()
        }
        expected |= {
            f"output/B_{_FORGE_NAME}.yaml": ("emitted_blueprint", {}),
            "builds/compile-time/cppdefs.opt": ("config", {"role": "cppdefs"}),
            "builds/run-time/namelist.nml": ("config", {"role": "namelist"}),
            "builds/run-time/marbl_in": ("config", {"role": "marbl_in"}),
            "logs/forge_x.log": ("log", {}),
        }
        assert {p: (v.kind, v.metadata) for p, v in found.items()} == expected

    def test_cdr_stem_is_the_one_input_data_names(self) -> None:
        from cstar.applications.forge.input_data import CDR_FORCING_NETCDF_STEM

        assert CDR_FORCING_NETCDF_STEM in {stem for stem, _, _ in INPUT_ARTIFACTS}


class _FakeExecutor:
    """The executor surface `input_products` reads."""

    def __init__(self, root: Path, name: str, planned: list[Path]) -> None:
        self.run_output_dir = root
        self.name = name
        self._planned = planned

    def _planned_netcdf_outputs(self) -> list[Path]:
        return self._planned


class TestInputProducts:
    def test_classifies_planned_files_by_stem(self, tmp_path: Path) -> None:
        paths = [
            tmp_path / "input_data" / f"{_DOMAIN}_{stem}.nc"
            for stem in ("grid", "grid_child", "boundary-bgc-x", "surprise", "river")
        ]
        missing = tmp_path / "input_data" / f"{_DOMAIN}_tidal.nc"
        unprefixed = tmp_path / "input_data" / "carbonate.nc"
        for path in [*paths, unprefixed]:
            _touch(path.parent, path.name)
        _touch(tmp_path, "blueprints/_grid.yaml")
        executor = _FakeExecutor(tmp_path, _FORGE_NAME, [*paths, missing, unprefixed])

        products = input_products(t.cast("t.Any", executor))

        assert [(p.kind, p.path, p.metadata) for p in products] == [
            (
                "grid",
                f"input_data/{_DOMAIN}_grid.nc",
                {"sidecar": "blueprints/_grid.yaml"},
            ),
            ("grid", f"input_data/{_DOMAIN}_grid_child.nc", {"role": "child"}),
            ("boundary_forcing", f"input_data/{_DOMAIN}_boundary-bgc-x.nc", {}),
            ("river_forcing", f"input_data/{_DOMAIN}_river.nc", {}),
        ]
        assert all(p.size_bytes == 0 for p in products)

    def test_a_sidecar_does_not_leak_into_the_stem_table(self, tmp_path: Path) -> None:
        """A product's metadata is a copy, so adding a sidecar leaves the table alone."""
        path = tmp_path / "input_data" / f"{_DOMAIN}_grid_child.nc"
        _touch(path.parent, path.name)
        _touch(tmp_path, "blueprints/_grid_child.yaml")

        input_products(t.cast("t.Any", _FakeExecutor(tmp_path, _FORGE_NAME, [path])))

        assert dict(INPUT_ARTIFACTS[1][2]) == {"role": "child"}


class TestSmallApplicationArtifacts:
    def test_nest_ic(self, tmp_path: Path) -> None:
        _touch(tmp_path, f"output/ic_from_parent_rst.{_STAMP}.nc", "output/other.nc")

        found = _expanded("nest_ic", tmp_path)

        assert {p: v.kind for p, v in found.items()} == {
            f"output/ic_from_parent_rst.{_STAMP}.nc": "initial_conditions"
        }

    def test_upscaler(self, tmp_path: Path) -> None:
        _touch(tmp_path, "output/upscaled_cdr.nc", "output/other.nc")

        found = _expanded("upscaler", tmp_path)

        assert {p: v.kind for p, v in found.items()} == {
            "output/upscaled_cdr.nc": "cdr_forcing"
        }

    def test_plotter(self, tmp_path: Path) -> None:
        _touch(tmp_path, "temp_s0_t1.png", "notes.txt")

        found = _expanded("plotter", tmp_path)

        assert {p: v.kind for p, v in found.items()} == {
            "temp_s0_t1.png": ArtifactKind.figure.value
        }
