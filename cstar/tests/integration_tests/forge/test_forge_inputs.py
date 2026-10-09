"""Tier 1: forge input generation against real roms-tools and real netCDF output.

Each case runs ``process_forge_blueprint`` once (no ROMS compile, no run) and the tests
inspect what landed under the working directory: the PIO-compatible input files, the
emitted ``roms_marbl`` blueprint, the rendered namelist and cppdefs.

Regenerate the namelist goldens after an intentional change with
``UPDATE_GOLDEN=1 pytest cstar/tests/integration_tests/forge -k golden``, review the
diff, and commit it. Regenerate one case's golden with ``-k "golden and <case>"``.
"""

import os
import re
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import f90nml
import pytest
import xarray as xr
import yaml
from typer.testing import CliRunner

from cstar.applications.forge.engine import process_forge_blueprint
from cstar.applications.forge.host import HostPaths
from cstar.applications.forge.namelist_model import check_cdr_lite_mode_roms
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.cli.blueprint.check import app as check_app
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.serialization import deserialize
from cstar.roms.namelist import namelist_schema_for_ref
from cstar.roms.precheck import (
    applies_to,
    check_output_streams_divide_rst,
    check_restart_period_divisible_by_dt,
)
from cstar.tests.integration_tests.cases import (
    DT,
    FORGE_CASES,
    PINNED_ROMS_REF,
    ROMS_REF,
    RUN_END,
    RUN_START,
)

if TYPE_CHECKING:
    from cstar.applications.forge.blueprint import ForgeBlueprint
    from cstar.applications.forge.engine import ForgeBlueprintExecutor
    from cstar.roms.namelist import RomsNamelistBase

pytestmark = pytest.mark.skipif(
    shutil.which("nccopy") is None,
    reason="nccopy is required to convert use_pio inputs to CDF-5",
)

FIXTURES = Path(__file__).parent / "fixtures"
CDF5_MAGIC = b"CDF\x05"
RESTART_PERIOD = 1800.0
"""The `output_period_rst` of the ``test-minimal`` OutputSpec, in seconds."""


@dataclass(frozen=True)
class Expectation:
    """What differs between the cases: BGC inputs, NHY/NOX forcing, and the build mode."""

    bgc_surface: bool
    bgc_input_stems: tuple[str, ...]
    nhy_nox: bool
    marbl: bool
    """Whether MARBL is built (``code.marbl``, ``#define MARBL``)."""
    cdr_lite: bool
    """Whether the ``cdr_lite`` build mode is on (``#define CDR_LITE``)."""


EXPECTATIONS = {
    "unified": Expectation(
        bgc_surface=True,
        bgc_input_stems=("surface-bgc", "boundary-bgc"),
        nhy_nox=True,
        marbl=True,
        cdr_lite=False,
    ),
    "constants": Expectation(
        bgc_surface=False,
        bgc_input_stems=("boundary-bgc",),
        nhy_nox=False,
        marbl=True,
        cdr_lite=False,
    ),
    "cdr_lite": Expectation(
        bgc_surface=False,
        bgc_input_stems=(),
        nhy_nox=False,
        marbl=False,
        cdr_lite=True,
    ),
}


@dataclass(frozen=True)
class Run:
    """The outcome of one ``process_forge_blueprint`` call."""

    case_name: str
    cfg: "ForgeBlueprint"
    forge_yaml: Path
    host: HostPaths
    executor: "ForgeBlueprintExecutor"

    @property
    def working_dir(self) -> Path:
        """Root of everything the run produced."""
        return self.host.working_dir

    @property
    def input_files(self) -> list[Path]:
        """All generated input netCDFs, sorted."""
        return sorted((self.working_dir / "input_data").glob("*.nc"))

    @property
    def blueprint_path(self) -> Path:
        """The emitted ``roms_marbl`` blueprint."""
        return self.working_dir / "blueprints" / f"B_{self.cfg.name}.yaml"


def _generate(
    case_name: str,
    working_dir: Path,
    factory: Callable[..., tuple["ForgeBlueprint", Path]],
) -> Run:
    """Resolve a case and run forge's generation (and configuration) into a directory.

    A ``cdr_lite`` case is skipped when ``ROMS_REF`` (the weekly matrix's
    ``CSTAR_IT_ROMS_REF``) is older than the mode supports.
    """
    if FORGE_CASES[case_name].bgc_mode == "cdr_lite":
        try:
            check_cdr_lite_mode_roms(ROMS_REF)
        except ValueError as exc:
            pytest.skip(str(exc))
    cfg, forge_yaml = factory(case_name, working_dir)
    host = HostPaths(
        working_dir=working_dir,
        source_data_cache=working_dir / "source_cache",
        system="test",
    )
    executor = process_forge_blueprint(cfg, host=host, use_dask=False)
    return Run(case_name, cfg, forge_yaml, host, executor)


@pytest.fixture(scope="module", params=sorted(EXPECTATIONS))
def run(
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
    forge_blueprint_factory: Callable[..., tuple["ForgeBlueprint", Path]],
) -> Run:
    """One generation per case, shared by every test of this module."""
    return _generate(
        request.param, tmp_path_factory.mktemp(request.param), forge_blueprint_factory
    )


@pytest.fixture
def expect(run: Run) -> Expectation:
    """The per-case expectations for ``run``."""
    return EXPECTATIONS[run.case_name]


def _pio_problems(path: Path) -> list[str]:
    """Mirror ``ROMSInputDataset.check_nc_pio_compatible`` for one file."""
    header = path.read_bytes()[:4]
    if header[:3] != b"CDF":
        return [f"{path.name}: not a classic-format netCDF file ({header!r})"]
    with xr.open_dataset(path, decode_times=False) as ds:
        return [
            f"{path.name}: variable {name!r} has dtype {var.dtype}"
            for name, var in ds.variables.items()
            if var.dtype.name.startswith(("int64", "uint"))
        ]


def _load_blueprint(path: Path) -> RomsMarblBlueprint:
    return deserialize(path, RomsMarblBlueprint)


def _normalize(text: str, working_dir: Path) -> str:
    """Replace the (possibly symlink-resolved) working dir so output is host-independent."""
    for root in {str(working_dir.resolve()), str(working_dir)}:
        text = text.replace(root, "<WORKDIR>")
    return text


def _read_namelist(run: Run) -> "RomsNamelistBase":
    bp = _load_blueprint(run.blueprint_path)
    schema = namelist_schema_for_ref(bp.code.roms.commit)
    return schema.read(run.working_dir / "builds" / "run-time" / "namelist.nml")


def _cppdefs(run: Run) -> str:
    return (run.working_dir / "builds" / "compile-time" / "cppdefs.opt").read_text()


def _raw_namelist(run: Run) -> f90nml.Namelist:
    """The rendered namelist as written, without the typed schema's defaults."""
    return f90nml.read(str(run.working_dir / "builds" / "run-time" / "namelist.nml"))


class TestInputs:
    """The generated input netCDFs."""

    def test_expected_files_exist(self, run: Run, expect: Expectation) -> None:
        names = [p.name for p in run.input_files]
        for stem in (
            "_grid.nc",
            "_initial_conditions.nc",
            "_surface-physics_",
            "_boundary-physics_",
            *expect.bgc_input_stems,
        ):
            assert any(stem in name for name in names), f"no {stem!r} file in {names}"
        assert any("surface-bgc" in n for n in names) is expect.bgc_surface

    def test_no_intermediate_nc4_files(self, run: Run) -> None:
        assert not list((run.working_dir / "input_data").glob("*_nc4.nc"))

    def test_cdf5_format(self, run: Run) -> None:
        assert run.input_files
        headers = {p.name: p.read_bytes()[:4] for p in run.input_files}
        assert all(h == CDF5_MAGIC for h in headers.values()), headers

    def test_pio_compatible_dtypes(self, run: Run) -> None:
        problems = [p for f in run.input_files for p in _pio_problems(f)]
        assert not problems


class TestBlueprint:
    """The emitted ``roms_marbl`` blueprint and its sidecar."""

    def test_blueprint_loads(self, run: Run) -> None:
        assert run.blueprint_path.is_file()
        bp = _load_blueprint(run.blueprint_path)
        assert bp.application == "roms_marbl"
        assert bp.code.roms.commit == ROMS_REF
        assert bp.partitioning.use_pio is True
        assert (bp.partitioning.n_procs_x, bp.partitioning.n_procs_y) == (2, 2)
        # the forge run that produced it, derived from the forge blueprint it read
        assert bp.provenance.generated_by is not None
        assert bp.provenance.generated_by.tool == "forge"
        assert bp.provenance.generated_by.working_dir == str(run.working_dir)
        producer = bp.provenance.derived_from[0]
        assert isinstance(producer, BlueprintIdentity)
        assert (producer.application, producer.name) == ("forge", run.cfg.name)
        assert producer.content_hash == run.cfg.content_hash()

    def test_marbl_code(self, run: Run, expect: Expectation) -> None:
        bp = _load_blueprint(run.blueprint_path)
        assert (bp.code.marbl is not None) is expect.marbl

    def test_settings_sidecar_exists(self, run: Run) -> None:
        sidecar = run.working_dir / "blueprints" / f"settings_B_{run.cfg.name}.yaml"
        assert sidecar.is_file()

    def test_data_locations_exist_under_input_data(self, run: Run) -> None:
        bp = _load_blueprint(run.blueprint_path)
        input_dir = (run.working_dir / "input_data").resolve()
        datasets = [
            bp.grid,
            bp.initial_conditions,
            bp.forcing.boundary,
            bp.forcing.surface,
        ]
        locations = [Path(str(r.location)) for ds in datasets for r in ds.data]
        assert locations
        for loc in locations:
            assert loc.is_file(), loc
            assert loc.resolve().parent == input_dir, loc

    def test_blueprint_check_cli(self, run: Run) -> None:
        result = CliRunner().invoke(check_app, [str(run.blueprint_path)])
        assert result.exit_code == 0, result.output

    def test_forge_blueprint_has_content_hash(self, run: Run) -> None:
        data = yaml.safe_load(run.forge_yaml.read_text())
        assert data["provenance"]["content_hash"]


class TestNamelist:
    """The rendered ``namelist.nml``."""

    def test_settings(self, run: Run, expect: Expectation) -> None:
        nml = _read_namelist(run)
        assert nml.time_stepping.dt == DT
        assert nml.time_stepping.ntimes == round(
            (RUN_END - RUN_START).total_seconds() / DT
        )
        assert nml.basic_output_settings.output_period_rst == RESTART_PERIOD
        assert nml.surf_frc_settings.interp_bulk_frc is False

        assert "pio_settings" in _raw_namelist(run)

        frcfiles = [Path(f).name for f in nml.forcing_files.frcfiles]
        assert any("surface-physics" in f for f in frcfiles)
        assert any("surface-bgc" in f for f in frcfiles) is expect.bgc_surface

    def test_matches_golden(self, run: Run) -> None:
        if ROMS_REF != PINNED_ROMS_REF:
            pytest.skip("namelist goldens are generated for the pinned ucla-roms ref")
        namelist = run.working_dir / "builds" / "run-time" / "namelist.nml"
        normalized = _normalize(namelist.read_text(), run.working_dir)
        golden = FIXTURES / f"golden_namelist_{run.case_name}.nml"

        if os.environ.get("UPDATE_GOLDEN"):
            golden.write_text(normalized)
            pytest.fail(
                f"UPDATE_GOLDEN=1: wrote {golden}. Review the diff and commit it, "
                "then rerun without UPDATE_GOLDEN to confirm the test passes."
            )

        assert normalized == golden.read_text(), (
            f"Rendered namelist drifted from {golden}. If intentional, regenerate with "
            "UPDATE_GOLDEN=1 pytest cstar/tests/integration_tests/forge -k golden, "
            "review the diff and commit it."
        )

    def test_precheck_passes(self, run: Run) -> None:
        nml = _read_namelist(run)
        settings = yaml.safe_load(
            (
                run.working_dir / "blueprints" / f"settings_B_{run.cfg.name}.yaml"
            ).read_text()
        )
        check_restart_period_divisible_by_dt(nml)
        if applies_to(type(nml)):
            check_output_streams_divide_rst(nml, settings["compile_time"]["cppdefs"])


class TestCppdefs:
    """The rendered ``cppdefs.opt``."""

    def test_flags(self, run: Run, expect: Expectation) -> None:
        text = _cppdefs(run)
        assert re.search(r"^#define PARALLEL_IO\b", text, re.MULTILINE)
        for keys, enabled in (
            (("MARBL",), expect.marbl),
            (("CDR_LITE",), expect.cdr_lite),
            (("NHY_FORCING", "NOX_FORCING"), expect.nhy_nox),
        ):
            verb = "define" if enabled else "undef"
            for key in keys:
                assert re.search(rf"^#{verb} {key}\b", text, re.MULTILINE), key


@pytest.mark.parametrize("run", ["cdr_lite"], indirect=True)
class TestCdrLite:
    """``bgc_mode: cdr_lite``: ucla-roms' CDR-lite tracers in place of MARBL."""

    TRACERS = ("temp", "salt", "CDR_OAE_ALK1", "CDR_OAE_DIC1", "CDR_DOR_DIC1")
    """The tracer axis of ``CDR_LITE_FORCING``: one OAE release (an ALK/DIC pair)
    and one DOR release."""

    def test_tracer_counts(self, run: Run) -> None:
        nml = _raw_namelist(run)
        assert nml["param_settings"]["nt_bgc"] == 0
        assert nml["param_settings"]["nt_passive"] == 0
        assert nml["param_settings"]["nt_cdr_oae"] == 1
        assert nml["param_settings"]["nt_cdr_dor"] == 1

    def test_cdr_namelist_switches(self, run: Run) -> None:
        nml = _raw_namelist(run)
        assert nml["cdr_frc_settings"]["cdr_source"] is True
        assert nml["cdr_lite_output_settings"]["do_cdr_lite_output"] is True
        assert nml["cdr_output_settings"]["do_cdr_output"] is False

    def test_cppdefs_cdr_forcing(self, run: Run) -> None:
        assert re.search(r"^#define CDR_FORCING\b", _cppdefs(run), re.MULTILINE)

    def test_carbonate_sensitivity_left_to_directive(self, run: Run) -> None:
        bp = _load_blueprint(run.blueprint_path)
        # supplied at run time by the carbonate-sensitivity-from directive
        assert bp.forcing.carbonate_sensitivity is None

    def test_cdr_forcing_tracer_axis_matches_namelist(self, run: Run) -> None:
        bp = _load_blueprint(run.blueprint_path)
        assert bp.cdr_forcing is not None
        cdr_file = Path(str(bp.cdr_forcing.data[0].location))
        # the suite-wide CDF-5 and PIO-dtype checks run on every ``input_files`` entry
        assert cdr_file.resolve() in {f.resolve() for f in run.input_files}
        param = _raw_namelist(run)["param_settings"]
        n_tracers = (
            2 + param["nt_passive"] + 2 * param["nt_cdr_oae"] + param["nt_cdr_dor"]
        )
        with xr.open_dataset(cdr_file, decode_times=False) as ds:
            assert tuple(str(n) for n in ds["tracer_name"].values) == self.TRACERS
            assert ds.sizes["ntracers"] == n_tracers == len(self.TRACERS)
            assert ds["cdr_trcflx"].sizes["ntracers"] == n_tracers


@pytest.mark.parametrize("case_name", ["unified", "cdr_lite"])
def test_rerun_reuses_inputs(
    case_name: str,
    tmp_path: Path,
    forge_blueprint_factory: Callable[..., tuple["ForgeBlueprint", Path]],
) -> None:
    """A second call on the same working directory leaves the inputs alone and
    re-emits the blueprint, changed only by the provenance of the new run.
    """
    first = _generate(case_name, tmp_path, forge_blueprint_factory)
    mtimes = {p: p.stat().st_mtime_ns for p in first.input_files}
    blueprint = yaml.safe_load(first.blueprint_path.read_text())
    assert mtimes

    process_forge_blueprint(first.cfg, host=first.host, use_dask=False)

    assert {p: p.stat().st_mtime_ns for p in first.input_files} == mtimes
    rerun = yaml.safe_load(first.blueprint_path.read_text())
    first_provenance = blueprint.pop("provenance")
    rerun_provenance = rerun.pop("provenance")
    assert rerun == blueprint
    assert rerun_provenance["derived_from"] == first_provenance["derived_from"]
    assert (
        rerun_provenance["generated_by"]["id"] != first_provenance["generated_by"]["id"]
    )
