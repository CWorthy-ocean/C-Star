"""Tests for run compression: the netCDF engine, file-type detection, the
compress blueprint, and `cstar run compress`."""

import contextlib
import typing as t
from datetime import UTC, datetime
from pathlib import Path

import netCDF4
import numpy as np
import pytest
import yaml
from typer.testing import CliRunner

from cstar.archive import compress as compress_mod
from cstar.archive.compress import (
    ORIGINALS_DIR_NAME,
    REPORT_NAME,
    CompressBlueprint,
    FileTypeSettings,
    compress_run,
)
from cstar.archive.detect import FileTypeDetector, FileTypeMatch, filename_template
from cstar.base.netcdf_compress import (
    ChunkSpec,
    Encoding,
    compress_netcdf,
    default_chunks,
    verify_compressed,
)
from cstar.cli.run.compress import app
from cstar.orchestration.manifest import (
    Attempt,
    Product,
    RunManifest,
    StepEntry,
    StepRecord,
    read_step_record,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity

START: t.Final = datetime(2026, 1, 1, tzinfo=UTC)
STEP: t.Final = "run"
STEP_DIR: t.Final = "tasks/run"


def _write_roms_file(
    path: Path, *, extra: t.Sequence[str] = (), ny: int = 20, nx: int = 16, records: int = 2
) -> Path:
    """Write a small CDF-5 file shaped like ROMS `_cdr` output."""
    rng = np.random.default_rng(0)
    with netCDF4.Dataset(path, "w", format="NETCDF3_64BIT_DATA") as ds:
        ds.createDimension("time", None)
        ds.createDimension("s_rho", 4)
        ds.createDimension("eta_rho", ny)
        ds.createDimension("xi_rho", nx)
        ds.createVariable("ocean_time", "f8", ("time",))[:] = np.arange(records) * 86400.0
        for name in ("hDIC", "hDIC_ALT_CO2", "temp", *extra):
            ds.createVariable(name, "f8", ("time", "s_rho", "eta_rho", "xi_rho"))[:] = (
                2000.0 + rng.random((records, 4, ny, nx))
            )
        ds.createVariable("FG_CO2", "f8", ("time", "eta_rho", "xi_rho"))[:] = rng.random(
            (records, ny, nx)
        )
    return path


def _build_run(root: Path, files: dict[str, t.Callable[[Path], Path]]) -> Path:
    """Build a finished one-step `ucla_roms` run whose step record lists `files`."""
    step_root = root / STEP_DIR
    (step_root / "output").mkdir(parents=True)
    products = []
    for relative, write in files.items():
        path = write(step_root / relative)
        products.append(Product(kind="output", path=relative, size_bytes=path.stat().st_size))
    (step_root / "notes.txt").write_text("not netCDF")
    write_step_record(
        step_root,
        StepRecord(
            run_id="r1",
            step=STEP,
            application="ucla_roms",
            blueprint=BlueprintIdentity(kind="Blueprint", application="ucla_roms", name="bp"),
            outcome="completed",
            finished_at=START,
            products=products,
        ),
    )
    write_run_manifest(
        root,
        RunManifest(
            run_id="r1",
            name="r1",
            kind="workplan",
            created_at=START,
            attempts=[Attempt(start_at=START, finished_at=START)],
            steps=[StepEntry(name=STEP, application="roms_marbl", working_dir=STEP_DIR)],
        ),
    )
    return root


@pytest.fixture(autouse=True)
def _no_manifest_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the lock (which lives in the user's state directory) out of these tests."""
    monkeypatch.setattr(compress_mod, "manifest_lock", lambda run_id: contextlib.nullcontext())


@pytest.fixture
def run_root(tmp_path: Path) -> Path:
    """A run with two `_cdr` files, a restart and a file with no spatial variables."""

    def one_d(path: Path) -> Path:
        with netCDF4.Dataset(path, "w", format="NETCDF3_64BIT_DATA") as ds:
            ds.createDimension("time", None)
            ds.createVariable("x", "f8", ("time",))[:] = [1.0]
        return path

    return _build_run(
        tmp_path / "run",
        {
            "output/output_cdr.20140301000000.nc": _write_roms_file,
            "output/output_cdr.20140302000000.nc": _write_roms_file,
            "output/output_rst.20140302000000.nc": lambda p: _write_roms_file(p, extra=("u",)),
            "output/output_scalar.20140302000000.nc": one_d,
        },
    )


class TestEngine:
    def test_protected_exact_reduced_within_tolerance(self, tmp_path: Path) -> None:
        src = _write_roms_file(tmp_path / "src.nc")
        enc = Encoding(float32=True, significant_digits=4, protected=("hDIC*", "FG_CO2"))
        result = compress_netcdf(src, tmp_path / "out.nc", enc)

        assert result.reduced == ("temp",)
        assert verify_compressed(src, tmp_path / "out.nc", enc) == []
        with netCDF4.Dataset(src) as s, netCDF4.Dataset(tmp_path / "out.nc") as d:
            assert d.data_model == "NETCDF4"
            assert d["temp"].dtype == np.float32
            assert d["ocean_time"].dtype == np.float64  # time is always exact
            np.testing.assert_array_equal(d["hDIC"][:], s["hDIC"][:])
            assert d["hDIC"].filters()["zlib"]

    def test_lossless_keeps_every_value(self, tmp_path: Path) -> None:
        src = _write_roms_file(tmp_path / "src.nc")
        compress_netcdf(src, tmp_path / "out.nc", Encoding())
        with netCDF4.Dataset(src) as s, netCDF4.Dataset(tmp_path / "out.nc") as d:
            for name in s.variables:
                np.testing.assert_array_equal(d[name][:], s[name][:])

    def test_chunk_override_and_default(self, tmp_path: Path) -> None:
        src = _write_roms_file(tmp_path / "src.nc")
        compress_netcdf(src, tmp_path / "out.nc", Encoding(chunks=ChunkSpec(x=8, z=2)))
        with netCDF4.Dataset(tmp_path / "out.nc") as d:
            assert d["temp"].chunking() == [1, 2, 20, 8]
            assert d["FG_CO2"].chunking() == [1, 20, 8]
        assert default_chunks((100, 1346, 674), 4)[0] == 5  # sqrt(100*262144/(1346*674)) ~ 5.4

    def test_verify_reports_a_changed_value(self, tmp_path: Path) -> None:
        src = _write_roms_file(tmp_path / "src.nc")
        out = tmp_path / "out.nc"
        compress_netcdf(src, out, Encoding())
        with netCDF4.Dataset(out, "a") as d:
            d["hDIC"][0, 0, 0, 0] = 0.0
        assert verify_compressed(src, out, Encoding()) == ["hDIC: values differ at record 0"]


class TestDetector:
    def test_template_replaces_timestamps_and_ranks(self) -> None:
        assert filename_template("output/output_cdr.20140301000000.nc") == "output/output_cdr.*.nc"
        assert filename_template("output_his.20140301000000.0012.nc") == "output_his.*.*.nc"
        assert filename_template("grid_201401.nc") == "grid_*.nc"

    def test_groups_by_template_and_contents(self, tmp_path: Path) -> None:
        files = [
            _write_roms_file(tmp_path / "output_his.20140101000000.nc"),
            _write_roms_file(tmp_path / "output_his.20140102000000.nc"),
            _write_roms_file(tmp_path / "output_his.20140103000000.nc", extra=("salt",)),
        ]
        detector = FileTypeDetector()
        data = detector.data_files(
            ("s", "ucla_roms", tmp_path, f.name, f.stat().st_size) for f in files
        )
        types = detector.detect(data)

        assert [(t_.name, len(t_.files)) for t_ in types] == [
            ("output_his-a", 2),
            ("output_his-b", 1),
        ]
        assert "salt" in types[1].match.requires_variables

    def test_match_ignores_dimension_sizes(self, tmp_path: Path) -> None:
        other = _write_roms_file(tmp_path / "output_cdr.20150101000000.nc", ny=7, nx=5)
        header = FileTypeDetector().read_header(other)
        match = FileTypeMatch(filename="output_cdr.*.nc", requires_variables=["hDIC", "temp"])
        assert match.matches(other.name, header)
        assert not match.matches(other.name.replace("cdr", "his"), header)


def _settings(name: str, glob: str, **kwargs: t.Any) -> FileTypeSettings:
    return FileTypeSettings(
        name=name,
        application="ucla_roms",
        match=FileTypeMatch(filename=glob),
        **kwargs,
    )


class TestCompressRun:
    def test_compresses_matching_types_and_records_sizes(self, run_root: Path) -> None:
        bp = CompressBlueprint(
            name="c",
            description="c",
            run=run_root,
            file_types=[
                _settings(
                    "output_cdr",
                    "output/output_cdr.*.nc",
                    compress=True,
                    float32=True,
                    significant_digits=4,
                    protected=["hDIC*", "FG_CO2"],
                ),
                _settings("output_rst", "output/output_rst.*.nc", compress=False),
            ],
            originals="keep",
        )
        outcomes = compress_run(bp)

        assert sorted(o.relative_path for o in outcomes) == [
            "output/output_cdr.20140301000000.nc",
            "output/output_cdr.20140302000000.nc",
        ]
        assert all(o.status == "compressed" and o.reduced == ["temp"] for o in outcomes)
        record = read_step_record(run_root / STEP_DIR)
        assert record is not None
        sizes = {p.path: p for p in record.products}
        cdr = sizes["output/output_cdr.20140301000000.nc"]
        assert cdr.size_bytes == (run_root / STEP_DIR / cdr.path).stat().st_size
        assert cdr.metadata["compression"] == "netcdf4;float32 x1"
        assert "compression" not in sizes["output/output_rst.20140302000000.nc"].metadata
        kept = run_root / ORIGINALS_DIR_NAME / STEP / "output/output_cdr.20140301000000.nc"
        assert kept.is_file()
        report = yaml.safe_load((run_root / REPORT_NAME).read_text())
        assert report["summary"]["compressed"] == 2

        # a second pass finds the files already compressed
        assert compress_run(bp) == []

    def test_delete_originals(self, run_root: Path) -> None:
        bp = CompressBlueprint(
            name="c",
            description="c",
            run=run_root,
            file_types=[_settings("all", "output/*.nc", compress=True)],
            originals="delete",
        )
        compress_run(bp)
        assert not (run_root / ORIGINALS_DIR_NAME).exists()
        assert not list((run_root / STEP_DIR).rglob("*.partial"))


class TestCli:
    def test_dry_run_writes_answers_blueprint(self, run_root: Path) -> None:
        result = CliRunner().invoke(app, [str(run_root), "--yes", "--now", "--dry-run"])

        assert result.exit_code == 0, result.output
        assert "output_scalar" not in result.output  # no 2-D/3-D variables
        assert "other runs read these as input" in result.output  # the restart
        (bp_path,) = (run_root / "compress").glob("B_*.yaml")
        bp = yaml.safe_load(bp_path.read_text())
        by_name = {f["name"]: f for f in bp["file_types"]}
        assert by_name["output_cdr"]["compress"] is True
        assert by_name["output_rst"]["compress"] is False
        assert "hDIC" in by_name["output_cdr"]["protected"]

    def test_questions_then_answers_replay(self, run_root: Path) -> None:
        # output_cdr: compress, float32, quantize, 3 digits, default chunks;
        # output_rst: no; then keep originals, files at once = 1
        answers = "y\ny\ny\n3\n\nn\nn\n1\n"
        result = CliRunner().invoke(app, [str(run_root), "--now"], input=answers)

        assert result.exit_code == 0, result.output
        (bp_path,) = (run_root / "compress").glob("B_*.yaml")
        by_name = {f["name"]: f for f in yaml.safe_load(bp_path.read_text())["file_types"]}
        assert by_name["output_cdr"]["significant_digits"] == 3
        with netCDF4.Dataset(run_root / STEP_DIR / "output/output_cdr.20140301000000.nc") as d:
            assert d["temp"].dtype == np.float32

        replay = CliRunner().invoke(
            app, [str(run_root), "--answers", str(bp_path), "--now", "--dry-run"]
        )
        assert replay.exit_code == 0, replay.output
        assert "settings from" in replay.output

    def test_submit_writes_a_workplan_with_slurm_resources(
        self, run_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CSTAR_SLURM_QUEUE", "shared")
        result = CliRunner().invoke(app, [str(run_root), "--yes", "--submit", "--dry-run"])

        assert result.exit_code == 0, result.output
        (wp_path,) = (run_root / "compress").glob("W_*.yaml")
        step = yaml.safe_load(wp_path.read_text())["steps"][0]
        assert step["application"] == "compress"
        assert step["compute_overrides"]["slurm"]["queue_name"] == "shared"
        assert step["compute_overrides"]["slurm"]["num_cpus"] == 8
