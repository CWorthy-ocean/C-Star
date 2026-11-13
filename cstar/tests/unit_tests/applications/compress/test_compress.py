"""Tests for run compression: the netCDF engine, file-type detection, the
compress blueprint, and `cstar run compress`.
"""

import contextlib
import logging
import typing as t
from collections.abc import Iterable, Iterator
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import netCDF4
import numpy as np
import pytest
import yaml
from typer.testing import CliRunner

from cstar.applications.compress import run as run_mod
from cstar.applications.compress.app import CompressRunner
from cstar.applications.compress.blueprint import (
    CompressBlueprint,
    FileTypeMatch,
    FileTypeSettings,
)
from cstar.applications.compress.detect import (
    Candidate,
    FileTypeDetector,
    filename_template,
)
from cstar.applications.compress.run import (
    ORIGINALS_DIR_NAME,
    PARTIAL_SUFFIX,
    REPORT_NAME,
    FileJob,
    _compress_one,
    compress_run,
    login_node_warning,
    settings_for,
)
from cstar.applications.core import ArtifactKind, RunnerRequest, get_application
from cstar.base.netcdf_compress import (
    ChunkSpec,
    Encoding,
    compress_netcdf,
    default_chunks,
    matches_any,
    time_dimension,
    verify_compressed,
)
from cstar.cli.run.compress import app
from cstar.entrypoint.config import ServiceConfiguration
from cstar.execution.handler import ExecutionStatus
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
from cstar.orchestration.tracking import Access, WorkplanRun
from cstar.system.manager import OutsideSchedulerJob

START: t.Final = datetime(2026, 1, 1, tzinfo=UTC)
STEP: t.Final = "run"
STEP_DIR: t.Final = "tasks/run"
LOOKUP: t.Final[str] = (
    "cstar.orchestration.index.TrackingRepository.get_workplan_run_sync"
)
"""The tracking lookup the run resolver makes, replaced so a run-id resolves to a record."""


def _write_roms_file(
    path: Path,
    *,
    extra: t.Sequence[str] = (),
    ny: int = 20,
    nx: int = 16,
    records: int = 2,
) -> Path:
    """Write a small CDF-5 file shaped like ROMS `_cdr` output."""
    rng = np.random.default_rng(0)
    with netCDF4.Dataset(path, "w", format="NETCDF3_64BIT_DATA") as ds:
        ds.createDimension("time", None)
        ds.createDimension("s_rho", 4)
        ds.createDimension("eta_rho", ny)
        ds.createDimension("xi_rho", nx)
        ds.createVariable("ocean_time", "f8", ("time",))[:] = (
            np.arange(records) * 86400.0
        )
        for name in ("hDIC", "hDIC_ALT_CO2", "temp", *extra):
            ds.createVariable(name, "f8", ("time", "s_rho", "eta_rho", "xi_rho"))[:] = (
                2000.0 + rng.random((records, 4, ny, nx))
            )
        ds.createVariable("FG_CO2", "f8", ("time", "eta_rho", "xi_rho"))[:] = (
            rng.random((records, ny, nx))
        )
    return path


def _build_run(
    root: Path,
    files: dict[str, t.Callable[[Path], Path]],
    *,
    kinds: dict[str, str] | None = None,
    record_products: bool = True,
) -> Path:
    """Build a finished one-step `ucla_roms` run whose step record lists `files`."""
    step_root = root / STEP_DIR
    (step_root / "output").mkdir(parents=True)
    products = []
    for relative, write in files.items():
        path = write(step_root / relative)
        kind = (kinds or {}).get(relative, "output")
        products.append(
            Product(kind=kind, path=relative, size_bytes=path.stat().st_size)
        )
    (step_root / "notes.txt").write_text("not netCDF")
    write_step_record(
        step_root,
        StepRecord(
            run_id="r1",
            step=STEP,
            application="ucla_roms",
            blueprint=BlueprintIdentity(
                kind="Blueprint", application="ucla_roms", name="bp"
            ),
            outcome="completed",
            finished_at=START,
            products=products if record_products else [],
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
            steps=[
                StepEntry(name=STEP, application="roms_marbl", working_dir=STEP_DIR)
            ],
        ),
    )
    return root


@pytest.fixture(autouse=True)
def _no_manifest_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the lock (which lives in the user's state directory) out of these tests."""
    monkeypatch.setattr(
        run_mod, "manifest_lock", lambda run_id: contextlib.nullcontext()
    )


RUN_FILES: t.Final[dict[str, t.Callable[[Path], Path]]] = {
    "output/output_cdr.20140301000000.nc": _write_roms_file,
    "output/output_cdr.20140302000000.nc": _write_roms_file,
    "output/output_rst.20140302000000.nc": lambda p: _write_roms_file(p, extra=("u",)),
}


def _one_d(path: Path) -> Path:
    with netCDF4.Dataset(path, "w", format="NETCDF3_64BIT_DATA") as ds:
        ds.createDimension("time", None)
        ds.createVariable("x", "f8", ("time",))[:] = [1.0]
    return path


@pytest.fixture
def run_root(tmp_path: Path) -> Path:
    """A run with two `_cdr` files, a restart (recorded as such) and a file with no spatial variables."""
    return _build_run(
        tmp_path / "run",
        {**RUN_FILES, "output/output_scalar.20140302000000.nc": _one_d},
        kinds={"output/output_rst.20140302000000.nc": ArtifactKind.restart.value},
    )


def invoke(
    *args: str, record: WorkplanRun | None = None, input: str | None = None
) -> t.Any:
    """Run `cstar run compress` with the tracking lookup answering `record`."""
    with mock.patch(LOOKUP, mock.Mock(return_value=record)):
        return CliRunner().invoke(app, list(args), input=input)


class TestEngine:
    def test_protected_exact_reduced_within_tolerance(self, tmp_path: Path) -> None:
        src = _write_roms_file(tmp_path / "src.nc")
        enc = Encoding(
            float32=True, significant_digits=4, protected=("hDIC*", "FG_CO2")
        )
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
        assert (
            default_chunks((100, 1346, 674), 4)[0] == 5
        )  # sqrt(100*262144/(1346*674)) ~ 5.4

    def test_verify_reports_a_changed_value(self, tmp_path: Path) -> None:
        src = _write_roms_file(tmp_path / "src.nc")
        out = tmp_path / "out.nc"
        compress_netcdf(src, out, Encoding())
        with netCDF4.Dataset(out, "a") as d:
            d["hDIC"][0, 0, 0, 0] = 0.0
        assert verify_compressed(src, out, Encoding()) == [
            "hDIC: values differ at record 0"
        ]

    def test_typed_attributes_are_cast_with_the_variable(self, tmp_path: Path) -> None:
        src = _write_roms_file(tmp_path / "src.nc")
        with netCDF4.Dataset(src, "a") as ds:
            for name in ("temp", "hDIC"):
                ds[name].setncatts(
                    {"missing_value": 1e20, "valid_range": [-5.0, 40.0], "units": "C"}
                )
        out = tmp_path / "out.nc"
        compress_netcdf(src, out, Encoding(float32=True, protected=("hDIC*",)))

        with netCDF4.Dataset(out) as d:
            d.set_auto_maskandscale(False)
            assert d["temp"].getncattr("missing_value").dtype == np.float32
            assert d["temp"].getncattr("valid_range").dtype == np.float32
            assert d["temp"].getncattr("units") == "C"
            assert d["hDIC"].getncattr("missing_value").dtype == np.float64

    def test_a_fixed_size_dimension_with_cf_time_units_is_the_record_dimension(
        self, tmp_path: Path
    ) -> None:
        """roms-tools boundary files have a fixed-size `bry_time`; records are
        still copied one at a time and chunked one per record.
        """
        src = tmp_path / "bry.nc"
        with netCDF4.Dataset(src, "w", format="NETCDF3_64BIT_DATA") as ds:
            ds.createDimension("bry_time", 3)
            ds.createDimension("s_rho", 4)
            ds.createDimension("xi_rho", 16)
            ds.createVariable("bry_time", "f8", ("bry_time",)).units = "days"
            abs_time = ds.createVariable("abs_time", "f8", ("bry_time",))
            abs_time.units = "hours since 1950-01-01"
            ds.createVariable("temp_south", "f8", ("bry_time", "s_rho", "xi_rho"))[
                :
            ] = np.ones((3, 4, 16))
        out = tmp_path / "out.nc"
        compress_netcdf(src, out, Encoding())

        with netCDF4.Dataset(src) as s, netCDF4.Dataset(out) as d:
            assert time_dimension(s) == "bry_time"
            assert d["temp_south"].chunking()[0] == 1

    def test_a_file_without_time_has_no_record_dimension(self, tmp_path: Path) -> None:
        path = tmp_path / "grid.nc"
        with netCDF4.Dataset(path, "w") as ds:
            ds.createDimension("eta_rho", 2)
            ds.createVariable("h", "f8", ("eta_rho",)).units = "meter"
        with netCDF4.Dataset(path) as ds:
            assert time_dimension(ds) is None


class TestDetector:
    def test_template_replaces_timestamps_and_ranks(self) -> None:
        assert (
            filename_template("output/output_cdr.20140301000000.nc")
            == "output/output_cdr.*.nc"
        )
        assert (
            filename_template("output_his.20140301000000.0012.nc")
            == "output_his.*.*.nc"
        )
        assert filename_template("grid_201401.nc") == "grid_*.nc"

    def test_groups_by_template_and_contents(self, tmp_path: Path) -> None:
        """A template with two variable sets splits; the type with more variables comes first."""
        files = [
            _write_roms_file(tmp_path / "output_his.20140101000000.nc"),
            _write_roms_file(tmp_path / "output_his.20140102000000.nc"),
            _write_roms_file(
                tmp_path / "output_his.20140103000000.nc", extra=("salt",)
            ),
        ]
        detector = FileTypeDetector()
        data = detector.data_files(
            Candidate("s", "ucla_roms", tmp_path, f.name, f.stat().st_size)
            for f in files
        )
        types = detector.detect(data)

        assert [(t_.name, len(t_.files)) for t_ in types] == [
            ("output_his-a", 1),
            ("output_his-b", 2),
        ]
        assert types[0].match.requires_variables[0] == "salt"

    def test_variants_sharing_their_first_variables_stay_apart(
        self, tmp_path: Path
    ) -> None:
        """Regression: with more spatial variables than a match requires, the
        variable that tells two variants apart must still be required, and each
        file must take its own type's settings.
        """
        shared = tuple(f"v{i:02d}" for i in range(9))
        a = _write_roms_file(tmp_path / "output_his.20140101000000.nc", extra=shared)
        b = _write_roms_file(
            tmp_path / "output_his.20140102000000.nc", extra=(*shared, "zeta_x")
        )
        detector = FileTypeDetector()
        data = detector.data_files(
            Candidate("s", "ucla_roms", tmp_path, f.name, f.stat().st_size)
            for f in (a, b)
        )
        types = detector.detect(data)
        settings = [
            FileTypeSettings(
                name=t_.name, application="ucla_roms", match=t_.match, compress=i == 0
            )
            for i, t_ in enumerate(types)
        ]

        assert "zeta_x" in types[0].match.requires_variables
        assert types[0].match.requires_variables != types[1].match.requires_variables
        chosen = {
            f.relative_path: getattr(settings_for(f, settings), "name", None)
            for f in data
        }
        assert chosen == {a.name: "output_his-b", b.name: "output_his-a"}
        # the order of the settings does not decide: the most specific match wins
        assert settings_for(data[1], list(reversed(settings))) is settings[0]

    def test_match_ignores_dimension_sizes(self, tmp_path: Path) -> None:
        other = _write_roms_file(tmp_path / "output_cdr.20150101000000.nc", ny=7, nx=5)
        header = FileTypeDetector().read_header(other)
        match = FileTypeMatch(
            filename="output_cdr.*.nc", requires_variables=["hDIC", "temp"]
        )
        assert match.matches(other.name, header)
        assert not match.matches(other.name.replace("cdr", "his"), header)

    def test_largest_record_bytes(self, tmp_path: Path) -> None:
        header = FileTypeDetector().read_header(_write_roms_file(tmp_path / "f.nc"))
        assert header is not None
        assert header.largest_record_bytes() == 4 * 20 * 16 * 8


def _settings(name: str, glob: str, **kwargs: t.Any) -> FileTypeSettings:
    return FileTypeSettings(
        name=name,
        application="ucla_roms",
        match=FileTypeMatch(filename=glob),
        **kwargs,
    )


class TestBlueprint:
    def test_significant_digits_need_float32(self) -> None:
        with pytest.raises(ValueError, match="significant_digits requires float32"):
            _settings("t", "*.nc", significant_digits=3)


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
        assert (
            "compression" not in sizes["output/output_rst.20140302000000.nc"].metadata
        )
        kept = (
            run_root / ORIGINALS_DIR_NAME / STEP / "output/output_cdr.20140301000000.nc"
        )
        assert kept.is_file()
        report = yaml.safe_load((run_root / REPORT_NAME).read_text())
        assert report["summary"]["compressed"] == 2
        assert "first and last record" in report["summary"]["verified"]

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

    def test_a_step_without_products_is_walked(self, tmp_path: Path) -> None:
        root = _build_run(tmp_path / "old", RUN_FILES, record_products=False)
        bp = CompressBlueprint(
            name="c",
            description="c",
            run=root,
            file_types=[_settings("cdr", "output/output_cdr.*.nc", compress=True)],
        )
        outcomes = compress_run(bp)
        assert sorted(o.relative_path for o in outcomes) == sorted(
            k for k in RUN_FILES if "cdr" in k
        )
        assert all(o.status == "compressed" for o in outcomes)

    def test_a_failure_moving_the_original_is_a_failed_outcome(
        self, run_root: Path
    ) -> None:
        """Regression: an error after the copy is written must not escape the
        worker; the file is kept, its partial removed, and the outcome reported.
        """
        blocker = run_root / "blocker"
        blocker.write_text("a file where the originals directory should be")
        job = FileJob(
            step=STEP,
            step_root=run_root / STEP_DIR,
            relative_path="output/output_cdr.20140301000000.nc",
            file_type="cdr",
            encoding=Encoding(),
            originals_dir=blocker,
        )
        outcome = _compress_one(job)

        assert outcome.status == "failed"
        assert outcome.problems and "NotADirectoryError" in outcome.problems[0]
        assert not list((run_root / STEP_DIR).rglob("*.partial"))
        with netCDF4.Dataset(run_root / STEP_DIR / job.relative_path) as ds:
            assert ds.data_model == "NETCDF3_64BIT_DATA"  # the original, untouched

    def test_outcomes_are_recorded_when_the_pool_breaks(
        self, run_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: what finished before the pool broke is recorded and reported."""
        real = run_mod._compress_one

        class SerialPool:
            """Runs each job on submit; the second job's worker 'dies'."""

            def __init__(self, max_workers: int, mp_context: t.Any = None) -> None:
                self.n = 0

            def __enter__(self) -> "SerialPool":
                return self

            def __exit__(self, *exc: object) -> None: ...

            def submit(self, fn: t.Any, job: t.Any) -> Future[t.Any]:
                self.n += 1
                future: Future[t.Any] = Future()
                if self.n == 2:
                    future.set_exception(
                        RuntimeError("A process in the pool was terminated")
                    )
                else:
                    future.set_result(real(job))
                return future

        monkeypatch.setattr(run_mod, "ProcessPoolExecutor", SerialPool)
        monkeypatch.setattr(run_mod, "as_completed", iter)  # submission order
        bp = CompressBlueprint(
            name="c",
            description="c",
            run=run_root,
            file_types=[_settings("cdr", "output/output_cdr.*.nc", compress=True)],
        )
        with pytest.raises(RuntimeError, match="terminated"):
            compress_run(bp)

        report = yaml.safe_load((run_root / REPORT_NAME).read_text())
        assert report["summary"]["compressed"] == 1
        record = read_step_record(run_root / STEP_DIR)
        assert record is not None
        assert len([p for p in record.products if "compression" in p.metadata]) == 1


class TestCli:
    def test_dry_run_writes_the_blueprint(self, run_root: Path) -> None:
        result = invoke(str(run_root), "--yes", "--now", "--dry-run")

        assert result.exit_code == 0, result.output
        assert "output_scalar" not in result.output  # no 2-D/3-D variables
        assert (
            "other runs read these as input" in result.output
        )  # the restart, by its kind
        assert "per worker" in result.output
        (bp_path,) = (run_root / "compress").glob("B_*.yaml")
        bp = yaml.safe_load(bp_path.read_text())
        by_name = {f["name"]: f for f in bp["file_types"]}
        assert by_name["output_cdr"]["compress"] is True
        assert by_name["output_rst"]["compress"] is False
        assert "hDIC" in by_name["output_cdr"]["protected"]

    def test_input_like_falls_back_to_the_name_without_kinds(
        self, tmp_path: Path
    ) -> None:
        root = _build_run(tmp_path / "old", RUN_FILES, record_products=False)
        result = invoke(str(root), "--yes", "--now", "--dry-run")

        assert result.exit_code == 0, result.output
        (bp_path,) = (root / "compress").glob("B_*.yaml")
        by_name = {
            f["name"]: f for f in yaml.safe_load(bp_path.read_text())["file_types"]
        }
        assert by_name["output_rst"]["compress"] is False
        assert by_name["output_cdr"]["compress"] is True

    def test_questions_then_blueprint_reuse(self, run_root: Path) -> None:
        # output_cdr: compress, float32, quantize, 3 digits, default chunks;
        # output_rst: no; then keep originals, files at once = 1
        answers = "y\ny\ny\n3\n\nn\nn\n1\n"
        result = invoke(str(run_root), "--now", input=answers)

        assert result.exit_code == 0, result.output
        (bp_path,) = (run_root / "compress").glob("B_*.yaml")
        by_name = {
            f["name"]: f for f in yaml.safe_load(bp_path.read_text())["file_types"]
        }
        assert by_name["output_cdr"]["significant_digits"] == 3
        with netCDF4.Dataset(
            run_root / STEP_DIR / "output/output_cdr.20140301000000.nc"
        ) as d:
            assert d["temp"].dtype == np.float32

        replay = invoke(
            str(run_root), "--blueprint", str(bp_path), "--now", "--dry-run"
        )
        assert replay.exit_code == 0, replay.output
        assert "settings from" in replay.output

    def test_submit_writes_a_workplan_with_slurm_resources(
        self, run_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CSTAR_SLURM_QUEUE", "shared")
        result = invoke(str(run_root), "--yes", "--submit", "--dry-run")

        assert result.exit_code == 0, result.output
        assert "for 8" in result.output  # the memory line names the workers
        (wp_path,) = (run_root / "compress").glob("W_*.yaml")
        step = yaml.safe_load(wp_path.read_text())["steps"][0]
        assert step["application"] == "compress"
        assert step["compute_overrides"]["slurm"]["queue_name"] == "shared"
        assert step["compute_overrides"]["slurm"]["num_cpus"] == 8

    def test_a_run_id_resolves_through_tracking(self, run_root: Path) -> None:
        record = WorkplanRun(output_path=run_root, run_id="r1", attached=True)
        result = invoke("r1", "--yes", "--now", "--dry-run", record=record)

        assert result.exit_code == 0, result.output
        assert list((run_root / "compress").glob("B_*.yaml"))

    def test_a_run_attached_read_only_is_refused(self, run_root: Path) -> None:
        """Regression: the command writes into the run, so it goes through `require_writable`."""
        record = WorkplanRun(
            output_path=run_root, run_id="r1", attached=True, access=Access.read_only
        )
        result = invoke("r1", "--yes", "--now", record=record)

        assert result.exit_code != 0
        assert "is attached read-only" in result.output
        assert not (run_root / "compress").exists()
        with netCDF4.Dataset(
            run_root / STEP_DIR / "output/output_cdr.20140301000000.nc"
        ) as d:
            assert d.data_model == "NETCDF3_64BIT_DATA"

    def test_an_unknown_application_protects_nothing_and_says_so(
        self, run_root: Path
    ) -> None:
        manifest_path = run_root / "cstar-run.yaml"
        manifest_path.write_text(
            manifest_path.read_text().replace("roms_marbl", "mystery_app")
        )
        result = invoke(str(run_root), "--yes", "--now", "--dry-run")

        assert result.exit_code == 0, result.output
        assert "'mystery_app' is not known to this build" in result.output
        assert "0 protected" in result.output


class TestReviewFollowUps:
    """Regressions from the adversarial review of the fixes."""

    def test_a_failed_final_rename_puts_the_original_back(
        self, run_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import os

        real = os.replace
        calls = {"n": 0}

        def flaky(src: t.Any, dst: t.Any) -> None:
            calls["n"] += 1
            if calls["n"] == 2:  # the rename of the partial over the source
                raise OSError(28, "No space left on device")
            real(src, dst)

        monkeypatch.setattr(run_mod.os, "replace", flaky)
        rel = "output/output_cdr.20140301000000.nc"
        job = FileJob(
            step=STEP,
            step_root=run_root / STEP_DIR,
            relative_path=rel,
            file_type="cdr",
            encoding=Encoding(),
            originals_dir=run_root / ORIGINALS_DIR_NAME,
        )
        outcome = _compress_one(job)

        assert outcome.status == "failed"
        assert "No space left" in outcome.problems[0]
        assert (run_root / STEP_DIR / rel).is_file()  # back where it was
        assert not (run_root / ORIGINALS_DIR_NAME / STEP / rel).exists()
        assert not list((run_root / STEP_DIR).rglob("*.partial"))

    def test_a_record_error_still_writes_the_report(
        self, run_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(*args: t.Any, **kwargs: t.Any) -> int:
            raise TimeoutError("lock")

        monkeypatch.setattr(run_mod, "update_products", boom)
        bp = CompressBlueprint(
            name="c",
            description="c",
            run=run_root,
            file_types=[_settings("cdr", "output/output_cdr.*.nc", compress=True)],
        )
        outcomes = compress_run(bp)

        assert all(o.status == "compressed" for o in outcomes)
        assert (
            yaml.safe_load((run_root / REPORT_NAME).read_text())["summary"][
                "compressed"
            ]
            == 2
        )

    def test_files_differing_only_in_non_spatial_variables_are_one_type(
        self, tmp_path: Path
    ) -> None:
        a = _write_roms_file(tmp_path / "output_his.20140101000000.nc")
        b = _write_roms_file(tmp_path / "output_his.20140102000000.nc")
        with netCDF4.Dataset(b, "a") as ds:
            ds.createVariable("q", "f8", ("time",))[:] = [1.0, 2.0]
        detector = FileTypeDetector()
        data = detector.data_files(
            Candidate("s", "ucla_roms", tmp_path, f.name, f.stat().st_size)
            for f in (a, b)
        )
        assert [(t_.name, len(t_.files)) for t_ in detector.detect(data)] == [
            ("output_his", 2)
        ]

    def test_the_estimate_follows_the_most_specific_match(self, tmp_path: Path) -> None:
        from cstar.cli.run.compress import _selected

        a = _write_roms_file(tmp_path / "output_his.20140101000000.nc")
        b = _write_roms_file(tmp_path / "output_his.20140102000000.nc", extra=("salt",))
        detector = FileTypeDetector()
        data = detector.data_files(
            Candidate("s", "ucla_roms", tmp_path, f.name, f.stat().st_size)
            for f in (a, b)
        )
        types = detector.detect(data)  # -a holds salt (b), -b does not (a)
        settings = [
            FileTypeSettings(
                name=t_.name, application="ucla_roms", match=t_.match, compress=i == 1
            )
            for i, t_ in enumerate(types)
        ]
        assert [t_.name for t_ in _selected(types, settings)] == ["output_his-b"]


class TestConsumedOutput:
    """Output another step reads: `_cdrgas` (ROMS forcing) and `_uscl` (the upscaler)."""

    CDRGAS: t.Final[str] = "output/output_cdrgas.20140302000000.nc"

    def _blueprint_settings(self, root: Path) -> dict[str, dict[str, t.Any]]:
        result = invoke(str(root), "--yes", "--now", "--dry-run")
        assert result.exit_code == 0, result.output
        (bp_path,) = (root / "compress").glob("B_*.yaml")
        return {f["name"]: f for f in yaml.safe_load(bp_path.read_text())["file_types"]}

    def test_carbonate_sensitivities_recorded_by_kind_default_to_not_compressed(
        self, tmp_path: Path
    ) -> None:
        """Regression: a CDR-LiTE run reads `_cdrgas` as ROMS forcing."""
        root = _build_run(
            tmp_path / "run",
            {**RUN_FILES, self.CDRGAS: _write_roms_file},
            kinds={self.CDRGAS: ArtifactKind.gas_exchange.value},
        )
        by_name = self._blueprint_settings(root)

        assert by_name["output_cdrgas"]["compress"] is False
        assert by_name["output_cdr"]["compress"] is True

    def test_carbonate_sensitivities_found_by_name_default_to_not_compressed(
        self, tmp_path: Path
    ) -> None:
        """Regression: the walked-run fallback knows `_cdrgas` from its stem, and
        does not mistake `_cdr` for it.
        """
        root = _build_run(
            tmp_path / "old",
            {**RUN_FILES, self.CDRGAS: _write_roms_file},
            record_products=False,
        )
        by_name = self._blueprint_settings(root)

        assert by_name["output_cdrgas"]["compress"] is False
        assert by_name["output_cdr"]["compress"] is True

    @pytest.mark.parametrize(
        "name",
        [
            "ddic_dco2",
            "ddic_dalk",
            "h_north",
            "lat_east",
            "ALK_add_west",
            "DIC_add_south",
            "CDR_OAE_ALK1_add_north",
            "CDR_DOR_DIC2_add_east",
            "avg_begin_time",
        ],
    )
    def test_what_other_steps_read_is_protected(self, name: str) -> None:
        """Regression: the carbonate sensitivities and the upscaler's inputs stay exact."""
        protected = get_application("ucla_roms").protected_variables()
        assert matches_any(name, protected)

    @pytest.mark.parametrize("name", ["temp", "salt", "u", "h", "zeta"])
    def test_ordinary_fields_stay_reducible(self, name: str) -> None:
        protected = get_application("ucla_roms").protected_variables()
        assert not matches_any(name, protected)


class _InProcessPool:
    """Runs each job on submit, in this process."""

    def __init__(self, max_workers: int, mp_context: t.Any = None) -> None: ...

    def __enter__(self) -> "_InProcessPool":
        return self

    def __exit__(self, *exc: object) -> None: ...

    def submit(self, fn: t.Any, job: t.Any) -> Future[t.Any]:
        future: Future[t.Any] = Future()
        future.set_result(fn(job))
        return future


def _cdr_blueprint(
    run_root: Path, glob: str = "output/output_cdr.*.nc"
) -> CompressBlueprint:
    return CompressBlueprint(
        name="c",
        description="c",
        run=run_root,
        file_types=[_settings("cdr", glob, compress=True)],
    )


class TestInterruptedRuns:
    def test_workers_are_spawned(
        self, run_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One start method on every platform and Python version."""
        seen: list[dict[str, t.Any]] = []

        def recording(**kwargs: t.Any) -> ProcessPoolExecutor:
            seen.append(kwargs)
            return ProcessPoolExecutor(**kwargs)

        monkeypatch.setattr(run_mod, "ProcessPoolExecutor", recording)
        compress_run(_cdr_blueprint(run_root))

        assert seen[0]["mp_context"].get_start_method() == "spawn"

    def test_each_file_is_recorded_as_it_finishes(
        self, run_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The first file is on disk in the step record before the second outcome is taken."""
        compressed_when_taken: list[int] = []

        def watching(futures: Iterable[Future[t.Any]]) -> Iterator[Future[t.Any]]:
            for future in futures:
                record = read_step_record(run_root / STEP_DIR)
                assert record is not None
                compressed_when_taken.append(
                    sum("compression" in p.metadata for p in record.products)
                )
                yield future

        monkeypatch.setattr(run_mod, "ProcessPoolExecutor", _InProcessPool)
        monkeypatch.setattr(run_mod, "as_completed", watching)
        compress_run(_cdr_blueprint(run_root))

        assert compressed_when_taken == [0, 1]

    def test_a_failing_record_warns_once_and_the_run_goes_on(
        self, run_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[int] = []

        def broken(root: Path, run_id: str, outcomes: list[t.Any]) -> int:
            calls.append(len(outcomes))
            raise OSError("state directory is read-only")

        warning = mock.Mock()
        monkeypatch.setattr(run_mod, "ProcessPoolExecutor", _InProcessPool)
        monkeypatch.setattr(run_mod, "as_completed", iter)
        monkeypatch.setattr(run_mod, "_record", broken)
        monkeypatch.setattr(run_mod.log, "warning", warning)

        outcomes = compress_run(_cdr_blueprint(run_root))

        assert [o.status for o in outcomes] == ["compressed", "compressed"]
        # one failed attempt as the first file finished, then one at the end:
        # a held lock costs its timeout twice, not once per file
        assert calls == [1, 2]
        assert (run_root / REPORT_NAME).is_file()
        (message,) = (
            c.args[0] for c in warning.call_args_list if "step records" in c.args[0]
        )
        assert "not updated for 2 file(s)" in message
        assert "read-only" in message

    def test_stray_partials_are_removed_at_start(self, run_root: Path) -> None:
        """A killed run leaves `<file>.partial`; a rerun clears it."""
        stray = (
            run_root / STEP_DIR / f"output/output_rst.20140302000000.nc{PARTIAL_SUFFIX}"
        )
        stray.write_bytes(b"half a file")

        compress_run(_cdr_blueprint(run_root))  # the restart is not compressed

        assert not stray.exists()


class TestCliWorkers:
    def test_workers_for_a_local_run(self, run_root: Path) -> None:
        result = invoke(str(run_root), "--yes", "--now", "--workers", "3", "--dry-run")

        assert result.exit_code == 0, result.output
        assert "with 3 worker(s)" in result.output
        (bp_path,) = (run_root / "compress").glob("B_*.yaml")
        assert yaml.safe_load(bp_path.read_text())["workers"] == 3

    def test_workers_are_the_jobs_cores(self, run_root: Path) -> None:
        result = invoke(
            str(run_root), "--yes", "--submit", "--workers", "4", "--dry-run"
        )

        assert result.exit_code == 0, result.output
        (wp_path,) = (run_root / "compress").glob("W_*.yaml")
        step = yaml.safe_load(wp_path.read_text())["steps"][0]
        assert step["compute_overrides"]["slurm"]["num_cpus"] == 4

    def test_workers_override_a_saved_blueprint(self, run_root: Path) -> None:
        invoke(str(run_root), "--yes", "--now", "--workers", "8", "--dry-run")
        (saved,) = (run_root / "compress").glob("B_*.yaml")
        replay = invoke(
            str(run_root),
            "--blueprint",
            str(saved),
            "--now",
            "--workers",
            "2",
            "--dry-run",
        )

        assert replay.exit_code == 0, replay.output
        newest = max(
            (run_root / "compress").glob("B_*.yaml"), key=lambda p: p.stat().st_mtime_ns
        )
        assert yaml.safe_load(newest.read_text())["workers"] == 2


class TestLoginNodeWarning:
    @pytest.mark.usefixtures("login_node")
    def test_the_command_warns_before_the_estimate(self, run_root: Path) -> None:
        result = invoke(str(run_root), "--yes", "--now")

        assert result.exit_code == 0, result.output
        assert "login node of fake_hpc" in result.output
        assert "CSTAR_ALLOW_LOGIN_NODE_RUN" in result.output
        assert result.output.index("login node") < result.output.index("Memory:")

    def test_no_warning_where_a_login_node_is_allowed(self, run_root: Path) -> None:
        """The autouse fixture sets `CSTAR_ALLOW_LOGIN_NODE_RUN=1`."""
        result = invoke(str(run_root), "--yes", "--now")

        assert result.exit_code == 0, result.output
        assert "login node" not in result.output

    @pytest.mark.usefixtures("login_node")
    def test_no_warning_for_a_dry_run(self, run_root: Path) -> None:
        result = invoke(str(run_root), "--yes", "--now", "--dry-run")

        assert "login node" not in result.output

    @pytest.mark.usefixtures("login_node")
    def test_no_warning_when_nothing_is_selected(self, tmp_path: Path) -> None:
        """A run of restarts only: `--yes` compresses nothing, so there is nothing to warn of."""
        rst = "output/output_rst.20140302000000.nc"
        root = _build_run(
            tmp_path / "run",
            {rst: RUN_FILES[rst]},
            kinds={rst: ArtifactKind.restart.value},
        )
        result = invoke(str(root), "--yes", "--now")

        assert result.exit_code == 0, result.output
        assert "Nothing compressed." in result.output
        assert "login node" not in result.output

    def test_an_unrecognised_cluster_is_not_told_to_submit(self) -> None:
        """C-Star cannot submit there, so the warning offers an allocation instead."""
        where = OutsideSchedulerJob(system="generic", unsupported_scheduler="PBS")
        message = login_node_warning(where, "submit it as a job (--submit)")

        assert "PBS scheduler" in message
        assert "--submit" not in message
        assert "inside an allocation" in message

    @pytest.mark.usefixtures("login_node")
    async def test_the_runner_warns(
        self, run_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        request: RunnerRequest[CompressBlueprint] = RunnerRequest(
            "unused://blueprint", CompressBlueprint
        )
        request._bp = _cdr_blueprint(run_root)
        runner = CompressRunner(
            request,
            ServiceConfiguration(
                as_service=False,
                loop_delay=0,
                health_check_frequency=None,
                log_level=logging.DEBUG,
                health_check_log_threshold=10,
                name="test_compress_runner",
            ),
        )
        monkeypatch.setattr(
            "cstar.applications.compress.app.compress_run", lambda bp: []
        )
        warning = mock.Mock()
        monkeypatch.setattr(runner.log, "warning", warning)

        await runner.run()

        assert runner.state.status == ExecutionStatus.COMPLETED
        message = warning.call_args.args[0]
        assert "login node of fake_hpc" in message
        assert "--submit" in message
