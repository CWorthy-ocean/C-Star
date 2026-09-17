from collections.abc import Callable
from pathlib import Path

import pytest
import xarray as xr
import yaml

from cstar.applications.roms_marbl.file_system import RomsFileSystemManager
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.applications.roms_marbl.resume import (
    RESUME_SUFFIX,
    find_resume_restart,
    prepare_resume_blueprint,
)
from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.serialization import deserialize

RESUME_LOGGER_NAME = "cstar.applications.roms_marbl.resume"
"""The logger name used by `cstar.applications.roms_marbl.resume`."""


def _write_netcdf(path: Path) -> None:
    """Write a tiny, real netCDF file at `path`, creating parents as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    xr.Dataset({"ocean_time": ("time", [0.0])}).to_netcdf(path)


def _write_corrupt(path: Path) -> None:
    """Write bytes that are not valid netCDF at `path`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not netcdf")


def _piece_name(ts: str, seg: str) -> str:
    """Build a partitioned restart file name for timestamp `ts`, segment `seg`."""
    return f"output_rst.{ts}.{seg}.nc"


def _whole_name(ts: str) -> str:
    """Build a whole (unpartitioned) restart file name for timestamp `ts`."""
    return f"output_rst.{ts}.nc"


class TestFindResumeRestart:
    """Tests for `find_resume_restart`."""

    def test_newest_complete_partitioned_set_wins(self, tmp_path: Path) -> None:
        """The newest timestamp with a complete partition set is returned."""
        older, newer = "20200601000000", "20200701000000"
        for ts in (older, newer):
            for seg in ("000", "001", "002", "003", "004", "005"):
                _write_netcdf(tmp_path / _piece_name(ts, seg))

        result = find_resume_restart(tmp_path, expected_pieces=6)

        assert result is not None
        assert result.formatted_timestamp == newer
        assert result.partition == 0

    def test_incomplete_newest_falls_back_to_older_complete(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An incomplete newest set is skipped (with a warning) in favor of an
        older, complete set.
        """
        older, newer = "20200601000000", "20200701000000"
        for seg in ("000", "001", "002", "003", "004", "005"):
            _write_netcdf(tmp_path / _piece_name(older, seg))
        for seg in ("000", "001", "002", "003", "004"):
            _write_netcdf(tmp_path / _piece_name(newer, seg))

        with caplog.at_level("WARNING", logger=RESUME_LOGGER_NAME):
            result = find_resume_restart(tmp_path, expected_pieces=6)

        assert result is not None
        assert result.formatted_timestamp == older
        assert any("5 of 6" in r.message for r in caplog.records)

    def test_corrupt_piece_falls_back_to_older_complete(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A newest set with one unreadable piece is skipped in favor of an
        older, fully-readable set.
        """
        older, newer = "20200601000000", "20200701000000"
        for seg in ("000", "001", "002", "003", "004", "005"):
            _write_netcdf(tmp_path / _piece_name(older, seg))
        for seg in ("000", "001", "002", "003", "004"):
            _write_netcdf(tmp_path / _piece_name(newer, seg))
        _write_corrupt(tmp_path / _piece_name(newer, "005"))

        with caplog.at_level("WARNING", logger=RESUME_LOGGER_NAME):
            result = find_resume_restart(tmp_path, expected_pieces=6)

        assert result is not None
        assert result.formatted_timestamp == older
        assert any("unreadable piece" in r.message for r in caplog.records)

    def test_nothing_usable_returns_none(self, tmp_path: Path) -> None:
        """`None` is returned when no timestamp has a complete piece set."""
        for seg in ("000", "001"):
            _write_netcdf(tmp_path / _piece_name("20200601000000", seg))

        assert find_resume_restart(tmp_path, expected_pieces=6) is None

    def test_empty_dir_returns_none(self, tmp_path: Path) -> None:
        """`None` is returned when the search directory has no candidates."""
        assert find_resume_restart(tmp_path, expected_pieces=6) is None
        assert find_resume_restart(tmp_path, expected_pieces=None) is None

    def test_pio_mode_picks_newest_whole_readable_file(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """In ParallelIO mode (`expected_pieces=None`), the newest readable
        whole file is chosen and a corrupt newer file is skipped.
        """
        older, newer, newest = "20200601000000", "20200701000000", "20200801000000"
        _write_netcdf(tmp_path / _whole_name(older))
        _write_netcdf(tmp_path / _whole_name(newer))
        _write_corrupt(tmp_path / _whole_name(newest))

        with caplog.at_level("WARNING", logger=RESUME_LOGGER_NAME):
            result = find_resume_restart(tmp_path, expected_pieces=None)

        assert result is not None
        assert result.formatted_timestamp == newer
        assert any("unreadable file" in r.message for r in caplog.records)

    def test_partition_pieces_ignored_when_expected_is_none(
        self, tmp_path: Path
    ) -> None:
        """Partition pieces are ignored in ParallelIO mode; no whole file
        means no usable restart, even though pieces exist.
        """
        for seg in ("000", "001", "002"):
            _write_netcdf(tmp_path / _piece_name("20200601000000", seg))

        assert find_resume_restart(tmp_path, expected_pieces=None) is None

    def test_whole_file_ignored_when_expected_is_int(self, tmp_path: Path) -> None:
        """A whole file is ignored when a partitioned restart is expected."""
        _write_netcdf(tmp_path / _whole_name("20200601000000"))

        assert find_resume_restart(tmp_path, expected_pieces=6) is None


@pytest.fixture
def bp_factory(tmp_path: Path, bp_templates_dir: Path) -> Callable[..., Path]:
    """Return a factory that writes a roms_marbl blueprint YAML file to disk.

    Parameters
    ----------
    tmp_path : Path
        Pytest's per-test temporary directory.
    bp_templates_dir : Path
        Fixture returning the path to the blueprint templates directory.

    Returns
    -------
    Callable[..., Path]
        Factory taking `use_pio`, `n_procs_x`, and `n_procs_y` keyword
        arguments and returning the path to the written blueprint.
    """

    def _make(*, use_pio: bool = False, n_procs_x: int = 2, n_procs_y: int = 3) -> Path:
        tpl_path = bp_templates_dir / "blueprint.yaml"
        with tpl_path.open() as fp:
            data = yaml.safe_load(fp)

        working_dir = tmp_path / "run"
        data["working_dir"] = str(working_dir)
        data["partitioning"]["use_pio"] = use_pio
        data["partitioning"]["n_procs_x"] = n_procs_x
        data["partitioning"]["n_procs_y"] = n_procs_y
        data["runtime_params"]["start_date"] = "2020-01-01 00:00:00"
        data["runtime_params"]["end_date"] = "2020-12-01 00:00:00"

        bp_path = tmp_path / "blueprint.yaml"
        with bp_path.open("w") as fp:
            yaml.safe_dump(data, fp)

        return bp_path

    return _make


class TestPrepareResumeBlueprint:
    """Tests for `prepare_resume_blueprint`."""

    def test_resumes_from_complete_partitioned_restart(
        self, bp_factory: Callable[..., Path]
    ) -> None:
        """A complete restart found in `temp_output` produces a derived
        blueprint continuing from it, and leaves the original untouched.
        """
        bp_path = bp_factory(use_pio=False, n_procs_x=2, n_procs_y=3)
        before = bp_path.read_bytes()

        bp = deserialize(bp_path, RomsMarblBlueprint)
        fs = RomsFileSystemManager(bp.working_dir)

        ts = "20200601000000"
        piece_paths = [
            fs.temp_output_dir / _piece_name(ts, seg)
            for seg in ("000", "001", "002", "003", "004", "005")
        ]
        for piece_path in piece_paths:
            _write_netcdf(piece_path)

        result = prepare_resume_blueprint(str(bp_path))

        expected_out = fs.run_dir / f"{bp_path.stem}.{RESUME_SUFFIX}.yaml"
        assert Path(result) == expected_out

        new_bp = deserialize(result, RomsMarblBlueprint)
        ic = new_bp.initial_conditions.data[0]
        assert Path(str(ic.location)) == piece_paths[0].resolve()
        assert ic.partitioned is True
        assert new_bp.runtime_params.start_date.strftime("%Y%m%d%H%M%S") == ts

        assert bp_path.read_bytes() == before

    def test_no_restart_found_returns_uri_unchanged(
        self, bp_factory: Callable[..., Path], caplog: pytest.LogCaptureFixture
    ) -> None:
        """No usable restart in `temp_output` leaves the blueprint untouched
        and warns.
        """
        bp_path = bp_factory(use_pio=False)

        with caplog.at_level("WARNING", logger=RESUME_LOGGER_NAME):
            result = prepare_resume_blueprint(str(bp_path))

        assert result == str(bp_path)
        assert any("No usable restart found" in r.message for r in caplog.records)

    def test_restart_at_or_after_end_date_raises(
        self, bp_factory: Callable[..., Path]
    ) -> None:
        """A restart at or after `end_date` raises `CstarExpectationFailed`,
        since the run appears to have already finished.
        """
        bp_path = bp_factory(use_pio=False, n_procs_x=2, n_procs_y=3)
        bp = deserialize(bp_path, RomsMarblBlueprint)
        fs = RomsFileSystemManager(bp.working_dir)

        ts = "20201201000000"  # == end_date
        for seg in ("000", "001", "002", "003", "004", "005"):
            _write_netcdf(fs.temp_output_dir / _piece_name(ts, seg))

        with pytest.raises(CstarExpectationFailed):
            prepare_resume_blueprint(str(bp_path))

    def test_use_pio_searches_output_dir_and_is_not_partitioned(
        self, bp_factory: Callable[..., Path]
    ) -> None:
        """A ParallelIO blueprint searches `output` (not `temp_output`) and
        the resulting override is not marked as partitioned.
        """
        bp_path = bp_factory(use_pio=True)
        bp = deserialize(bp_path, RomsMarblBlueprint)
        fs = RomsFileSystemManager(bp.working_dir)

        ts = "20200601000000"
        whole_path = fs.output_dir / _whole_name(ts)
        _write_netcdf(whole_path)

        result = prepare_resume_blueprint(str(bp_path))

        new_bp = deserialize(result, RomsMarblBlueprint)
        ic = new_bp.initial_conditions.data[0]
        assert Path(str(ic.location)) == whole_path.resolve()
        assert ic.partitioned is False
