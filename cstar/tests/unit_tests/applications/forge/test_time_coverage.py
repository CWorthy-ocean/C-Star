"""The time-coverage checks on written input files (FG-07 / I-S2-3).

Each test writes small real netCDF files in the layout roms-tools uses: relative time
variables in ``days`` or ``seconds`` since the model reference date, an ``abs_time``
copy ROMS does not read, and ``cycle_length`` on a climatology's time variables.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import netCDF4
import numpy as np
import pytest
import roms_tools as rt

import cstar.applications.ucla_roms.models as cstar_models
from cstar.applications.forge import models as forge_models
from cstar.applications.forge import source_datasets
from cstar.applications.forge.input_generator import RomsInputGenerator
from cstar.applications.forge.time_coverage import (
    SOURCE_RECORD_ATTR,
    check_forcing_covers_window,
    check_initial_time,
    initial_time,
    restamp_initial_time,
)

if TYPE_CHECKING:
    from cstar.applications.ucla_roms.models import ForcingConfiguration

pytestmark = pytest.mark.real_time_coverage

REF = datetime(2000, 1, 1)
START = datetime(2012, 1, 1, 6)
END = datetime(2012, 1, 1, 13)


def _days(t: datetime) -> float:
    return (t - REF).total_seconds() / 86400


def _write_forcing(
    path: Path,
    times: list[datetime],
    *,
    time_name: str = "time",
    rad_shift: timedelta | None = None,
    cycle_length: float | None = None,
) -> Path:
    """A surface/boundary-style file: relative time(s) in days plus one data variable."""
    with netCDF4.Dataset(path, "w") as nc:
        nc.createDimension(time_name, len(times))
        nc.createDimension("eta_rho", 2)
        nc.createDimension("xi_rho", 3)
        var = nc.createVariable(time_name, "f8", (time_name,))
        var.units = "days"
        var.long_name = f"relative time: days since {REF}"
        if cycle_length is not None:
            var.cycle_length = cycle_length
        var[:] = [_days(t) for t in times]
        if rad_shift is not None:
            rad = nc.createVariable("rad_time", "f8", (time_name,))
            rad.units = "days"
            rad[:] = [_days(t + rad_shift) for t in times]
        abs_time = nc.createVariable("abs_time", "f8", (time_name,))
        abs_time.units = "hours since 1950-01-01"
        abs_time[:] = [0.0] * len(times)
        data = nc.createVariable("sustr", "f4", (time_name, "eta_rho", "xi_rho"))
        data[:] = np.zeros((len(times), 2, 3), dtype="f4")
    return path


def _write_ic(path: Path, record: datetime) -> Path:
    """An initial-conditions-style file stamped at ``record``, as roms-tools writes it."""
    with netCDF4.Dataset(path, "w") as nc:
        nc.createDimension("ocean_time", 1)
        nc.createDimension("s_rho", 2)
        ocean_time = nc.createVariable("ocean_time", "f8", ("ocean_time",))
        ocean_time.units = "seconds"
        ocean_time.long_name = f"relative time: seconds since {REF}"
        ocean_time[:] = [(record - REF).total_seconds()]
        abs_time = nc.createVariable("abs_time", "f8", ("ocean_time",))
        abs_time.units = f"days since {record:%Y-%m-%d %H:%M:%S}"
        abs_time[:] = [0.0]
        temp = nc.createVariable("temp", "f4", ("ocean_time", "s_rho"))
        temp[:] = np.array([[10.0, 4.0]], dtype="f4")
        nc.ini_time = str(START)
    return path


class TestCheckForcingCoversWindow:
    def _check(self, paths) -> None:
        check_forcing_covers_window(
            paths, start=START, end=END, reference=REF, label="boundary forcing"
        )

    def test_records_at_the_window_edges_cover_it(self, tmp_path: Path) -> None:
        """A record exactly at the start and one exactly at the end are enough."""
        self._check(_write_forcing(tmp_path / "f.nc", [START, END]))

    def test_first_record_after_the_start_is_refused(self, tmp_path: Path) -> None:
        path = _write_forcing(tmp_path / "f.nc", [datetime(2012, 1, 1, 12), END])
        with pytest.raises(
            ValueError,
            match=r"first 'time' record is 2012-01-01 12:00:00, after the start",
        ):
            self._check(path)

    def test_last_record_before_the_end_is_refused(self, tmp_path: Path) -> None:
        path = _write_forcing(tmp_path / "f.nc", [START, datetime(2012, 1, 1, 12)])
        with pytest.raises(
            ValueError,
            match=r"last 'time' record is 2012-01-01 12:00:00, before the end",
        ) as err:
            self._check(path)
        assert "f.nc" in str(err.value)
        assert "--clobber" in str(err.value)

    def test_split_files_are_pooled(self, tmp_path: Path) -> None:
        """A yearly split covers a window across New Year only taken together."""
        year1 = _write_forcing(tmp_path / "f_2012.nc", [datetime(2012, 12, 31)])
        year2 = _write_forcing(tmp_path / "f_2013.nc", [datetime(2013, 1, 2)])
        check_forcing_covers_window(
            [year1, year2],
            start=datetime(2012, 12, 31, 12),
            end=datetime(2013, 1, 1, 6),
            reference=REF,
            label="surface forcing",
        )
        with pytest.raises(ValueError, match="before the end"):
            check_forcing_covers_window(
                [year1],
                start=datetime(2012, 12, 31, 12),
                end=datetime(2013, 1, 1, 6),
                reference=REF,
                label="surface forcing",
            )

    def test_each_time_variable_is_checked(self, tmp_path: Path) -> None:
        """``rad_time`` 30 min behind ``time`` runs out first, and ROMS stops on it."""
        path = _write_forcing(
            tmp_path / "f.nc", [START, END], rad_shift=timedelta(minutes=-30)
        )
        with pytest.raises(
            ValueError, match=r"last 'rad_time' record is 2012-01-01 12:30:00"
        ) as err:
            self._check(path)
        assert "'time'" not in str(err.value)

    def test_a_climatology_is_exempt(self, tmp_path: Path) -> None:
        """ROMS cycles a variable with ``cycle_length`` whatever the window."""
        months = [datetime(2000, m, 15) for m in range(1, 13)]
        self._check(
            _write_forcing(
                tmp_path / "clim.nc", months, time_name="bry_time", cycle_length=365.25
            )
        )

    def test_abs_time_is_not_read(self, tmp_path: Path) -> None:
        """``abs_time`` (zeros here, in other units) does not decide coverage."""
        self._check(_write_forcing(tmp_path / "f.nc", [START, END]))

    def test_a_file_without_a_known_time_variable_is_refused(
        self, tmp_path: Path
    ) -> None:
        """An unrecognised layout fails loudly instead of passing unchecked."""
        path = tmp_path / "f.nc"
        with netCDF4.Dataset(path, "w") as nc:
            nc.createDimension("time", 2)
            var = nc.createVariable("time", "f8", ("time",))
            var.units = "days since 2000-01-01"
            var[:] = [0.0, 1.0]
        with pytest.raises(ValueError, match="no time variable in units of"):
            self._check(path)

    def test_seconds_are_read_as_seconds(self, tmp_path: Path) -> None:
        path = tmp_path / "f.nc"
        with netCDF4.Dataset(path, "w") as nc:
            nc.createDimension("cdr_time", 2)
            var = nc.createVariable("cdr_time", "f8", ("cdr_time",))
            var.units = "seconds"
            var[:] = [(START - REF).total_seconds(), (END - REF).total_seconds()]
        self._check(path)


class TestInitialTime:
    def test_a_file_at_the_start_passes(self, tmp_path: Path) -> None:
        path = _write_ic(tmp_path / "ic.nc", START)
        assert initial_time(path, REF) == START
        check_initial_time(path, start=START, reference=REF)

    def test_a_file_at_another_time_is_refused(self, tmp_path: Path) -> None:
        path = _write_ic(tmp_path / "ic.nc", datetime(2012, 1, 1, 12))
        with pytest.raises(
            ValueError,
            match=r"starts the model at 2012-01-01 12:00:00, but the run starts at 2012-01-01 06:00:00",
        ):
            check_initial_time([path], start=START, reference=REF)

    def test_an_ocean_time_in_other_units_is_refused(self, tmp_path: Path) -> None:
        """Units ROMS does not read fail with a message, not a ``KeyError``."""
        path = _write_ic(tmp_path / "ic.nc", START)
        with netCDF4.Dataset(path, "r+") as nc:
            nc["ocean_time"].units = "hours"
        with pytest.raises(ValueError, match=r"'ocean_time' is in units 'hours'"):
            check_initial_time(path, start=START, reference=REF)
        with pytest.raises(ValueError, match=r"'ocean_time' is in units 'hours'"):
            restamp_initial_time(path, start=START, reference=REF)


class TestRestampInitialTime:
    def test_a_later_record_is_stamped_at_the_start(self, tmp_path: Path) -> None:
        record = datetime(2012, 1, 1, 12)
        path = _write_ic(tmp_path / "ic.nc", record)

        assert restamp_initial_time(path, start=START, reference=REF) == record

        assert initial_time(path, REF) == START
        with netCDF4.Dataset(path) as nc:
            assert nc.getncattr(SOURCE_RECORD_ATTR) == str(record)
            assert nc["abs_time"].units == "days since 2012-01-01 06:00:00"
            assert float(nc["abs_time"][0]) == 0.0
            np.testing.assert_array_equal(nc["temp"][:], [[10.0, 4.0]])

    def test_a_file_at_the_start_is_left_alone(self, tmp_path: Path) -> None:
        path = _write_ic(tmp_path / "ic.nc", START)
        before = path.read_bytes()

        assert restamp_initial_time(path, start=START, reference=REF) is None

        assert path.read_bytes() == before


# ---------------------------------------------------------------------------
# The checks as the input generator's IC, surface and boundary steps run them
# ---------------------------------------------------------------------------
@pytest.fixture
def generator(tmp_path: Path) -> RomsInputGenerator:
    """A generator for ``START``..``END`` whose roms-tools objects the tests mock."""
    source_data = MagicMock(spec=source_datasets.SourceDatasets)
    source_file = tmp_path / "source.nc"
    source_file.touch()
    source_data.path_for_source = MagicMock(return_value=source_file)
    source_data.dataset_key_for_source = MagicMock(
        side_effect=lambda name, glorys_layout=None: name.upper()
    )
    source_data.streamable_for_source = MagicMock(return_value=False)
    source_data.derived_for_source = MagicMock(return_value=False)
    (tmp_path / "blueprints").mkdir()
    (tmp_path / "input_data").mkdir()
    glorys = forge_models.SourceSpec(name="GLORYS")
    forcing_override = {
        "initial_conditions": forge_models.InitialConditionsInput(
            source=glorys
        ).model_dump(),
        "forcing": {
            "surface": [
                forge_models.SurfaceForcingItem(
                    source=forge_models.SourceSpec(name="ERA5"), type="physics"
                ).model_dump()
            ],
            "boundary": forge_models.BoundaryForcing(source=glorys).model_dump(),
        },
    }
    return RomsInputGenerator(
        domain_name="test_grid",
        start_date=START,
        end_date=END,
        model_reference_date=REF,
        forcing_override=forcing_override,
        grid=rt.Grid(
            nx=4, ny=4, size_x=100, size_y=100, center_lon=0, center_lat=55, N=3
        ),
        boundaries=forge_models.OpenBoundaries(
            north=True, south=True, east=True, west=False
        ),
        source_data=source_data,
        ucla_roms_blueprint_dir=tmp_path / "blueprints",
        partitioning=cstar_models.PartitioningParameterSet(n_procs_x=1, n_procs_y=1),
        use_dask=False,
        input_data_dir=tmp_path / "input_data",
    )


def _ic_location(generator: RomsInputGenerator) -> str:
    """The one initial-conditions file the step recorded."""
    ic = generator.ucla_roms_blueprint_elements.initial_conditions
    assert ic is not None
    (resource,) = ic.data
    return str(resource.location)


def _forcing(generator: RomsInputGenerator) -> "ForcingConfiguration":
    forcing = generator.ucla_roms_blueprint_elements.forcing
    assert forcing is not None
    return forcing


def _mock_ic_writing(record: datetime) -> MagicMock:
    ic = MagicMock()
    ic.save.side_effect = lambda path, **kw: [_write_ic(Path(path), record)]
    return ic


class TestInitialConditionsStep:
    @patch("cstar.applications.forge.input_generator.rt.InitialConditions")
    def test_flex_time_stamps_the_record_at_the_start(
        self, mock_ic_class: MagicMock, generator: RomsInputGenerator
    ) -> None:
        record = datetime(2012, 1, 1, 12)
        mock_ic_class.return_value = _mock_ic_writing(record)

        with pytest.warns(UserWarning, match=r"at 2012-01-01 12:00:00 \(6 h later\)"):
            generator._generate_initial_conditions(allow_flex_time=True)

        location = _ic_location(generator)
        assert initial_time(location, REF) == START
        with netCDF4.Dataset(location) as nc:
            assert nc.getncattr(SOURCE_RECORD_ATTR) == str(record)

    @patch("cstar.applications.forge.input_generator.rt.InitialConditions")
    def test_flex_time_on_a_record_at_the_start_changes_nothing(
        self, mock_ic_class: MagicMock, generator: RomsInputGenerator
    ) -> None:
        mock_ic_class.return_value = _mock_ic_writing(START)

        generator._generate_initial_conditions(allow_flex_time=True)

        with netCDF4.Dataset(_ic_location(generator)) as nc:
            assert SOURCE_RECORD_ATTR not in nc.ncattrs()

    @patch("cstar.applications.forge.input_generator.rt.InitialConditions")
    def test_without_flex_time_a_mismatch_is_refused(
        self, mock_ic_class: MagicMock, generator: RomsInputGenerator
    ) -> None:
        mock_ic_class.return_value = _mock_ic_writing(datetime(2012, 1, 1, 12))

        with pytest.raises(ValueError, match="starts the model at 2012-01-01 12:00:00"):
            generator._generate_initial_conditions()

    @patch("cstar.applications.forge.input_generator.rt.InitialConditions")
    def test_a_reused_file_for_another_start_is_refused(
        self, mock_ic_class: MagicMock, generator: RomsInputGenerator
    ) -> None:
        """A file left by a run with another start date (or with flexible start time
        by an earlier release) is checked on reuse, not trusted.
        """
        planned = generator._forcing_filename(input_name="initial_conditions")
        _write_ic(planned, datetime(2012, 1, 1, 12))
        generator._existing_planned_outputs = {planned.resolve()}

        with pytest.raises(ValueError, match="--clobber"):
            generator._generate_initial_conditions(allow_flex_time=True)
        mock_ic_class.assert_not_called()


class TestForcingSteps:
    @patch("cstar.applications.forge.input_generator.rt.SurfaceForcing")
    def test_surface_forcing_ending_early_is_refused(
        self, mock_sf_class: MagicMock, generator: RomsInputGenerator
    ) -> None:
        frc = MagicMock()
        frc.save.side_effect = lambda path, **kw: [
            _write_forcing(Path(path), [START, datetime(2012, 1, 1, 12)])
        ]
        mock_sf_class.return_value = frc

        with pytest.raises(
            ValueError, match=r"physics surface forcing.*before the end"
        ):
            generator._generate_surface_forcing(
                key="forcing.surface", source={"name": "ERA5"}, type="physics"
            )

    @patch("cstar.applications.forge.input_generator.rt.SurfaceForcing")
    def test_surface_forcing_covering_the_window_is_recorded(
        self, mock_sf_class: MagicMock, generator: RomsInputGenerator
    ) -> None:
        frc = MagicMock()
        frc.save.side_effect = lambda path, **kw: [
            _write_forcing(Path(path), [START, END])
        ]
        mock_sf_class.return_value = frc

        generator._generate_surface_forcing(
            key="forcing.surface", source={"name": "ERA5"}, type="physics"
        )

        assert _forcing(generator).surface.data

    @patch("cstar.applications.forge.input_generator.rt.BoundaryForcing")
    def test_boundary_forcing_starting_late_is_refused(
        self, mock_bf_class: MagicMock, generator: RomsInputGenerator
    ) -> None:
        bry = MagicMock()
        bry.bgc = []
        bry.physics.save.side_effect = lambda path, **kw: [
            _write_forcing(
                Path(path), [datetime(2012, 1, 1, 12), END], time_name="bry_time"
            )
        ]
        mock_bf_class.return_value = bry

        with pytest.raises(
            ValueError,
            match=r"physics boundary forcing.*'bry_time' record is 2012-01-01 12:00:00, after the start",
        ):
            generator._generate_boundary_forcing(source={"name": "GLORYS"})
        assert not _forcing(generator).boundary.data
