import shutil
from pathlib import Path

import numpy as np
import pytest

from cstar.base.utils import _replace_text_in_file
from cstar.roms import ROMSRuntimeSettings


@pytest.fixture
def example_runtime_settings():
    """Fixture providing a `ROMSRuntimeSettings` instance for testing.

    The example instance corresponds to the file `fixtures/example_runtime_settings.in`
    in order to test the `ROMSRuntimeSettings.to_file` and `from_file` methods.

    Paths do not correspond to real files.

    Yields
    ------
    ROMSRuntimeSettings
       The example ROMSRuntimeSettings instance
    """

    yield ROMSRuntimeSettings(
        title = "Example runtime settings",
        time_stepping={"ntimes": 360, "dt": 60, "ndtfast": 60, "ninfo": 1},
        bottom_drag={
            "rdrg": 0.0e-4,
            "rdrg2": 1e-3,
            "zob": 1e-2,
            "cdb_min": 1e-4,
            "cdb_max": 1e-2,
        },
        initial={"nrrec": 1, "ininame": Path("input_datasets/roms_ini.nc")},
        forcing={
            "filenames": [
                Path("input_datasets/roms_frc.nc"),
                Path("input_datasets/roms_frc_bgc.nc"),
                Path("input_datasets/roms_bry.nc"),
                Path("input_datasets/roms_bry_bgc.nc"),
            ]
        },
        output_root_name = "ROMS_test",
        s_coord={"theta_s": 5.0, "theta_b": 2.0, "tcline": 300.0},
        rho0=1000.0,
        lin_rho_eos={"Tcoef": 0.2, "T0": 1.0, "Scoef": 0.822, "S0": 1.0},
        marbl_biogeochemistry={
            "marbl_namelist_fname": Path("marbl_in"),
            "marbl_tracer_list_fname": Path("marbl_tracer_list_fname"),
            "marbl_diag_list_fname": Path("marbl_diagnostic_output_list"),
        },
        lateral_visc = 0.0,
        gamma2 = 1.0,
        tracer_diff2 = [0.,]*38,
        vertical_mixing={"Akv_bak": 0, "Akt_bak": np.zeros(37)},
        my_bak_mixing={"Akq_bak": 1.0e-5, "q2nu2": 0.0, "q2nu4": 0.0},
        sss_correction = 7.777,
        sst_correction = 10.0,
        ubind = 0.1,
        v_sponge = 0.0,
        grid = Path("input_datasets/roms_grd.nc"),
        climatology = Path("climfile2.nc"),
    )


class TestROMSRuntimeSettings:
    def test_to_file(self, example_runtime_settings, tmp_path):
        """Test the ROMSRuntimeSettings.to_file method.

        This test writes the example ROMSRuntimeSettings instance
        defined by the example_runtime_settings fixture to a temporary
        file and compares each non-commented line in the example `.in`
        file `fixtures/example_runtime_settings.in` with those in the
        temporary file.

        Mocks and Fixtures
        ------------------
        example_runtime_settings: ROMSRuntimeSettings
           Fixture returning an example ROMSRuntimeSettings instance
        tmp_path: Path
           Fixture creating and returning a temporary pathlib.Path

        Asserts
        -------
        - The lines in the written file match those in the reference file
        """

        example_runtime_settings.to_file(tmp_path / "test.in")

        with (
            open(tmp_path / "test.in") as out_f,
            open(
                Path(__file__).parent / "fixtures/example_runtime_settings.in"
            ) as ref_f,
        ):
            ref = [
                line for line in ref_f.readlines() if not line.strip().startswith("!")
            ]
            out = out_f.readlines()
            assert ref == out, f"Expected \n{ref}\n,got\n{out}"

    def test_from_file(self, example_runtime_settings):
        """Test the ROMSRuntimeSettings.from_file method.

        This test compares the ROMSRuntimeSettings instance created from
        the reference file `fixtures/example_runtime_settings.in` with the
        example instance returned by the `example_runtime_settings` fixture.

        Mocks and Fixtures
        ------------------
        example_runtime_settings: ROMSRuntimeSettings
           Fixture returning an example ROMSRuntimeSettings instance

        Asserts
        -------
        - Compares each attribute of the reference and tested ROMSRuntimeSettings
          instances and checks for equality.
        """
        tested_settings = ROMSRuntimeSettings.from_file(
            Path(__file__).parent / "fixtures/example_runtime_settings.in"
        )
        expected_settings = example_runtime_settings

        assert tested_settings.title == expected_settings.title
        assert tested_settings.time_stepping == expected_settings.time_stepping
        assert tested_settings.bottom_drag == expected_settings.bottom_drag
        assert tested_settings.initial == expected_settings.initial
        assert tested_settings.forcing == expected_settings.forcing
        assert tested_settings.output_root_name == expected_settings.output_root_name
        assert tested_settings.s_coord == expected_settings.s_coord
        assert tested_settings.rho0 == expected_settings.rho0
        assert tested_settings.lin_rho_eos == expected_settings.lin_rho_eos
        assert (
            tested_settings.marbl_biogeochemistry
            == expected_settings.marbl_biogeochemistry
        )
        assert tested_settings.lateral_visc == expected_settings.lateral_visc
        assert tested_settings.gamma2 == expected_settings.gamma2
        assert tested_settings.my_bak_mixing == expected_settings.my_bak_mixing
        assert tested_settings.sss_correction == expected_settings.sss_correction
        assert tested_settings.sst_correction == expected_settings.sst_correction
        assert tested_settings.ubind == expected_settings.ubind
        assert tested_settings.v_sponge == expected_settings.v_sponge
        assert tested_settings.grid == expected_settings.grid
        assert tested_settings.climatology == expected_settings.climatology
        assert tested_settings.tracer_diff2 == expected_settings.tracer_diff2
        assert tested_settings.vertical_mixing == expected_settings.vertical_mixing

    def test_from_file_with_missing_optional_sections(self, tmp_path):
        """Confirms that ROMSRuntimeSettings.from_file sets the attributes corresponding
        to settings that are not present in the file to None.

        This test copies the reference file in `fixtures/example_runtime_settings.in`
        to a temporary path and modifies it to remove the value of the `climatology`
        entry, then confirms that `ROMSRuntimeSettings.from_file(tmp_file).climatology
        is None

        Mocks and Fixtures
        ------------------
        tmp_path: Path
           Fixture creating and returning a temporary pathlib.Path

        Asserts
        -------
        - The ROMSRuntimeSettings instance returned by `from_file` with the modified
          file has `None` for its `climatology` attribute
        """

        modified_file = tmp_path / "modified_example_settings.in"
        shutil.copy2(
            Path(__file__).parent / "fixtures/example_runtime_settings.in",
            modified_file,
        )
        _replace_text_in_file(modified_file, "climfile2.nc", "")
        tested_settings = ROMSRuntimeSettings.from_file(modified_file)
        assert tested_settings.climatology is None

    def test_from_file_raises_if_nonexistent(self):
        """Test that ROMSRuntimeSettings.from_file raises a FileNotFoundError if the
        supplied file does not exist."""
        with pytest.raises(FileNotFoundError, match="does not exist"):
            ROMSRuntimeSettings.from_file("nonexistentfile.in")

    def test_file_roundtrip(self, example_runtime_settings, tmp_path):
        """Tests that the `to_file`/`from_file` roundtrip results in a functionally
        indentical ROMSRuntimeSettings instance.

        Mocks and Fixtures
        ------------------
        - example_runtime_settings: ROMSRuntimeSettings
           A fixture returning an example ROMSRuntimeSettings instance
        - tmp_path: Path
           Fixture creating and returning a temporary pathlib.Path

        Asserts
        -------
        - Each attribute in the instance returned by `from_file` is equal
          to those in the instance passed to `to_file`
        """

        expected_settings = example_runtime_settings
        expected_settings.to_file(tmp_path / "test.in")

        tested_settings = ROMSRuntimeSettings.from_file(tmp_path / "test.in")

        assert tested_settings.title == expected_settings.title
        assert tested_settings.time_stepping == expected_settings.time_stepping
        assert tested_settings.bottom_drag == expected_settings.bottom_drag
        assert tested_settings.initial == expected_settings.initial
        assert tested_settings.forcing == expected_settings.forcing
        assert tested_settings.output_root_name == expected_settings.output_root_name
        assert tested_settings.s_coord == expected_settings.s_coord
        assert tested_settings.rho0 == expected_settings.rho0
        assert tested_settings.lin_rho_eos == expected_settings.lin_rho_eos
        assert (
            tested_settings.marbl_biogeochemistry
            == expected_settings.marbl_biogeochemistry
        )
        assert tested_settings.lateral_visc == expected_settings.lateral_visc
        assert tested_settings.gamma2 == expected_settings.gamma2
        assert tested_settings.my_bak_mixing == expected_settings.my_bak_mixing
        assert tested_settings.sss_correction == expected_settings.sss_correction
        assert tested_settings.sst_correction == expected_settings.sst_correction
        assert tested_settings.ubind == expected_settings.ubind
        assert tested_settings.v_sponge == expected_settings.v_sponge
        assert tested_settings.grid == expected_settings.grid
        assert tested_settings.climatology == expected_settings.climatology
        assert tested_settings.tracer_diff2 == expected_settings.tracer_diff2
        assert tested_settings.vertical_mixing == expected_settings.vertical_mixing


class TestStrAndRepr:
    def test_str(self, example_runtime_settings):
        """Test that the __str__ function of ROMSRuntimeSettings matches an expected
        string for the example instance.

        Mocks and Fixtures
        ------------------
        example_runtime_settings: ROMSRuntimeSettings
           A fixture returning an example ROMSRuntimeSettings instance

        Asserts
        -------
        - str(example_runtime_settings) matches an expected reference string
        """
        expected_str = """ROMSRuntimeSettings
-------------------
Title (`ROMSRuntimeSettings.title`): Example runtime settings
Output filename prefix (`ROMSRuntimeSettings.output_root_name`): ROMS_test
Time stepping (`ROMSRuntimeSettings.time_stepping`):
- Number of steps (`ntimes`) = 360,
- Time step (`dt`, sec) = 60,
- Mode-splitting ratio (`ndtfast`) = 60,
- Runtime diagnostic frequency (`ninfo`, steps) = 1
Bottom drag (`ROMSRuntimeSettings.bottom_drag`):
- Linear bottom drag coefficient (`rdrg`, m/s) = 0.0,
- Quadratic bottom drag coefficient (`rdrg2`, nondim) = 0.001
- Bottom roughness height (`zob`,m) = 0.01
Grid file (`ROMSRuntimeSettings.grid`): input_datasets/roms_grd.nc
Initial conditions file (`ROMSRuntimeSettings.initial`): input_datasets/roms_ini.nc
Forcing file(s): [PosixPath('input_datasets/roms_frc.nc'),
          PosixPath('input_datasets/roms_frc_bgc.nc'),
          PosixPath('input_datasets/roms_bry.nc'),
          PosixPath('input_datasets/roms_bry_bgc.nc')]
Boussinesq reference density (`rho0`, kg/m3) = 1000.0
Linear equation of state parameters (`ROMSRuntimeSettings.lin_rho_eos`):
- Thermal expansion coefficient, ⍺ (`Tcoef`, kg/m3/K) = 0.2,
- Reference temperature (`T0`, °C) = 1.0,
- Haline contraction coefficient, β (`Scoef`, kg/m3/PSU) = 0.822,
- Reference salinity (`S0`, psu) = 1.0
Horizontal Laplacian kinematic viscosity (`ROMSRuntimeSettings.lateral_visc`, m2/s) = 0.0
Boundary slipperiness parameter (`ROMSRuntimeSettings.gamma2`, free-slip=+1, no-slip=-1) = 1.0
Horizontal Laplacian mixing coefficients for tracers (`ROMSRuntimeSettings.tracer_diff2`, m2/s) = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
Vertical mixing parameters (`ROMSRuntimeSettings.vertical_mixing`):
- Background vertical viscosity (`Akv_bak`, m2/s) = 0.0,
- Background vertical mixing for tracers (`Akt_bak`, m2/s) = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
Mellor-Yamada Level 2.5 turbulent closure parameters (`ROMSRuntimeSettings.my_bak_mixing`):
- Background vertical TKE mixing [`Akq_bak`, m2/s] = 1e-05,
- Horizontal Laplacian TKE mixing [`q2nu2`, m2/s] = 0.0,
- Horizontal biharmonic TKE mixing [`q2nu4`, m4/s] = 0.0,
SSS correction (`ROMSRuntimeSettings.sss_correction`): 7.777
SST correction (`ROMSRuntimeSettings.sst_correction`): 10.0
Open boundary binding velocity (`ROMSRuntimeSettings.ubind`, m/s) = 0.1
Maximum sponge layer viscosity (`ROMSRuntimeSettings.v_sponge`, m2/s) = 0.0
Climatology data files (`ROMSRuntimeSettings.climatology`): climfile2.nc"""

        assert (
            str(example_runtime_settings) == expected_str
        ), f"expected \n{expected_str}\n, got\n{str(example_runtime_settings)}"

    def test_repr(self, example_runtime_settings):
        """Test that the __repr__ function of ROMSRuntimeSettings matches an expected
        string for the example instance.

        Mocks and Fixtures
        ------------------
        example_runtime_settings: ROMSRuntimeSettings
           A fixture returning an example ROMSRuntimeSettings instance

        Asserts
        -------
        - repr(example_runtime_settings) matches an expected reference string
        """
        expected_repr = """ROMSRuntimeSettings(title='Example runtime settings', time_stepping={'ntimes': 360, 'dt': 60, 'ndtfast': 60, 'ninfo': 1}, bottom_drag={'rdrg': 0.0, 'rdrg2': 0.001, 'zob': 0.01}, initial={'nrrec': 1, 'ininame': PosixPath('input_datasets/roms_ini.nc')}, forcing=["('filenames', [PosixPath('input_datasets/roms_frc.nc'), PosixPath('input_datasets/roms_frc_bgc.nc'), PosixPath('input_datasets/roms_bry.nc'), PosixPath('input_datasets/roms_bry_bgc.nc')])"], output_root_name='ROMS_test', grid='input_datasets/roms_grd.nc', climatology='climfile2.nc', rho0=1000.0, lin_rho_eos={'Tcoef': 0.2, 'T0': 1.0, 'Scoef': 0.822, 'S0': 1.0}, lateral_visc=0.0, gamma2=1.0, tracer_diff2=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], vertical_mixing={'Akv_bak': 0.0, 'Akt_bak': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}, my_bak_mixing={'Akq_bak': 1e-05, 'q2nu2': 0.0, 'q2nu4': 0.0}, sss_correction=7.777, sst_correction=10.0, ubind=0.1, v_sponge=0.0)"""

        assert expected_repr == repr(
            example_runtime_settings
        ), f"expected \n{expected_repr}\n, got\n{repr(example_runtime_settings)}"
