import pytest
import numpy as np
from pathlib import Path
from cstar.roms import ROMSRuntimeSettings


@pytest.fixture
def example_runtime_settings():
    return ROMSRuntimeSettings(
        title="Example runtime settings",
        time_stepping=[360, 60, 60, 1],
        bottom_drag=[0.0e-4, 1e-3, 1e-2, 1e-4, 1e-2],
        initial=[1, Path("input_datasets/roms_ini.nc")],
        forcing=[
            Path("input_datasets/roms_frc.nc"),
            Path("input_datasets/roms_frc_bgc.nc"),
            Path("input_datasets/roms_bry.nc"),
            Path("input_datasets/roms_bry_bgc.nc"),
        ],
        output_root_name="ROMS_test",
        s_coord=[5.0, 2.0, 300.0],
        rho0=1000.0,
        lin_rho_eos=[0.2, 1.0, 0.822, 1.0],
        marbl_biogeochemistry=[
            Path("marbl_in"),
            Path("marbl_tracer_output_list"),
            Path("marbl_diagnostic_output_list"),
        ],
        lateral_visc=0.0,
        gamma2=1.0,
        tracer_diff2=np.zeros(38),
        vertical_mixing=[0, np.zeros(37)],
        my_bak_mixing=[1.0e-5, 0.0, 0.0],
        sss_correction=7.777,
        sst_correction=10.0,
        ubind=0.1,
        v_sponge=0.0,
        grid=Path("input_datasets/roms_grd.nc"),
        climatology=Path("climfile2.nc"),
    )


class TestROMSRuntimeSettings:
    def test_to_file(self, example_runtime_settings, tmp_path):
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
            assert ref == out

    def test_from_file(self, example_runtime_settings):
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
        assert np.array_equal(
            tested_settings.tracer_diff2, expected_settings.tracer_diff2
        )

        assert {
            k: (v.tolist() if isinstance(v, np.ndarray) else v)
            for k, v in tested_settings.vertical_mixing.items()
        } == {
            k: (v.tolist() if isinstance(v, np.ndarray) else v)
            for k, v in expected_settings.vertical_mixing.items()
        }

    def test_from_file_raises_if_nonexistent(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="does not exist"):
            ROMSRuntimeSettings.from_file(tmp_path / "nonexistentfile.in")

    def test_file_roundtrip(self, example_runtime_settings, tmp_path):
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
        assert np.array_equal(
            tested_settings.tracer_diff2, expected_settings.tracer_diff2
        )

        assert {
            k: (v.tolist() if isinstance(v, np.ndarray) else v)
            for k, v in tested_settings.vertical_mixing.items()
        } == {
            k: (v.tolist() if isinstance(v, np.ndarray) else v)
            for k, v in expected_settings.vertical_mixing.items()
        }


class TestStrAndRepr:
    def test_str(self, example_runtime_settings):
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
S-coordinate parameters (`ROMSRuntimeSettings.s_coord`):
Surface stretching parameter (`theta_s`) = 5.0,
Bottom stretching parameter (`theta_b`) = 2.0,
Critical depth (`hc` or `tcline`, m) = 300.0
Boussinesq reference density (`rho0`, kg/m3) = 1000.0
Linear equation of state parameters (`ROMSRuntimeSettings.lin_rho_eos`):
- Thermal expansion coefficient, ⍺ (`Tcoef`, kg/m3/K) = 0.2,
- Reference temperature (`T0`, °C) = 1.0,
- Haline contraction coefficient, β (`Scoef`, kg/m3/PSU) = 0.822,
- Reference salinity (`S0`, psu) = 1.0
MARBL input (`ROMSRuntimeSettings.marbl_biogeochemistry`):
- MARBL runtime settings file: marbl_in,
- MARBL output tracer list: marbl_tracer_output_list,
- MARBL output diagnostics list: marbl_diagnostic_output_list
Horizontal Laplacian kinematic viscosity (`ROMSRuntimeSettings.lateral_visc`, m2/s) = 0.0
Boundary slipperiness parameter (`ROMSRuntimeSettings.gamma2`, free-slip=+1, no-slip=-1) = 1.0
Horizontal Laplacian mixing coefficients for tracers (`ROMSRuntimeSettings.tracer_diff2`, m2/s) = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
Vertical mixing parameters (`ROMSRuntimeSettings.vertical_mixing`):
- Background vertical viscosity (`Akv_bak`, m2/s) = 0,
- Background vertical mixing for tracers (`Akt_bak`, m2/s) = [0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0.
 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0. 0.],
Mellor-Yamada Level 2.5 turbulent closure parameters (`ROMSRuntimeSettings.my_bak_mixing`):
- Backround vertical TKE mixing [`Akq_bak`, m2/s] = 1e-05,
- Horizontal Laplacian TKE mixing [`q2nu2`, m2/s] = 0.0,
- Horizontal biharmonic TKE mixing [`q2nu4`, m4/s] = 0.0,
SSS correction (`ROMSRuntimeSettings.sss_correction`): 7.777
SST correction (`ROMSRuntimeSettings.sst_correction`): 10.0
Open boundary binding velocity (`ROMSRuntimeSettings.ubind`, m/s) = 0.1
Maximum sponge layer viscosity (`ROMSRuntimeSettings.v_sponge`, m2/s) = 0.0
Climatology data files (`ROMSRuntimeSettings.climatology`): climfile2.nc"""

        assert str(example_runtime_settings) == expected_str

    def test_repr(self, example_runtime_settings):
        expected_repr = """ROMSRuntimeSettings(title='Example runtime settings', time_stepping={'ntimes': 360, 'dt': 60, 'ndtfast': 60, 'ninfo': 1}, bottom_drag={'rdrg': 0.0, 'rdrg2': 0.001, 'zob': 0.01}, initial={'nrrec': 1, 'ininame': PosixPath('input_datasets/roms_ini.nc')}, forcing=['input_datasets/roms_frc.nc', 'input_datasets/roms_frc_bgc.nc', 'input_datasets/roms_bry.nc', 'input_datasets/roms_bry_bgc.nc'], output_root_name='ROMS_test', grid='input_datasets/roms_grd.nc', climatology='climfile2.nc', s_coord={'theta_s': 5.0, 'theta_b': 2.0, 'tcline': 300.0}, rho0=1000.0, lin_rho_eos={'Tcoef': 0.2, 'T0': 1.0, 'Scoef': 0.822, 'S0': 1.0}, marbl_biogeochemistry={'marbl_namelist_fname': PosixPath('marbl_in'), 'marbl_tracer_list_fname': PosixPath('marbl_tracer_output_list'), 'marbl_diag_list_fname': PosixPath('marbl_diagnostic_output_list')}, lateral_visc=0.0, gamma2=1.0, tracer_diff2=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], vertical_mixing={'Akv_bak': 0, 'Akt_bak': array([0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
       0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
       0., 0., 0.])}, my_bak_mixing={'Akq_bak': 1e-05, 'q2nu2': 0.0, 'q2nu4': 0.0}, sss_correction=7.777, sst_correction=10.0, ubind=0.1, v_sponge=0.0)"""

        assert expected_repr == repr(example_runtime_settings)
