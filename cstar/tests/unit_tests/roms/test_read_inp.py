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
            assert out_f.readlines() == ref_f.readlines()

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
