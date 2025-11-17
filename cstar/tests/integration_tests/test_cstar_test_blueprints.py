import logging
from pathlib import Path
from unittest import mock

import pytest

import cstar
from cstar.roms.simulation import ROMSSimulation
from cstar.tests.integration_tests.config import TEST_CONFIG


class TestCStar:
    @pytest.mark.parametrize(
        "test_config_key",
        [
            "test_case_remote_with_netcdf_datasets",
            "test_case_local_with_netcdf_datasets",
        ],
    )
    def test_cstar(
        self,
        tmp_path: Path,
        modify_template_blueprint,
        mock_lmod_path: Path,
        fetch_remote_test_case_data,
        test_config_key,
        log: logging.Logger,
    ):
        """Run the C-Star minimal test case from a selection of different blueprints.

        Parameters:
        -----------
        tmp_path:
           Built-in pytest fixture to create temporary directories
        modify_template_blueprint:
           Fixture to modify the contents of a template blueprint and save to a
           temporary file (from which the ROMSSimulation instance is created)
        fetch_remote_test_case_data:
           Fixture to fetch data needed for the C-Star test case, such as additional
           code for compiling ROMS, yaml files to supply to roms-tools, etc.
           These are saved to a subdirectory of the system cache specified by
           CSTAR_TEST_DATA_DIRECTORY in the config.py module
        test_config_key (str):
           Key determining the specific variation of the test case that is run, as
           defined in the dictionary TEST_CONFIG in the config.py module
        log (logging.Logger):
            Logger instance for logging messages during test execution.
        """
        ext_root = tmp_path / "externals"

        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.lmod_path", mock_lmod_path
            ),
            mock.patch.object(
                cstar.system.environment.CStarEnvironment, "package_root", new=ext_root
            ),
        ):
            if "local" in test_config_key:
                fetch_remote_test_case_data()

            config = TEST_CONFIG.get(test_config_key)
            if config is None:
                raise ValueError(
                    "No test configuration found for the provided key: "
                    f"{test_config_key}. Please check the TEST_CONFIG dictionary."
                )
            template_blueprint = config.get("template_blueprint_path")
            strs_to_replace = config.get("strs_to_replace")

            log.info(f"Creating ROMSSimulation in {tmp_path / 'cstar_test_simulation'}")
            modified_blueprint = modify_template_blueprint(
                template_blueprint_path=template_blueprint,
                strs_to_replace=strs_to_replace,
                out_dir=tmp_path / "cstar_test_simulation",
            )
            cstar_test_case = ROMSSimulation.from_blueprint(
                modified_blueprint,
            )

            cstar_test_case.setup()

            cstar_test_case.build()
            cstar_test_case.pre_run()
            test_process = cstar_test_case.run()
            test_process.updates(seconds=60)
            cstar_test_case.post_run()
