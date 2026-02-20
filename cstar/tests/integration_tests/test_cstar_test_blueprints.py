import logging
import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.roms.simulation import ROMSSimulation


class TestCStar:
    @pytest.mark.parametrize(
        "test_config_key",
        [
            "test_case_remote_with_netcdf_datasets",
            "test_case_local_with_netcdf_datasets",
        ],
    )
    @pytest.mark.asyncio
    async def test_cstar(
        self,
        tmp_path: Path,
        modify_template_blueprint: t.Callable[
            [Path | str, dict[str, str], Path | str], str
        ],
        mock_lmod_path: Path,
        fetch_remote_test_case_data: t.Callable[[], None],
        test_config_key: str,
        log: logging.Logger,
        integration_test_configuration: dict[str, dict[str, str | dict[str, str]]],
    ) -> None:
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
           the `cstar_test_data_directory` fixture
        test_config_key (str):
           Key determining the specific variation of the test case that is run, as
           defined in the `integration_test_configuration` fixture
        log (logging.Logger):
            Logger instance for logging messages during test execution.
        """
        ext_root = tmp_path / "externals"

        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.lmod_path", mock_lmod_path
            ),
            mock.patch(
                "cstar.system.environment.CStarEnvironment.package_root", new=ext_root
            ),
        ):
            if "local" in test_config_key:
                fetch_remote_test_case_data()

            config = integration_test_configuration.get(test_config_key)
            if config is None:
                msg = (
                    "No test configuration found for the provided key: "
                    f"{test_config_key}. Please check the TEST_CONFIG dictionary."
                )
                raise ValueError(msg)

            template_blueprint_path = str(config.get("template_blueprint_path"))
            strs_to_replace = t.cast("dict[str, str]", config.get("strs_to_replace"))
            out_dir = tmp_path / "cstar_test_simulation"

            msg = f"Creating ROMSSimulation in {tmp_path / 'cstar_test_simulation'}"
            log.info(msg)
            modified_blueprint = modify_template_blueprint(
                template_blueprint_path,
                strs_to_replace,
                out_dir,
            )
            cstar_test_case = ROMSSimulation.from_blueprint(modified_blueprint)

            cstar_test_case.setup()
            cstar_test_case.build()
            cstar_test_case.pre_run()
            test_process = cstar_test_case.run()
            await test_process.updates(seconds=60)
            cstar_test_case.post_run()
