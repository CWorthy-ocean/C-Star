import pytest
from cstar import Case
from cstar.tests.integration_tests.config import TEST_CONFIG


class TestCStar:
    @pytest.mark.parametrize(
        "test_config_key",
        [
            "test_case_remote_with_netcdf_datasets",
            "test_case_remote_with_yaml_datasets",
            "test_case_local_with_netcdf_datasets",
            "test_case_local_with_yaml_datasets",
        ],
    )
    def test_cstar(
        self,
        tmpdir,
        mock_user_input,
        modify_template_blueprint,
        fetch_roms_tools_source_data,
        fetch_remote_test_case_data,
        test_config_key,
    ):
        """Run the C-Star minimal test case from a selection of different blueprints.

        Parameters:
        -----------
        tmpdir:
           Built-in pytest fixture to create temporary directories
        mock_user_input:
           Fixture to simulate user-supplied input
        modify_template_blueprint:
           Fixture to modify the contents of a template blueprint and save to a
           temporary file (from which the Case instance is created)
        fetch_roms_tools_source_data:
           Fixture to fetch source data needed by roms-tools to generate input
           datasets for ROMS. Source data are saved to the system cache at the
           location specified by ROMS_TOOLS_DATA_DIRECTORY in the config.py module.
           A symlink to this cache directory containing this source data is
           created wherever this test is run and deleted on exit.
        fetch_remote_test_case_data:
           Fixture to fetch data needed for the C-Star test case, such as additional
           code for compiling ROMS, yaml files to supply to roms-tools, etc.
           These are saved to a subdirectory of the system cache specified by
           CSTAR_TEST_DATA_DIRECTORY in the config.py module
        test_config_key (str):
           Key determining the specific variation of the test case that is run, as
           defined in the dictionary TEST_CONFIG in the config.py module
        """

        # Regardless of remote or local, if yaml_datasets we need roms-tools support data
        if "yaml_datasets" in test_config_key:
            fetch_roms_tools_source_data(symlink_path="roms_tools_datasets")

        if "local" in test_config_key:
            fetch_remote_test_case_data()

        config = TEST_CONFIG.get(test_config_key)
        template_blueprint = config.get("template_blueprint_path")
        strs_to_replace = config.get("strs_to_replace")

        print(f"Creating Case in {tmpdir / 'cstar_test_case'}")
        modified_blueprint = modify_template_blueprint(
            template_blueprint_path=template_blueprint, strs_to_replace=strs_to_replace
        )

        cstar_test_case = Case.from_blueprint(
            modified_blueprint,
            caseroot=tmpdir / "cstar_test_case",
            start_date="20120101 12:00:00",
            end_date="20120101 12:10:00",
        )

        with mock_user_input("y"):
            cstar_test_case.setup()

        cstar_test_case.to_blueprint(tmpdir / "test_blueprint_persistence.yaml")
        cstar_test_case.build()
        cstar_test_case.pre_run()
        cstar_test_case.run()
        cstar_test_case.post_run()
