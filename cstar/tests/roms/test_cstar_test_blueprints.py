import pytest
from cstar.tests.config import TEST_CONFIG


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
        template_blueprint_to_case,
        fetch_roms_tools_source_data,
        fetch_remote_test_case_data,
        test_config_key,
    ):
        # Regardless of remote or local, if yaml_datasets we need roms-tools support data
        if "yaml_datasets" in test_config_key:
            fetch_roms_tools_source_data(symlink_path="roms_tools_datasets")

        if "local" in test_config_key:
            fetch_remote_test_case_data()

        config = TEST_CONFIG.get(test_config_key)
        template_blueprint = config.get("template_blueprint_path")
        strs_to_replace = config.get("strs_to_replace")

        print(f"Creating Case in {tmpdir / 'cstar_test_case'}")
        cstar_test_case = template_blueprint_to_case(
            template_blueprint_path=template_blueprint,
            strs_to_replace=strs_to_replace,
            caseroot=tmpdir / "cstar_test_case",
            start_date="20120101 12:00:00",
            end_date="20120101 12:10:00",
        )

        with mock_user_input("y"):
            cstar_test_case.setup()

        cstar_test_case.persist(tmpdir / "test_blueprint_persistence.yaml")
        cstar_test_case.build()
        cstar_test_case.pre_run()
        cstar_test_case.run()
        cstar_test_case.post_run()
