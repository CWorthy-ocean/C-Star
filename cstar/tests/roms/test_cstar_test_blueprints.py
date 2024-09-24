import cstar
import pytest


class TestCStar:
    @pytest.mark.parametrize(
        "blueprint_key",
        ["cstar_test_with_netcdf_datasets", "cstar_test_with_yaml_datasets"],
    )
    @pytest.mark.parametrize(
        "use_local_sources",
        [
            False,
        ],
    )
    def test_cstar(
        self,
        tmpdir,
        mock_user_input,
        blueprint_as_path,
        fetch_roms_tools_source_data,
        fetch_remote_test_case_data,
        blueprint_key,
        use_local_sources,
    ):
        # Regardless of remote or local, if yaml_datasets we need roms-tools support data
        if blueprint_key == "cstar_test_with_yaml_datasets":
            # SP=Path("roms_tools_datasets").resolve()
            fetch_roms_tools_source_data(symlink_path="roms_tools_datasets")

        if use_local_sources:
            fetch_remote_test_case_data()

        cstar_test_base_blueprint = blueprint_as_path(
            blueprint_key, use_local_sources=use_local_sources
        )
        print(f"Creating Case in {tmpdir / 'cstar_test_case'}")
        cstar_test_case = cstar.Case.from_blueprint(
            blueprint=cstar_test_base_blueprint,
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
