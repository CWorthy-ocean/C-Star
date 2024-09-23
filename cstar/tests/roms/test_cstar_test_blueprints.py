import pytest

import cstar


class TestCStar:
    @pytest.mark.parametrize("use_local_sources", [False])
    def test_cstar(
        self,
        tmpdir,
        mock_user_input,
        blueprint_as_path,
        use_local_sources,
    ):
        cstar_test_base_blueprint = blueprint_as_path(
            "cstar_test_with_netcdf_datasets", use_local_sources=use_local_sources
        )

        cstar_test_case = cstar.Case.from_blueprint(
            blueprint=cstar_test_base_blueprint,
            caseroot=tmpdir,
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
