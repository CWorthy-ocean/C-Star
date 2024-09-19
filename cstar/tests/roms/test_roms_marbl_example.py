import pytest

import cstar


class TestRomsMarbl:
    @pytest.mark.parametrize("local_sources", [False])
    def test_roms_marbl(
        self,
        tmpdir,
        mock_user_input,
        test_blueprint_as_path,
        local_sources,
    ):
        roms_marbl_base_blueprint_filepath = test_blueprint_as_path(
            "ROMS_MARBL", local=local_sources
        )

        roms_marbl_case = cstar.Case.from_blueprint(
            blueprint=roms_marbl_base_blueprint_filepath,
            caseroot=tmpdir,
            start_date="20120103 12:00:00",
            end_date="20120103 12:30:00",
        )

        with mock_user_input("y"):
            roms_marbl_case.setup()

        # TODO why are we persisting this blueprint file then not using it again in the test?
        roms_marbl_case.persist(tmpdir / "test_blueprint.yaml")

        roms_marbl_case.build()
        roms_marbl_case.pre_run()
        roms_marbl_case.run()
        roms_marbl_case.post_run()
