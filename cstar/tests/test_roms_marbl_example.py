import cstar


class TestRomsMarbl:
    def test_roms_marbl_remote_files(
        self, tmpdir, mock_user_input, example_blueprint_as_path
    ):
        """Test using URLs to point to input datasets"""

        roms_marbl_base_blueprint_filepath = example_blueprint_as_path("ROMS_MARBL")

        roms_marbl_remote_case = cstar.Case.from_blueprint(
            blueprint=roms_marbl_base_blueprint_filepath,
            caseroot=tmpdir,
            start_date="20120103 12:00:00",
            end_date="20120103 12:30:00",
        )

        with mock_user_input("y"):
            # do we actually need user input for all these steps?
            roms_marbl_remote_case.setup()

            # why are we persisting this blueprint file then not using it again in the test?
            # roms_marbl_remote_case.persist(tmpdir / "test_blueprint.yaml")

            roms_marbl_remote_case.build()
            roms_marbl_remote_case.pre_run()
            roms_marbl_remote_case.run()
            roms_marbl_remote_case.post_run()

    # @pytest.mark.xfail(reason="not yet implemented")
    def test_roms_marbl_local_files(
        self, tmpdir, mock_user_input, example_blueprint_as_path
    ):
        """Test using available local input datasets"""

        roms_marbl_base_blueprint_filepath_local_data = example_blueprint_as_path(
            "ROMS_MARBL",
            make_local=True,  # TODO use pytest.mark.parametrize to collapse these tests into one?
        )

        # TODO have a fixture that downloads the files to a temporary directory?
        # Does that basically just mean running case.setup()?

        roms_marbl_remote_case = cstar.Case.from_blueprint(
            blueprint=roms_marbl_base_blueprint_filepath_local_data,
            caseroot=tmpdir,
            start_date="20120103 12:00:00",
            end_date="20120103 12:30:00",
        )

        with mock_user_input("y"):
            # do we actually need user input for all these steps?
            roms_marbl_remote_case.setup()

            # why are we persisting this blueprint file then not using it again in the test?
            # roms_marbl_remote_case.persist(tmpdir / "test_blueprint.yaml")

            roms_marbl_remote_case.build()
            roms_marbl_remote_case.pre_run()
            roms_marbl_remote_case.run()
            roms_marbl_remote_case.post_run()
