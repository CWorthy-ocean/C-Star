from cstar.marbl.external_codebase import MARBLExternalCodeBase


class TestMARBLExternalCodeBaseInit:
    """Test initialization of MARBLExternalCodeBase"""

    def test_init_with_args(self):
        """Test that MARBLExternalCodeBase is initialized correctly with user args"""
        source_repo = "https://github.com/dafyddstephenson/MARBL.git"
        checkout_target = "main"
        marbl_codebase = MARBLExternalCodeBase(
            source_repo=source_repo, checkout_target=checkout_target
        )
        assert marbl_codebase.source.location == source_repo
        assert marbl_codebase.source.checkout_target == checkout_target
        assert (
            marbl_codebase._default_source_repo
            == "https://github.com/marbl-ecosys/MARBL.git"
        )
        assert marbl_codebase._default_checkout_target == "marbl0.45.0"
        assert marbl_codebase.root_env_var == "MARBL_ROOT"

    def test_init_without_args(self):
        """Test that the defaults are set correctly if MARBLExternalCodeBase initialized
        without args.
        """
        marbl_codebase = MARBLExternalCodeBase()
        assert marbl_codebase.source.location == marbl_codebase._default_source_repo
        assert (
            marbl_codebase.source.checkout_target
            == marbl_codebase._default_checkout_target
        )


class TestMARBLExternalCodeBaseConfigure:
    """TODO Add tests for new .get() and .configure() methods"""

    pass
