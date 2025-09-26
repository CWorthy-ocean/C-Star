import unittest.mock as mock

from cstar.marbl.external_codebase import MARBLExternalCodeBase


class TestMARBLExternalCodeBaseInit:
    """Test initialization of MARBLExternalCodeBase"""

    @mock.patch(
        "cstar.marbl.external_codebase.MARBLExternalCodeBase.is_configured",
        new_callable=mock.PropertyMock,
        return_value=False,
    )
    @mock.patch("cstar.base.external_codebase.SourceData.__init__")
    def test_init_with_args(self, mock_source_init, mock_is_configured):
        mock_source_init.return_value = None  # __init__ should return None

        source_repo = "https://github.com/dafyddstephenson/MARBL.git"
        checkout_target = "main"

        marbl_codebase = MARBLExternalCodeBase(
            source_repo=source_repo, checkout_target=checkout_target
        )

        mock_source_init.assert_called_once_with(
            location=source_repo, identifier=checkout_target
        )

        assert (
            marbl_codebase._default_source_repo
            == "https://github.com/marbl-ecosys/MARBL.git"
        )

        assert marbl_codebase._default_checkout_target == "marbl0.45.0"
        assert marbl_codebase.root_env_var == "MARBL_ROOT"

    @mock.patch(
        "cstar.marbl.external_codebase.MARBLExternalCodeBase.is_configured",
        new_callable=mock.PropertyMock,
        return_value=False,
    )
    @mock.patch("cstar.base.external_codebase.SourceData.__init__")
    def test_init_without_args(self, mock_source_init, mock_is_configured):
        """Test that the defaults are set correctly if MARBLExternalCodeBase initialized
        without args.
        """
        mock_source_init.return_value = None
        marbl_codebase = MARBLExternalCodeBase()

        mock_source_init.assert_called_once_with(
            location=marbl_codebase._default_source_repo,
            identifier=marbl_codebase._default_checkout_target,
        )


class TestMARBLExternalCodeBaseConfigure:
    """TODO Add tests for new .get() and .configure() methods"""

    pass
