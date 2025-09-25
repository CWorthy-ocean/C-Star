from cstar.roms.external_codebase import ROMSExternalCodeBase


class TestROMSExternalCodeBaseInit:
    def test_init_with_args(self):
        """Test ROMSExternalCodeBase initializes correctly with arguments."""
        source_repo = "https://www.github.com/CESR-lab/ucla-roms.git"
        checkout_target = "246c11fa537145ba5868f2256dfb4964aeb09a25"
        roms_codebase = ROMSExternalCodeBase(
            source_repo=source_repo, checkout_target=checkout_target
        )
        assert roms_codebase.source.location == source_repo
        assert roms_codebase.source.checkout_target == checkout_target
        assert (
            roms_codebase._default_source_repo
            == "https://github.com/CWorthy-ocean/ucla-roms.git"
        )
        assert roms_codebase._default_checkout_target == "main"
        assert roms_codebase.root_env_var == "ROMS_ROOT"

    def test_init_without_args(self):
        """Test ROMSExternalCodeBase uses defaults when no args provided."""
        roms_codebase = ROMSExternalCodeBase()
        assert (
            roms_codebase.source.checkout_target
            == roms_codebase._default_checkout_target
        )
        assert roms_codebase.source.location == roms_codebase._default_source_repo


class TestROMSExternalCodeBaseConfigure:
    """TODO Add tests for new .get() and .configure() methods. Previous tests:
    test_get_success
        Verifies that `get` completes successfully, setting environment variables and
        calling necessary subprocesses when all commands succeed.
    test_make_nhmg_failure
        Ensures that `get` raises an error with a descriptive message when the `make nhmg`
        command fails during installation.
    test_make_tools_roms_failure
        Confirms that `get` raises an error with an appropriate message if `make Tools-Roms`
        fails after `make nhmg` succeeds.


    """

    pass
