import os
import pytest
from unittest import mock
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.system.manager import cstar_sysmgr


@pytest.fixture
def marbl_codebase():
    """Fixture providing a configured instance of `MARBLExternalCodeBase` for
    testing."""
    source_repo = "https://github.com/marbl-ecosys/MARBL.git"
    checkout_target = "marbl0.45.0"
    return MARBLExternalCodeBase(
        source_repo=source_repo, checkout_target=checkout_target
    )


def test_default_source_repo(marbl_codebase):
    """Test if the default source repo is set correctly."""
    assert (
        marbl_codebase.default_source_repo
        == "https://github.com/marbl-ecosys/MARBL.git"
    )


def test_default_checkout_target(marbl_codebase):
    """Test if the default checkout target is set correctly."""
    assert marbl_codebase.default_checkout_target == "marbl0.45.0"


def test_expected_env_var(marbl_codebase):
    """Test if the expected environment variable is set correctly."""
    assert marbl_codebase.expected_env_var == "MARBL_ROOT"


def test_defaults_are_set():
    """Test that the defaults are set correctly if MARBLExternalCodeBase initialized
    without args."""

    marbl_codebase = MARBLExternalCodeBase()
    assert marbl_codebase.source_repo == "https://github.com/marbl-ecosys/MARBL.git"
    assert marbl_codebase.checkout_target == "marbl0.45.0"


class TestMARBLExternalCodeBaseGet:
    """Test cases for the `get` method of `MARBLExternalCodeBase`.

    Tests
    -----
    test_get_success
        Ensures that `get` completes successfully, setting environment variables and
        calling necessary methods when subprocess calls succeed.
    test_make_failure
        Verifies that `get` raises an error with a descriptive message when the
        `make` command fails during installation.

    Fixtures
    --------
    marbl_codebase : MARBLExternalCodeBase
        Provides a mock instance of `MARBLExternalCodeBase` with the default repository and
        checkout target.
    tmp_path : pathlib.Path
        Supplies a temporary directory for isolated file operations during testing.

    Mocks
    -----
    mock_subprocess_run : MagicMock
        Mocks `subprocess.run` to simulate `make` commands and other shell operations.
    mock_clone_and_checkout : MagicMock
        Mocks `_clone_and_checkout` to simulate repository cloning and checkout.
    mock_update_user_dotenv : MagicMock
        Mocks `_update_user_dotenv` to simulate writing environment variables to a config file.
    env_patch : MagicMock
        Mocks `os.environ` to control environment variables during tests.
    """

    def setup_method(self):
        """Common setup before each test method."""
        # Mock subprocess, _clone_and_checkout, and _update_user_dotenv
        self.mock_subprocess_run = mock.patch("subprocess.run").start()
        self.mock_clone_and_checkout = mock.patch(
            "cstar.marbl.external_codebase._clone_and_checkout"
        ).start()
        self.mock_update_user_dotenv = mock.patch(
            "cstar.marbl.external_codebase._update_user_dotenv"
        ).start()

        # Clear environment variables
        self.env_patch = mock.patch.dict(os.environ, {}, clear=True)
        self.env_patch.start()

    def teardown_method(self):
        """Common teardown after each test method."""
        mock.patch.stopall()

    def test_get_success(self, marbl_codebase, tmp_path):
        """Test that the get method succeeds when subprocess calls succeed."""
        # Setup:
        ## Make temporary target dir
        marbl_dir = tmp_path / "marbl"

        ## Mock success of calls to subprocess.run:
        self.mock_subprocess_run.return_value.returncode = 0

        # Test
        ## Call the get method
        marbl_codebase.get(target=marbl_dir)

        # Assertions:
        ## Check environment variables
        assert os.environ["MARBL_ROOT"] == str(marbl_dir)

        ## Check that _clone_and_checkout was (mock) called correctly
        self.mock_clone_and_checkout.assert_called_once_with(
            source_repo=marbl_codebase.source_repo,
            local_path=marbl_dir,
            checkout_target=marbl_codebase.checkout_target,
        )

        ## Check that _update_user_dotenv was (mock) called correctly
        env_file_str = f'MARBL_ROOT="{marbl_dir}"\n'
        self.mock_update_user_dotenv.assert_called_once_with(env_file_str)

        self.mock_subprocess_run.assert_called_once_with(
            f"make {cstar_sysmgr.environment.compiler} USEMPI=TRUE",
            cwd=f"{marbl_dir}/src",
            capture_output=True,
            text=True,
            shell=True,
        )

    def test_make_failure(self, marbl_codebase, tmp_path):
        """Test that the get method raises an error when 'make' fails."""

        ## There are two subprocess calls, we'd like one fail, one pass:
        self.mock_subprocess_run.side_effect = [
            mock.Mock(returncode=1, stderr="Compiling MARBL failed successfully"),
        ]

        # Test
        with pytest.raises(
            RuntimeError,
            match="Error 1 when compiling MARBL. STDERR stream: \n Compiling MARBL failed successfully",
        ):
            marbl_codebase.get(target=tmp_path)
