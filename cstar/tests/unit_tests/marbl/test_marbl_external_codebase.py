import os
import pathlib
from unittest import mock

import dotenv
import pytest

from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.system.manager import cstar_sysmgr


class TestMARBLExternalCodeBaseInit:
    """Test initialization of MARBLExternalCodeBase"""

    def test_init_with_args(self):
        """Test that MARBLExternalCodeBase is initialized correctly with user args"""
        source_repo = "https://github.com/dafyddstephenson/MARBL.git"
        checkout_target = "main"
        marbl_codebase = MARBLExternalCodeBase(
            source_repo=source_repo, checkout_target=checkout_target
        )
        assert marbl_codebase.source_repo == source_repo
        assert marbl_codebase.checkout_target == checkout_target
        assert (
            marbl_codebase.default_source_repo
            == "https://github.com/marbl-ecosys/MARBL.git"
        )
        assert marbl_codebase.default_checkout_target == "marbl0.45.0"
        assert marbl_codebase.expected_env_var == "MARBL_ROOT"

    def test_init_without_args(self):
        """Test that the defaults are set correctly if MARBLExternalCodeBase initialized
        without args.
        """
        marbl_codebase = MARBLExternalCodeBase()
        assert marbl_codebase.source_repo == marbl_codebase.default_source_repo
        assert marbl_codebase.checkout_target == marbl_codebase.default_checkout_target


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
    tmp_path : pathlib.Path
        Supplies a temporary directory for isolated file operations during testing.

    Mocks
    -----
    mock_subprocess_run : MagicMock
        Mocks `subprocess.run` to simulate `make` commands and other shell operations.
    mock_clone_and_checkout : MagicMock
        Mocks `_clone_and_checkout` to simulate repository cloning and checkout.
    env_patch : MagicMock
        Mocks `os.environ` to control environment variables during tests.
    """

    def setup_method(self):
        """Common setup before each test method."""
        # Mock subprocess and _clone_and_checkout
        self.mock_subprocess_run = mock.patch("subprocess.run").start()
        self.mock_clone_and_checkout = mock.patch(
            "cstar.marbl.external_codebase._clone_and_checkout"
        ).start()
        self.marbl_codebase = MARBLExternalCodeBase()
        # Clear environment variables
        self.env_patch = mock.patch.dict(os.environ, {}, clear=True)
        self.env_patch.start()

    def teardown_method(self):
        """Common teardown after each test method."""
        mock.patch.stopall()

    def test_get_success(
        self,
        dotenv_path: pathlib.Path,
        marbl_path: pathlib.Path,
    ):
        """Test that the get method succeeds when subprocess calls succeed."""
        # Setup:
        ## Mock success of calls to subprocess.run:
        self.mock_subprocess_run.return_value.returncode = 0

        key = self.marbl_codebase.expected_env_var
        value = str(marbl_path)

        with mock.patch(
            "cstar.system.environment.CStarEnvironment.user_env_path",
            new_callable=mock.PropertyMock,
            return_value=dotenv_path,
        ):
            # Test
            ## Call the get method
            self.marbl_codebase.get(target=marbl_path)

            # Assertions:
            ## Check environment variables
            assert os.environ[key] == str(value)

            ## Check that _clone_and_checkout was (mock) called correctly
            self.mock_clone_and_checkout.assert_called_once_with(
                source_repo=self.marbl_codebase.source_repo,
                local_path=marbl_path,
                checkout_target=self.marbl_codebase.checkout_target,
            )

            ## Check that environment was updated correctly
            actual_value = dotenv.get_key(dotenv_path, key)
            assert actual_value == value

            self.mock_subprocess_run.assert_called_once_with(
                f"make {cstar_sysmgr.environment.compiler} USEMPI=TRUE",
                cwd=marbl_path / "src",
                capture_output=True,
                text=True,
                shell=True,
            )

    def test_make_failure(self, tmp_path):
        """Test that the get method raises an error when 'make' fails."""
        ## There are two subprocess calls, we'd like one fail, one pass:
        dotenv_path = tmp_path / ".cstar.env"

        self.mock_subprocess_run.side_effect = [
            mock.Mock(returncode=1, stderr="Mocked MARBL Compilation Failure"),
        ]

        # Test
        with (
            pytest.raises(
                RuntimeError,
                match=(
                    "Error when compiling MARBL. Return Code: `1`. STDERR:\n"
                    "Mocked MARBL Compilation Failure"
                ),
            ),
            mock.patch(
                "cstar.system.environment.CStarEnvironment.user_env_path",
                new_callable=mock.PropertyMock,
                return_value=dotenv_path,
            ),
        ):
            self.marbl_codebase.get(target=tmp_path)
