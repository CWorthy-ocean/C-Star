import pathlib
import subprocess
import unittest.mock as mock

import dotenv
import pytest

import cstar
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.system.manager import cstar_sysmgr


class TestMARBLExternalCodeBaseInit:
    """Test initialization of MARBLExternalCodeBase"""

    @mock.patch(
        "cstar.marbl.external_codebase.MARBLExternalCodeBase.is_configured",
        new_callable=mock.PropertyMock,
        return_value=False,
    )
    @mock.patch("cstar.base.external_codebase.SourceData.__init__")
    def test_init_with_args(self, mock_source_init, mock_is_configured):
        """Test Initialization with provided source_repo and checkout_target args

        Asserts SourceData is instantiated with the correct parameters.
        """
        mock_source_init.return_value = None  # __init__ should return None

        source_repo = "https://github.com/dafyddstephenson/MARBL.git"
        checkout_target = "main"

        MARBLExternalCodeBase(source_repo=source_repo, checkout_target=checkout_target)

        mock_source_init.assert_called_once_with(
            location=source_repo, identifier=checkout_target
        )

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
        assert (
            marbl_codebase._default_source_repo
            == "https://github.com/marbl-ecosys/MARBL.git"
        )

        assert marbl_codebase._default_checkout_target == "marbl0.45.0"
        assert marbl_codebase.root_env_var == "MARBL_ROOT"


class TestMARBLExternalCodeBaseConfigure:
    def test_configure_success(
        self,
        marblexternalcodebase_staged,
        dotenv_path: pathlib.Path,
        marbl_path: pathlib.Path,
    ):
        """Test that the _configure method succeeds when subprocess calls succeed."""
        mecb = marblexternalcodebase_staged
        # marbl_path = tmp_path
        with mock.patch.object(
            cstar.marbl.external_codebase, "_run_cmd"
        ) as mock_run_cmd:
            mock_run_cmd.return_value.returncode = 0
            mecb._configure()

        ## Check that environment was updated correctly
        actual_value = dotenv.get_key(dotenv_path, mecb.root_env_var)
        assert actual_value == str(mecb.working_copy.path)

        mock_run_cmd.assert_called_once_with(
            f"make {cstar_sysmgr.environment.compiler} USEMPI=TRUE",
            cwd=marbl_path / "src",
            msg_pre="Compiling MARBL...",
            msg_post=f"MARBL successfully installed at {marbl_path}",
            msg_err="Error when compiling MARBL.",
            raise_on_error=True,
        )

    @mock.patch("cstar.base.utils.subprocess.run")
    def test_make_failure(
        self,
        mock_subprocess,
        marblexternalcodebase_staged,
        tmp_path,
        dotenv_path,
    ):
        """Test that the _configure method raises an error when 'make' fails."""
        mock_subprocess.side_effect = [
            subprocess.CompletedProcess(
                args=["make"],
                returncode=1,
                stdout="",
                stderr="Mocked MARBL Compilation Failure",
            )
        ]
        mecb = marblexternalcodebase_staged

        # Test
        with pytest.raises(
            RuntimeError,
            match=(
                "Error when compiling MARBL. Return Code: `1`. STDERR:\n"
                "Mocked MARBL Compilation Failure"
            ),
        ):
            mecb._configure()

    def test_is_configured_when_configured(
        self,
        marblexternalcodebase_staged,
        tmp_path,
        dotenv_path,
    ):
        """Tests that the `is_configured` property is True when all conditions met.

        Conditions:
        - environment variable is defined
        - local repository clone is clean
        - lib file exists
        - inc dir is populated
        """
        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value={"MARBL_ROOT": tmp_path},
            ),
            mock.patch(
                "cstar.marbl.external_codebase._check_local_repo_changed_from_remote",
                return_value=False,
            ),
            mock.patch("pathlib.Path.exists", return_value=True),
            mock.patch(
                "pathlib.Path.iterdir", return_value=["fake", "files", "in", "dir"]
            ),
        ):
            assert marblexternalcodebase_staged.is_configured

    @pytest.mark.parametrize(
        "env_var_defined, repo_changed, lib_exists, inc_dir_contents, expected",
        [
            # 1. Missing MARBL_ROOT
            (False, False, True, ["fake", "filelist"], False),
            # 2. Repo has changed
            (True, True, True, ["fake", "filelist"], False),
            # 3. Library file missing
            (True, False, False, ["fake", "filelist"], False),
            # 4. Include dir empty
            (True, False, True, [], False),
            # 5. Fully configured
            (True, False, True, ["fake", "filelist"], True),
        ],
    )
    def test_is_configured_variants(
        self,
        marblexternalcodebase_staged: MARBLExternalCodeBase,
        tmp_path: pathlib.Path,
        env_var_defined: bool,
        repo_changed: bool,
        lib_exists: bool,
        inc_dir_contents: list[str],
        expected: bool,
    ):
        """Tests all possible combinations of conditions for `MARBLExternalCodeBase.is_configured`.

        Parameters
        ----------
        env_var_defined (bool):
            Whether the MARBL environment variable is defined
        repo_changed (bool):
            Whether the local repository clone is clean
        lib_exists (bool):
            Whether the library file for this configuration exists
        inc_dir_contents (list[str]):
            The (mocked) contents of the include directory
        expected (bool):
            The expected outcome of the property with the other parameters.
        """
        env_vars = {"MARBL_ROOT": str(tmp_path)} if env_var_defined else {}

        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value=env_vars,
            ),
            mock.patch(
                "cstar.marbl.external_codebase._check_local_repo_changed_from_remote",
                return_value=repo_changed,
            ),
            mock.patch("pathlib.Path.exists", return_value=lib_exists),
            mock.patch("pathlib.Path.iterdir", return_value=inc_dir_contents),
        ):
            assert marblexternalcodebase_staged.is_configured is expected
