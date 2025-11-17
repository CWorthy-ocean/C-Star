import os
import subprocess
from collections import ChainMap
from collections.abc import Callable, Generator
from pathlib import Path
from unittest.mock import Mock, PropertyMock, call, mock_open, patch

import pytest

import cstar
from cstar.system.environment import CStarEnvironment


class MockEnvironment(cstar.system.environment.CStarEnvironment):
    def __init__(
        self,
        system_name="mock_system",
        mpi_exec_prefix="mock_mpi_prefix",
        compiler="mock_compiler",
    ):
        super().__init__(
            system_name=system_name,
            mpi_exec_prefix=mpi_exec_prefix,
            compiler=compiler,
        )


class TestSetupEnvironmentFromFiles:
    """Tests related to loading environment configuration from .env and .lmod files.

    Tests
    -----
    - test_load_lmod_modules: Confirms correct module loading sequence for systems with Linux Environment Modules.
    - test_env_file_loading: Validates environment variable loading and merging from .env files.
    - test_env_file_updating: Tests that environment variables are updated correctly in the user .env file

    Methods
    -------
    - get_expected_lmod_modules: Retrieves expected modules for each environment from system files.
    """

    @pytest.mark.parametrize("lmod_syshost", ["perlmutter", "derecho", "expanse"])
    @patch("cstar.base.utils.subprocess.run")
    @patch.object(
        cstar.system.environment.CStarEnvironment,
        "uses_lmod",
        new_callable=PropertyMock,
        return_value=True,
    )
    @patch.dict(
        "cstar.system.environment.os.environ", {"LMOD_CMD": "/mock/lmod"}, clear=True
    )
    def test_load_lmod_modules(self, mock_uses_lmod, mock_run, lmod_syshost):
        """Tests that the load_lmod_modules function correctly interacts with Linux
        Envionment Modules.

        This test uses the `get_expected_lmod_modules` method (below) to read the .lmod file
        corresponding to the system being mocked, then checks that C-Star's `module load` calls
        correspond to the modules described in the file.

        Mocks
        -----
        - mock_run: used to simulate successful calls to subprocess for `module <command> python`
        - uses_lmod is mocked to always return True (system uses Linux Environment Modules)
        - the $LMOD_SYSHOST environment variable is mocked to represent the system being tested
        - the $LMOD_CMD environment variable is mocked to represent the system's Lmod command

        Asserts
        -------
        - Confirms the correct sequence of subprocess calls for resetting and loading modules.
        """
        # Set up the LMOD_SYSHOST environment variable
        with patch.dict(
            "cstar.system.environment.os.environ", {"LMOD_SYSHOST": lmod_syshost}
        ):
            # Simulate subprocess.run returning valid Python code in stdout
            mock_run.return_value.stdout = (
                "os.environ['PATH'] = '/mocked/path:' + os.environ.get('PATH', '')"
            )
            # # Simulate success for each subprocess call
            mock_run.return_value.returncode = 0
            mock_run.return_value.stderr = ""

            # Instantiate the environment, which should trigger load_lmod_modules
            env = MockEnvironment(system_name=lmod_syshost)
            # Retrieve the expected modules for the given environment
            expected_modules = self.get_expected_lmod_modules(env)

            # Define expected subprocess calls
            expected_calls = [
                call(
                    "/mock/lmod python reset",
                    capture_output=True,
                    shell=True,
                    text=True,
                ),
            ] + [
                call(
                    f"/mock/lmod python load {mod}",
                    capture_output=True,
                    shell=True,
                    text=True,
                )
                for mod in expected_modules
            ]

            mock_run.assert_has_calls(expected_calls, any_order=False)

    def get_expected_lmod_modules(self, env: CStarEnvironment) -> list[str]:
        """Retrieves the expected list of Lmod modules for a given system from a .lmod
        file.

        Returns
        -------
        lmod_list: Module names to be loaded, based on the system's `.lmod` file.
        """
        with open(env.lmod_path) as file:
            return file.readlines()

    @patch.dict(
        "os.environ",
        {
            "NETCDF_FORTRANHOME": "/mock/netcdf",
            "MVAPICH2HOME": "/mock/mpi",
            "LMOD_SYSHOST": "perlmutter",
            "LMOD_DIR": "/mock/lmod",  # Ensures `uses_lmod` is valid
        },
        clear=True,
    )
    def test_env_file_loading(
        self,
        tmp_path: Path,
        dotenv_path: Path,
        system_dotenv_path: Path,
    ):
        """Tests that environment variables are loaded and expanded correctly from .env
        files.

        Mocks
        -----
        - tmp_path creates temporary, emulated system and user .env files
        - CStarEnvironment.package_root is patched to the temporary directory to load these .env files

        Asserts
        -------
        - Confirms merged environment variables with expected values after expansion.
        """
        # Write simulated system .env content to the appropriate location
        system_dotenv_path.write_text(
            "NETCDFHOME=${NETCDF_FORTRANHOME}/\n"
            "MPIHOME=${MVAPICH2HOME}/\n"
            "MPIROOT=${MVAPICH2HOME}/\n"
        )
        # Write simulated user .env content with an overriding variable
        dotenv_path.write_text(
            "MPIROOT=/override/mock/mpi\n"
            "CUSTOM_VAR=custom_value\n"
        )  # fmt: skip
        # Patch the root path and expanduser to point to our temporary files
        with (
            patch.object(
                cstar.system.environment.CStarEnvironment, "package_root", new=tmp_path
            ),
        ):
            # Instantiate the environment to trigger loading the environment variables
            env = MockEnvironment()
            # Define expected final environment variables after merging and expansion
            expected_env_vars = {
                "NETCDFHOME": "/mock/netcdf/",  # Expanded from ${NETCDF_FORTRANHOME}
                "MPIHOME": "/mock/mpi/",  # Expanded from ${MVAPICH2HOME}
                "MPIROOT": "/override/mock/mpi",  # User-defined override
                "CUSTOM_VAR": "custom_value",
            }

            # Assert that environment variables were loaded, expanded, and merged as expected
            assert dict(env.environment_variables) == expected_env_vars

    @patch.dict(
        "os.environ",
        {
            "NETCDF_FORTRANHOME": "/mock/netcdf",
            "MVAPICH2HOME": "/mock/mpi",
            "LMOD_SYSHOST": "perlmutter",
            "LMOD_DIR": "/mock/lmod",  # Ensures `uses_lmod` is valid
        },
        clear=True,
    )
    def test_env_file_updating(
        self,
        dotenv_path: Path,
        mock_system_name: str,
        system_dotenv_path: Path,
        tmp_path: Path,
    ):
        """Tests that environment variables are updated correctly in the user .env file.

        Mocks
        -----
        - tmp_path creates temporary, emulated system and user .env files
        - CStarEnvironment.package_root is patched to the temporary directory to load these .env files

        Asserts
        -------
        - Confirms merged environment variables with expected values after expansion.
        """
        # Write simulated system .env content to the appropriate location
        exp_system_env = {
            "NETCDFHOME": "${NETCDF_FORTRANHOME}/",
            "MPIHOME": "${MVAPICH2HOME}/",
            "MPIROOT": "${MVAPICH2HOME}/",
        }
        sys_env_content = "\n".join(f"{k}={v}" for k, v in exp_system_env.items())
        system_dotenv_path.write_text(sys_env_content)
        # Write simulated user .env content
        exp_user_env = {
            "MPIROOT": "/user-overridden/mpi",
            "CUSTOM_VAR": "custom_value",
            "EMPTY": "",
        }
        user_env_content = "\n".join(f"{k}={v}" for k, v in exp_user_env.items())
        dotenv_path.write_text(f"{user_env_content}\n")

        # Patch the root path and expanduser to point to our temporary files
        with (
            patch(
                "cstar.system.environment.CStarEnvironment.system_env_path",
                new_callable=PropertyMock,
                return_value=system_dotenv_path,
            ),
        ):
            # Instantiate the environment to trigger loading the environment variables
            env = CStarEnvironment(
                system_name=mock_system_name,
                mpi_exec_prefix="mpi-prefix",
                compiler="gnu",
            )

            # Confirm variables written in system.env file are available
            actual_env = env.environment_variables
            for key in exp_system_env:
                assert key in actual_env

            # Confirm variables set (or overriden) in user .env match
            for key, exp_value in exp_user_env.items():
                assert actual_env[key] == exp_value

            k0, v0 = "NETCDFHOME", "updated/value"
            k1, v1 = "TEST_VAR", "test-value"
            k2, v2 = "OTHER_EMPTY", ""
            updated_vars = [(k0, v0), (k1, v1), (k2, v2)]

            for key, exp_value in updated_vars:
                env.set_env_var(key, exp_value)

            # Confirm setting a key on CStarEnvironment is loaded to os.env and persisted
            raw_env = dotenv_path.read_text()

            for key, exp_value in updated_vars:
                # Confirm the active environment variable is updated
                assert exp_value in os.environ.get(key, "key-not-found")

                # Confirm the value stored in the user environment is updated
                assert exp_value in env.environment_variables[key]

                # Confirm the value is persisted to disk
                assert key in raw_env

    @patch.dict(
        "os.environ",
        {
            "NETCDF_FORTRANHOME": "/mock/netcdf",
            "MVAPICH2HOME": "/mock/mpi",
            "LMOD_SYSHOST": "perlmutter",
            "LMOD_DIR": "/mock/lmod",  # Ensures `uses_lmod` is valid
        },
        clear=True,
    )
    @pytest.mark.parametrize(
        "system_name,expected_path",
        [
            ["perlmutter", "additional_files/lmod_lists/perlmutter.lmod"],
            ["derecho", "additional_files/lmod_lists/derecho.lmod"],
            ["expanse", "additional_files/lmod_lists/expanse.lmod"],
        ],
    )
    def test_lmod_path(
        self,
        tmp_path: Path,
        system_name: str,
        expected_path: str,
        custom_system_env: Callable[[dict[str, str]], Generator[Mock, None, None]],
        custom_user_env: Callable[[dict[str, str]], Generator[Mock, None, None]],
    ):
        """Verify that the lmod_path property returns the correct path based on the
        system name.

        Mocks
        -----
        - tmp_path creates temporary, emulated system and user .env files
        - CStarEnvironment.package_root is patched to the temporary directory to load these .env files

        Asserts
        -------
        - Confirms merged environment variables with expected values after expansion.
        """
        # Write simulated system .env content to the appropriate location
        custom_system_env(
            {
                "NETCDFHOME": "${NETCDF_FORTRANHOME}/",
                "MPIHOME": "${MVAPICH2HOME}/",
                "MPIROOT": "${MVAPICH2HOME}/",
            }
        )

        # Write simulated user .env content
        custom_user_env(
            {
                "MPIROOT": "/user-overridden/mpi",
                "CUSTOM_VAR": "custom_value",
                "EMPTY": "",
            }
        )

        # Patch the root path and expanduser to point to our temporary files
        with patch.object(CStarEnvironment, "package_root", new=tmp_path):
            # Instantiate the environment to trigger loading the environment variables
            env = CStarEnvironment(
                system_name=system_name,
                mpi_exec_prefix="mpi-prefix",
                compiler="gnu",
            )

            assert env.lmod_path == tmp_path / expected_path

    @patch.dict(
        "os.environ",
        {
            "NETCDF_FORTRANHOME": "/mock/netcdf",
            "MVAPICH2HOME": "/mock/mpi",
            "LMOD_SYSHOST": "perlmutter",
            "LMOD_DIR": "/mock/lmod",  # Ensures `uses_lmod` is valid
        },
        clear=True,
    )
    @pytest.mark.parametrize(
        ("package_root", "system_name"),
        [
            ["foo/bar", "system-name-1"],
            ["boo/far", "system-name-2"],
            ["foz/baz", "system-name-3"],
        ],
    )
    def test_system_env_path(
        self,
        tmp_path: Path,
        package_root: str,
        system_name: str,
    ):
        """Verify that the system env property is based on package root and system name.

        Mocks
        -----
        - tmp_path creates temporary, emulated system and user .env files
        - CStarEnvironment.package_root is patched to the temporary directory to load these .env files
        - system_name provides a test-safe system name

        Asserts
        -------
        - Confirms merged environment variables with expected values after expansion.
        """
        # Patch the root path and expanduser to point to our temporary files
        tmp_pkg_root = tmp_path / package_root

        env = CStarEnvironment(
            system_name=system_name,
            mpi_exec_prefix="mpi-prefix",
            compiler="gnu",
        )

        with patch.object(CStarEnvironment, "package_root", new=tmp_pkg_root):
            # Instantiate the environment to trigger loading the environment variables

            system_actual = env.system_env_path
            expected = tmp_pkg_root / f"additional_files/env_files/{system_name}.env"

            assert system_actual == expected

    @patch.dict(
        "os.environ",
        {},
        clear=True,
    )
    def test_env_file_load_count(
        self, mock_system_name: str, dotenv_path: Path
    ) -> None:
        """Verify that env files are reloaded after an update.

        Mocks
        -----
        - mock_system_name provides a temporary system name for the environment

        Asserts
        -------
        - Confirms merged environment variables with expected values after expansion.
        """
        sys_var = "system-var"
        usr_var = "user-var"
        exp_system_env = {sys_var: "system-value"}
        exp_user_env = {usr_var: "user-value"}

        new_var, new_value = "new-user-var", "new-user-value"
        updates = {new_var: new_value}

        env0 = ChainMap(exp_system_env, exp_user_env)
        env1 = ChainMap(env0, updates)

        # patch the _load function so we can show loads only occur after updates
        with (
            patch(
                "cstar.system.environment.CStarEnvironment._load_env",
                new_callable=Mock,
                side_effect=[env0, env1],
            ) as loader,
        ):
            # Instantiate the environment to trigger loading the environment variables
            env = CStarEnvironment(
                system_name=mock_system_name,
                mpi_exec_prefix="mpi-prefix",
                compiler="gnu",
            )

            initial_num_calls = 1
            assert loader.call_count == initial_num_calls

            assert sys_var in env.environment_variables
            assert usr_var in env.environment_variables

            # no load from disk should occur
            assert loader.call_count == initial_num_calls

            env.set_env_var(new_var, new_value)

            # update should trigger load from disk
            assert loader.call_count == initial_num_calls + 1
            assert new_var in env.environment_variables

    @patch.dict(
        "os.environ",
        {},
        clear=True,
    )
    def test_env_vars_frozen(
        self,
        mock_system_name: str,
    ) -> None:
        """Verify that modifying the output from the .environment_variables property
        does not result in a change to the actual environment.

        Mocks
        -----
        - mock_system_name provides a temporary system name for the environment

        Asserts
        -------
        - Confirm that CStarEnvironment does not allow environment variables to
          be manipulated outside of using `set_env_var`
        """
        sys_var = "system-var"
        exp_system_env = {sys_var: "system-value"}

        with patch(
            "cstar.system.environment.CStarEnvironment._load_env",
            new_callable=Mock,
            side_effect=[exp_system_env],
        ):
            # Instantiate the environment to trigger loading the environment variables
            env = CStarEnvironment(
                system_name=mock_system_name,
                mpi_exec_prefix="mpi-prefix",
                compiler="gnu",
            )

            # baseline test the property and expected values match
            loaded_vars = env.environment_variables
            assert loaded_vars == exp_system_env

            # manipulate loaded vars and verify env is not changed
            inserted_var = "inserted-var"
            loaded_vars[inserted_var] = "inserted-value"

            assert inserted_var not in env.environment_variables


class TestStrAndReprMethods:
    """Tests for the __str__ and __repr__ methods of CStarEnvironment.

    Tests
    -----
    - test_str_method: Validates formatted output of the __str__ method.
    - test_repr_method: Confirms accurate representation of state in __repr__.
    """

    @patch.object(
        CStarEnvironment,
        "uses_lmod",
        new_callable=PropertyMock(return_value=False),
    )
    def test_str_method(self, tmp_path: Path, custom_system_env, custom_user_env):
        """Tests that __str__ produces a formatted, readable summary.

        Mocks
        -----
        - Mock values for system_name and environment_variables.

        Asserts
        -------
        - Confirms that str(env) matches the expected formatted output, including all key attributes
          like system name, scheduler, compiler, primary queue, and environment variables.
        """
        # Set up our mock environment with some sample properties

        # Manually construct the expected string output
        expected_str = (
            "MockEnvironment\n"
            "---------------\n"  # Length of dashes matches "MockEnvironment"
            "Compiler: mock_compiler\n"
            "MPI Exec Prefix: mock_mpi_prefix\n"
            "Uses Lmod: False\n"
            "Environment Variables:\n"
            "    VAR1: value1\n"
            "    VAR2: value2"
        )

        with patch(
            "cstar.system.environment.CStarEnvironment._load_env",
            new_callable=Mock,
            return_value={"VAR1": "value1", "VAR2": "value2"},
        ):
            # Instantiate the environment to trigger loading the cstar environment variables
            env = MockEnvironment()
            assert str(env) == expected_str

    @patch.object(
        CStarEnvironment,
        "uses_lmod",
        new_callable=PropertyMock(return_value=False),
    )
    def test_repr_method(self, _mock_uses_lmod):
        """Tests that __repr__ produces a detailed, state-reflective representation.

        Mocks
        -----
        - Mock values for system_name.

        Asserts
        -------
        - Confirms that repr(env) matches the expected output format, which includes initialization
          state and key properties like compiler, scheduler, and uses_lmod status.
        """
        # Similar to above, with mock values
        env = MockEnvironment()

        # Manually construct the expected repr output
        expected_repr = (
            "MockEnvironment(system_name='mock_system', compiler='mock_compiler')"
            "\nState: <uses_lmod=False>"
        )

        assert repr(env) == expected_repr


class TestExceptions:
    """Tests for exception handling across CStarEnvironment methods.

    Tests
    -----
    - test_system_name_raises_environment_error_for_missing_lmod_vars: Confirms error on missing Lmod variables.
    - test_system_name_raises_environment_error_for_missing_platform_info: Confirms error on missing platform info.
    - test_root_raises_import_error_when_package_not_found: Confirms ImportError when root package is missing.
    - test_load_lmod_modules_raises_environment_error_when_lmod_not_used: Confirms error when Lmod isn't used.
    - test_load_lmod_modules_raises_runtime_error_on_module_reset_failure: Confirms RuntimeError on module reset failure.
    - test_load_lmod_modules_raises_runtime_error_on_module_load_failure: Confirms RuntimeError on module load failure.
    - test_cores_per_node_raises_error_when_cpu_count_is_none: Confirms error when cpu_count is None.
    """

    def setup_method(self):
        """Sets up common patches for subprocess and environment variables.

        Mocks
        -----
        - subprocess.run: Simulates subprocess calls to avoid real system command execution.
        - CStarEnvironment.uses_lmod: Patched to simulate environments that use or don’t use Lmod.
        - os.environ: Cleared and patched with specific values for test isolation.
        """
        self.subprocess_patcher = patch(
            "cstar.base.utils.subprocess.run",
            return_value=subprocess.CompletedProcess(args="module reset", returncode=0),
        )
        self.mock_subprocess = self.subprocess_patcher.start()
        self.uses_lmod_patcher = patch.object(
            MockEnvironment, "uses_lmod", new_callable=PropertyMock
        )
        self.mock_uses_lmod = self.uses_lmod_patcher.start()

        self.os_environ_patcher = patch.dict(
            "cstar.system.environment.os.environ",
            {"LMOD_SYSHOST": "mock_system"},
            clear=True,
        )
        self.os_environ_patcher.start()

    def teardown_method(self):
        """Stops all patches after each test."""
        self.subprocess_patcher.stop()
        self.uses_lmod_patcher.stop()
        self.os_environ_patcher.stop()

    @patch("cstar.system.environment.importlib.util.find_spec", return_value=None)
    def test_package_root_raises_import_error_when_package_not_found(
        self, mock_find_spec
    ):
        """Tests that missing package spec raises an ImportError in package_root
        property.

        Mocks
        -----
        - importlib.util.find_spec: Returns None to simulate missing package spec.

        Asserts
        -------
        - Raises ImportError with a message indicating top-level package could not be found.
        """
        self.mock_uses_lmod.return_value = False
        with pytest.raises(ImportError, match="Top-level package '.*' not found"):
            MockEnvironment().package_root

    def test_load_lmod_modules_raises_environment_error_when_lmod_not_used(self):
        """Tests that load_lmod_modules raises an EnvironmentError if Lmod is not used.

        Mocks
        -----
        - CStarEnvironment.uses_lmod: Returns False to simulate an environment that doesn’t use Lmod.

        Asserts
        -------
        - Raises EnvironmentError with a message indicating Lmod modules are not supported on the system.
        """
        self.mock_uses_lmod.return_value = False
        with pytest.raises(
            EnvironmentError, match="does not appear to use Linux Environment Modules"
        ):
            env = MockEnvironment()
            env.load_lmod_modules(lmod_file="/some/file")

    @patch.dict(
        "cstar.system.environment.os.environ", {"LMOD_CMD": "/mock/lmod"}, clear=True
    )
    @patch("cstar.base.utils.subprocess.run")
    def test_load_lmod_modules_raises_runtime_error_on_module_reset_failure(
        self, mock_subprocess
    ):
        """Tests that a RuntimeError is raised if `module reset` fails.

        Mocks
        -----
        - mock_subprocess mocks a failure for the `module reset` call
        - os.environ: Sets "LMOD_CMD" to a mock path ("/mock/lmod") to simulate Lmod availability.

        Asserts
        -------
        - Raises RuntimeError with a message indicating failure of the "module reset" command.
        - subprocess.run is called with the expected command for `module reset`.
        """
        mock_subprocess.return_value.returncode = 1  # Simulate failure
        mock_subprocess.return_value.stderr = "Module reset error"

        with pytest.raises(
            RuntimeError,
            match=(
                "Linux Environment Modules command `/mock/lmod python reset` failed. "
                "Return Code: `1`. STDERR:\nModule reset error"
            ),
        ):
            MockEnvironment()

        # Verify that subprocess was called with the correct command
        mock_subprocess.assert_called_once_with(
            "/mock/lmod python reset", shell=True, text=True, capture_output=True
        )

    @patch.dict(
        "cstar.system.environment.os.environ", {"LMOD_CMD": "/mock/lmod"}, clear=True
    )
    @patch("builtins.open", new_callable=mock_open, read_data="module1\nmodule2\n")
    def test_load_lmod_modules_raises_runtime_error_on_module_load_failure(
        self, mock_open_file
    ):
        """Tests that a RuntimeError is raised if `module load` fails.

        Mocks
        -----
        - builtins.open: Simulates the .lmod file with two modules ("module1" and "module2").
        - os.environ: Sets "LMOD_CMD" to a mock path ("/mock/lmod") to simulate Lmod availability.

        Asserts
        -------
        - Raises RuntimeError with a message indicating failure of the "module load" command.
        - subprocess.run is called with the expected commands `reset` and `load`
        """
        # Define side effects for subprocess.run
        side_effects = [
            subprocess.CompletedProcess(
                args="/mock/lmod python reset", returncode=0, stdout="pass\n"
            ),  # Successful reset
            subprocess.CompletedProcess(
                args="/mock/lmod python load module1",
                returncode=1,
                stderr="Module load error",
            ),  # Failing module1 load
        ]
        self.mock_subprocess.side_effect = side_effects

        # Run the test and expect a RuntimeError

        with pytest.raises(
            RuntimeError,
            match=(
                "Linux Environment Modules command `/mock/lmod python load module1` "
                "failed. Return Code: `1`. STDERR:\nModule load error"
            ),
        ):
            MockEnvironment()

        assert mock_open_file.called
