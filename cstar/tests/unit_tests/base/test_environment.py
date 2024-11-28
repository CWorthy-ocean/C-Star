import pytest
from unittest.mock import patch, call, PropertyMock, mock_open
import cstar
import subprocess


class MockEnvironment(cstar.base.environment.CStarEnvironment):
    def __init__(
        self,
        system_name="mock_system",
        mpi_exec_prefix="mock_mpi_prefix",
        compiler="mock_compiler",
        queue_flag="mock_queue_flag",
        primary_queue="mock_primary_queue",
        mem_per_node_gb=0,
        cores_per_node=0,
        max_walltime="00:00:00",
        other_scheduler_directives=None,
    ):
        if other_scheduler_directives is None:
            other_scheduler_directives = {"--mock": "directive"}

        super().__init__(
            system_name=system_name,
            mpi_exec_prefix=mpi_exec_prefix,
            compiler=compiler,
            queue_flag=queue_flag,
            primary_queue=primary_queue,
            mem_per_node_gb=mem_per_node_gb,
            cores_per_node=cores_per_node,
            max_walltime=max_walltime,
            other_scheduler_directives=other_scheduler_directives,
        )


class TestSetupEnvironmentFromFiles:
    """Tests related to loading environment configuration from .env and .lmod files.

    Tests
    -----
    - test_load_lmod_modules: Confirms correct module loading sequence for systems with Linux Environment Modules.
    - test_env_file_loading: Validates environment variable loading and merging from .env files.

    Methods
    -------
    - get_expected_lmod_modules: Retrieves expected modules for each environment from system files.
    """

    @pytest.mark.parametrize("lmod_syshost", ["perlmutter", "derecho", "expanse"])
    @patch("cstar.base.environment.subprocess.run")
    @patch.object(
        cstar.base.environment.CStarEnvironment,
        "uses_lmod",
        new_callable=PropertyMock,
        return_value=True,
    )
    @patch.dict(
        "cstar.base.environment.os.environ", {"LMOD_CMD": "/mock/lmod"}, clear=True
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
            "cstar.base.environment.os.environ", {"LMOD_SYSHOST": lmod_syshost}
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

    def get_expected_lmod_modules(self, env):
        """Retrieves the expected list of Lmod modules for a given system from a .lmod
        file.

        Returns
        -------
        lmod_list: Module names to be loaded, based on the system's `.lmod` file.
        """

        lmod_file_path = (
            f"{env.package_root}/additional_files/lmod_lists/{env._system_name}.lmod"
        )
        with open(lmod_file_path) as file:
            lmod_list = file.readlines()
            return lmod_list

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
    @patch.object(
        cstar.base.environment.CStarEnvironment, "uses_lmod", return_value=True
    )
    @patch.object(
        cstar.base.environment.CStarEnvironment, "load_lmod_modules", return_value=None
    )
    def test_env_file_loading(self, mock_load_lmod, mock_uses_lmod, tmp_path):
        """Tests that environment variables are loaded and expanded correctly from .env
        files.

        Mocks
        -----
        - tmp_path creates temporary, emulated system and user .env files
        - CStarEnvironment.package_root is patched to the temporary directory to load these .env files
        - CStarEnvironment.uses_lmod is patched to always be true
        - load_lmod_modules is patched to do nothing

        Asserts
        -------
        - Confirms merged environment variables with expected values after expansion.
        """

        # Set up the <root>/additional_files/env_files structure within tmp_path
        root_path = tmp_path
        env_files_dir = root_path / "additional_files" / "env_files"
        env_files_dir.mkdir(parents=True)

        # Define paths for the system and user .env files
        system_env_file_path = env_files_dir / "mock_system.env"
        user_env_file_path = tmp_path / ".cstar.env"

        # Write simulated system .env content to the appropriate location
        system_env_file_path.write_text(
            "NETCDFHOME=${NETCDF_FORTRANHOME}/\n"
            "MPIHOME=${MVAPICH2HOME}/\n"
            "MPIROOT=${MVAPICH2HOME}/\n"
        )
        # Write simulated user .env content with an overriding variable
        user_env_file_path.write_text(
            "MPIROOT=/override/mock/mpi\n" "CUSTOM_VAR=custom_value\n"
        )
        # Patch the root path and expanduser to point to our temporary files
        with patch.object(
            cstar.base.environment.CStarEnvironment, "package_root", new=root_path
        ):
            with patch(
                "cstar.base.environment.Path.expanduser",
                return_value=user_env_file_path,
            ):
                # # Instantiate the environment to trigger loading the environment variables
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


class TestStrAndReprMethods:
    """Tests for the __str__ and __repr__ methods of CStarEnvironment.

    Tests
    -----
    - test_str_method: Validates formatted output of the __str__ method.
    - test_repr_method: Confirms accurate representation of state in __repr__.
    """

    def test_str_method(self):
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
        with (
            patch.object(
                MockEnvironment, "environment_variables", new_callable=PropertyMock
            ) as mock_env_vars,
        ):
            mock_env_vars.return_value = {"VAR1": "value1", "VAR2": "value2"}

            env = MockEnvironment()
            # Manually construct the expected string output
            expected_str = (
                "MockEnvironment\n"
                "---------------\n"  # Length of dashes matches "MockEnvironment"
                "Scheduler: None\n"
                "Compiler: mock_compiler\n"
                "Primary Queue: mock_primary_queue\n"
                "MPI Exec Prefix: mock_mpi_prefix\n"
                "Cores per Node: 0\n"
                "Memory per Node (GB): 0\n"
                "Max Walltime: 00:00:00\n"
                "Uses Lmod: False\n"
                "Environment Variables:\n"
                "    VAR1: value1\n"
                "    VAR2: value2"
            )

            assert str(env) == expected_str

    def test_repr_method(self):
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
            "MockEnvironment(system_name='mock_system', compiler='mock_compiler', scheduler=None, "
            "primary_queue='mock_primary_queue', cores_per_node=0, mem_per_node_gb=0, "
            "max_walltime='00:00:00')"
            "\nState: <uses_lmod=False>"
        )

        assert repr(env) == expected_repr


class TestSchedulerProperty:
    """Tests for the scheduler property in CStarEnvironment.

    Tests
    -----
    - test_scheduler_detects_slurm: Confirms Slurm detection when sinfo is present.
    - test_scheduler_detects_pbs: Confirms PBS detection when qstat is present.
    - test_scheduler_detects_no_scheduler: Confirms None is returned when no scheduler is detected.
    """

    def setup_method(self):
        """Patches shutil.which to simulate different scheduler installations.

        Mocks
        -----
        - shutil.which: Returns None by default to simulate the absence of schedulers,
          and is modified in each test case to simulate specific scheduler binaries.
        """
        # Patch `shutil.which` for each test, starting with a default return of None
        self.which_patcher = patch(
            "cstar.base.environment.shutil.which", return_value=None
        )
        self.mock_which = self.which_patcher.start()

    def teardown_method(self):
        """Stops shutil.which patch after each test."""
        self.which_patcher.stop()

    def test_scheduler_detects_slurm(self):
        """Tests that scheduler property detects Slurm when sinfo is present.

        Mocks
        -----
        - shutil.which: Returns a path for "sinfo" to simulate the presence of Slurm.

        Asserts
        -------
        - The scheduler property returns "slurm" when Slurm binaries are found.
        """
        # Set up `shutil.which` to simulate finding "sinfo" for Slurm
        self.mock_which.side_effect = {"sinfo": "/usr/bin/sinfo"}.get
        env = MockEnvironment()
        assert env.scheduler == "slurm"

    def test_scheduler_detects_pbs(self):
        """Tests that scheduler property detects PBS when qstat is present.

        Mocks
        -----
        - shutil.which: Returns a path for "qstat" to simulate the presence of PBS.

        Asserts
        -------
        - The scheduler property returns "pbs" when PBS binaries are found.
        """
        # Set up `shutil.which` to simulate finding "qstat" for PBS
        self.mock_which.side_effect = {"qstat": "/usr/bin/qstat"}.get
        env = MockEnvironment()
        assert env.scheduler == "pbs"

    def test_scheduler_detects_no_scheduler(self):
        """Tests that scheduler property returns None when no scheduler binaries are
        detected.

        Mocks
        -----
        - shutil.which: Returns None for all scheduler binaries, simulating no detected scheduler.

        Asserts
        -------
        - The scheduler property is None when no recognized scheduler binaries are present.
        """
        # With `shutil.which` returning None, no scheduler should be detected
        env = MockEnvironment()
        assert env.scheduler is None


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
            "cstar.base.environment.subprocess.run",
            return_value=subprocess.CompletedProcess(args="module reset", returncode=0),
        )
        self.mock_subprocess = self.subprocess_patcher.start()
        self.uses_lmod_patcher = patch.object(
            MockEnvironment, "uses_lmod", new_callable=PropertyMock
        )
        self.mock_uses_lmod = self.uses_lmod_patcher.start()

        self.os_environ_patcher = patch.dict(
            "cstar.base.environment.os.environ",
            {"LMOD_SYSHOST": "mock_system"},
            clear=True,
        )
        self.os_environ_patcher.start()

    def teardown_method(self):
        """Stops all patches after each test."""
        self.subprocess_patcher.stop()
        self.uses_lmod_patcher.stop()
        self.os_environ_patcher.stop()

    @patch("cstar.base.environment.importlib.util.find_spec", return_value=None)
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
        "cstar.base.environment.os.environ", {"LMOD_CMD": "/mock/lmod"}, clear=True
    )
    @patch("cstar.base.environment.subprocess.run")
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
            match="Linux Environment Modules command \n/mock/lmod python reset \n failed with code 1. STDERR: Module reset error",
        ):
            MockEnvironment()

        # Verify that subprocess was called with the correct command
        mock_subprocess.assert_called_once_with(
            "/mock/lmod python reset", shell=True, text=True, capture_output=True
        )

    @patch.dict(
        "cstar.base.environment.os.environ", {"LMOD_CMD": "/mock/lmod"}, clear=True
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
            match=r"Linux Environment Modules command\s+\n/mock/lmod python load module1\s+\n failed with code 1\. STDERR: Module load error",
        ):
            MockEnvironment()
