# cstar/base/environment.py                           169      9    95%   245, 262, 277, 290, 292, 306, 331, 472, 494
import os
import pytest
from unittest.mock import patch, call, PropertyMock, mock_open
import cstar
import subprocess


class TestSetEnvironment:
    """Tests covering the set_environment() function in cstar.base.environment.

    Tests
    -----
    - test_set_environment_perlmutter: Validates that PerlmutterEnvironment is returned when conditions are met.
    - test_set_environment_expanse: Validates that ExpanseEnvironment is returned when conditions are met.
    - test_set_environment_derecho: Validates that DerechoEnvironment is returned when conditions are met.
    - test_set_environment_macos_arm: Ensures MacOSARMEnvironment is returned for macOS ARM.
    - test_set_environment_linux_x86: Ensures LinuxX86Environment is returned for generic Linux x86_64.
    - test_set_environment_unsupported_system: Confirms EnvironmentError is raised for unsupported systems.
    """

    def setup_method(self):
        """Sets up common patches for each test in this class.

        Mocks
        -----
        - platform.system: Always returns "Linux" by default.
        - platform.machine: Always returns "x86_64" by default.
        - os.environ: Cleared and reset with default values for each test.
        """

        # Patch platform system and machine by default to "Linux" and "x86_64"
        self.patcher_system = patch(
            "cstar.base.environment.platform.system", return_value="Linux"
        )
        self.patcher_machine = patch(
            "cstar.base.environment.platform.machine", return_value="x86_64"
        )
        self.mock_system = self.patcher_system.start()
        self.mock_machine = self.patcher_machine.start()

        # Clear os.environ and set common environment variables
        self.patcher_environ = patch.dict(
            "cstar.base.environment.os.environ", clear=True
        )
        self.mock_environ = self.patcher_environ.start()

    def teardown_method(self):
        """Stops all patches after each test."""
        patch.stopall()

    def test_set_environment_perlmutter(self):
        """Tests that set_environment returns PerlmutterEnvironment under appropriate
        conditions.

        Mocks
        -----
        - os.environ["LMOD_SYSHOST"] set to "perlmutter".

        Asserts
        -------
        - The environment instance is of type PerlmutterEnvironment.
        """
        os.environ["LMOD_SYSHOST"] = "perlmutter"
        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.PerlmutterEnvironment)

    def test_set_environment_expanse(self):
        """Tests that set_environment returns ExpanseEnvironment under appropriate
        conditions.

        Mocks
        -----
        - os.environ["LMOD_SYSHOST"] set to "expanse".

        Asserts
        -------
        - The environment instance is of type ExpanseEnvironment.
        """

        os.environ["LMOD_SYSHOST"] = "expanse"
        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.ExpanseEnvironment)

    def test_set_environment_derecho(self):
        """Tests that set_environment returns DerechoEnvironment under appropriate
        conditions.

        Mocks
        -----
        - os.environ["LMOD_SYSHOST"] set to "derecho".

        Asserts
        -------
        - The environment instance is of type DerechoEnvironment.
        """

        os.environ["LMOD_SYSHOST"] = "derecho"
        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.DerechoEnvironment)

    def test_set_environment_macos_arm(self):
        """Tests that set_environment returns MacOSARMEnvironment for macOS ARM.

        Mocks
        -----
        - platform.system set to "Darwin".
        - platform.machine set to "arm64".

        Asserts
        -------
        - The environment instance is of type MacOSARMEnvironment.
        """

        # Override return values for macOS ARM
        self.mock_system.return_value = "Darwin"
        self.mock_machine.return_value = "arm64"
        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.MacOSARMEnvironment)

    def test_set_environment_linux_x86(self):
        """Tests that set_environment returns LinuxX86Environment for generic Linux
        x86_64.

        Asserts
        -------
        - The environment instance is of type LinuxX86Environment.
        """

        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.LinuxX86Environment)

    def test_set_environment_unsupported_system(self):
        """Tests that set_environment raises an error for unsupported environments.

        Mocks
        -----
        - platform.system set to "Windows".
        - platform.machine set to "AMD64".

        Asserts
        -------
        - Raises EnvironmentError for unsupported systems.
        """

        # Override return values for an unsupported system
        self.mock_system.return_value = "Windows"
        self.mock_machine.return_value = "AMD64"
        with pytest.raises(EnvironmentError):
            cstar.base.environment.set_environment()


class TestCStarEnvironmentSubclassDefaults:
    """Tests default configuration of CStarEnvironment subclasses.

    Tests
    -----
    - test_perlmutter_environment_defaults: Confirms PerlmutterEnvironment default configurations.
    - test_derecho_environment_defaults: Confirms DerechoEnvironment default configurations.
    - test_expanse_environment_defaults: Confirms ExpanseEnvironment default configurations.
    - test_macos_arm_environment_defaults: Validates MacOSARMEnvironment configuration, including core count.
    - test_linux_x86_environment_defaults: Validates LinuxX86Environment configuration, including core count.
    """

    def setup_method(self):
        """Sets up patches for platform and environment-specific properties.

        Mocks
        -----
        - platform.system and platform.machine return default values for Linux.
        - os.environ is cleared for isolated environment testing.
        - CStarEnvironment.uses_lmod and CStarEnvironment.load_lmod_modules are patched for control over Lmod settings.
        """

        # Set up default patches for platform, environment variables, uses_lmod, and load_lmod_modules
        self.patch_system = patch(
            "cstar.base.environment.platform.system", return_value="Linux"
        )
        self.patch_machine = patch(
            "cstar.base.environment.platform.machine", return_value="x86_64"
        )
        self.patch_environ = patch.dict(
            "cstar.base.environment.os.environ", {}, clear=True
        )
        self.patch_uses_lmod = patch.object(
            cstar.base.environment.CStarEnvironment,
            "uses_lmod",
            new_callable=PropertyMock,
        )
        self.patch_load_lmod_modules = patch.object(
            cstar.base.environment.CStarEnvironment,
            "load_lmod_modules",
            return_value=None,
        )

        # Start patches and assign mock objects for possible adjustments
        self.mock_system = self.patch_system.start()
        self.mock_machine = self.patch_machine.start()
        self.mock_environ = self.patch_environ.start()
        self.mock_uses_lmod = self.patch_uses_lmod.start()
        self.mock_load_lmod_modules = self.patch_load_lmod_modules.start()

    def teardown_method(self):
        """Stops all patches after each test."""
        patch.stopall()

    def test_perlmutter_environment_defaults(self):
        """Tests that PerlmutterEnvironment has the expected default configuration.

        Mocks
        -----
        - os.environ["LMOD_SYSHOST"] set to "perlmutter".
        - CStarEnvironment.uses_lmod returns True.

        Asserts
        -------
        - Confirms values for Perlmutter-specific properties: mpi_exec_prefix, compiler, queue settings, and hardware specs.
        """

        # Set specific environment variables and uses_lmod for Perlmutter
        os.environ["LMOD_SYSHOST"] = "perlmutter"
        self.mock_uses_lmod.return_value = True

        env = cstar.base.environment.PerlmutterEnvironment()
        assert env.system_name == "perlmutter"
        assert env.mpi_exec_prefix == "srun"
        assert env.compiler == "gnu"
        assert env.queue_flag == "qos"
        assert env.primary_queue == "regular"
        assert env.cores_per_node == 128
        assert env.mem_per_node_gb == 512
        assert env.max_walltime == "24:00:00"
        assert env.other_scheduler_directives == {"-C": "cpu"}

    def test_derecho_environment_defaults(self):
        """Tests that DerechoEnvironment has the expected default configuration.

        Mocks
        -----
        - os.environ["LMOD_SYSHOST"] set to "derecho".
        - CStarEnvironment.uses_lmod returns True.

        Asserts
        -------
        - Confirms values for Derecho-specific properties: mpi_exec_prefix, compiler, queue settings, and hardware specs.
        """

        # Set specific environment variables and uses_lmod for Derecho
        os.environ["LMOD_SYSHOST"] = "derecho"
        self.mock_uses_lmod.return_value = True

        env = cstar.base.environment.DerechoEnvironment()
        assert env.system_name == "derecho"
        assert env.mpi_exec_prefix == "mpirun"
        assert env.compiler == "intel"
        assert env.queue_flag == "q"
        assert env.primary_queue == "main"
        assert env.cores_per_node == 128
        assert env.mem_per_node_gb == 256
        assert env.max_walltime == "12:00:00"

    def test_expanse_environment_defaults(self):
        """Tests that ExpanseEnvironment has the expected default configuration.

        Mocks
        -----
        - os.environ["LMOD_SYSHOST"] set to "expanse".
        - CStarEnvironment.uses_lmod returns True.

        Asserts
        -------
        - Confirms values for Expanse-specific properties: mpi_exec_prefix, compiler, queue settings, and hardware specs.
        """

        # Set specific environment variables and uses_lmod for Expanse
        os.environ["LMOD_SYSHOST"] = "expanse"
        self.mock_uses_lmod.return_value = True

        env = cstar.base.environment.ExpanseEnvironment()
        assert env.system_name == "expanse"
        assert env.mpi_exec_prefix == "srun --mpi=pmi2"
        assert env.compiler == "intel"
        assert env.queue_flag == "partition"
        assert env.primary_queue == "compute"
        assert env.cores_per_node == 128
        assert env.mem_per_node_gb == 256
        assert env.max_walltime == "48:00:00"

    @patch("cstar.base.environment.os.cpu_count", return_value=10)
    def test_macos_arm_environment_defaults(self, mock_cpu_count):
        """Tests default configuration of MacOSARMEnvironment, including core count.

        Mocks
        -----
        - platform.system set to "Darwin".
        - platform.machine set to "arm64".
        - CStarEnvironment.uses_lmod returns False.

        Asserts
        -------
        - Confirms MacOS-specific defaults, including core count based on mocked os.cpu_count.
        """

        # Modify platform system and machine for macOS ARM test
        self.mock_system.return_value = "Darwin"
        self.mock_machine.return_value = "arm64"
        self.mock_uses_lmod.return_value = False  # Ensure uses_lmod is False for Mac

        env = cstar.base.environment.MacOSARMEnvironment()
        assert env.system_name == "darwin_arm64"
        assert env.mpi_exec_prefix == "mpirun"
        assert env.compiler == "gnu"
        assert env.cores_per_node == 10  # Based on mocked os.cpu_count() return value
        assert env.primary_queue is None
        assert env.other_scheduler_directives == {}
        assert env.queue_flag is None

    @patch("cstar.base.environment.os.cpu_count", return_value=12)
    def test_linux_x86_environment_defaults(self, mock_cpu_count):
        """Tests default configuration of LinuxX86Environment, including core count.

        Mocks
        -----
        - platform and machine values set for Linux.
        - os.cpu_count returns 12.

        Asserts
        -------
        - Confirms core count and other Linux-specific defaults.
        """

        # No changes needed, as defaults match Linux x86_64 setup in setup_method
        self.mock_uses_lmod.return_value = (
            False  # Ensure uses_lmod is False for Linux X86
        )

        env = cstar.base.environment.LinuxX86Environment()
        assert env.system_name == "linux_x86_64"
        assert env.mpi_exec_prefix == "mpirun"
        assert env.compiler == "gnu"
        assert env.cores_per_node == 12  # Based on mocked os.cpu_count() return value


class TestSetupEnvironmentFromFiles:
    """Tests related to loading environment configuration from .env and .lmod files.

    Tests
    -----
    - test_load_lmod_modules: Confirms correct module loading sequence for Lmod environments.
    - test_env_file_loading: Validates environment variable loading and merging from .env files.
    - get_expected_lmod_modules: Retrieves expected modules for each environment from system files.
    """

    @pytest.mark.parametrize(
        "env_class, lmod_syshost",
        [
            (cstar.base.environment.PerlmutterEnvironment, "perlmutter"),
            (cstar.base.environment.DerechoEnvironment, "derecho"),
            (cstar.base.environment.ExpanseEnvironment, "expanse"),
        ],
    )
    @patch("cstar.base.environment.subprocess.run")
    @patch.object(
        cstar.base.environment.CStarEnvironment,
        "uses_lmod",
        new_callable=PropertyMock,
        return_value=True,
    )
    def test_load_lmod_modules(self, mock_uses_lmod, mock_run, env_class, lmod_syshost):
        """Tests that the load_lmod_modules function correctly interacts with Linux
        Envionment Modules.

        Mocks
        -----
        - subprocess.run simulates successful 'module load' calls
        - uses_lmod is mocked to always return True (system uses Linux Environment Modules)

        Asserts
        -------
        - Confirms the correct sequence of subprocess calls for resetting and loading modules.
        """

        # Set up the LMOD_SYSHOST environment variable
        with patch.dict(
            "cstar.base.environment.os.environ", {"LMOD_SYSHOST": lmod_syshost}
        ):
            # Simulate success for each subprocess call
            mock_run.return_value.returncode = 0
            mock_run.return_value.stderr = ""

            # Instantiate the environment, which should trigger load_lmod_modules
            env = env_class()

            # Retrieve the expected modules for the given environment
            expected_modules = self.get_expected_lmod_modules(env)

            # Define expected subprocess calls
            expected_calls = [
                call("module reset", capture_output=True, shell=True, text=True),
            ] + [
                call(f"module load {mod}", capture_output=True, shell=True, text=True)
                for mod in expected_modules
            ]

            # Assert subprocess.run was called with the expected calls
            mock_run.assert_has_calls(expected_calls, any_order=False)

    def get_expected_lmod_modules(self, env):
        """Retrieves the expected list of Lmod modules for a given system from a .lmod
        file.

        Returns
        -------
        list: Module names to be loaded, based on the system's `.lmod` file.
        """

        lmod_file_path = (
            f"{env.root}/additional_files/lmod_lists/{env.system_name}.lmod"
        )
        with open(lmod_file_path) as file:
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
        - Mock files are created (using tmp_path) for both system and user .env files.
        - CStarEnvironment.root is patched to a temporary directory to load mock files.

        Asserts
        -------
        - Confirms merged environment variables with expected values after expansion.
        """

        # Set up the <root>/additional_files/env_files structure within tmp_path
        root_path = tmp_path
        env_files_dir = root_path / "additional_files" / "env_files"
        env_files_dir.mkdir(parents=True)

        # Define paths for the system and user .env files
        system_env_file_path = env_files_dir / "perlmutter.env"
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
            cstar.base.environment.CStarEnvironment, "root", new=root_path
        ):
            with patch(
                "cstar.base.environment.Path.expanduser",
                return_value=user_env_file_path,
            ):
                # Instantiate the environment to trigger loading the environment variables
                env = cstar.base.environment.PerlmutterEnvironment()

                # Define expected final environment variables after merging and expansion
                expected_env_vars = {
                    "NETCDFHOME": "/mock/netcdf/",  # Expanded from ${NETCDF_FORTRANHOME}
                    "MPIHOME": "/mock/mpi/",  # Expanded from ${MVAPICH2HOME}
                    "MPIROOT": "/override/mock/mpi",  # User-defined override
                    "CUSTOM_VAR": "custom_value",
                }

                # Assert that environment variables were loaded, expanded, and merged as expected
                assert dict(env.environment_variables) == expected_env_vars


class MockEnvironment(cstar.base.environment.CStarEnvironment):
    """Mock subclass to test __str__ and __repr__ outputs.

    Properties
    ----------
    - compiler: Returns "mock_compiler" as the compiler type.
    - mpi_exec_prefix: Returns "mock_mpi_prefix" as the MPI execution prefix.
    """

    @property
    def compiler(self) -> str:
        return "mock_compiler"

    @property
    def mpi_exec_prefix(self) -> str:
        return "mock_mpi_prefix"


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
                MockEnvironment, "system_name", new_callable=PropertyMock
            ) as mock_system_name,
            patch.object(
                MockEnvironment, "environment_variables", new_callable=PropertyMock
            ) as mock_env_vars,
        ):
            mock_system_name.return_value = "mock_system"
            mock_env_vars.return_value = {"VAR1": "value1", "VAR2": "value2"}

            env = MockEnvironment()

            # Manually construct the expected string output
            expected_str = (
                "MockEnvironment\n"
                "---------------\n"  # Length of dashes matches "MockEnvironment"
                "System Name: mock_system\n"
                "Scheduler: None\n"
                "Compiler: mock_compiler\n"
                "Primary Queue: None\n"
                "MPI Exec Prefix: mock_mpi_prefix\n"
                "Cores per Node: Not specified\n"
                "Memory per Node (GB): Not specified\n"
                "Max Walltime: Not specified\n"
                "Uses Lmod: No\n"
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
        with patch.object(
            MockEnvironment, "system_name", new_callable=PropertyMock
        ) as mock_system_name:
            mock_system_name.return_value = "mock_system"

            env = MockEnvironment()

            # Manually construct the expected repr output
            expected_repr = (
                "MockEnvironment() \n"
                "State: <system_name='mock_system', compiler='mock_compiler', scheduler='None', "
                "primary_queue='None', cores_per_node=None, mem_per_node_gb=None, "
                "max_walltime='None', uses_lmod=False>"
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

    def test_system_name_raises_environment_error_for_missing_lmod_vars(self):
        """Tests that missing Lmod variables raise an EnvironmentError.

        Mocks
        -----
        - CStarEnvironment.uses_lmod: Returns True to indicate Lmod is used.
        - os.environ: Cleared to remove required Lmod variables.

        Asserts
        -------
        - Raises EnvironmentError with a message indicating missing 'LMOD_SYSHOST' or 'LMOD_SYSTEM_NAME'.
        """
        self.mock_uses_lmod.return_value = True
        with patch.dict("cstar.base.environment.os.environ", {}, clear=True):
            with pytest.raises(
                EnvironmentError, match="LMOD_SYSHOST.*LMOD_SYSTEM_NAME.*not defined"
            ):
                MockEnvironment()

    @patch("cstar.base.environment.platform.system", return_value=None)
    @patch("cstar.base.environment.platform.machine", return_value=None)
    def test_system_name_raises_environment_error_for_missing_platform_info(
        self, mock_system, mock_machine
    ):
        """Tests that missing platform info raises an EnvironmentError.

        Mocks
        -----
        - platform.system: Returns None to simulate missing platform information.
        - platform.machine: Returns None to simulate missing machine type information.

        Asserts
        -------
        - Raises EnvironmentError with a message indicating system type determination failure.
        """

        self.mock_uses_lmod.return_value = False
        with pytest.raises(EnvironmentError, match="cannot determine your system type"):
            MockEnvironment()

    @patch("cstar.base.environment.importlib.util.find_spec", return_value=None)
    def test_root_raises_import_error_when_package_not_found(self, mock_find_spec):
        """Tests that missing package spec raises an ImportError in root property.

        Mocks
        -----
        - importlib.util.find_spec: Returns None to simulate missing package spec.

        Asserts
        -------
        - Raises ImportError with a message indicating top-level package could not be found.
        """
        with pytest.raises(ImportError, match="Top-level package '.*' not found"):
            MockEnvironment()

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
            env.load_lmod_modules()

    def test_load_lmod_modules_raises_runtime_error_on_module_reset_failure(self):
        """Tests that failed module reset raises a RuntimeError.

        Mocks
        -----
        - subprocess.run: Returns a non-zero code and error message to simulate a failed "module reset" command.

        Asserts
        -------
        - Raises RuntimeError with a message indicating failure of the "module reset" command.
        """

        self.mock_uses_lmod.return_value = True
        self.mock_subprocess.return_value.returncode = 1  # Simulate failure
        self.mock_subprocess.return_value.stderr = "Module reset error"

        with pytest.raises(
            RuntimeError, match="Error 1.*when attempting to run module reset"
        ):
            env = MockEnvironment()
            env.load_lmod_modules()

    @patch("builtins.open", new_callable=mock_open, read_data="module1\nmodule2\n")
    def test_load_lmod_modules_raises_runtime_error_on_module_load_failure(
        self, mock_open_file
    ):
        """Tests that failed module load raises a RuntimeError.

        Mocks
        -----
        - subprocess.run: Simulates successful "module reset" and failed "module load" commands.
        - open: Reads module list from a mocked .lmod file.

        Asserts
        -------
        - Raises RuntimeError with a message indicating failure of a "module load" command.
        """

        self.mock_uses_lmod.return_value = True

        # Set up the mock to simulate successful `module reset` and failed `module load`
        def subprocess_side_effect(cmd, capture_output, shell, text):
            if "module reset" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0
                )  # success for reset
            elif "module load" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=1, stderr="Module load error"
                )  # failure for load

        self.mock_subprocess.side_effect = subprocess_side_effect

        with pytest.raises(
            RuntimeError, match="Error 1.*when attempting to run module load"
        ):
            MockEnvironment()

    @pytest.mark.parametrize(
        "environment_class",
        [
            cstar.base.environment.MacOSARMEnvironment,
            cstar.base.environment.LinuxX86Environment,
        ],
    )
    @patch("cstar.base.environment.os.cpu_count", return_value=None)
    def test_cores_per_node_raises_error_when_cpu_count_is_none(
        self, mock_cpu_count, environment_class
    ):
        """Tests that cores_per_node raises an EnvironmentError when cpu_count is None.

        Mocks
        -----
        - os.cpu_count: Returns None to simulate an undetectable CPU count.

        Asserts
        -------
        - Raises EnvironmentError with a message indicating failure to determine CPU count.
        """

        env = environment_class()
        with pytest.raises(
            EnvironmentError, match="unable to determine number of cpus"
        ):
            _ = env.cores_per_node
