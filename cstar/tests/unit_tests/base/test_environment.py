# cstar/base/environment.py                           171     42    75%   68-82, 93, 156, 164, 193, 227, 233, 244, 261, 276, 288-293, 305, 330, 342, 354, 411, 467-471, 489-493
# Missing:
# - str and repr
# - both raises in system_name
# - raise in root
# - raises in load_lmod_modules
# - all "pass" abstract properties
# - the scheduler property using shutil
# - queue flag in base class (just None)
# - other_scheduler_directives in base class (just {})
# - cores_per_node in base class (just None)
# - mem_per_node_gb in base class (just None)
# - cores_per_node for Mac and Linux


import os
import pytest
from unittest.mock import patch, call, PropertyMock
import cstar


class TestSetEnvironment:
    """Tests covering the set_environment() function in cstar.base.environment."""

    def setup_method(self):
        """Sets up common patches for each test in this class."""
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
        conditions."""
        os.environ["LMOD_SYSHOST"] = "perlmutter"
        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.PerlmutterEnvironment)

    def test_set_environment_expanse(self):
        """Tests that set_environment returns ExpanseEnvironment under appropriate
        conditions."""
        os.environ["LMOD_SYSHOST"] = "expanse"
        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.ExpanseEnvironment)

    def test_set_environment_derecho(self):
        """Tests that set_environment returns DerechoEnvironment under appropriate
        conditions."""
        os.environ["LMOD_SYSHOST"] = "derecho"
        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.DerechoEnvironment)

    def test_set_environment_macos_arm(self):
        """Tests that set_environment returns MacOSARMEnvironment for macOS ARM."""
        # Override return values for macOS ARM
        self.mock_system.return_value = "Darwin"
        self.mock_machine.return_value = "arm64"
        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.MacOSARMEnvironment)

    def test_set_environment_linux_x86(self):
        """Tests that set_environment returns LinuxX86Environment for generic Linux
        x86_64."""
        env = cstar.base.environment.set_environment()
        assert isinstance(env, cstar.base.environment.LinuxX86Environment)

    def test_set_environment_unsupported_system(self):
        """Tests that set_environment raises an error for unsupported environments."""
        # Override return values for an unsupported system
        self.mock_system.return_value = "Windows"
        self.mock_machine.return_value = "AMD64"
        with pytest.raises(EnvironmentError):
            cstar.base.environment.set_environment()


class TestCStarEnvironmentSubclassDefaults:
    def setup_method(self):
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
        # Stop all patches after each test
        patch.stopall()

    def test_perlmutter_environment_defaults(self):
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
        # Modify platform system and machine for macOS ARM test
        self.mock_system.return_value = "Darwin"
        self.mock_machine.return_value = "arm64"
        self.mock_uses_lmod.return_value = False  # Ensure uses_lmod is False for Mac

        env = cstar.base.environment.MacOSARMEnvironment()
        assert env.system_name == "darwin_arm64"
        assert env.mpi_exec_prefix == "mpirun"
        assert env.compiler == "gnu"
        assert env.cores_per_node == 10  # Based on mocked os.cpu_count() return value

    @patch("cstar.base.environment.os.cpu_count", return_value=12)
    def test_linux_x86_environment_defaults(self, mock_cpu_count):
        # No changes needed, as defaults match Linux x86_64 setup in setup_method
        self.mock_uses_lmod.return_value = (
            False  # Ensure uses_lmod is False for Linux X86
        )

        env = cstar.base.environment.LinuxX86Environment()
        assert env.system_name == "linux_x86_64"
        assert env.mpi_exec_prefix == "mpirun"
        assert env.compiler == "gnu"
        assert env.cores_per_node == 12  # Based on mocked os.cpu_count() return value


@pytest.mark.parametrize(
    "env_class, lmod_syshost",
    [
        (cstar.base.environment.PerlmutterEnvironment, "perlmutter"),
        (cstar.base.environment.DerechoEnvironment, "derecho"),
        (cstar.base.environment.ExpanseEnvironment, "expanse"),
    ],
)
class TestLoadLmodModules:
    @patch("cstar.base.environment.subprocess.run")
    @patch.object(
        cstar.base.environment.CStarEnvironment,
        "uses_lmod",
        new_callable=PropertyMock,
        return_value=True,
    )
    def test_load_lmod_modules_calls(
        self, mock_uses_lmod, mock_run, env_class, lmod_syshost
    ):
        """Tests that load_lmod_modules makes the correct calls for each environment
        using Lmod."""

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
        """Reads and returns the list of expected modules from the `.lmod` file for the
        environment."""
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
@patch.object(cstar.base.environment.CStarEnvironment, "uses_lmod", return_value=True)
@patch.object(
    cstar.base.environment.CStarEnvironment, "load_lmod_modules", return_value=None
)
def test_env_file_loading(mock_load_lmod, mock_uses_lmod, tmp_path):
    """Tests that environment variables are loaded and merged correctly, handling
    chained variables."""

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
    print(f"System .env file written to: {system_env_file_path}")
    print(f"System .env file contents:\n{system_env_file_path.read_text()}")

    # Write simulated user .env content with an overriding variable
    user_env_file_path.write_text(
        "MPIROOT=/override/mock/mpi\n" "CUSTOM_VAR=custom_value\n"
    )
    print(f"User .env file written to: {user_env_file_path}")
    print(f"User .env file contents:\n{user_env_file_path.read_text()}")

    # Patch the root path and expanduser to point to our temporary files
    with patch.object(cstar.base.environment.CStarEnvironment, "root", new=root_path):
        print(f"Patched root path: {cstar.base.environment.CStarEnvironment.root}")
        with patch(
            "cstar.base.environment.Path.expanduser", return_value=user_env_file_path
        ):
            # Instantiate the environment to trigger loading the environment variables
            env = cstar.base.environment.PerlmutterEnvironment()

            # Print the loaded environment variables for debugging
            print("Loaded environment variables:")
            print(dict(env.environment_variables))

            # Define expected final environment variables after merging and expansion
            expected_env_vars = {
                "NETCDFHOME": "/mock/netcdf/",  # Expanded from ${NETCDF_FORTRANHOME}
                "MPIHOME": "/mock/mpi/",  # Expanded from ${MVAPICH2HOME}
                "MPIROOT": "/override/mock/mpi",  # User-defined override
                "CUSTOM_VAR": "custom_value",
            }

            # Assert that environment variables were loaded, expanded, and merged as expected
            assert dict(env.environment_variables) == expected_env_vars
