import os
import subprocess
import typing as t
from pathlib import Path
from unittest import mock
from unittest.mock import Mock, PropertyMock, call, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cstar.base.env import (
    ENV_CSTAR_CACHE_HOME,
    ENV_CSTAR_CONFIG_HOME,
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_DEBUG_BUILD_MODE,
    ENV_CSTAR_DISABLE_BUILD_VERIFICATION,
    ENV_CSTAR_DISABLE_MIGRATION,
    ENV_CSTAR_SLURM_DISABLE_MT,
    ENV_CSTAR_STATE_HOME,
    FLAG_OFF,
    FLAG_ON,
    get_env_item,
    hpc_data_directory,
)
from cstar.base.feature import is_flag_enabled
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)
from cstar.system.environment import (
    CStarEnvironment,
    LmodEnvSettings,
    SlurmSettingsBase,
)
from cstar.system.manager import registered_context

if t.TYPE_CHECKING:
    from importlib.machinery import ModuleSpec


class MockEnvironment(CStarEnvironment):
    def __init__(
        self,
        system_name="mock_system",
        mpi_exec_prefix="mock_mpi_prefix",
        compiler="mock_compiler",
        modules: t.Sequence[str] = (),
        env: t.Mapping[str, str] | None = None,
    ):
        super().__init__(
            system_name=system_name,
            mpi_exec_prefix=mpi_exec_prefix,
            compiler=compiler,
            modules=modules,
            env=env,
        )


class TestSetupEnvironmentFromToolchain:
    """Tests related to loading a toolchain's modules and env into the process.

    Tests
    -----
    - test_load_lmod_modules: Confirms correct module loading sequence for systems with Linux Environment Modules.
    - test_env_loading: Validates environment variable loading and expansion from a toolchain's env.
    - test_env_updating: Tests that environment variables are updated correctly after loading.
    """

    @pytest.mark.parametrize(
        "lmod_syshost",
        ["perlmutter", "derecho", "expanse", "anvil", "oscar", "stampede3"],
    )
    @patch(
        "cstar.base.utils.subprocess.run",
        autospec=True,
    )
    @patch.object(
        CStarEnvironment,
        "uses_lmod",
        new_callable=PropertyMock,
        return_value=True,
    )
    @patch.dict(
        "cstar.system.environment.os.environ", {"LMOD_CMD": "/mock/lmod"}, clear=True
    )
    def test_load_lmod_modules(
        self,
        mock_uses_lmod: bool,
        mock_run: Mock,
        lmod_syshost: t.Literal[
            "anvil", "derecho", "elja", "expanse", "oscar", "perlmutter", "stampede3"
        ],
    ):
        """Tests that the load_lmod_modules function correctly interacts with Linux
        Envionment Modules.

        This test reads the modules of the cpu toolchain in the system's bundled system
        file, builds the environment from them, then checks that C-Star's `module load`
        calls correspond to those modules.

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

            # The modules the system's file lists for its cpu toolchain
            expected_modules = list(
                registered_context(lmod_syshost).toolchain("cpu").modules
            )
            assert expected_modules

            # Instantiate the environment, which should trigger load_lmod_modules
            MockEnvironment(system_name=lmod_syshost, modules=expected_modules)

            # Define expected subprocess calls
            modules = [
                call(
                    f"/mock/lmod python load {module.strip()}",
                    capture_output=True,
                    shell=True,
                    text=True,
                )
                for module in expected_modules
            ]
            expected_calls = [
                call(
                    "/mock/lmod python reset",
                    capture_output=True,
                    shell=True,
                    text=True,
                ),
                *modules,
            ]

            mock_run.assert_has_calls(expected_calls, any_order=False)

    @patch("cstar.base.utils.subprocess.run", autospec=True)
    @patch.object(
        CStarEnvironment, "uses_lmod", new_callable=PropertyMock, return_value=True
    )
    @patch.dict(
        "cstar.system.environment.os.environ", {"LMOD_CMD": "/mock/lmod"}, clear=True
    )
    def test_load_lmod_modules_one_load_per_entry(
        self, mock_uses_lmod: bool, mock_run: Mock
    ):
        """Each entry of the module list is the argument of one `module load`, so an
        entry naming several modules loads them in a single call, in list order.
        """
        mock_run.return_value.stdout = ""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stderr = ""

        MockEnvironment(
            system_name="multi",
            modules=["cpu/1.0", "intel/19.1 mvapich2/2.3.4", "cray-netcdf/4.9.2.3"],
        )

        commands = [c.args[0] for c in mock_run.call_args_list]
        assert commands == [
            "/mock/lmod python reset",
            "/mock/lmod python load cpu/1.0",
            "/mock/lmod python load intel/19.1 mvapich2/2.3.4",
            "/mock/lmod python load cray-netcdf/4.9.2.3",
        ]

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
    def test_env_loading(self):
        """Tests that environment variables are loaded and expanded correctly from a
        toolchain's env.

        Asserts
        -------
        - Confirms the loaded environment variables have the expected values after expansion.
        """
        env = MockEnvironment(
            env={
                "NETCDFHOME": "${NETCDF_FORTRANHOME}/",
                "MPIHOME": "${MVAPICH2HOME}/",
                "MPIROOT": "${MVAPICH2HOME}/",
            }
        )
        # Define expected final environment variables after expansion
        expected_env_vars = {
            "NETCDFHOME": "/mock/netcdf/",  # Expanded from ${NETCDF_FORTRANHOME}
            "MPIHOME": "/mock/mpi/",  # Expanded from ${MVAPICH2HOME}
            "MPIROOT": "/mock/mpi/",  # Expanded from ${MVAPICH2HOME}
        }

        assert dict(env.environment_variables) == expected_env_vars
        for key, value in expected_env_vars.items():
            assert os.environ[key] == value

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
    def test_env_updating(
        self,
        mock_system_name: str,
    ):
        """Tests that environment variables are updated correctly.

        Asserts
        -------
        - Confirms the toolchain's variables are loaded and `set_env_var` updates the process.
        """
        exp_system_env = {
            "NETCDFHOME": "${NETCDF_FORTRANHOME}/",
            "MPIHOME": "${MVAPICH2HOME}/",
            "MPIROOT": "${MVAPICH2HOME}/",
        }

        # Instantiate the environment to trigger loading the environment variables
        env = CStarEnvironment(
            system_name=mock_system_name,
            mpi_exec_prefix="mpi-prefix",
            compiler="gnu",
            env=exp_system_env,
        )

        # Confirm variables written in the toolchain's env are available
        actual_env = env.environment_variables
        for key in exp_system_env:
            assert key in actual_env

        k0, v0 = "NETCDFHOME", "updated/value"
        k1, v1 = "TEST_VAR", "test-value"
        k2, v2 = "OTHER_EMPTY", ""
        updated_vars = [(k0, v0), (k1, v1), (k2, v2)]

        for key, exp_value in updated_vars:
            env.set_env_var(key, exp_value)

        for key, exp_value in updated_vars:
            # Confirm the active environment variable is updated
            assert exp_value in os.environ.get(key, "key-not-found")

    @pytest.mark.parametrize(
        ("system_name", "profile", "source", "expected"),
        [
            ["perlmutter", "", None, "the cpu toolchain of the perlmutter system"],
            ["perlmutter", "gpu", None, "the gpu toolchain of the perlmutter system"],
            [
                "perlmutter",
                "",
                Path("/pkg/systems/perlmutter.yaml"),
                "the cpu toolchain in /pkg/systems/perlmutter.yaml",
            ],
            [
                "perlmutter",
                "gpu",
                Path("/pkg/systems/perlmutter.yaml"),
                "the gpu toolchain in /pkg/systems/perlmutter.yaml",
            ],
        ],
    )
    def test_env_origin(
        self, system_name: str, profile: str, source: Path | None, expected: str
    ):
        """Verify that env_origin names the toolchain and where its modules and env
        were defined: the system file when there is one, else the system.
        """
        env = CStarEnvironment(
            system_name=system_name,
            mpi_exec_prefix="mpi-prefix",
            compiler="gnu",
            profile=profile,
            source=source,
        )

        assert env.env_origin == expected

    def test_env_expansion_order(self):
        """A `${VAR}` sees the toolchain's earlier entries first, then the process
        environment; a variable that is set nowhere expands to an empty string, and a
        bare `$VAR` is expanded afterwards.
        """
        with patch.dict(
            os.environ, {"SHARED": "process", "ONLY_PROCESS": "proc"}, clear=True
        ):
            env = CStarEnvironment(
                system_name="mock",
                mpi_exec_prefix="mpirun",
                compiler="gnu",
                env={
                    "SHARED": "toolchain",
                    "FIRST": "${SHARED}/a",
                    "SECOND": "${FIRST}/b:${ONLY_PROCESS}",
                    "LATER": "${NOT_YET}",
                    "NOT_YET": "set-late",
                    "UNSET": "x${NOWHERE}y",
                    "BARE": "$ONLY_PROCESS/bin",
                },
            )
            loaded = env.environment_variables

            assert loaded["FIRST"] == "toolchain/a"
            assert loaded["SECOND"] == "toolchain/a/b:proc"
            assert loaded["LATER"] == ""
            assert loaded["UNSET"] == "xy"
            assert loaded["BARE"] == "proc/bin"
            assert os.environ["SHARED"] == "toolchain"

        # leaving the patched environment restores it
        assert "FIRST" not in os.environ

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
    def test_str_method(self, _mock_uses_lmod):
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
        - CStarEnvironment.uses_lmod: Patched to simulate environments that use or don't use Lmod.
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
        self,
        _mock_find_spec: "ModuleSpec | None",
    ) -> None:
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

    @patch("platform.system", mock.PropertyMock(return_value="NotLinux"))
    @patch("platform.machine", mock.PropertyMock(return_value="x86_64"))
    def test_load_lmod_modules_raises_environment_error_when_lmod_not_used(self):
        """Tests that load_lmod_modules raises an EnvironmentError if Lmod is not used.

        Mocks
        -----
        - CStarEnvironment.uses_lmod: Returns False to simulate an environment that doesn't use Lmod.

        Asserts
        -------
        - Raises EnvironmentError with a message indicating Lmod modules are not supported on the system.
        """
        self.mock_uses_lmod.return_value = False

        with (
            pytest.raises(
                EnvironmentError,
                match="does not appear to use Linux Environment Modules",
            ),
        ):
            env = MockEnvironment()
            env.load_lmod_modules()

    def test_lmod_system_default_modules_is_set(self) -> None:
        """Verify that LMOD_SYSTEM_DEFAULT_MODULES is set when missing and
        we attempt to load modules.

        Asserts
        -------
        - The modules are reset and loaded in order, and the variable is set afterwards.
        """
        mock_module_names = ["mock-module-1", "mock-module-2"]
        modules_var: t.Final[str] = "LMOD_SYSTEM_DEFAULT_MODULES"
        modules_exp_value: t.Final[str] = "__NO_SYSTEM_DEFAULT_MODULES__"

        with (
            # ensure the variable isn't set at the outset by clearing os.environ.
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch.object(MockEnvironment, "_call_lmod") as mock_call_lmod,
        ):
            env = MockEnvironment(modules=mock_module_names)
            env.load_lmod_modules()

            # confirm the reset and load fired
            mock_call_lmod.assert_has_calls(
                [
                    mock.call("reset"),
                    *[mock.call(f"load {m}") for m in mock_module_names],
                ]
            )

            # confirm the env var is set after the call completes
            assert os.environ[modules_var] == modules_exp_value

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

    @patch("platform.system", mock.PropertyMock(return_value="Linux"))
    @patch("platform.machine", mock.PropertyMock(return_value="x86_64"))
    @patch.dict(
        "cstar.system.environment.os.environ", {"LMOD_CMD": "/mock/lmod"}, clear=True
    )
    def test_load_lmod_modules_raises_runtime_error_on_module_load_failure(self):
        """Tests that a RuntimeError is raised if `module load` fails.

        Mocks
        -----
        - the toolchain lists one entry naming two modules ("module1 module2").
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
                "Linux Environment Modules command `/mock/lmod python load module1 module2` "
                "failed. Return Code: `1`. STDERR:\nModule load error"
            ),
        ):
            MockEnvironment(modules=["module1 module2"])


@pytest.mark.parametrize(
    ("xdg_var", "xdg_value", "cstar_var", "cstar_val"),
    [
        (
            "XDG_CACHE_HOME",
            "/foo/boop",
            ENV_CSTAR_CACHE_HOME,
            "~/my-state/cstar-cache",
        ),
        (
            "XDG_CONFIG_HOME",
            "/foo/borp",
            ENV_CSTAR_CONFIG_HOME,
            "~/my-state/cstar-config",
        ),
        (
            "XDG_DATA_HOME",
            "/foo/blep",
            ENV_CSTAR_DATA_HOME,
            "~/my-state/cstar-data",
        ),
        (
            "XDG_STATE_HOME",
            "/foo/beep",
            ENV_CSTAR_STATE_HOME,
            "~/my-state/cstar-state",
        ),
    ],
)
def test_get_env_item_cstar_override(
    xdg_var: str,
    xdg_value: str,
    cstar_var: str,
    cstar_val: str,
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify the XDG config is ignored when the CSTAR_XXX_HOME variables are set."""
    monkeypatch.setenv(xdg_var, xdg_value)
    monkeypatch.setenv(cstar_var, cstar_val)
    env_item = get_env_item(cstar_var)

    assert env_item.value == cstar_val


@pytest.mark.parametrize(
    ("xdg_var", "xdg_value", "cstar_var"),
    [
        (
            "XDG_CACHE_HOME",
            "/foo/boop",
            ENV_CSTAR_CACHE_HOME,
        ),
        (
            "XDG_CONFIG_HOME",
            "/foo/borp",
            ENV_CSTAR_CONFIG_HOME,
        ),
        (
            "XDG_DATA_HOME",
            "/foo/blep",
            ENV_CSTAR_DATA_HOME,
        ),
        (
            "XDG_STATE_HOME",
            "/foo/beep",
            ENV_CSTAR_STATE_HOME,
        ),
    ],
)
def test_get_env_item_indirect(
    xdg_var: str,
    xdg_value: str,
    cstar_var: str,
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify the XDG config is used when the CSTAR_XXX_HOME variables are not set."""
    monkeypatch.delenv(xdg_var, raising=False)
    monkeypatch.delenv(cstar_var, raising=False)
    monkeypatch.setenv(xdg_var, xdg_value)
    env_item = get_env_item(cstar_var)

    with mock.patch("cstar.base.env.EnvItem.default_factory", None):
        actual_value = env_item.value

    assert actual_value == xdg_value


@pytest.mark.parametrize(
    ("xdg_var", "xdg_value", "cstar_var"),
    [
        (
            "XDG_CACHE_HOME",
            "",
            ENV_CSTAR_CACHE_HOME,
        ),
        (
            "XDG_CONFIG_HOME",
            "",
            ENV_CSTAR_CONFIG_HOME,
        ),
        (
            "XDG_STATE_HOME",
            "",
            ENV_CSTAR_STATE_HOME,
        ),
    ],
)
def test_get_env_item_default(
    xdg_var: str,
    xdg_value: str,
    cstar_var: str,
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify the default is returned when XDG and CSTAR_XXX_HOME variables are not set."""
    monkeypatch.delenv(cstar_var, raising=False)
    monkeypatch.setenv(xdg_var, xdg_value)

    env_item = get_env_item(cstar_var)

    assert env_item.value == env_item.default


@pytest.mark.parametrize(
    ("xdg_var", "xdg_value", "cstar_var"),
    [
        (
            "XDG_DATA_HOME",
            "",
            ENV_CSTAR_DATA_HOME,
        ),
    ],
)
def test_get_env_item_default_from_factory(
    xdg_var: str,
    xdg_value: str,
    cstar_var: str,
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify the default factory is invoked."""
    monkeypatch.delenv(cstar_var, raising=False)
    monkeypatch.setenv(xdg_var, xdg_value)
    # computed here, not at collection, so the autouse fixtures apply to both sides
    factory_value = hpc_data_directory()

    env_item = get_env_item(cstar_var)
    actual_value = env_item.value

    if factory_value:
        # ensure that a default factory function is evaluated
        assert actual_value == factory_value
    else:
        assert actual_value == env_item.default


def test_get_env_item_value_set(
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify the default is not returned if an env item is set."""
    cstar_var = ENV_CSTAR_DEBUG_BUILD_MODE
    default_value = "default-value"
    actual_value = "actual-value"

    monkeypatch.delenv(cstar_var, raising=False)
    monkeypatch.setenv(cstar_var, actual_value)

    env_item = get_env_item(cstar_var)
    with mock.patch.multiple(env_item, default=default_value, default_factory=None):
        env_value = env_item.value

    assert env_value == actual_value


def test_get_env_item_value_not_set(
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify the default is returned if an env item is not set."""
    cstar_var = ENV_CSTAR_DEBUG_BUILD_MODE
    default_value = "default-value"

    monkeypatch.delenv(cstar_var, raising=False)

    env_item = get_env_item(cstar_var)
    with mock.patch.multiple(env_item, default=default_value, default_factory=None):
        env_value = env_item.value

    assert env_value == default_value


def test_get_env_item_value_ignore_factory(
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify the non-null factory value is not returned if the value is set."""
    cstar_var = ENV_CSTAR_DEBUG_BUILD_MODE
    default_value = "default-value"
    actual_value = "actual-value"
    factory_value = "factory-value"

    monkeypatch.delenv(cstar_var, raising=False)
    monkeypatch.setenv(cstar_var, actual_value)

    env_item = get_env_item(cstar_var)
    with mock.patch.multiple(
        env_item, default=default_value, default_factory=lambda _item: factory_value
    ):
        env_value = env_item.value

    assert env_value == actual_value


def test_get_env_item_value_use_factory(
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify the non-null factory value is returned if the value is not set."""
    cstar_var = ENV_CSTAR_DEBUG_BUILD_MODE
    default_value = "default-value"
    factory_value = "factory-value"

    monkeypatch.delenv(cstar_var, raising=False)

    env_item = get_env_item(cstar_var)
    with mock.patch.multiple(
        env_item, default=default_value, default_factory=lambda _item: factory_value
    ):
        env_value = env_item.value

    assert env_value == factory_value


def test_get_env_item_value_ignore_null_factory(
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify the null factory value is not returned and the default is returned."""
    cstar_var = ENV_CSTAR_DEBUG_BUILD_MODE
    default_value = "default-value"

    monkeypatch.delenv(cstar_var, raising=False)

    env_item = get_env_item(cstar_var)
    with mock.patch.multiple(
        env_item, default=default_value, default_factory=lambda _item: None
    ):
        env_value = env_item.value

    assert env_value == default_value


@pytest.mark.parametrize(
    ("xdg_var", "xdg_value", "cstar_var"),
    [
        (
            "XDG_CACHE_HOME",
            "",
            ENV_CSTAR_CACHE_HOME,
        )
    ],
)
def test_env_show_default(
    xdg_var: str,
    xdg_value: str,
    cstar_var: str,
) -> None:
    """Verify the XDG config is ignored when the CSTAR_XXX_HOME variables are set."""
    with patch.dict(os.environ, {xdg_var: xdg_value, cstar_var: ""}, clear=True):
        from cstar.cli.environment.show import show

        show("file")


@pytest.mark.parametrize(
    "key",
    [
        ENV_CSTAR_DEBUG_BUILD_MODE,
        ENV_CSTAR_DISABLE_MIGRATION,
        ENV_CSTAR_DISABLE_BUILD_VERIFICATION,
        ENV_CSTAR_SLURM_DISABLE_MT,
    ],
)
def test_is_flag_enabled(key: str) -> None:
    """Verify the utility `is_flag_enabled` determines the correct value for
    a flag when it is set to on, set to off, and not set at all.
    """
    with mock.patch.dict(os.environ, {key: FLAG_ON}, clear=True):
        assert is_flag_enabled(key)

    with mock.patch.dict(os.environ, {key: FLAG_OFF}, clear=True):
        assert not is_flag_enabled(key)

    with mock.patch.dict(os.environ, {}, clear=True):
        assert not is_flag_enabled(key)


@pytest.mark.parametrize(
    "fixture_name",
    [
        "env_empty_lmod",
        "env_clear_lmod",
    ],
)
def test_lmodenvsettings_notset(
    request: pytest.FixtureRequest, fixture_name: str
) -> None:
    """Verify an environment that contains no LMOD env vars does not result
    in an exception when creating the settings instance.

    Parameterized with: all keys have empty value, all keys missing
    """
    _ = request.getfixturevalue(fixture_name)
    settings = LmodEnvSettings()

    assert settings.CMD == ""
    assert settings.DIR == ""
    assert settings.PKG == ""
    assert settings.ROOT == ""
    assert settings.SYSHOST == ""
    assert settings.SYSTEM_DEFAULT_MODULES == ""
    assert settings.SYSTEM_NAME == ""
    assert settings.VERSION == ""


@pytest.mark.parametrize(
    "fixture_name",
    [
        "env_full_lmod",
        "env_full_strippable_lmod",
    ],
)
def test_lmodenvsettings(request: pytest.FixtureRequest, fixture_name: str) -> None:
    """Verify LMOD settings are loaded correctly from the environment."""
    _ = request.getfixturevalue(fixture_name)

    settings = LmodEnvSettings()

    assert settings.CMD == "CMD-value"
    assert settings.DIR == "DIR-value"
    assert settings.PKG == "PKG-value"
    assert settings.ROOT == "ROOT-value"
    assert settings.SYSHOST == "SYSHOST-value"
    assert settings.SYSTEM_DEFAULT_MODULES == "SYSTEM_DEFAULT_MODULES-value"
    assert settings.SYSTEM_NAME == "SYSTEM_NAME-value"
    assert settings.VERSION == "VERSION-value"


def test_lmodenvsettings_variable_resolution(
    lmod_settings_keys: list[str],
) -> None:
    """Verify key resolution given a field name."""
    for key in lmod_settings_keys:
        expected_key = f"LMOD_{key}"
        assert LmodEnvSettings.variable(key) == expected_key


def test_slurm_settings_max_walltime_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the maximum walltime falls back to the registered default of
    the environment variable when the variable is not set.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture used to configure the environment.
    """
    monkeypatch.setenv(ENV_CSTAR_SLURM_ACCOUNT, "an-account")
    monkeypatch.setenv(ENV_CSTAR_SLURM_QUEUE, "a-queue")
    monkeypatch.delenv(ENV_CSTAR_SLURM_MAX_WALLTIME, raising=False)
    item = get_env_item(ENV_CSTAR_SLURM_MAX_WALLTIME)
    monkeypatch.setattr(item, "default", "12:34:56")

    settings = SlurmSettingsBase()

    assert settings.SLURM_MAX_WALLTIME == "12:34:56"
