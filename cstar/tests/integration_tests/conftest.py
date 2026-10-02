import logging
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import pooch
import pytest

from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.base.log import get_logger
from cstar.tests.integration_tests.cases import (
    DT,
    FORGE_CASES,
    MODEL_REFERENCE_DATE,
    MODEL_SPEC,
    ROMS_REF,
    RUN_END,
    RUN_START,
)
from cstar.tests.integration_tests.cli_harness import make_shim
from cstar.tests.integration_tests.data_registry import (
    CACHE_NAME,
    TEST_DATA_BASE_URL,
    fetch_all,
)

if TYPE_CHECKING:
    from cstar.applications.forge.blueprint import ForgeBlueprint
    from cstar.catalog.domain_catalog import LayeredCatalog

CATALOG_SOURCE = Path(__file__).parent / "catalog"


@pytest.fixture
def log() -> logging.Logger:
    return get_logger("cstar.tests.integration_tests")


@pytest.fixture(scope="session")
def cstar_shim(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A ``cstar`` executable pinned to this checkout, for running the real CLI.

    Returns
    -------
    Path
        The shim's path; pair with `make_cli_env` and `run_cstar` from ``cli_harness``.
    """
    return make_shim(tmp_path_factory.mktemp("cstar_shim"))


@pytest.fixture
def modify_template_blueprint(
    tmpdir: str,
) -> Callable[[Path | str, dict[str, str], Path | str], str]:
    """Fixture that provides a factory function for modifying template blueprint files.

    This fixture returns a function that can returns a path to a modified a blueprint
    template file based on specified string replacements.

    Parameters:
    -----
    tmpdir:
       Pytest fixture used to create a temporary directory in which to hold a modified version
       of the template file during the test.

    Returns:
    --------
    _modify_template_blueprint, Callable[[Path | str, dict[str, str]], Path]:
       The factory function
    """

    def _modify_template_blueprint(
        template_blueprint_path: Path | str,
        strs_to_replace: dict,
        out_dir: Path | str,
    ) -> str:
        """Creates a temporary, customized blueprint file from a template.

        This function reads a blueprint template file, performs string replacements as specified
        by `strs_to_replace`, saves the modified content to a temporary file within the `tmpdir`
        provided by pytest.

        Parameters:
        -----------
        template_blueprint_path (Path | str):
           The path to the blueprint template file.
        strs_to_replace (dict[str, str]):
           A dictionary where keys are substrings to find in the template,
           and values are the replacements.

        Returns:
        --------
        modified_blueprint_path:
           A temporary path to a modified version of the template blueprint file.
        """
        template_blueprint_path = Path(template_blueprint_path)

        with open(template_blueprint_path) as template_file:
            template_content = template_file.read()

        modified_template_content = template_content
        for oldstr, newstr in strs_to_replace.items():
            modified_template_content = modified_template_content.replace(
                oldstr, newstr
            )
        modified_template_content = modified_template_content.replace(
            "<working_dir>", str(out_dir)
        )
        temp_path = tmpdir.join(template_blueprint_path.name)
        with open(temp_path, "w") as temp_file:
            temp_file.write(modified_template_content)

        return temp_path

    return _modify_template_blueprint


@pytest.fixture
def integration_test_configuration(
    tests_path: Path,
) -> dict[str, dict[str, str | dict[str, str]]]:
    """Fixture returning a dictionary containing configuration for running multiple
    test simulations.

    Parameters
    ----------
    tests_path : Path
        Fixture returning the directory containing c-star tests; used to build
        absolute paths for other file located relative to the tests directory.
    Returns
    -------
    dict[str, str | dict[str, str]]
    """
    ## Configuration of different cases to test
    return {
        # Remote case, NetCDF
        "test_case_remote_with_netcdf_datasets": {
            "template_blueprint_path": f"{tests_path}/integration_tests/blueprints/blueprint_template.yaml",
            "strs_to_replace": {
                "<input_datasets_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case/raw/roms_tools_3_1_2/input_datasets/ROMS",
                "<additional_code_location>": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git",
            },
        },
    }


@pytest.fixture(scope="session")
def integration_test_data() -> dict[str, Path]:
    """Fetch the pinned upstream source data.

    A failed fetch skips the session for offline developer runs but errors under CI
    (``CI`` set), where a skip would turn the job green.

    Returns
    -------
    dict[str, Path]
        Mapping of registry filename to its path in the pooch cache.
    """
    try:
        data = fetch_all()
    except OSError as exc:
        if os.environ.get("CI"):
            raise
        pytest.skip(
            f"cannot fetch integration test data from {TEST_DATA_BASE_URL}: {exc}"
        )
    return data


@pytest.fixture(scope="session")
def test_catalog_root(
    tmp_path_factory: pytest.TempPathFactory,
    integration_test_data: dict[str, Path],
) -> Path:
    """Copy the test-local catalog layer, substituting ``${TEST_DATA}`` in its YAML.

    Parameters
    ----------
    tmp_path_factory : pytest.TempPathFactory
        Used to create the session-scoped copy.
    integration_test_data : dict[str, Path]
        Requested so the data is fetched before any spec points at it.

    Returns
    -------
    Path
        Root of the substituted catalog layer.
    """
    root = tmp_path_factory.mktemp("test_catalog") / "catalog"
    shutil.copytree(CATALOG_SOURCE, root)
    data_dir = str(pooch.os_cache(CACHE_NAME))
    for yaml_file in root.rglob("*.yaml"):
        yaml_file.write_text(yaml_file.read_text().replace("${TEST_DATA}", data_dir))
    return root


@pytest.fixture(scope="session")
def test_catalog(test_catalog_root: Path) -> "LayeredCatalog":
    """The test-local catalog layer stacked on top of the bundled catalog."""
    from cstar.catalog.domain_catalog import build_catalog_stack

    return build_catalog_stack([str(test_catalog_root)])


@pytest.fixture(scope="session")
def forge_blueprint_factory(
    test_catalog: "LayeredCatalog",
) -> Callable[..., tuple["ForgeBlueprint", Path]]:
    """Provide a factory that resolves a case from ``FORGE_CASES`` into a blueprint.

    Returns
    -------
    Callable[..., tuple[ForgeBlueprint, Path]]
        ``(case_name, working_dir, *, name=None)`` -> the resolved blueprint and the
        path of the ``forge_blueprint.yaml`` written into ``working_dir``. No network
        access is needed because ``dt`` is supplied.
    """

    def _factory(
        case_name: str, working_dir: Path, *, name: str | None = None
    ) -> tuple["ForgeBlueprint", Path]:
        case = FORGE_CASES[case_name]
        domain = test_catalog.domain_data(case.domain)
        cfg = build_forge_blueprint(
            model_dir=test_catalog.model_dir(MODEL_SPEC),
            grid_name=domain["grid_name"],
            grid_kwargs=domain["grid_kwargs"],
            open_boundaries=domain["open_boundaries"],
            partitioning=domain["partitioning"],
            start_date=RUN_START,
            end_date=RUN_END,
            model_reference_date=MODEL_REFERENCE_DATE,
            dt=DT,
            forcing_inputs=test_catalog.forcing_data(case.forcing),
            output_settings=test_catalog.output_data("test-minimal"),
            name=name or f"it-{case_name}",
            compile_time_overrides=case.compile_time_overrides,
            roms_ref=ROMS_REF,
        )
        working_dir.mkdir(parents=True, exist_ok=True)
        cfg.working_dir = working_dir
        return cfg, cfg.to_yaml(working_dir / "forge_blueprint.yaml")

    return _factory
