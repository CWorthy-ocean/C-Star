"""Pytest configuration and shared fixtures for cstar-forge tests."""

import os
import sys
import tempfile
from pathlib import Path

# Add project root to path so we can import cstar_forge package
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Force the default catalog's user (writable) layer to a throwaway temp
# directory, unconditionally overriding whatever CSTAR_FORGE_CATALOG the
# developer's real shell/env may already have set. Without this, a developer
# with CSTAR_FORGE_CATALOG unset would get the module's real default --
# ~/cstar-forge-data/catalog -- as the user layer, and the test suite must
# never read from or write into that real directory. This has to happen at
# conftest *import* time (module level, before any test module runs): the
# default catalog is a lazy singleton (see cstar_forge.domain_catalog's
# module __getattr__), and `from cstar_forge.domain_catalog import
# default_catalog` in any test module constructs it immediately at
# collection, using whatever CSTAR_FORGE_CATALOG is set at that moment.
os.environ["CSTAR_FORGE_CATALOG"] = tempfile.mkdtemp(prefix="cstar-forge-test-catalog-")

import pytest  # noqa: E402  (must follow the sys.path bootstrap above)


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers",
        "real_template_staging: keep ForgeExecutor._template_repo_args unpatched (test the "
        "real cfg->AdditionalCode-args mapping instead of the offline working-tree redirect)",
    )


@pytest.fixture(autouse=True)
def _offline_template_staging(monkeypatch, request):
    """Stage render templates from the local working tree instead of GitHub.

    The executor fetches templates via C-Star's ``AdditionalCode`` from the git ref in
    ``code.templates_*`` (``https://…/cstar-forge.git`` @ main). In the suite we point
    ``location`` at the local ``templates/<stage>`` directory so staging is offline and
    sees the working tree — the *real* AdditionalCode local-copy path is exercised, only
    the source location is redirected (no network, no clone, no mock of the staging).

    Opt out with ``@pytest.mark.real_template_staging`` to assert the true cfg->args mapping.
    """
    if request.node.get_closest_marker("real_template_staging"):
        return
    from cstar_forge.forge.executor import ForgeExecutor

    def _local_args(self, stage):
        repo = getattr(self.code_spec, f"templates_{stage}")
        return {
            "location": str(project_root / repo.directory),
            "subdir": "",
            "checkout_target": "",
            "files": list(repo.files),
        }

    monkeypatch.setattr(ForgeExecutor, "_template_repo_args", _local_args)


@pytest.fixture
def test_data_dir():
    """Path to test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture
def workflows_dir():
    """Path to workflows directory."""
    return Path(__file__).parent.parent / "workflows"


@pytest.fixture
def real_models_yaml():
    """Path to the actual models.yaml file in the cstar_forge package."""
    # Use the same pattern as config.py: get path relative to package location
    import cstar_forge

    return Path(cstar_forge.config.paths.models_yaml)
