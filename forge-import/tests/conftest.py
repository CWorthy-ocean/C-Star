"""Pytest configuration and shared fixtures for cstar-forge tests."""

import os
import tempfile
from pathlib import Path

import pytest

project_root = Path(__file__).parent.parent


def pytest_configure(config):
    """Register custom markers and isolate the writable catalog layer.

    The catalog override has to happen before collection: the default catalog is
    a lazy singleton (see ``cstar_forge.domain_catalog``'s module ``__getattr__``),
    and several test modules import ``default_catalog`` at module level, which
    constructs it with whatever ``CSTAR_FORGE_CATALOG`` is set at that moment.
    Point it at a throwaway directory unconditionally, overriding the developer's
    real shell value, so the suite never reads from or writes into
    ``~/cstar-forge-data/catalog``.
    """
    os.environ["CSTAR_FORGE_CATALOG"] = tempfile.mkdtemp(
        prefix="cstar-forge-test-catalog-"
    )
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
