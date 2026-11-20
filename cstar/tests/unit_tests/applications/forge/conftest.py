"""Pytest configuration and shared fixtures for cstar-forge tests."""

import os
import tempfile
from pathlib import Path, PurePosixPath

import pytest

import cstar

_NEW_TEMPLATES_ROOT = (
    Path(cstar.__file__).parent / "additional_files" / "templates" / "forge"
)


def pytest_configure(config):
    """Register custom markers and isolate the writable catalog layer.

    The catalog override has to happen before collection: the default catalog is
    a lazy singleton (see ``cstar.catalog.domain_catalog``'s module ``__getattr__``),
    and several test modules import ``default_catalog`` at module level, which
    constructs it with whatever ``CSTAR_CATALOG`` is set at that moment.
    Point it at a throwaway directory unconditionally, overriding the developer's
    real shell value, so the suite never reads from or writes into
    ``~/cstar/catalog``.

    ``network`` and ``slow`` are already registered globally in the repo's
    ``pyproject.toml``; only ``real_template_staging``, ``real_time_coverage`` and
    ``integration`` are forge-specific.
    """
    os.environ["CSTAR_CATALOG"] = tempfile.mkdtemp(prefix="cstar-forge-test-catalog-")
    config.addinivalue_line(
        "markers",
        "real_template_staging: keep ForgeExecutor._template_repo_args unpatched (test the "
        "real cfg->AdditionalCode-args mapping instead of the offline working-tree redirect)",
    )
    config.addinivalue_line(
        "markers",
        "real_time_coverage: keep the input generator's time-coverage checks (they "
        "open the written netCDF files, which most unit tests fake as empty files)",
    )
    config.addinivalue_line(
        "markers",
        "integration: tests requiring roms-tools/network-backed construction "
        "(deselect with -m 'not integration')",
    )


@pytest.fixture(autouse=True)
def _offline_template_staging(monkeypatch, request):
    """Stage render templates from the local working tree instead of GitHub.

    The executor fetches templates via C-Star's ``AdditionalCode`` from the git ref in
    ``code.templates_*`` (this repository at the ModelSpec's ``templates_commit``). In
    the suite we map a stage's ``directory`` (the current
    ``cstar/additional_files/templates/forge/<stage>`` form, or the legacy
    ``templates/<stage>`` form of older blueprints) onto the working tree's bundled
    copy and point ``location`` there so
    staging is offline and sees the working tree — the *real* AdditionalCode local-copy
    path is exercised, only the source location is redirected (no network, no clone, no
    mock of the staging).

    Opt out with ``@pytest.mark.real_template_staging`` to assert the true cfg->args mapping.
    """
    if request.node.get_closest_marker("real_template_staging"):
        return
    from cstar.applications.forge.executor import ForgeExecutor

    def _local_args(self, stage):
        repo = getattr(self.code_spec, f"templates_{stage}")
        parts = PurePosixPath(repo.directory).parts
        for prefix in (
            ("cstar", "additional_files", "templates", "forge"),
            ("templates",),
        ):
            if parts[: len(prefix)] == prefix:
                parts = parts[len(prefix) :]
                break
        sub = PurePosixPath(*parts)
        return {
            "location": str(_NEW_TEMPLATES_ROOT / sub),
            "subdir": "",
            "checkout_target": "",
            "files": list(repo.files),
        }

    monkeypatch.setattr(ForgeExecutor, "_template_repo_args", _local_args)
    # The redirect above serves the *working tree*'s bundled templates regardless of
    # which commit a ModelSpec's `file_hashes` actually pins, so a real hash check
    # here would fail for any ModelSpec pinned at a commit older than the bundled
    # copy (see the drift documented in ModelTemplates.file_hashes' docstring) --
    # no-op it; ``real_template_staging`` tests exercise the real check instead.
    monkeypatch.setattr(
        ForgeExecutor, "_verify_template_hashes", lambda self, stage, dest: None
    )


@pytest.fixture(autouse=True)
def _skip_time_coverage_checks(monkeypatch, request):
    """Skip the input generator's checks of the written files' time axes.

    Most generation tests mock roms-tools and hand the steps empty ``touch()``-ed
    files as their outputs, which the checks cannot open. The checks have their own
    tests on real netCDF files; opt back in with ``@pytest.mark.real_time_coverage``.
    """
    if request.node.get_closest_marker("real_time_coverage"):
        return
    from cstar.applications.forge import input_generator

    monkeypatch.setattr(
        input_generator, "check_forcing_covers_window", lambda *a, **kw: None
    )
    monkeypatch.setattr(input_generator, "check_initial_time", lambda *a, **kw: None)
    monkeypatch.setattr(input_generator, "restamp_initial_time", lambda *a, **kw: None)


# No separate cache-home isolation fixture needed here: the repo-wide autouse
# ``mock_xdg_dirs`` fixture (``cstar/tests/unit_tests/conftest.py``) already
# points ``CSTAR_CACHE_HOME`` -- and so ``DirectoryManager.cache_home()``,
# where ``ForgeExecutor._template_cache_dir`` puts the staging cache -- at a
# throwaway per-test directory for every test in the suite.
