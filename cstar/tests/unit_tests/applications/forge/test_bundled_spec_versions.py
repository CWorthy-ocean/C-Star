"""Every shipped catalog spec file holds its kind's current `schema_version`.

Covers the bundled specs and the integration-test catalog. The key-subset check
also keeps the hand-written key tables in `cstar.catalog.spec_schema` honest.
"""

import typing as t
from pathlib import Path

import pytest
import yaml

import cstar
from cstar.catalog.spec_schema import (
    SPEC_SCHEMA_VERSIONS,
    known_spec_keys,
    migrate_spec,
)
from cstar.system.migration import KEY_SV

if t.TYPE_CHECKING:
    from cstar.orchestration.models import CatalogSpecKind

_CSTAR = Path(cstar.__file__).parent
_CATALOGS = (
    _CSTAR / "catalog" / "bundled",
    _CSTAR / "tests" / "integration_tests" / "catalog",
)
_SPEC_FILES = sorted(p for root in _CATALOGS for p in root.glob("*Spec/*.yaml"))

assert _SPEC_FILES, "no spec files found: the catalog globs are stale"


@pytest.mark.parametrize("path", _SPEC_FILES, ids=lambda p: f"{p.parent.name}/{p.stem}")
def test_spec_file_is_current(path: Path) -> None:
    """A spec file is at its kind's current version and in its current shape."""
    kind = t.cast("CatalogSpecKind", path.parent.name)
    data = yaml.safe_load(path.read_text())

    content = {key: value for key, value in data.items() if key != KEY_SV}

    assert data[KEY_SV] == SPEC_SCHEMA_VERSIONS[kind]
    # read as unversioned, so the migrations run: the content is already current
    assert not migrate_spec(kind, content, path).changed
    assert set(content) <= known_spec_keys(kind)
