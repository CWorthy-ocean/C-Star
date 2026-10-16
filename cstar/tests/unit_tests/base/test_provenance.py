from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from unittest import mock

import pytest

from cstar.base import provenance
from cstar.base.provenance import (
    DEFAULT_PACKAGES,
    canonical_hash,
    package_version,
    package_versions,
    system_name,
)


def test_package_version_of_installed_package() -> None:
    """Verify an installed package reports its version."""
    assert package_version("pytest") == pytest.__version__


def test_package_version_of_missing_package() -> None:
    """Verify a package that is not installed reports `None`."""
    assert package_version("not-a-real-package-name") is None


def test_package_versions_skips_missing_packages() -> None:
    """Verify only installed packages appear in the mapping."""

    def fake(name: str) -> str:
        if name == "absent":
            raise PackageNotFoundError(name)
        return "1.2.3"

    with mock.patch.object(provenance, "_pkg_version", fake):
        assert package_versions(["present", "absent"]) == {"present": "1.2.3"}
        assert package_versions() == dict.fromkeys(DEFAULT_PACKAGES, "1.2.3")


def test_system_name_is_the_hosts_name() -> None:
    """Verify the host's name is returned."""
    with mock.patch("cstar.system.manager.HostNameEvaluator") as evaluator:
        evaluator.return_value.name = "anvil"
        assert system_name() == "anvil"


def test_system_name_is_empty_when_the_host_cannot_be_named() -> None:
    """Verify an unidentifiable host yields an empty name."""
    with mock.patch("cstar.system.manager.HostNameEvaluator") as evaluator:
        type(evaluator.return_value).name = mock.PropertyMock(side_effect=OSError)
        assert system_name() == ""


def test_canonical_hash_ignores_key_order_and_stringifies_odd_values() -> None:
    """Verify the digest depends on content, not key order or JSON-unfriendly types."""
    when = datetime(2026, 10, 16, tzinfo=UTC)
    first = canonical_hash({"b": [1, 2], "a": when})
    second = canonical_hash({"a": str(when), "b": [1, 2]})

    assert first == second
    assert first != canonical_hash({"a": str(when), "b": [2, 1]})
