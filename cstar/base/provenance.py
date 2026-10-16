"""Facts about the environment a run was produced in: package versions and host."""

import hashlib
import json
import typing as t
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

if t.TYPE_CHECKING:
    from collections.abc import Sequence

DEFAULT_PACKAGES: t.Final[tuple[str, ...]] = ("cstar-ocean", "roms-tools")
"""The packages whose versions shape what C-Star produces."""


def package_version(name: str) -> str | None:
    """Best-effort installed version of a package, or `None` if it isn't installed.

    No git-describe step is needed: both ``cstar-ocean`` and ``roms-tools`` version
    themselves via ``setuptools_scm``, so an editable/dev checkout's installed
    version already embeds commit info (e.g. ``0.8.1.dev2+gcb931baef``). Never
    raises.

    Parameters
    ----------
    name : str
        The distribution name of the package.

    Returns
    -------
    str | None
        The installed version, or `None` when the package is not installed.
    """
    try:
        return _pkg_version(name)
    except PackageNotFoundError:
        return None


def package_versions(names: "Sequence[str]" = DEFAULT_PACKAGES) -> dict[str, str]:
    """The installed versions of a set of packages, keyed by package name.

    Parameters
    ----------
    names : Sequence[str]
        The distribution names of the packages.

    Returns
    -------
    dict[str, str]
        The version of each package that is installed
        (``{"cstar-ocean": "0.16.0", "roms-tools": "3.2.1"}``); a package that
        isn't installed is left out.
    """
    return {
        name: version
        for name in names
        if (version := package_version(name)) is not None
    }


def system_name() -> str:
    """The C-Star system this process runs on, or ``""`` when it can't be named.

    Best effort: the name is informational metadata, never configuration, so a
    host C-Star cannot identify must not stop a record from being stamped.

    Returns
    -------
    str
        The system name, or an empty string when the host cannot be identified.
    """
    # Lazy: keeps importing this module light.
    from cstar.system.manager import HostNameEvaluator

    try:
        return HostNameEvaluator().name
    except OSError:
        return ""


def canonical_hash(data: t.Any) -> str:
    """Fingerprint JSON-able data independent of key order.

    Parameters
    ----------
    data : Any
        The data to hash; values JSON cannot encode are hashed by their string form.

    Returns
    -------
    str
        The SHA-256 hex digest of the data's sorted-keys, compact JSON.
    """
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
