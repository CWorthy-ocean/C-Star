"""Earlier names of applications, and the current name each one resolves to.

An application's name is written into the documents C-Star reads and writes
(blueprints, workplans, run records) and names its run and catalog directories.
When an application is renamed, documents written under the earlier name must
keep loading, so the old name is recorded here as an alias of the new one.

The table sits in ``base/`` because several layers need it and none of them can
import another's home: orchestration models canonicalise the names they load,
the execution file system maps a name to a run directory, the application
registry resolves a name to its definition, and the migrator plans against it.
``orchestration`` cannot import ``applications``, so the table cannot live with
the registry.
"""

import typing as t
from collections.abc import Mapping
from types import MappingProxyType

APPLICATION_ALIASES: t.Final[Mapping[str, str]] = MappingProxyType(
    {"roms_marbl": "ucla_roms"}
)
"""Earlier names of applications, mapped to the current name.

Documents written under an alias keep loading.
"""


def canonical_application(name: str) -> str:
    """Return the current name of an application.

    Parameters
    ----------
    name : str
        An application name, current or earlier. Matched exactly: application
        names are module names, so case and whitespace are significant.

    Returns
    -------
    str
        The name that `name` is an alias of, or `name` itself when it is not
        an alias.
    """
    return APPLICATION_ALIASES.get(name, name)


def legacy_names(canonical: str) -> tuple[str, ...]:
    """Return the earlier names of an application.

    Parameters
    ----------
    canonical : str
        The current name of an application.

    Returns
    -------
    tuple[str, ...]
        The aliases that map to `canonical`, in table order; empty when the
        application was never renamed.
    """
    return tuple(
        alias for alias, current in APPLICATION_ALIASES.items() if current == canonical
    )
