import typing as t

from cstar.base.exceptions import CstarError
from cstar.base.log import LoggingMixin


class Registry(LoggingMixin):
    """An in-memory cache of key-value pairs used to identify a type used for
    a specific role.
    """

    _role: str
    """The role of the item being registered, e.g. `blueprint`."""
    _storage: t.ClassVar[dict[str, dict[str, t.Any]]] = {}
    """The internal memory of the registry.

    The registry is nested so that a single class can be re-used to perform registration
    for many different reasons. The role (or category) specifies which sub-mapping to
    place a given registration into.
    """

    def __init__(self, role: str) -> None:
        """Initialize a registry instance.

        Parameters
        ----------
        role : str
            A unique role name that is used to store a collection of related registry entries.
        """
        self._role = role

        if role not in Registry._storage:
            Registry._storage[role] = {}
        self._lookup = Registry._storage[role]

    @classmethod
    def runner_registry(cls) -> "Registry":
        """Convenience method for accessing the types registered as application runners.

        Returns
        -------
        Registry
            The runner instance used to handle registration of blueprint runners.
        """
        return Registry("runner")

    @classmethod
    def blueprint_registry(cls) -> "Registry":
        """Convenience method for accessing the types registered as application blueprints.

        Returns
        -------
        Registry
            The runner instance used to handle registration of blueprints.
        """
        return Registry("blueprint")

    def put(self, key: str, klass: type) -> bool:
        """Register a key-to-class lookup.

        Parameters
        ----------
        key : str
            The key used when looking up the type.
        klass : type
            The type held as the value in the key-value pair.

        Returns
        -------
        bool
            `True` if registry is updated, `False` if mapping exists.
        """
        current = self._lookup.get(key, None)

        if current and current == klass:
            self.log.debug(f"Attempted to re-register the {self._role!r} for: {key}")
            return False

        self._lookup[key] = klass

        self.log.debug(f"Registry `{self._role}::{key}` updated {klass.__name__!r}")
        return True

    def get(self, key: str) -> type:
        """Retrieve the type referenced by the key.

        Parameters
        ----------
        key : str
            The key to look up.
        klass : type
            The type held as the value in the key-value pair.

        Returns
        -------
        type
            The type held in the mapping

        Raises
        ------
        CStarError
            If the lookup fails to find a mapping.
        """
        key = key.strip() if key else ""
        if not key:
            raise ValueError("A registry key must be specified")

        found_type = self._lookup.get(key, None)
        if not found_type:
            raise CstarError(f"No {self._role!r} is registered for the key: {key!r}")
        return found_type

    @classmethod
    def get_categorical(cls, key: str, *categories: str) -> tuple[type, ...]:
        """Retrieve the same key across multiple categories."""
        results: list[type] = []
        for category in categories:
            if item := cls._storage[category].get(key):
                results.append(item)
            else:
                raise ValueError(
                    f"Unable to locate categorical registrations: {key}, {','.join(categories)}"
                )
        return tuple(results)
