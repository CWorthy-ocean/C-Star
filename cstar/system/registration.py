import typing as t
from collections import defaultdict
from collections.abc import Callable

from cstar.base.exceptions import CstarError
from cstar.base.log import LoggingMixin

TValue = t.TypeVar("TValue")


class Registrar(LoggingMixin, t.Generic[TValue]):
    """An in-memory cache of key-value pairs used to identify a type used for
    a specific role.
    """

    """The role of the item being registered, e.g. `blueprint`."""
    _storage: t.ClassVar[dict[str, dict[str, t.Any]]]
    """The internal memory of the registry.

    The registry is nested so that a single class can be re-used to perform registration
    for many different reasons. The role (or category) specifies which sub-mapping to
    place a given registration into.
    """
    _role: str
    """The registry role the instance will retrieve records for."""
    _lookup: dict[str, t.Any]
    """The branch of the registry used for the target role."""

    def __init__(self, role: str) -> None:
        """Initialize a registry instance.

        Parameters
        ----------
        role : str
            A unique role name that is used to store a collection of related registry entries.
        """
        if not hasattr(Registrar, "_storage"):
            Registrar._storage = defaultdict(dict)
        self._role = role
        self._lookup = Registrar._storage[role]

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
            msg = f"Attempted to re-register the {self._role!r} for: {key}"
            self.log.debug(msg)
            return False

        self._lookup[key] = klass

        msg = f"Registry `{self._role}::{key}` updated {klass.__name__!r}"
        self.log.debug(msg)
        return True

    def get(self, key: str) -> type[TValue]:
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
            msg = "A registry key must be specified"
            raise ValueError(msg)

        found_type = self._lookup.get(key, None)
        if not found_type:
            msg = f"No {self._role!r} is registered for the key: {key!r}"
            raise CstarError(msg)
        return found_type

    @classmethod
    def get_categorical(cls, key: str, *categories: str) -> tuple[type[TValue], ...]:
        """Retrieve the same key across multiple categories."""
        results = [
            (cat, cls._storage[cat].get(key))
            for cat in categories
            if cls._storage[cat].get(key)
        ]

        if missing := set(categories).difference(x[0] for x in results):
            msg = f"Unable to locate categorical registrations: {key}, {','.join(missing)}"
            raise ValueError(msg)
        return tuple(x[1] for x in results if x[1])


def register_handler(
    role: str,
    application: str,
) -> Callable[[type[TValue]], type[TValue]]:
    """Decorator used to register a type as a handler for some process.

    Parameters
    ----------
    category : str
        A string identifying the handler category, e.g. "blueprint" or "runner".
    application : str
        The name of the application the handler will be registered for, e.g. "roms_marbl"

    Returns
    -------
        A function that will add the handler to the registry then return it.
    """

    def register_handler_direct(klass: type[TValue]) -> type[TValue]:
        """Register the specified type as a blueprint.

        Parameters
        ----------
        klass : type[THandler]
            The type to be registered
        """
        registry = Registrar[TValue](role)
        registry.put(application, klass)

        return klass

    return register_handler_direct
