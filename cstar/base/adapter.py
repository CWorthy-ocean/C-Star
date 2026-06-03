import abc
import typing as t
from copy import deepcopy

_Tin = t.TypeVar("_Tin")
_Tout_co = t.TypeVar("_Tout_co", covariant=True)


class ModelAdapter(t.Protocol, t.Generic[_Tin, _Tout_co]):
    """Contract exposing a mechanism to adapt a source model to a target type."""

    model: _Tin

    def __init__(self, model: _Tin) -> None:
        self.model = model

    def adapt(self) -> _Tout_co | None:
        """Adapt the source model to the target output type.

        Returns
        -------
        _Tout
            The instance converted from the source model
        """
        ...


class SchemaAdapter(abc.ABC, ModelAdapter[dict[str, t.Any], dict[str, t.Any]]):
    """Contract exposing a mechanism to adapt a source model to a target type."""

    SCHEMA_VERSION_KEY: t.Final[str] = "schema_version"

    def __init__(
        self,
        model: dict[str, t.Any],
    ) -> None:
        super().__init__(model)

    @classmethod
    @abc.abstractmethod
    def application(cls) -> str: ...

    @classmethod
    @abc.abstractmethod
    def source(cls) -> str:
        """Return the schema version the adapter can accept as input."""
        ...

    @classmethod
    @abc.abstractmethod
    def target(cls) -> str:
        """Return the schema version the adapter will produce after `adapt` is called."""
        ...

    @classmethod
    @abc.abstractmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        """Perform version-specific modifications to the source model that
        result in a model compliant with the target version.

        Parameters
        ----------
        model : dict[str, t.Any]
            The original model

        Returns
        -------
        dict[str, t.Any]
            A migrated version of the model
        """
        ...

    def adapt(self) -> dict[str, t.Any]:
        """Perform modifications to the source model that result in a model
        compliant with the target version.

        This is the main adapter endpoint. It ensures the schema version is
        updated after any version-specific subclasses perform their changes.
        """
        clone: dict[str, t.Any] = deepcopy(self.model) if self.model else {}

        if migrated := self._migrate_schema(clone):
            migrated[self.SCHEMA_VERSION_KEY] = self.target()

        return migrated
