import typing as t

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
