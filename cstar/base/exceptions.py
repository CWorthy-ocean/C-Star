import typing as t

if t.TYPE_CHECKING:
    from collections.abc import Sequence


class CstarError(Exception):
    """Base class for all Cstar exceptions."""


class BlueprintError(CstarError):
    """Exception raised for errors in blueprint processing."""

    def __init__(self, message: str) -> None:
        """Initialize BlueprintError with a message."""
        super().__init__(message)


class SimulationError(CstarError):
    """Exception raised for errors in simulation processing."""

    def __init__(self, message: str) -> None:
        """Initialize SimulationError with a message."""
        super().__init__(message)


class BlueprintDeferredError(CstarError):
    """Raised when blueprint content is requested for a step whose blueprint is deferred to runtime and does not exist yet."""

    def __init__(self, message: str) -> None:
        """Initialize BlueprintDeferredError with a message."""
        super().__init__(message)


class CstarExpectationFailed(Exception):
    """Raise this error when a component cannot proceed due to prerequisites."""

    ...


class CstarUserConfigError(CstarError):
    """The user's configuration file names settings it may not set."""


class CatalogLocationError(CstarError, ValueError):
    """A catalog location (a `CSTAR_CATALOG` entry or a catalog URL) that C-Star
    cannot use as given; a `ValueError` too, as callers caught before it had a
    type of its own.
    """


class InvalidForgeBlueprintError(BlueprintError, ValueError):
    """A forge blueprint whose settings Forge refuses before staging any data; a
    `ValueError` too, as callers caught before it had a type of its own.
    """

    def __init__(self, problems: "Sequence[str]") -> None:
        """Initialize the error from the problems that refuse the blueprint."""
        self.problems = list(problems)
        super().__init__(
            "forge_blueprint has invalid values (fix before processing):\n  "
            + "\n  ".join(self.problems)
        )
