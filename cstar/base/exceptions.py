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
