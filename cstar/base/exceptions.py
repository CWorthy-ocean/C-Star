class CstarException(Exception):
    """Base class for all Cstar exceptions."""

    pass


class BlueprintError(CstarException):
    """Exception raised for errors in blueprint processing."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
