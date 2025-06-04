from typing import Protocol


class Preparable(Protocol):
    """A protocol exposing an API for objects to return their local configuration
    status."""

    @property
    def is_setup(self) -> bool:
        """Return True if the local configuration is up to date, False otherwise.

        Returns:
        -------
        is_setup: bool
            True if the code is available locally, False otherwise.
        """
        ...
