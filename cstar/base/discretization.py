from abc import ABC


class Discretization(ABC):
    """Marker base class for component-specific discretization/partitioning
    parameters.

    Subclasses hold the parameters relevant to their component (e.g.
    processor counts for ROMS); this base class carries no attributes of
    its own.
    """
