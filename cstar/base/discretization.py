from abc import ABC


class Discretization(ABC):
    """Holds discretization information about a Component.

    Attributes:
    -----------

    time_step: int
        The time step with which to run the Component
    """

    def __init__(
        self,
        time_step: int,
    ):
        """Initialize a Discretization object from basic discretization parameters.

        Parameters:
        -----------
        time_step: int
            The time step with which to run the Component

        Returns:
        --------
        Discretization:
            An initialized Discretization object
        """

        self.time_step: int = time_step

    def __str__(self) -> str:
        # Discretisation
        disc_str = ""

        if hasattr(self, "time_step") and self.time_step is not None:
            disc_str += "\ntime_step: " + str(self.time_step) + "s"
        if len(disc_str) > 0:
            classname = self.__class__.__name__
            header = classname
            disc_str = header + "\n" + "-" * len(classname) + disc_str

        return disc_str

    def __repr__(self) -> str:
        repr_str = ""
        repr_str = f"{self.__class__.__name__}("
        if hasattr(self, "time_step") and self.time_step is not None:
            repr_str += f"time_step = {self.time_step}, "
        repr_str = repr_str.strip(", ")
        repr_str += ")"
        return repr_str
