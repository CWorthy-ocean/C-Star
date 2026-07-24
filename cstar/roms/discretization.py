from cstar.base.discretization import Discretization


class ROMSDiscretization(Discretization):
    """An implementation of the Discretization class for ROMS.

    Additional attributes:
    ----------------------
    n_procs_x: int
        The number of parallel processors over which to subdivide the x axis of the domain.
    n_procs_y: int
        The number of parallel processors over which to subdivide the y axis of the domain.

    Properties:
    -----------
    n_procs_tot: int
        The value of n_procs_x * n_procs_y
    """

    def __init__(
        self,
        time_step: int,
        n_procs_x: int = 1,
        n_procs_y: int = 1,
    ):
        """Initialize a ROMSDiscretization object from basic discretization parameters.

        Parameters:
        -----------
        time_step: int
            The time step with which to run the Component
        n_procs_x: int
           The number of parallel processors over which to subdivide the x axis of the domain.
        n_procs_y: int
           The number of parallel processors over which to subdivide the y axis of the domain.


        Returns:
        --------
        ROMSDiscretization:
            An initialized ROMSDiscretization object
        """
        super().__init__(time_step)
        self.n_procs_x = n_procs_x
        self.n_procs_y = n_procs_y

    @property
    def n_procs_tot(self) -> int:
        """Total number of processors required by this ROMS configuration."""
        return self.n_procs_x * self.n_procs_y
