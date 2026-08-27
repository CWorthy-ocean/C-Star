from cstar.base.discretization import Discretization


class ROMSDiscretization(Discretization):
    """An implementation of the Discretization class for ROMS.

    Attributes:
    -----------
    n_procs_x: int or None
        The number of parallel processors over which to subdivide the x axis of the domain.
    n_procs_y: int or None
        The number of parallel processors over which to subdivide the y axis of the domain.
    n_cores: int or None
        The total number of cores to use, e.g. when the tiling layout is
        determined automatically rather than from `n_procs_x`/`n_procs_y`.

    Properties:
    -----------
    n_procs_tot: int or None
        `n_cores` if set, otherwise `n_procs_x * n_procs_y` if both are set,
        otherwise `None`.
    """

    def __init__(
        self,
        n_procs_x: int | None = None,
        n_procs_y: int | None = None,
        n_cores: int | None = None,
    ):
        """Initialize a ROMSDiscretization object from basic discretization parameters.

        Parameters:
        -----------
        n_procs_x: int, optional
           The number of parallel processors over which to subdivide the x axis of the domain.
        n_procs_y: int, optional
           The number of parallel processors over which to subdivide the y axis of the domain.
        n_cores: int, optional
           The total number of cores to use, e.g. when the tiling layout is
           determined automatically rather than from `n_procs_x`/`n_procs_y`.

        Returns:
        --------
        ROMSDiscretization:
            An initialized ROMSDiscretization object
        """
        self.n_procs_x = n_procs_x
        self.n_procs_y = n_procs_y
        self.n_cores = n_cores

    @property
    def n_procs_tot(self) -> int | None:
        """Total number of processors required by this ROMS configuration.

        Returns `n_cores` if set, else `n_procs_x * n_procs_y` if both are
        set, else `None`.
        """
        if self.n_cores is not None:
            return self.n_cores
        if self.n_procs_x is not None and self.n_procs_y is not None:
            return self.n_procs_x * self.n_procs_y
        return None

    def __str__(self) -> str:
        disc_str = ""
        for attr in ("n_procs_x", "n_procs_y", "n_cores"):
            value = getattr(self, attr)
            if value is not None:
                disc_str += f"\n{attr}: {value}"
        if len(disc_str) > 0:
            classname = self.__class__.__name__
            header = classname
            disc_str = header + "\n" + "-" * len(classname) + disc_str

        return disc_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}("
        parts = [
            f"{attr} = {value}"
            for attr in ("n_procs_x", "n_procs_y", "n_cores")
            if (value := getattr(self, attr)) is not None
        ]
        repr_str += ", ".join(parts)
        repr_str += ")"
        return repr_str
