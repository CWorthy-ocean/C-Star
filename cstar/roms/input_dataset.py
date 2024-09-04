from abc import ABC
from pathlib import Path
from cstar.base.input_dataset import InputDataset


class ROMSInputDataset(InputDataset, ABC):
    (
        """
    ROMS-specific implementation of `InputDataset` (doc below)

    Extends `get()` method to generate dataset using roms-tools in the case that `source`
    points to a yaml file.

    Docstring for InputDataset:
    ---------------------------
    """
    ) + (InputDataset.__doc__ or "")


class ROMSModelGrid(ROMSInputDataset):
    """
    An implementation of the ROMSInputDataset class for model grid files.
    """

    def get(self, local_dir: str | Path) -> None:
        if self.source.source_type == "yaml":
            print("You are trying to make a ROMS grid from a yaml file!")
        else:
            super().get(local_dir)

    pass


class ROMSInitialConditions(ROMSInputDataset):
    """
    An implementation of the ROMSInputDataset class for model initial condition files.
    """

    pass


class ROMSTidalForcing(ROMSInputDataset):
    """
    An implementation of the ROMSInputDataset class for model tidal forcing files.
    """

    pass


class ROMSBoundaryForcing(ROMSInputDataset):
    """
    An implementation of the ROMSInputDataset class for model boundary condition files.
    """

    pass


class ROMSSurfaceForcing(ROMSInputDataset):
    """
    An implementation of the ROMSInputDataset class for model surface forcing files.
    """

    pass
