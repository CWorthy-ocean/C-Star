import pooch

from abc import ABC
from pathlib import Path
from typing import Optional
from cstar.base.input_dataset import InputDataset
from cstar.roms.utils import _modify_roms_tools_yaml


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

    def get(
        self,
        local_dir: str | Path,
        np_xi: Optional[int] = None,
        np_eta: Optional[int] = None,
    ) -> None:
        local_dir = Path(local_dir)
        if self.source.source_type == "yaml":
            local_file = local_dir / Path(self.source.location).stem

            if self.source.location_type == "url":
                pooch.retrieve(
                    self.source.location, known_hash=self.file_hash, path=local_file
                )
                yaml_location = local_file
            elif self.source.location_type == "path":
                yaml_location = Path(self.source.location)

            import roms_tools as rt

            # Copy the yaml
            _modify_roms_tools_yaml(
                input_file=yaml_location, output_file=local_file, new_entries={}
            )

            roms_grd = rt.Grid.from_yaml(self.source.location)
            roms_grd.save(local_file.stem, np_xi=np_xi, np_eta=np_eta)
            self.local_path = local_file

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
