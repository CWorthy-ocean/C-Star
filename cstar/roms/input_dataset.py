import yaml
import shutil
import datetime as dt

from abc import ABC
from pathlib import Path
from typing import Optional
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

    def get_from_yaml(
        self,
        local_dir: str | Path,
        start_date: Optional[dt.datetime] = None,
        end_date: Optional[dt.datetime] = None,
        np_xi: Optional[int] = None,
        np_eta: Optional[int] = None,
    ) -> None:
        """
        docstring
        """
        # If it's not a yaml, we're done
        if self.source.source_type != "yaml":
            raise ValueError(
                "Attempted to call `ROMSInputDataset.get_from_yaml() "
                + "but ROMSInputDataset.source.source_type is "
                + f"{self.source.source_type}, not 'yaml'"
            )

        # Ensure we're working with a Path object
        local_dir = Path(local_dir)

        # First, get the file as usual
        self.get(local_dir)

        # Make sure that the local copy is not a symlink
        # (as InputDataset.get() symlinks files that didn't need to be downloaded)
        yaml_file = local_dir / Path(self.source.location).name
        if yaml_file.is_symlink():
            actual_path = yaml_file.resolve()
            yaml_file.unlink()
            shutil.copy2(actual_path, yaml_file)
            yaml_file = actual_path

        # Now modify the local copy of the yaml file as needed:
        with open(yaml_file, "r") as F:
            _, header, yaml_data = F.read().split("---", 2)
            yaml_dict = yaml.safe_load(yaml_data)

        roms_tools_class_name = list(yaml_dict.keys())[-1]

        start_time = start_date.isoformat() if start_date is not None else None
        end_time = end_date.isoformat() if end_date is not None else None

        yaml_entries_to_modify = {
            "start_time": start_time,
            "ini_time": start_time,
            "end_time": end_time,
        }

        for key, value in yaml_entries_to_modify.items():
            if key in yaml_dict[roms_tools_class_name].keys():
                yaml_dict[roms_tools_class_name][key] = value
            # else:
            #     raise ValueError(
            #         f"Cannot replace entry {key} in "
            #         + f"roms_tools yaml file {yaml_file} under {roms_tools_class_name}. "
            #         + "No such entry."
            #     )

        with open(yaml_file, "w") as F:
            F.write(f"---{header}---\n" + yaml.dump(yaml_dict))

        # Finally, make a roms-tools object from the modified yaml
        import roms_tools

        roms_tools_class = getattr(roms_tools, roms_tools_class_name)
        roms_tools_class_instance = roms_tools_class.from_yaml(self.source.location)

        # ... and save:
        if (np_eta is not None) and (np_xi is not None):
            roms_tools_class_instance.save(
                local_dir / "PARTITIONED" / yaml_file.stem, np_xi=np_xi, np_eta=np_eta
            )
            parted_dir = yaml_file.parent / "PARTITIONED"
            self.local_partitioned_files = list(
                parted_dir.glob(f"{yaml_file.stem}.*.nc")
            )
        else:
            savepath = Path(f"{local_dir/yaml_file.stem}.nc")
            roms_tools_class_instance.save(savepath)
            self.local_path = savepath


class ROMSModelGrid(ROMSInputDataset):
    """
    An implementation of the ROMSInputDataset class for model grid files.
    """

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
