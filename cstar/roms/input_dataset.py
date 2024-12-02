import yaml
import shutil
import dateutil
import datetime as dt
import roms_tools

from abc import ABC
from pathlib import Path
from typing import Optional, List
from cstar.base.input_dataset import InputDataset
from cstar.base.utils import _list_to_concise_str


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

    partitioned_files: List[Path] = []

    def __str__(self) -> str:
        base_str = super().__str__()
        if hasattr(self, "partitioned_files") and len(self.partitioned_files) > 0:
            base_str += "\nPartitioned files: "
            base_str += _list_to_concise_str(
                [str(f) for f in self.partitioned_files], pad=20
            )
        return base_str

    def __repr__(self) -> str:
        repr_str = super().__repr__()
        if hasattr(self, "partitioned_files") and len(self.partitioned_files) > 0:
            info_str = "partitioned_files = "
            info_str += _list_to_concise_str(
                [str(f) for f in self.partitioned_files], pad=29
            )
            if "State:" in repr_str:
                repr_str = repr_str.strip(",>")
                repr_str += ",\n" + (" " * 8) + info_str + "\n>"
            else:
                repr_str += f"\nState: <{info_str}>"

        return repr_str

    def get_from_yaml(
        self,
        local_dir: str | Path,
        start_date: Optional[dt.datetime] | str = None,
        end_date: Optional[dt.datetime] | str = None,
        np_xi: Optional[int] = None,
        np_eta: Optional[int] = None,
    ) -> None:
        """Make this input dataset available as a netCDF file in `local_dir`

        This method uses the roms-tools python package to produce a UCLA-ROMS-compatible
        netCDF file from a roms-tools compatible yaml file.

        Steps:
        i. Obtain a working copy of the yaml file in `local_dir`
        from InputDataset.source.location.
        ii. Modify the working copy of the yaml file so any time-varying datasets
        are given the correct start and end date.
        iii. Pass the modified yaml to roms-tools and save the resulting
        object to netCDF.

        Parameters:
        -----------
        local_dir (str or Path):
           The directory in which to save the input dataset netCDF file
        start_date,end_date (dt.datetime, optional):
           If the dataset to be created is time-varying, it is made using these dates
        np_xi, np_eta (int, optional):
           If desired, save a partitioned copy of the input dataset to be used when
           running ROMS in parallel. np_xi is the number of x-direction processors,
           np_eta is the number of y-direction processors
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

        yaml_keys = list(yaml_dict.keys())
        if len(yaml_keys) == 1:
            roms_tools_class_name = yaml_keys[0]
        elif len(yaml_keys) == 2:
            roms_tools_class_name = [y for y in yaml_keys if y != "Grid"][0]
        else:
            raise ValueError(
                f"roms tools yaml file has {len(yaml_keys)} sections. "
                + "Expected 'Grid' and one other class"
            )
        if isinstance(start_date, str):
            start_date = dateutil.parser.parse(start_date)
        if isinstance(end_date, str):
            end_date = dateutil.parser.parse(end_date)
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

        with open(yaml_file, "w") as F:
            F.write(f"---{header}---\n" + yaml.dump(yaml_dict))

        # Finally, make a roms-tools object from the modified yaml
        # import roms_tools

        roms_tools_class = getattr(roms_tools, roms_tools_class_name)

        # roms-tools currently requires dask for every class except Grid
        # in order to use wildcards in filepaths (known xarray issue):

        if roms_tools_class_name == "Grid":
            roms_tools_class_instance = roms_tools_class.from_yaml(yaml_file)
        else:
            roms_tools_class_instance = roms_tools_class.from_yaml(
                yaml_file, use_dask=True
            )

        # ... and save:
        print(f"Saving roms-tools dataset created from {yaml_file}...")
        if (np_eta is not None) and (np_xi is not None):
            savepath = roms_tools_class_instance.save(
                local_dir / "PARTITIONED" / yaml_file.stem, np_xi=np_xi, np_eta=np_eta
            )
            self.partitioned_files = savepath

        else:
            savepath = roms_tools_class_instance.save(
                Path(f"{local_dir/yaml_file.stem}.nc")
            )

            self.working_path = savepath[0] if len(savepath) == 1 else savepath


class ROMSModelGrid(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model grid files."""

    pass


class ROMSInitialConditions(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model initial condition
    files."""

    pass


class ROMSTidalForcing(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model tidal forcing files."""

    pass


class ROMSBoundaryForcing(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model boundary condition
    files."""

    pass


class ROMSSurfaceForcing(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model surface forcing
    files."""

    pass
