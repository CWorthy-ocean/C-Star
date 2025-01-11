import yaml
import shutil
import dateutil
import datetime as dt
import roms_tools

from abc import ABC
from pathlib import Path
from typing import Optional, List
from cstar.base.input_dataset import InputDataset
from cstar.base.utils import _list_to_concise_str, _get_sha256_hash


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

    def get(
        self,
        local_dir: str | Path,
        start_date: Optional[dt.datetime] | str = None,
        end_date: Optional[dt.datetime] | str = None,
        np_xi: Optional[int] = None,
        np_eta: Optional[int] = None,
    ) -> None:
        """Make this input dataset available as a netCDF file in `local_dir`.

        This method extends the `InputDataset.get()` method to accommodate
        instances where the source is a `roms-tools`-compatible `yaml` file.

        Steps:
        i. Fetch the source file to `local_dir` using `InputDataset.get()`.
            If the file is not in `yaml` format, we are done.
        ii. If the file is in `yaml` format, modify the local copy so any
            time-varying datasets are given the correct start and end date
        iii. Pass the modified yaml to roms-tools and save the resulting
            object to netCDF.
        iv. Update the working_path attribute and cache the metadata and
            checksums of any produced netCDF files

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
        # Ensure we're working with a Path object
        local_dir = Path(local_dir).resolve()

        # If `working_path` is set, determine we're not fetching to the same parent dir:
        if self.working_path is None:
            working_path_parent = None
        elif isinstance(self.working_path, list):
            working_path_parent = self.working_path[0].parent
        else:
            working_path_parent = self.working_path.parent

        if (self.exists_locally) and (working_path_parent == local_dir):
            print(f"Input dataset already exists in {working_path_parent}, skipping.")
            return

        super().get(local_dir=local_dir)

        # If it's not a yaml, we're done
        if self.source.source_type != "yaml":
            return

        # Make sure that the local copy is not a symlink
        # (as InputDataset.get() symlinks files that didn't need to be downloaded)
        local_path = local_dir / Path(self.source.basename)
        if local_path.is_symlink():
            actual_path = local_path.resolve()
            local_path.unlink()
            shutil.copy2(actual_path, local_path)
            local_path = actual_path

        # Now modify the local copy of the yaml file as needed:
        with open(local_path, "r") as F:
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

        with open(local_path, "w") as F:
            F.write(f"---{header}---\n" + yaml.dump(yaml_dict))

        # Finally, make a roms-tools object from the modified yaml
        # import roms_tools

        roms_tools_class = getattr(roms_tools, roms_tools_class_name)

        # roms-tools currently requires dask for every class except Grid
        # in order to use wildcards in filepaths (known xarray issue):

        if roms_tools_class_name == "Grid":
            roms_tools_class_instance = roms_tools_class.from_yaml(local_path)
        else:
            roms_tools_class_instance = roms_tools_class.from_yaml(
                local_path, use_dask=True
            )

        # ... and save:
        print(f"Saving roms-tools dataset created from {local_path}...")
        if (np_eta is not None) and (np_xi is not None):
            savepath = roms_tools_class_instance.save(
                local_dir / "PARTITIONED" / local_path.stem, np_xi=np_xi, np_eta=np_eta
            )
            self.partitioned_files = savepath

        else:
            savepath = roms_tools_class_instance.save(
                Path(f"{local_dir/local_path.stem}.nc")
            )
        self.working_path = savepath[0] if len(savepath) == 1 else savepath

        self._local_file_hash_cache = {
            path: _get_sha256_hash(path.resolve()) for path in savepath
        }  # 27
        self._local_file_stat_cache = {path: path.stat() for path in savepath}


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
