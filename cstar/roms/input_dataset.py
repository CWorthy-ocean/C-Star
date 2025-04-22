import yaml
import tempfile
import requests
import roms_tools

from abc import ABC
from pathlib import Path
from typing import Optional, List, Any
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

    def partition(self, np_xi: int, np_eta: int):
        """Partition a netCDF dataset into tiles to run ROMS in parallel.

        Takes a local InputDataset and parallelisation parameters and uses
        roms-tools' partition_netcdf method to create multiple, smaller
        netCDF files, each of which corresponds to a processor used by ROMS.

        Parameters:
        -----------
        np_xi (int):
           The number of tiles in the x direction
        np_eta (int):
           The number of tiles in the y direction

        Notes:
        ------
        - This method will only work on ROMSInputDataset instances corresponding
           to locally available files, i.e. ROMSInputDataset.get() has been called.
        - This method sets the ROMSInputDataset.partitioned_files attribute
        """

        if not self.exists_locally:
            raise ValueError(
                f"working_path of InputDataset \n {self.working_path}, "
                + "refers to a non-existent file"
                + "\n call InputDataset.get() and try again."
            )
        else:
            assert self.working_path is not None  # if exists_locally then can't be None

        if isinstance(self.working_path, list):
            # if single InputDataset corresponds to many files, check they're colocated
            if not all(
                [d.parent == self.working_path[0].parent for d in self.working_path]
            ):
                raise ValueError(
                    f"A single input dataset exists in multiple directories: {self.working_path}."
                )

            # If they are, we want to partition them all in the same place
            # partdir = self.working_path[0].parent  # / "PARTITIONED"
            id_files_to_partition = self.working_path[:]

        else:
            id_files_to_partition = [
                self.working_path,
            ]
            # partdir = self.working_path.parent  # / "PARTITIONED"

            # partdir.mkdir(parents=True, exist_ok=True)
        parted_files = []

        for idfile in id_files_to_partition:
            print(f"Partitioning {idfile} into ({np_xi},{np_eta})")
            parted_files += roms_tools.partition_netcdf(
                idfile, np_xi=np_xi, np_eta=np_eta
            )

            # [p.rename(partdir / p.name) for p in parted_files[-1]]
            # [p.rename(partdir / p.name) for p in parted_files]
            # parted_files = [partdir / p.name for p in parted_files]
        self.partitioned_files = [f.resolve() for f in parted_files]

    def get(
        self,
        local_dir: str | Path,
        np_xi: Optional[int] = None,
        np_eta: Optional[int] = None,
    ) -> None:
        """Make this input dataset available as a netCDF file in `local_dir`.

        This method extends the `InputDataset.get()` method to accommodate
        instances where the source is a `roms-tools`-compatible `yaml` file.

        Steps:

        1. Fetch the source file to `local_dir` using `InputDataset.get()`.
           If the file is not in `yaml` format, we are done.
        2. If the file is in `yaml` format, modify the local copy so any
           time-varying datasets are given the correct start and end date
        3. Pass the modified yaml to roms-tools and save the resulting
           object to netCDF.
        4. Update the working_path attribute and cache the metadata and
           checksums of any produced netCDF files

        Parameters:
        -----------
        local_dir (str or Path):
            The directory in which to save the input dataset netCDF file
        np_xi, np_eta (int, optional):
            If desired, save a partitioned copy of the input dataset to be used when
            running ROMS in parallel. np_xi is the number of x-direction processors,
            np_eta is the number of y-direction processors
        """

        # Ensure we're working with a Path object
        local_dir = Path(local_dir).expanduser().resolve()
        local_dir.mkdir(parents=True, exist_ok=True)

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

        if self.source.source_type != "yaml":
            super().get(local_dir=local_dir)
            return
        elif self.source.location_type == "path":
            with open(Path(self.source.location).expanduser()) as F:
                raw_yaml_text = F.read()
        elif self.source.location_type == "url":
            raw_yaml_text = requests.get(self.source.location).text
        _, header, yaml_data = raw_yaml_text.split("---", 2)

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
        start_time = (
            self.start_date.isoformat() if self.start_date is not None else None
        )
        end_time = self.end_date.isoformat() if self.end_date is not None else None

        yaml_entries_to_modify = {
            "start_time": start_time,
            "ini_time": start_time,
            "end_time": end_time,
        }

        for key, value in yaml_entries_to_modify.items():
            if key in yaml_dict[roms_tools_class_name].keys():
                yaml_dict[roms_tools_class_name][key] = value

        roms_tools_class = getattr(roms_tools, roms_tools_class_name)

        # Create a temporary file that deletes itself when closed
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as temp_file:
            from_yaml_kwargs: dict[Any, Any] = {}
            temp_file.write(f"---{header}---\n" + yaml.dump(yaml_dict))
            temp_file.flush()  # Ensure data is written to disk

            from_yaml_kwargs["filepath"] = temp_file.name
            # roms-tools currently requires dask for every class except Grid,RiverForcing
            # in order to use wildcards in filepaths (known xarray issue):
            if roms_tools_class_name not in ["Grid", "RiverForcing"]:
                from_yaml_kwargs["use_dask"] = True

            roms_tools_class_instance = roms_tools_class.from_yaml(**from_yaml_kwargs)
        ##

        # ... and save:
        print(f"Saving roms-tools dataset created from {self.source.location}...")
        save_kwargs: dict[Any, Any] = {}
        save_kwargs["filepath"] = Path(
            f"{local_dir/Path(self.source.location).stem}.nc"
        )

        savepath = roms_tools_class_instance.save(**save_kwargs)
        self.partitioned_files = savepath

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


class ROMSRiverForcing(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for river forcing files."""

    pass


class ROMSForcingCorrections(ROMSInputDataset):
    """ROMS forcing correction file, such as SW correction or restoring fields.

    These are used by older ROMS configurations, and included in C-Star to support them.

    This file must not be generated from a roms-tools YAML. It should point directly to
    a NetCDF or similar file.
    """

    def validate(self):
        if self.source.source_type == "yaml":
            raise TypeError(
                "Hey, you! we said no funny business! -Scotty E."
                f"{self.__class__.__name__} cannot be initialized with a source YAML file. "
                "Please provide a direct path or URL to a dataset (e.g., NetCDF)."
            )
