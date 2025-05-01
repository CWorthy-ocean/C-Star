import datetime as dt
import tempfile
from abc import ABC
from pathlib import Path
from typing import Any, List, Optional

import requests
import roms_tools
import yaml

from cstar.base.input_dataset import InputDataset
from cstar.base.utils import _get_sha256_hash, _list_to_concise_str


class ROMSInputDataset(InputDataset, ABC):
    (
        (
            """
    ROMS-specific implementation of `InputDataset` (doc below)

    Extends `get()` method to generate dataset using roms-tools in the case that `source`
    points to a yaml file.

    Docstring for InputDataset:
    ---------------------------
    """
        )
        + (InputDataset.__doc__ or "")
    )

    partitioned_files: List[Path] = []

    def __init__(
        self,
        location: str,
        file_hash: Optional[str] = None,
        start_date: Optional[str | dt.datetime] = None,
        end_date: Optional[str | dt.datetime] = None,
        n_source_partitions: int = 1,
    ):
        super().__init__(
            location=location,
            file_hash=file_hash,
            start_date=start_date,
            end_date=end_date,
        )
        self.n_source_partitions = n_source_partitions

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

    @property
    def _is_partitioned(self):
        # Largely cribbed from InputDataset.exists_locally - TODO consolidate!
        # TODO this check alone isn't enough - perhaps user wants to partition again with different np_xi and np_eta
        if (
            (self.partitioned_files is None)
            or (len(self.partitioned_files) == 0)
            or (not self._local_file_stat_cache)
        ):
            return False

        for path in self.partitioned_files:
            # Check if the file exists
            if not path.exists():
                return False

            # Retrieve the cached stats
            cached_stats = self._local_file_stat_cache.get(path)
            if cached_stats is None:
                return False

            # Compare size first
            current_stats = path.stat()
            if current_stats.st_size != cached_stats.st_size:
                return False

            # Compare modification time, fallback to hash check if mismatched
            if current_stats.st_mtime != cached_stats.st_mtime:
                current_hash = _get_sha256_hash(path.resolve())
                if (self._local_file_hash_cache is None) or (
                    self._local_file_hash_cache.get(path) != current_hash
                ):
                    return False

        return True

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
        if self._is_partitioned:
            self.log.info(f"⏭️  {self.__class__.__name__} already partitioned, skipping")
            return
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
            id_files_to_partition = self.working_path[:]

        else:
            id_files_to_partition = [
                self.working_path,
            ]

        parted_files = []

        for idfile in id_files_to_partition:
            self.log.info(f"Partitioning {idfile} into ({np_xi},{np_eta})")
            parted_files += roms_tools.partition_netcdf(
                idfile, np_xi=np_xi, np_eta=np_eta
            )

        self.partitioned_files = [f.resolve() for f in parted_files]
        self._local_file_hash_cache.update(
            {path: _get_sha256_hash(path.resolve()) for path in parted_files}
        )  # 27
        self._local_file_stat_cache.update({path: path.stat() for path in parted_files})

    def get(
        self,
        local_dir: str | Path,
    ) -> None:
        """Ensure this input dataset is available as a NetCDF file in `local_dir`.

        If the source is not a YAML file, this method delegates to the base class
        `InputDataset.get()`. Otherwise, it processes the YAML using `roms-tools`
        to create the dataset.

        Parameters:
        -----------
        local_dir (str or Path):
            Directory to save the dataset files.
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
            self.log.info(f"⏭️ {self.working_path} already exists, skipping.")
            return

        if self.source.source_type == "yaml":
            self._get_from_yaml(local_dir=local_dir)
        elif self.n_source_partitions > 1:
            self._get_from_partitioned_source(local_dir=local_dir)
        else:
            super().get(local_dir=local_dir)

    def _get_from_partitioned_source(self, local_dir: Path) -> None:
        ndigits = len(str(self.n_source_partitions))
        parted_files: list[Path] = []

        for i in range(self.n_source_partitions):
            source = self.source.location.replace(".nc", f".{i:0{ndigits}d}.nc")
            source_basename = self.source.basename.replace(
                ".nc", f".{i:0{ndigits}d}.nc"
            )

            self._symlink_or_download_from_source(
                source_location=source,
                location_type=self.source.location_type,
                expected_file_hash=None,
                target_path=local_dir / source_basename,
                logger=self.log,
            )

            parted_files.append(local_dir / source_basename)

        self.partitioned_files = parted_files
        self.working_path = parted_files

        self._local_file_hash_cache = {
            path: _get_sha256_hash(path.resolve()) for path in parted_files
        }  # 27
        self._local_file_stat_cache = {path: path.stat() for path in parted_files}

    def _get_from_yaml(self, local_dir: str | Path) -> None:
        """Handle the special case where the input dataset source is a `roms-tools`
        compatible YAML file.

        This method:
        - Fetches and optionally modifies the YAML based on start/end dates,
        - Creates a `roms-tools` instance from a modified temporary copy of the YAML
        - Saves this `roms-tools` class instance to a netCDF file in `local_dir`
        - Updates `working_path` and caches file metadata.

        Parameters:
        -----------
        local_dir (Path):
            Directory where the resulting NetCDF files should be saved.
        """

        # Ensure we're working with a Path object
        local_dir = Path(local_dir).expanduser().resolve()
        local_dir.mkdir(parents=True, exist_ok=True)

        if self.source.source_type != "yaml":
            raise ValueError(
                "_get_from_yaml requires a ROMSInputDataset whose source_type is yaml"
            )

        if self.source.location_type == "path":
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
        self.log.info(
            f"💾 Saving roms-tools dataset created from {self.source.location}..."
        )
        save_kwargs: dict[Any, Any] = {}
        save_kwargs["filepath"] = Path(
            f"{local_dir/Path(self.source.location).stem}.nc"
        )

        savepath = roms_tools_class_instance.save(**save_kwargs)
        self.working_path = savepath[0] if len(savepath) == 1 else savepath

        self._local_file_hash_cache.update(
            {path: _get_sha256_hash(path.resolve()) for path in savepath}
        )  # 27
        self._local_file_stat_cache.update({path: path.stat() for path in savepath})


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
