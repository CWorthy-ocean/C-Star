import datetime as dt
import shutil
import tempfile
from abc import ABC
from pathlib import Path
from typing import Any

import requests
import roms_tools
import yaml

from cstar.base.input_dataset import InputDataset
from cstar.base.utils import _get_sha256_hash, _list_to_concise_str


class ROMSPartitioning:
    """Describes a partitioning of a ROMS input dataset into a grid of subdomains.

    This object stores the number of partitions along the xi and eta dimensions,
    along with the list of corresponding file paths. It supports indexing and
    length queries like a sequence.

    Parameters
    ----------
    np_xi : int
        Number of partitions along the xi (longitude-like) axis.
    np_eta : int
        Number of partitions along the eta (latitude-like) axis.
    files : list of Path
        List of paths to partitioned NetCDF files. The expected length is
        `np_xi * np_eta`, and the order should be consistent with the ROMS
        partitioning convention.

    Attributes
    ----------
    np_xi : int
        Number of xi-direction partitions.
    np_eta : int
        Number of eta-direction partitions.
    files : list of Path
        Paths to the partitioned files.
    """

    def __init__(self, np_xi: int, np_eta: int, files: list[Path]):
        self.np_xi = np_xi
        self.np_eta = np_eta
        self.files = files
        self._local_file_hash_cache: dict = {}
        self._local_file_stat_cache: dict = {}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(np_xi={self.np_xi}, np_eta={self.np_eta}, files={_list_to_concise_str(self.files, pad=43)})"

    def __len__(self):
        return len(self.files)


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

    def __init__(
        self,
        location: str,
        file_hash: str | None = None,
        start_date: str | dt.datetime | None = None,
        end_date: str | dt.datetime | None = None,
        source_np_xi: int | None = None,
        source_np_eta: int | None = None,
    ):
        super().__init__(
            location=location,
            file_hash=file_hash,
            start_date=start_date,
            end_date=end_date,
        )

        self.source_np_xi = source_np_xi
        self.source_np_eta = source_np_eta
        self.partitioning: ROMSPartitioning | None = None

    @property
    def source_partitioning(self) -> tuple[int, int] | None:
        if (self.source_np_xi is not None) and (self.source_np_eta is not None):
            return (self.source_np_xi, self.source_np_eta)
        return None

    def to_dict(self) -> dict:
        input_dataset_dict = super().to_dict()
        if self.source_partitioning is not None:
            input_dataset_dict["source_np_xi"] = self.source_np_xi
            input_dataset_dict["source_np_eta"] = self.source_np_eta
        return input_dataset_dict

    def __str__(self) -> str:
        base_str = super().__str__()
        if self.partitioning is not None:
            base_str += f"\nPartitioning: {self.partitioning}"
        return base_str

    def __repr__(self) -> str:
        repr_str = super().__repr__()
        if self.partitioning is not None:
            info_str = f"partitioning  = {self.partitioning}"
            if "State:" in repr_str:
                repr_str = repr_str.strip(",>")
                repr_str += ",\n" + (" " * 8) + info_str + "\n>"
            else:
                repr_str += f"\nState: <{info_str}>"

        return repr_str

    def partition(
        self, np_xi: int, np_eta: int, overwrite_existing_files: bool = False
    ):
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
        overwrite_existing_files (bool, optional):
           If `True` and this `ROMSInputDataset` has already been partitioned,
           the existing files will be overwritten

        Notes:
        ------
        - This method will only work on ROMSInputDataset instances corresponding
           to locally available files, i.e. ROMSInputDataset.get() has been called.
        - This method sets the ROMSInputDataset.partitioning attribute
        """

        # Helper functions
        def validate_partitioning_request() -> bool:
            """Helper function to skip, raise, or proceed with partitioning request."""
            if (self.partitioning is not None) and (not overwrite_existing_files):
                if (self.partitioning.np_xi == np_xi) and (
                    self.partitioning.np_eta == np_eta
                ):
                    self.log.info(
                        f"⏭️  {self.__class__.__name__} already partitioned, skipping"
                    )
                    return False
                else:
                    raise FileExistsError(
                        f"The file has already been partitioned into a different arrangement "
                        f"({self.partitioning.np_xi},{self.partitioning.np_eta}). "
                        "To overwrite these files, try again with overwrite_existing_files=True"
                    )

            if not self.exists_locally:
                raise ValueError(
                    f"working_path of InputDataset \n {self.working_path}, "
                    + "refers to a non-existent file"
                    + "\n call InputDataset.get() and try again."
                )
            return True

        def get_files_to_partition():
            """Helper function to obtain a list of files associated with this
            ROMSInputDataset to partition.
            """
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
            return id_files_to_partition

        def partition_files(files: list[Path]) -> list[Path]:
            """Helper function that wraps the actual roms_tools.partition_netcdf
            call.
            """
            new_parted_files = []

            for idfile in files:
                self.log.info(f"Partitioning {idfile} into ({np_xi},{np_eta})")
                new_parted_files.extend(
                    roms_tools.partition_netcdf(idfile, np_xi=np_xi, np_eta=np_eta)
                )

            return [f.resolve() for f in new_parted_files]

        def backup_existing_partitioned_files(files: list[Path]):
            """Helper function to move existing parted files to a tmp dir while
            attempting to create new ones.
            """
            tmpdir = tempfile.TemporaryDirectory()
            backup_path = Path(tmpdir.name)

            for f in files:
                shutil.move(f.resolve(), backup_path / f.name)
            return tmpdir, backup_path

        def restore_existing_partitioned_files(
            backup_dir: Path, restore_paths: list[Path]
        ):
            """Helper function to restore existing parted files if partitioning
            fails.
            """
            for f in restore_paths:
                shutil.move(backup_dir / f.name, f.resolve())

        # Main logic:
        if not validate_partitioning_request():
            return

        id_files_to_partition = get_files_to_partition()
        existing_files = self.partitioning.files if self.partitioning else None
        tempdir_obj, backupdir, partitioning_succeeded = None, None, False

        try:
            if existing_files:
                tempdir_obj, backupdir = backup_existing_partitioned_files(
                    existing_files
                )

            new_files = partition_files(id_files_to_partition)
            self._update_partitioning_attribute(
                parted_files=new_files, new_np_xi=np_xi, new_np_eta=np_eta
            )
            partitioning_succeeded = True
        finally:
            if (existing_files) and (not partitioning_succeeded) and (backupdir):
                self.log.error("Partitioning failed - restoring previous files")
                restore_existing_partitioned_files(backupdir, existing_files)
            if tempdir_obj:
                tempdir_obj.cleanup()

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
        elif self.source_partitioning is not None:
            self._get_from_partitioned_source(
                local_dir=local_dir,
                source_np_xi=self.source_partitioning[0],
                source_np_eta=self.source_partitioning[1],
            )
        else:
            super().get(local_dir=local_dir)

    def _get_from_partitioned_source(
        self, local_dir: Path, source_np_xi: int, source_np_eta: int
    ) -> None:
        n_source_partitions = source_np_xi * source_np_eta
        ndigits = len(str(n_source_partitions))
        parted_files: list[Path] = []

        for i in range(n_source_partitions):
            old_suffix = f".{0:0{ndigits}d}.nc"
            new_suffix = f".{i:0{ndigits}d}.nc"
            source = self.source.location.replace(old_suffix, new_suffix)
            source_basename = self.source.basename.replace(old_suffix, new_suffix)

            self._symlink_or_download_from_source(
                source_location=source,
                location_type=self.source.location_type,
                expected_file_hash=None,
                target_path=local_dir / source_basename,
                logger=self.log,
            )

            parted_files.append(local_dir / source_basename)

        self._update_partitioning_attribute(
            new_np_xi=source_np_xi, new_np_eta=source_np_eta, parted_files=parted_files
        )
        self.working_path = parted_files
        assert self.partitioning is not None
        self._local_file_stat_cache.update(self.partitioning._local_file_stat_cache)
        self._local_file_hash_cache.update(self.partitioning._local_file_hash_cache)

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
            f"{local_dir / Path(self.source.location).stem}.nc"
        )

        savepath = roms_tools_class_instance.save(**save_kwargs)
        self.working_path = savepath[0] if len(savepath) == 1 else savepath

        self._local_file_hash_cache.update(
            {path: _get_sha256_hash(path.resolve()) for path in savepath}
        )  # 27
        self._local_file_stat_cache.update({path: path.stat() for path in savepath})

    def _update_partitioning_attribute(
        self, new_np_xi: int, new_np_eta: int, parted_files: list[Path]
    ):
        self.partitioning = ROMSPartitioning(
            np_xi=new_np_xi, np_eta=new_np_eta, files=parted_files
        )

        self.partitioning._local_file_hash_cache = {
            path: _get_sha256_hash(path.resolve()) for path in parted_files
        }  # 27
        self.partitioning._local_file_stat_cache = {
            path: path.stat() for path in parted_files
        }

    @property
    def path_for_roms(self) -> list[Path]:
        """Returns a list of Paths corresponding to this ROMSInputDataset that can be
        read by ROMS.

        Useful in the case of partitioned
        source files, where the `working_path` is a list of partitioned files
        e.g., `my_grid.0.nc`, `my_grid.1.nc`, etc., but ROMS
        expects to see `my_grid.nc`
        """
        if self.partitioning is not None:
            ndigits = len(str(self.partitioning.np_xi * self.partitioning.np_eta))
            zero_str = "." + "0" * ndigits + ".nc"
            zero_files = [f for f in self.partitioning.files if zero_str in str(f)]
            return [Path(str(f).replace(zero_str, ".nc")) for f in zero_files]

        raise FileNotFoundError(
            "ROMS requires files to be partitioned for use. "
            "Call ROMSInputDataset.partition() or ROMSSimulation.pre_run() "
            "and try again"
        )


class ROMSModelGrid(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model grid files."""

    pass


class ROMSInitialConditions(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model initial condition
    files.
    """

    pass


class ROMSTidalForcing(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model tidal forcing files."""

    pass


class ROMSBoundaryForcing(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model boundary condition
    files.
    """

    pass


class ROMSSurfaceForcing(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for model surface forcing
    files.
    """

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
