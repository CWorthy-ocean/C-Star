import datetime as dt
import shutil
import tempfile
from abc import ABC
from pathlib import Path

import roms_tools

from cstar.base.input_dataset import InputDataset
from cstar.base.utils import _list_to_concise_str, coerce_datetime
from cstar.io.constants import FileEncoding
from cstar.io.source_data import SourceData, SourceDataCollection
from cstar.io.staged_data import StagedDataCollection


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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(np_xi={self.np_xi}, np_eta={self.np_eta}, files={_list_to_concise_str(self.files, pad=43)})"

    def __len__(self):
        return len(self.files)


class ROMSInputDataset(InputDataset, ABC):
    (
        (
            """
    ROMS-specific implementation of `InputDataset` (doc below)

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
        self.start_date = coerce_datetime(start_date) if start_date else None
        self.end_date = coerce_datetime(end_date) if end_date else None

        self.source_np_xi = source_np_xi
        self.source_np_eta = source_np_eta
        self.partitioning: ROMSPartitioning | None = None
        self.source = SourceData(location=location, identifier=file_hash)

        if self.source_partitioning:
            if file_hash:
                raise NotImplementedError(
                    "Cannot use hash verification with partitioned source files"
                )
            n_source_partitions = self.source_np_xi * self.source_np_eta  # type: ignore[operator]
            ndigits = len(str(n_source_partitions))
            locations = []
            for i in range(n_source_partitions):
                old_suffix = f".{0:0{ndigits}d}.nc"
                new_suffix = f".{i:0{ndigits}d}.nc"
                locations.append(location.replace(old_suffix, new_suffix))
            self.partitioned_source = SourceDataCollection.from_locations(locations)
        self._working_copy = None
        self.validate()

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
                    f"local path(s) to InputDataset \n {self._local}, "
                    + "refers to a non-existent file(s)"
                    + "\n call InputDataset.get() and try again."
                )
            return True

        def list_files_to_partition() -> list[Path]:
            """Helper function to obtain a list of files associated with this ROMSInputDataset to partition."""
            if not self.working_copy:
                return []
            if isinstance(self.working_copy, StagedDataCollection):
                # if single InputDataset corresponds to many files, check they're colocated
                if not all(
                    [
                        d.parent == self.working_copy.common_parent
                        for d in self.working_copy.paths
                    ]
                ):
                    raise ValueError(
                        f"A single input dataset exists in multiple directories: {self.working_copy.paths}."
                    )

                # If they are, we want to partition them all in the same place
                id_files_to_partition = self.working_copy.paths

            else:
                id_files_to_partition = [
                    self.working_copy.path,
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

        id_files_to_partition = list_files_to_partition()
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
        """Obtain and stage this ROMSInputDataset, making it locally available to C-Star.

        This method updates the `ROMSInputDataset.working_copy` attribute.

        Parameters:
        -----------
        local_dir (str or Path):
            Directory to save the dataset files.
        """
        # Ensure we're working with a Path object
        local_dir = Path(local_dir).expanduser().resolve()
        local_dir.mkdir(parents=True, exist_ok=True)

        # partitioned source
        if self.source_partitioning:
            self._get_from_partitioned_source(local_dir)

        # regular (netCDF) source
        else:
            super().get(local_dir=local_dir)

    def _get_from_partitioned_source(self, local_dir: Path) -> None:
        """Stages partitioned source files, checking pre-existence individually."""
        # If some (or all) files exist, go through and check which ones (if any) to stage:
        if self.working_copy:
            for i, s in enumerate(self.partitioned_source):
                target_path = local_dir / s.basename
                if self.working_copy and self.working_copy[i].path == target_path:  # type: ignore[index]
                    self.log.info(f"⏭️ {target_path} already exists, skipping.")
                    continue
                else:
                    self._working_copy.append(s.stage(local_dir))  # type: ignore[union-attr]
            return
        # Otherwise stage them all:
        else:
            self._working_copy = self.partitioned_source.stage(local_dir)

    def _update_partitioning_attribute(
        self, new_np_xi: int, new_np_eta: int, parted_files: list[Path]
    ):
        self.partitioning = ROMSPartitioning(
            np_xi=new_np_xi, np_eta=new_np_eta, files=parted_files
        )

    @property
    def path_for_roms(self) -> list[Path]:
        """Returns a list of Paths corresponding to this ROMSInputDataset that can be
        read by ROMS.

        Useful in the case of partitioned
        source files, where the `working_copy` lists individual partitioned files
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
        if self.source._classification.value.file_encoding == FileEncoding.TEXT:
            raise TypeError(
                f"{self.__class__.__name__} cannot be initialized with a source YAML file. "
                "Please provide a direct path or URL to a dataset (e.g., NetCDF)."
            )


class ROMSCdrForcing(ROMSInputDataset):
    """An implementation of the ROMSInputDataset class for CDR forcing files."""

    pass
