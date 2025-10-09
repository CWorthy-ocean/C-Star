from pathlib import Path

from cstar.base.gitutils import git_location_to_raw
from cstar.base.log import LoggingMixin
from cstar.io.constants import SourceClassification
from cstar.io.source_data import SourceDataCollection, _SourceInspector
from cstar.io.staged_data import StagedDataCollection


class AdditionalCode(LoggingMixin):
    """Additional code contributing to a model simulation.

    Additional code is assumed to be kept in a single directory or
    subdirectory of a repository (described by the `source` attribute).

    Attributes:
    -----------
    source: DataSource
        Describes the location and type of source data (e.g. repository,directory)
    subdir: str
        Subdirectory of source.location in which the additional code is kept
        (used if, e.g., source.location is a remote repository)
    checkout_target: Optional, str
        Used if source.source_type is 'repository'.
        A tag, git hash, or other target to check out.
    files: Optional, list of strs
        Path(s) relative to the subdirectory `subdir` of `source.location`
        to the additional code files
    working_path: Path, default None
        The local path to the additional code. Set when `get()` method is called.

    Methods:
    --------
    get(local_dir):
       Fetch the directory containing this additional code and copy it to `local_dir`.
       If source.source_type is 'repository', and source.location_type is 'url',
       clone repository to a temporary directory, checkout `checkout_target`,
       and move files in `subdir` associated with this AdditionalCode instance to `local_dir`.
    check_exists_locally(local_dir):
       Verify whether the files associated with this AdditionalCode instance can be found at `local_dir`
    """

    files: list[str]

    def __init__(
        self,
        location: str,
        subdir: str = "",
        checkout_target: str = "",
        files: list[str] = [],
    ):
        """Initialize an AdditionalCode object from a DataSource  and a list of code
        files.

        Parameters:
        -----------
        location: str
            url or path pointing to the additional code directory or repository, used to set `source` attribute
        subdir: str
           Subdirectory of `location` in which to look for files
           (e.g. if `location` points to a remote repository)
        checkout_target: Optional, str
            Used if source.source_type is 'repository'. A tag, git hash, or other target to check out.
        files: Optional, list of strs
            Path(s) relative to the subdirectory `subdir` of `source.location`
            to the additional code files

        Returns:
        --------
        AdditionalCode
            An initialized AdditionalCode object
        """
        if (
            _SourceInspector(location).classify()
            == SourceClassification.REMOTE_REPOSITORY
        ):
            source = SourceDataCollection.from_locations(
                locations=[
                    git_location_to_raw(location, checkout_target, f, subdir)
                    for f in files
                ]
            )
        else:
            source = SourceDataCollection.from_locations(
                locations=[f"{location}/{subdir}/{f}" for f in files]
            )
        self.source: SourceDataCollection = source
        # Initialize object state
        self._working_copy: StagedDataCollection | None = None

    def __str__(self) -> str:
        base_str = self.__class__.__name__ + "\n"
        base_str += "-" * (len(base_str) - 1)
        base_str += "\nLocations:\n   "
        base_str += "\n   ".join(self.source.locations)
        base_str += f"\nWorking copy: {self.working_copy}"
        base_str += f"\nExists locally: {self.exists_locally}"
        if not self.exists_locally:
            base_str += " (get with AdditionalCode.get())"
        return base_str

    def __repr__(self) -> str:
        # Constructor-style section:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nlocations = {self.source.locations!r},"
        repr_str += "\n)"
        # Additional info:
        info_str = ""
        if self.working_copy is not None:
            info_str += f"working_copy = {self.working_copy},"
            info_str += f"exists_locally = {self.exists_locally}"
        if len(info_str) > 0:
            repr_str += f"\nState: <{info_str}>"
        return repr_str

    @property
    def working_copy(self) -> StagedDataCollection | None:
        return self._working_copy

    @property
    def exists_locally(self) -> bool:
        """Determine whether a local working copy of the AdditionalCode exists at
        self.working_path (bool)
        """
        if (self.working_copy) and not (self.working_copy.changed_from_source):
            return True
        return False

    def get(self, local_dir: str | Path) -> None:
        """Stage the AdditionalCode files to `local_dir`

        Parameters:
        -----------
        local_dir: str | Path
            The local directory to stage the AdditionalCode in
        """
        self._working_copy = self.source.stage(local_dir)
