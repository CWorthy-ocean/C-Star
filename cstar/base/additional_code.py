from collections.abc import Iterable
from pathlib import Path

from cstar.base.log import LoggingMixin
from cstar.io.source_data import SourceDataCollection
from cstar.io.staged_data import StagedDataCollection


class AdditionalCode(LoggingMixin):
    """Additional code contributing to a model simulation.

    Additional code is assumed to be kept in a single directory or
    subdirectory of a repository (described by the `source` attribute).

    Attributes:
    -----------
    source: SourceData
        Describes the location of and classifies the source data
    working_copy: Path, default None
        The local path to the additional code. Set when `get()` method is called.
    exists_locally: bool
        True if this AdditionalCode has been staged for use locally

    Methods:
    --------
    get(local_dir):
        Stage this InputDataset for local use by C-Star
    """

    files: Iterable[str]

    def __init__(
        self,
        location: str,
        subdir: str = "",
        checkout_target: str = "",
        files: Iterable[str] = (),
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
            Used if the source is a remote repository. A tag, git hash, or other target to check out.
        files: Optional, list of strs
            Path(s) to the additional code files relative to the subdirectory `subdir` of `location`

        Returns:
        --------
        AdditionalCode
            An initialized AdditionalCode object
        """
        self.source = SourceDataCollection.from_common_location(
            common_location=location,
            subdir=subdir,
            checkout_target=checkout_target,
            files=files,
        )

        self._constructor_args = {
            "location": location,
            "subdir": subdir,
            "checkout_target": checkout_target,
            "files": files,
        }
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
        for k, v in self._constructor_args.items():
            repr_str += f"\n{k}={v},"
        repr_str = repr_str[:-2] + ")"

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
        """The staged, local version of this AdditionalCode (if available).

        Set by AdditionalCode.get()
        """
        return self._working_copy

    @property
    def exists_locally(self) -> bool:
        """Determine whether an unmodified local working copy of the AdditionalCode is available"""
        if self.working_copy and not self.working_copy.changed_from_source:
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

    def to_dict(self) -> dict:
        return self._constructor_args
