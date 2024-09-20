import shutil
import tempfile
from typing import Optional, List
from pathlib import Path
from cstar.base.datasource import DataSource
from cstar.base.utils import _clone_and_checkout, _list_to_concise_str


class AdditionalCode:
    """
    Additional code contributing to a model component

    Additional code is assumed to be kept in a single directory or
    subdirectory of a repository (described by the `source` attribute)
    with this structure:

    <additional_code_dir>
        ├ namelists
        |       ├ <namelist_file_1>
        |       |       ...
        |       └ <namelist_file_N>
        └ source_mods
                     ├ <source_code_file_1>
                     |       ...
                     └ <source_code_file_N>

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
    source_mods: Optional, list of strs
        Path(s) relative to the subdirectory `subdir` of `source.location`
        to any code that is needed to compile a unique instance of the model component
    namelists: str or list of strs
        Path(s) relative to the subdirectory `subdir` of `source.location`
        to any code that is needed at runtime for the model component
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

    def __init__(
        self,
        location: str,
        subdir: str = "",
        checkout_target: Optional[str] = None,
        source_mods: Optional[List[str]] = None,
        namelists: Optional[List[str]] = None,
    ):
        """
        Initialize an AdditionalCode object from a DataSource  and a list of code files

        Parameters:
        -----------
        location: str
            url or path pointing to the additional code directory or repository, used to set `source` attribute
        subdir: str
           Subdirectory of `location` in which to look for files
           (e.g. if `location` points to a remote repository)
        checkout_target: Optional, str
            Used if source.source_type is 'repository'. A tag, git hash, or other target to check out.
        source_mods: Optional, str or list of strs
            Path(s) relative to the subdirectory `subdir` of `source.location`
            to any code that is needed to compile a unique instance of the model component
        namelists: Optional, str or list of strs
            Path(s) relative to the subdirectory `subdir` of `source.location`
            to any code that is needed at runtime for the model component

        Returns:
        --------
        AdditionalCode
            An initialized AdditionalCode object

        """
        self.source: DataSource = DataSource(location)
        self.subdir: str = subdir
        self.checkout_target: Optional[str] = checkout_target
        self.source_mods: Optional[List[str]] = source_mods
        self.namelists: Optional[List[str]] = namelists
        self.working_path: Optional[Path] = None

        # If there are namelists, make a parallel attribute to keep track of the ones we are editing
        # AdditionalCode.get() determines which namelists are editable templates and updates this list
        if self.namelists:
            self.modified_namelists: list = []

    def __str__(self) -> str:
        base_str = self.__class__.__name__ + "\n"
        base_str += "-" * (len(base_str) - 1)
        base_str += f"\nLocation: {self.source.location}"
        base_str += f"\nsubdirectory: {self.subdir}"
        base_str += f"\nWorking path: {self.working_path}"
        base_str += f"\nExists locally: {self.exists_locally}"
        if not self.exists_locally:
            base_str += " (get with AdditionalCode.get())"
        if self.source_mods is not None:
            base_str += "\nSource code modification files (paths relative to above location and subdirectory)):"
            for filename in self.source_mods:
                base_str += f"\n    {filename}"
        if self.namelists is not None:
            base_str += "\nNamelist files (paths relative to above location):"
            for filename in self.namelists:
                base_str += f"\n    {filename}"
                if filename[-9:] == "_TEMPLATE":
                    base_str += f"      ({filename[:-9]} will be used by C-Star based on this template)"
        return base_str

    def __repr__(self) -> str:
        # Constructor-style section:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nlocation = {self.source.location!r},"
        repr_str += f"\nsubdir = {self.subdir!r}"
        if hasattr(self, "checkout_target"):
            repr_str += f"\ncheckout_target = {self.checkout_target!r},"
        if hasattr(self, "source_mods") and self.source_mods is not None:
            repr_str += "\nsource_mods = " + _list_to_concise_str(self.source_mods)
        if hasattr(self, "namelists") and self.namelists is not None:
            repr_str += "\nnamelists = " + _list_to_concise_str(self.namelists, pad=13)
        repr_str += "\n)"
        # Additional info:
        info_str = ""
        if self.working_path is not None:
            info_str += f"working_path = {self.working_path},"
            info_str += f"exists_locally = {self.exists_locally}"
        if len(info_str) > 0:
            repr_str += f"\nState: <{info_str}>"
        return repr_str

    @property
    def exists_locally(self):
        """
        Determine whether a local working copy of the AdditionalCode exists at self.working_path (bool)
        """
        if self.working_path is None:
            return False

        for file_type in ["source_mods", "namelists"]:
            file_list = getattr(self, file_type)
            for f in file_list:
                if not (self.working_path / f).exists():
                    return False

        return True

    def get(self, local_dir: str | Path) -> None:
        """
        Copy the required AdditionalCode files to `local_dir`

        If AdditionalCode.source describes a remote repository,
        this is cloned into a temporary directory first.

        Parameters:
        -----------
        local_dir: str | Path
            The local directory (typically `Case.caseroot`) in which to fetch the additional code.

        """
        local_dir = Path(local_dir).resolve()
        try:
            tmp_dir = None  # initialise the tmp_dir variable in case we need it later
            # CASE 1: Additional code is in a remote repository:
            if (self.source.location_type == "url") and (
                self.source.source_type == "repository"
            ):
                if self.checkout_target is None:
                    raise ValueError(
                        "AdditionalCode.source points to a repository but AdditionalCode.checkout_target is None"
                    )
                else:
                    assert isinstance(
                        self.checkout_target, str
                    ), "We have just verified checkout_target is not None"
                tmp_dir = tempfile.mkdtemp()
                _clone_and_checkout(
                    source_repo=self.source.location,
                    local_path=tmp_dir,
                    checkout_target=self.checkout_target,
                )
                source_dir = Path(f"{tmp_dir}/{self.subdir}")
            # CASE 2: Additional code is in a local directory/repository
            elif (self.source.location_type == "path") and (
                (self.source.source_type == "directory")
                or (self.source.source_type == "repository")
            ):
                source_dir = Path(self.source.location) / self.subdir

            else:
                raise ValueError(
                    "Invalid source for AdditionalCode. "
                    + "AdditionalCode.source.location_type and "
                    + "AdditionalCode.source.source_type should be "
                    + "'url' and 'repository', or 'path' and 'repository', or"
                    + "'path' and 'directory', not"
                    + f"'{self.source.location_type}' and '{self.source.source_type}'"
                )

            # Now go through the file and copy them to local_dir
            for file_type in ["source_mods", "namelists"]:
                file_list = getattr(self, file_type)

                if file_list is None:
                    continue
                (local_dir / file_type).mkdir(parents=True, exist_ok=True)

                for f in file_list:
                    src_file_path = source_dir / f
                    tgt_file_path = local_dir / file_type / Path(f).name
                    print(
                        f"copying {src_file_path.relative_to(source_dir)} to {tgt_file_path.parent}"
                    )
                    if src_file_path.exists():
                        shutil.copy(src_file_path, tgt_file_path)
                    else:
                        raise FileNotFoundError(
                            f"Error: {src_file_path} does not exist."
                        )
                    # Special case for template namelists:
                    if (
                        file_type == "namelists"
                        and str(src_file_path)[-9:] == "_TEMPLATE"
                    ):
                        print(
                            f"copying {tgt_file_path} to editable namelist {str(tgt_file_path)[:-9]}"
                        )
                        shutil.copy(tgt_file_path, str(tgt_file_path)[:-9])
                        if hasattr(self, "modified_namelists"):
                            self.modified_namelists.append(f[:-9])
                        else:
                            self.modified_namelists = [
                                f[:-9],
                            ]

            self.working_path = local_dir
        finally:
            if tmp_dir:
                shutil.rmtree(tmp_dir)
