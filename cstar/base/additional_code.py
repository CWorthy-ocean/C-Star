import shutil
import tempfile
from typing import Optional, List
from pathlib import Path
from cstar.base.datasource import DataSource
from cstar.base.base_model import BaseModel
from cstar.base.utils import _clone_and_checkout


class AdditionalCode:
    """
    Additional code contributing to a unique instance of a base model, e.g. namelists, source modifications, etc.

    Additional code is assumed to be kept in a single directory or repository (described by the `source` attribute)
    with this structure:

    <additional_code_dir>
       ├── namelists
       |      └ <base_model_name>
       |              ├ <namelist_file_1>
       |              |       ...
       |              └ <namelist_file_N>
       └── source_mods
              └ <base_model_name>
                      ├ <source_code_file_1>
                      |       ...
                      └ <source_code_file_N>

    Attributes:
    -----------
    base_model: BaseModel
        The base model with which this additional code is associated
    source: DataSource
        Describes the location and type of source data (e.g. repository,directory)
    checkout_target: Optional, str
        Used if source.source_type is 'repository'. A tag, git hash, or other target to check out.
    source_mods: Optional, list of strs
        Path(s) relative to the top level of `source.location` to any code that is needed to compile a unique instance of the base model
    namelists: str or list of strs
        Path(s) relative to the top level of `source.location` to any code that is needed at runtime for the base model
    exists_locally: bool, default None
        Set to True if source.location_type is 'path', or if AdditionalCode.get() has been called.
        Is also set by the `check_exists_locally()` method.
    local_path: Path, default None
        The local path to the additional code. Set when `get()` method is called, or if source.location_type is 'path'.

    Methods:
    --------
    get(local_dir):
       Fetch the directory containing this additional code and copy it to `local_dir`.
       If source.source_type is 'repository', and source.location_type is 'url',
       clone repository to a temporary directory, checkout `checkout_target`,
       and move files associated with this AdditionalCode instance to `local_dir`.
    check_exists_locally(local_dir):
       Verify whether the files associated with this AdditionalCode instance can be found at `local_dir`
    """

    def __init__(
        self,
        base_model: BaseModel,
        location: str,
        checkout_target: Optional[str] = None,
        source_mods: Optional[List[str]] = None,
        namelists: Optional[List[str]] = None,
    ):
        """
        Initialize an AdditionalCode object from a DataSource  and a list of code files

        Parameters:
        -----------
        base_model: BaseModel
            The base model with which this additional code is associated
        location: str
            url or path pointing to the additional code directory or repository, used to set `source` attribute
        checkout_target: Optional, str
            Used if source.source_type is 'repository'. A tag, git hash, or other target to check out.
        source_mods: Optional, str or list of strs
            Path(s) relative to the top level of `source.location` to any code that is needed to compile a unique instance of the base model
        namelists: Optional, str or list of strs
            Path(s) relative to the top level of `source.location` to any code that is needed at runtime for the base model

        Returns:
        --------
        AdditionalCode
            An initialized AdditionalCode object

        """

        self.base_model: BaseModel = base_model
        self.source: DataSource = DataSource(location)
        self.checkout_target: Optional[str] = checkout_target
        self.source_mods: Optional[List[str]] = source_mods
        self.namelists: Optional[List[str]] = namelists
        self.exists_locally: Optional[bool] = None
        self.local_path: Optional[Path] = None

        # If there are namelists, make a parallel attribute to keep track of the ones we are editing
        # AdditionalCode.get() determines which namelists are editable templates and updates this list
        if self.namelists:
            self.modified_namelists: list = []

        if self.source.location_type == "path":
            self.exists_locally = True
            self.local_path = Path(self.source.location).resolve()

    def __str__(self) -> str:
        base_str = (
            "AdditionalCode"  # associated with {self.base_model.name} base model"
        )
        base_str += "\n---------------------"
        base_str += f"\nBase model: {self.base_model.name}"
        base_str += f"\nLocation: {self.source.location}"
        if self.exists_locally is not None:
            base_str += f"\n Exists locally: {self.exists_locally}"
        if self.local_path is not None:
            base_str += f"\n Local path: {self.local_path}"
        if self.source_mods is not None:
            base_str += (
                "\nSource code modification files (paths relative to above location)):"
            )
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
        repr_str=f"{self.__class__.__name__}"
        repr_str+=f"\nbase_model = ({self.base_model.__class__.__name__} instance),"
        repr_str+=f"\nlocation = {self.source.location},"
        if hasattr(self,"checkout_target"):
            repr_str+=f"\ncheckout_target = {self.checkout_target}"
        if hasattr(self,"source_mods"):
            repr_str+=f"\nsource_mods = {self.source_mods},"
        if hasattr(self,"namelists"):
            repr_str+=f"\nnamelists = {self.namelists},"
        repr_str+="\n)"
        return repr_str

    def get(self, local_dir: str | Path) -> None:
        """
        Copy the required AdditionalCode files to `local_dir`

        If AdditionalCode.source describes a remote repository, this is cloned into a temporary directory first.

        Parameters:
        -----------
        local_dir: str
            The local path (typically `Case.caseroot`) where the additional code will be curated
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
                source_dir = Path(tmp_dir)
            # CASE 2: Additional code is in a local directory/repository
            elif (self.source.location_type == "path") and (
                (self.source.source_type == "directory")
                or (self.source.source_type == "repository")
            ):
                source_dir = Path(self.source.location)

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
                tgt_dir = local_dir / file_type / self.base_model.name
                tgt_dir.mkdir(parents=True, exist_ok=True)

                for f in file_list:
                    src_file_path = source_dir / f
                    tgt_file_path = tgt_dir / Path(f).name
                    print(f"copying {src_file_path} to {tgt_file_path}")
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

            self.local_path = local_dir
            self.exists_locally = True
        finally:
            if tmp_dir:
                shutil.rmtree(tmp_dir)

    def check_exists_locally(self, local_dir: str | Path) -> bool:
        """
        Checks whether this AdditionalCode  has already been fetched to the local machine

        Behaves similarly to get() but verifies that the actions of get() have been performed.
        Updates the "AdditionalCode.exists_locally" attribute.

        Parameters:
        -----------
        local_dir (str):
            The local path to check for the existence of this additional code

        Returns:
        --------
        exists_locally (bool):
            True if the method has verified the local existence of the additional code
        """
        local_dir = Path(local_dir).resolve()

        # FIXME: this method, unlike InputDataset.check_exists_locally(), only matches filenames

        for file_type in ["source_mods", "namelists"]:
            file_list = getattr(self, file_type)
            if file_list is None:
                continue

            tgt_dir = local_dir / file_type / self.base_model.name
            for f in file_list:
                tgt_file_path = tgt_dir / Path(f).name
                if not tgt_file_path.exists():
                    self.exists_locally = False
                    return False

        if not self.exists_locally:
            self.local_path = local_dir
            self.exists_locally = True
        return True
