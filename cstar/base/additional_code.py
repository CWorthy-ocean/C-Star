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

    Additional code is assumed to be kept in a git-controlled repository (`source_repo`), and obtaining the code
    is handled by git commands.

    Attributes:
    -----------
    base_model: BaseModel
        The base model with which this additional code is associated
    source_repo: str
        URL pointing to a git-controlled repository containing the additional code
    checkout_target: str
        A tag, git hash, or other target to check out the source repo at the correct point in its history
    source_mods: str or list of strs
        Path(s) from the top level of `source_repo` to any code that is needed to compile a unique instance of the base model
    namelists: str or list of strs
        Path(s) from the top level of `source_repo` to any code that is needed at runtime for the base model
    exists_locally: bool, default None
        True if the additional code has been fetched to the local machine, set when `check_exists_locally()` method is called

    local_path: str, default None
        The path to where the additional code has been fetched locally, set when the `get()` method is called

    Methods:
    --------
    get(local_path):
       Clone the `source_repo` repository to a temporary directory, checkout `checkout_target`,
       and move files associated with this AdditionalCode instance to `local_path`.
    check_exists_locally(local_path):
       Verify whether the files associated with this AdditionalCode instance can be found at `local_path`
    """

    def __init__(
        self,
        base_model: BaseModel,
        source: DataSource,
        checkout_target: Optional[str] = None,
        source_mods: Optional[List[str]] = None,
        namelists: Optional[List[str]] = None,
    ):
        """
        Initialize an AdditionalCode object from a repository URL and a list of code files

        Parameters:
        -----------
        base_model: BaseModel
            The base model with which this additional code is associated
        source_repo: str
            URL pointing to a git-controlled repository containing the additional code
        checkout_target: str
            A tag, git hash, or other target to check out the source repo at the correct point in its history
        source_mods: str or list of strs
            Path(s) from the top level of `source_repo` to any code that is needed to compile a unique instance of the base model
        namelists: str or list of strs
            Path(s) from the top level of `source_repo` to any code that is needed at runtime for the base model

        Returns:
        --------
        AdditionalCode
            An initialized AdditionalCode object

        """

        # TODO:  Type check here
        self.base_model: BaseModel = base_model
        self.source: DataSource = source
        self.checkout_target: Optional[str] = checkout_target
        self.source_mods: Optional[List[str]] = source_mods
        self.namelists: Optional[List[str]] = namelists
        self.exists_locally: Optional[bool] = None
        self.local_path: Optional[str] = None

        if self.source.location_type == "path":
            self.exists_locally = True
            self.local_path = self.source.location

    def __str__(self):
        base_str = (
            "AdditionalCode"  # associated with {self.base_model.name} base model"
        )
        base_str += "\n---------------------"
        base_str += f"\nBase model: {self.base_model.name}"
        # FIXME update after sorting all this ish out
        # base_str += f"\nAdditional code repository URL: {self.source_repo} (checkout target: {self.checkout_target})"
        if self.exists_locally is not None:
            base_str += f"\n Exists locally: {self.exists_locally}"
        if self.local_path is not None:
            base_str += f"\n Local path: {self.local_path}"
        if self.source_mods is not None:
            base_str += "\nSource code modification files (paths relative to repository top level):"
            for filename in self.source_mods:
                base_str += f"\n    {filename}"
        if self.namelists is not None:
            base_str += "\nNamelist files (paths relative to repository top level):"
            for filename in self.namelists:
                base_str += f"\n    {filename}"
        return base_str

    def __repr__(self):
        return self.__str__()

    def get(self, local_dir: str):
        """
        Clone `source_repo` into a temporary directory and move required files to `local_dir`.

        This method:
        1. Clones the `source_repo` repository into a temporary directory (deleted after call)
        2. Checks out the `checkout_target` (a tag or commit hash) to move to the correct point in the commit history
        3. Loops over the paths described in `source_mods` and `namelists` and
           moves those files to `local_dir/source_mods/base_model.name/` and `local_dir/namelists/base_model.name`,
           respectively.

        Clone the `source_repo` repository to a temporary directory, checkout `checkout_target`,
        and move files associated with this AdditionalCode instance to `local_dir`.

        Parameters:
        -----------
        local_dir: str
            The local path (typically `Case.caseroot`) where the additional code will be curated
        """

        try:
            tmp_dir = None  # initialise the tmp_dir variable in case we need it later

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
                    + f"{self.source.location_type} and {self.source.source_type}"
                )

            for file_type in ["source_mods", "namelists"]:
                file_list = getattr(self, file_type)

                if file_list is None:
                    continue
                tgt_dir = Path(local_dir) / file_type / self.base_model.name
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
            self.local_path = local_dir
            self.exists_locally = True
        finally:
            if tmp_dir:
                shutil.rmtree(tmp_dir)

    def check_exists_locally(self, local_dir: str) -> bool:
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

        # FIXME: this method, unlike InputDataset.check_exists_locally(), only matches filenames

        for file_type in ["source_mods", "namelists"]:
            file_list = getattr(self, file_type)
            if file_list is None:
                continue

            # tgt_dir = local_dir + "/" + file_type + "/" + self.base_model.name
            tgt_dir = Path(local_dir) / file_type / self.base_model.name
            for f in file_list:
                # tgt_file_path = tgt_dir + "/" + os.path.basename(f)
                tgt_file_path = tgt_dir / Path(f).name
                if not tgt_file_path.exists():
                    self.exists_locally = False
                    return False

        if not self.exists_locally:
            self.local_path = local_dir
            self.exists_locally = True
        return True
