import os
import shutil
import tempfile
import subprocess
from typing import Optional,Union, List
from .cstar_base_model import BaseModel


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
    local_path: str, default None
        The path to where the additional code has been fetched locally, set when the `get()` method is called

    Methods:
    --------
    get(local_path):
       Clone the `source_repo` repository to a temporary directory, checkout `checkout_target`,
       and move files associated with this AdditionalCode instance to `local_path`.
    
    """

    def __init__(self,  base_model: BaseModel,
                       source_repo: str,
                   checkout_target: str,
                       source_mods: Optional[Union[str, List[str]]] = None,
                         namelists: Optional[Union[str, List[str]]] = None
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
                                             
        #TODO:  Type check here
        self.base_model         = base_model
        self.source_repo        = source_repo
        self.checkout_target    = checkout_target
        self.source_mods        = source_mods
        self.namelists          = namelists
        self.local_path         = None
                                             
    def get(self, local_path):
        """
        Clone `source_repo` into a temporary directory and move required files to `local_path`.

        This method:
        1. Clones the `source_repo` repository into a temporary directory (deleted after call)
        2. Checks out the `checkout_target` (a tag or commit hash) to move to the correct point in the commit history
        3. Loops over the paths described in `source_mods` and `namelists` and
           moves those files to `local_path/source_mods/base_model.name/` and `local_path/namelists/base_model.name`,
           respectively.
        
        Clone the `source_repo` repository to a temporary directory, checkout `checkout_target`,
        and move files associated with this AdditionalCode instance to `local_path`.

        Parameters:
        -----------
        local_path: str
            The local path (typically `Case.caseroot`) where the additional code will be curated
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            print(f'cloning {self.source_repo} into temporary directory {tmp_dir}')
            subprocess.run(f"git clone {self.source_repo} {tmp_dir}",check=True,shell=True)
            subprocess.run(f"git checkout {self.checkout_target}",cwd=tmp_dir,shell=True)
            # TODO if checkout fails, this should fail
            
            for file_type in ['source_mods','namelists']:
                file_list = getattr(self,file_type)
                if file_list is None: continue
                tgt_dir=local_path+'/'+file_type+'/'+self.base_model.name
                os.makedirs(tgt_dir,exist_ok=True)
                for f in file_list:
                    tmp_file_path=tmp_dir+'/'+f
                    tgt_file_path=tgt_dir+'/'+os.path.basename(f)
                    print('moving ' +tmp_file_path+ ' to '+tgt_file_path)
                    if os.path.exists(tmp_file_path):
                        shutil.move(tmp_file_path,tgt_file_path)
                    else:
                        raise FileNotFoundError(f"Error: {tmp_file_path} does not exist.")
        self.local_path=local_path
        
