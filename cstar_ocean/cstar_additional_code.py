import os
import shutil
import tempfile
import subprocess
from typing import Optional
from .cstar_base_model import BaseModel


class AdditionalCode:
    """Additional code contributing to a unique instance of the BaseModel,
    e.g. source code modifications, namelists, etc."""

    def __init__(self,  base_model: BaseModel,
                       source_repo: str,
                   checkout_target: str,
                       source_mods: Optional[list] = None,
                         namelists: Optional[list] = None,
                       run_scripts: Optional[list] = None,
                processing_scripts: Optional[list] = None
                 ):
                   
        # Type check here
        self.base_model         = base_model
        self.source_repo        = source_repo
        self.checkout_target    = checkout_target
        self.source_mods        = source_mods
        self.namelists          = namelists
        self.run_scripts        = run_scripts
        self.processing_scripts = processing_scripts
        
    def get(self, local_path):
        # options:
        # clone into caseroot and be done with it
        # clone to a temporary folder and populate caseroot by copying

        
        # TODO:
        # e.g. git clone roms_marbl_example and distribute files based on tree

        with tempfile.TemporaryDirectory() as tmp_dir:
            print(f'cloning {self.source_repo} into temporary directory {tmp_dir}')
            subprocess.run(f"git clone {self.source_repo} {tmp_dir}",check=True,shell=True)
            subprocess.run(f"git checkout {self.checkout_target}",cwd=tmp_dir,shell=True)
            # TODO if checkout fails, this should fail
            
            for file_type in ['source_mods','namelists','run_scripts','processing_scripts']:
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
        
