# Still todo list:

import os
import re
import yaml
import pooch
import shutil
import tempfile
import subprocess
from abc import ABC, abstractmethod
from typing import List, Union, Optional
from . import _CSTAR_ROOT, _CSTAR_COMPILER  # ,_CONFIG_FILE


################################################################################
# Methods of use:
def _get_hash_from_checkout_target(repo_url, checkout_target):
    # First check if the checkout target is a 7 or 40 digit hexadecimal string
    is_potential_hash = bool(re.match(r"^[0-9a-f]{7}$", checkout_target)) or bool(
        re.match(r"^[0-9a-f]{40}$", checkout_target)
    )

    # Then try ls-remote to see if there is a match
    # (no match if either invalid target or a valid hash):
    ls_remote = subprocess.run(
        "git ls-remote " + repo_url + " " + checkout_target,
        shell=True,
        capture_output=True,
        text=True,
    ).stdout

    if len(ls_remote) == 0:
        if is_potential_hash:
            # just return the input target assuming a hash, but can't validate
            return checkout_target
        else:
            raise ValueError(
                "supplied checkout_target does not appear "
                + "to be a valid reference for this repository"
            )
    else:
        return ls_remote.split()[0]


################################################################################
# Ingredients that go into a component


class BaseModel(ABC):
    """The model from which this model component is derived,
    incl. source code and commit/tag (e.g. MARBL v0.45.0)"""

    def __init__(self, source_repo=None, checkout_target=None):
        # Type check here
        self.source_repo = (
            source_repo if source_repo is not None else self.default_source_repo
        )
        self.checkout_target = (
            checkout_target
            if checkout_target is not None
            else self.default_checkout_target
        )
        self.checkout_hash = _get_hash_from_checkout_target(
            self.source_repo, self.checkout_target
        )
        self.repo_basename = os.path.basename(self.source_repo).replace(".git", "")

    @property
    @abstractmethod
    def name(self):
        """The name of the base model"""

    @property
    @abstractmethod
    def default_source_repo(self):
        '''Default source repository, defined in subclasses, e.g. https://github.com/marbl-ecosys/MARBL.git'''

    @property
    @abstractmethod
    def default_checkout_target(self):
        """Default checkout target, defined in subclasses, e.g. marblv0.45.0"""

    @property
    @abstractmethod
    def expected_env_var(self):
        """environment variable associated with the base model, e.g. MARBL_ROOT"""

    @abstractmethod
    def _base_model_adjustments(self):
        """If there are any adjustments we need to make to the base model
        after a clean checkout, do them here. For instance, we would like
        to replace the Makefiles that are bundled with ROMS with
        machine-agnostic equivalents"""

    def check(self):
        """Check if we already have the BaseModel installed on this system"""

        # check 1: X_ROOT variable is in user's env
        env_var_exists = self.expected_env_var in os.environ

        # check 2: X_ROOT points to the correct repository
        if env_var_exists:
            local_root = os.environ[self.expected_env_var]
            env_var_repo_remote = subprocess.run(
                f"git -C {local_root} remote get-url origin",
                shell=True,
                capture_output=True,
                text=True,
            ).stdout.replace("\n", "")
            env_var_matches_repo = self.source_repo == env_var_repo_remote

            if not env_var_matches_repo:
                raise EnvironmentError(
                    "System environment variable "
                    + f"'{self.expected_env_var}' points to"
                    + "a github repository whose "
                    + f"remote: \n '{env_var_repo_remote}' \n"
                    + "does not match that expected by C-Star: \n"
                    + f"{self.source_repo}."
                    + "Your environment may be misconfigured."
                )
            else:
                # check 3: local basemodel repo HEAD matches correct checkout hash:
                head_hash = subprocess.run(
                    f"git -C {local_root} rev-parse HEAD",
                    shell=True,
                    capture_output=True,
                    text=True,
                ).stdout.replace("\n", "")
                head_hash_matches_checkout_hash = head_hash == self.checkout_hash
                if head_hash_matches_checkout_hash:
                    print(
                        f"PLACEHOLDER MESSAGE: {self.expected_env_var}"
                        + f"points to the correct repo {self.source_repo}"
                        + f"at the correct hash {self.checkout_hash}. Proceeding"
                    )
                else:
                    print(
                        "############################################################\n"
                        + f"{self.expected_env_var} points to the correct repo "
                        + f"{self.source_repo} but HEAD is at: \n"
                        + f"{head_hash}, rather than the hash associated with "
                        + f"checkout_target {self.checkout_target}:\n"
                        + f"{self.checkout_hash}"
                        + "\n############################################################"
                    )
                    yn = input("Would you like to checkout this target now?")
                    while True:
                        if yn.casefold() in ["y", "yes"]:
                            subprocess.run(
                                f"git -C {local_root} checkout {self.checkout_target}",
                                shell=True,
                            )
                            self._base_model_adjustments()
                        elif yn.casefold() in ["n","no"]:
                            raise EnvironmentError()
                        else:
                            print("invalid selection; enter 'y' or 'n'")
        else:  # env_var_exists False (e.g. ROMS_ROOT not defined)
            ext_dir = _CSTAR_ROOT + "/externals/" + self.repo_basename
            print(
                "#######################################################\n"
                + self.expected_env_var
                + " not found in current environment. \n"
                + "if this is your first time running a C-Star case that "
                + f"uses {self.name}, you will need to set it up.\n"
                + f"It is recommended that you install {self.name} in \n"
                + f"{ext_dir}"
                + "\n#######################################################"
            )
            while True:
                yn = input(
                    "Would you like to do this now? "
                    + "('y', 'n', or 'custom' to install at a custom path)\n"
            )
                if yn.casefold() in ["y", "yes", "ok"]:
                    self.get(ext_dir)
                    break
                elif yn.casefold in ['n','no']:
                    raise EnvironmentError()
                elif yn.casefold() == 'custom':
                    custom_path = input(
                        "Enter custom path for install:\n"
                    )
                    self.get(os.path.abspath(custom_path))
                    break
                else:
                    print("invalid selection; enter 'y','n',or 'custom'")
                


@abstractmethod
def get(self, target):
    """clone the basemodel code to your local machine"""


class ROMSBaseModel(BaseModel):
    @property
    def name(self):
        return "ROMS"

    @property
    def default_source_repo(self):
        return "https://github.com/CESR-lab/ucla-roms.git"

    @property
    def default_checkout_target(self):
        return "main"

    @property
    def expected_env_var(self):
        return "ROMS_ROOT"

    def _base_model_adjustments(self):
        shutil.copytree(
            _CSTAR_ROOT + "/additional_files/ROMS_Makefiles/",
            os.environ[self.expected_env_var],
            dirs_exist_ok=True,
        )

    def get(self, target):
        # Get the REPO and checkout the right version
        subprocess.run(f"git clone {self.source_repo} {target}", shell=True)
        subprocess.run(f"git -C {target} checkout {self.checkout_target}", shell=True)

        # Set environment variables for this session:
        os.environ["ROMS_ROOT"] = target
        os.environ["PATH"] += ":" + target + "/Tools-Roms/"

        # Set the configuration file to be read by __init__.py for future sessions:
        #                            === TODO ===
        # config_file_str=\
        # f'os.environ["ROMS_ROOT"]="{target}"\nos.environ["PATH"]+=":"+\
        # "{target}/Tools-Roms"\n'
        # if not os.path.exists(_CONFIG_FILE):
        # config_file_str='import os\n'+config_file_str
        # with open(_CONFIG_FILE,'w') as f:
        # f.write(config_file_str)

        # Distribute custom makefiles for ROMS
        self._base_model_adjustments()

        # Make things
        subprocess.run(
            f"make nhmg COMPILER={_CSTAR_COMPILER}", cwd=target + "/Work", shell=True
        )
        subprocess.run(
            f"make COMPILER={_CSTAR_COMPILER}", cwd=target + "/Tools-Roms", shell=True
        )


class MARBLBaseModel(BaseModel):
    @property
    def name(self):
        return "MARBL"

    @property
    def default_source_repo(self):
        return "https://github.com/marbl-ecosys/MARBL.git"

    @property
    def default_checkout_target(self):
        return "v0.45.0"

    @property
    def expected_env_var(self):
        return "MARBL_ROOT"

    def _base_model_adjustments(self):
        pass

    def get(self,target):
        # TODO: this is copypasta from the ROMSBaseModel get method
        subprocess.run(f"git clone {self.source_repo} {target}", shell=True)
        subprocess.run(f"git -C {target} checkout {self.checkout_target}", shell=True)

        # Set environment variables for this session:
        os.environ["MARBL_ROOT"] = target

        # Set the configuration file to be read by __init__.py for future sessions:
        #                              ===TODO===
        # config_file_str=f'os.environ["MARBL_ROOT"]="{target}"\n'
        # if not os.path.exists(_CONFIG_FILE):
        #    config_file_str='import os\n'+config_file_str
        # with open(_CONFIG_FILE,'w') as f:
        #        f.write(config_file_str)

        # Make things
        subprocess.run(
            f"make {_CSTAR_COMPILER} USEMPI=TRUE", cwd=target + "/src", shell=True
        )


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
            print(f'cloning {self.source_repo} into temporary directory')
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

        
                    
class InputDataset:
    """Any spatiotemporal data needed by the model.
    For now this will be NetCDF only,
    but we can imagine interfacing with equivalent ROMS Tools classes"""

    def __init__(self, base_model: BaseModel,
                           source: str,
                        file_hash: str):
        
        self.base_model = base_model
        self.source = source
        self.file_hash = file_hash

    def get(self,local_path):
        #NOTE: default timeout was leading to a lot of timeouterrors
        downloader=pooch.HTTPDownloader(timeout=120)
        to_fetch=pooch.create(
            path=local_path,
            base_url=os.path.dirname(self.source),
            registry={os.path.basename(self.source):self.file_hash})

        to_fetch.fetch(os.path.basename(self.source),downloader=downloader)
            

class ModelGrid(InputDataset):
    pass


class InitialConditions(InputDataset):
    pass


class TidalForcing(InputDataset):
    pass


class BoundaryForcing(InputDataset):
    pass


class SurfaceForcing(InputDataset):
    pass


################################################################################


class Component(ABC):
    """A model component of this Case, e.g. ROMS as the ocean physics component"""

    def __init__(
        self,
        base_model: BaseModel,
        discretization:  Optional[dict] = None,
        additional_code: Optional[Union[AdditionalCode, List[AdditionalCode]]] = None,
        input_datasets:  Optional[Union[InputDataset, List[InputDataset]]] = None,
    ):
        # Do Type checking here
        self.base_model = base_model
        self.additional_code = additional_code
        self.input_datasets = input_datasets


    def __repr__(self):
        return (f"Component(base_model={self.base_model},"+\
                     f"additional_code={self.additional_code},"+\
                      f"input_datasets={self.input_datasets}")


    @abstractmethod
    def build(self):
        ''' Build the model component'''
        
    @abstractmethod
    def pre_run(self):
        '''Things to do before running with this component'''
    
class ROMSComponent(Component):

    def __init__(
            self,
        base_model: BaseModel,
        discretization:  Optional[dict] = None,
        additional_code: Optional[Union[AdditionalCode, List[AdditionalCode]]] = None,
        input_datasets:  Optional[Union[InputDataset, List[InputDataset]]] = None,
        nx:        Optional[int] = None,
        ny:        Optional[int] = None,
        n_levels:  Optional[int] = None,
        n_procs_x: Optional[int] = None,
        n_procs_y: Optional[int] = None
        ):
        super().__init__(
            base_model=base_model,
            additional_code=additional_code,
            input_datasets=input_datasets)
        # QUESTION: should all these attrs be passed in as a single "discretization" arg of type dict?
        self.nx=nx
        self.ny=ny
        self.n_levels=n_levels
        self.n_procs_x=n_procs_x
        self.n_procs_y=n_procs_y

    
            
    def build(self,local_path):
        ''' We need here to go to the additional code/source mods and run make'''
        subprocess.run(f'make COMPILER={_CSTAR_COMPILER}',\
                       cwd=local_path+'/source_mods/ROMS',shell=True)

    def pre_run(self,local_path):
        ''' Before running ROMS, we need to run partit on all the netcdf files'''

        
        if self.input_datasets is not None:
            datasets_to_partition=self.input_datasets if isinstance(self.input_datasets,list) else [self.input_datasets,]

            for f in datasets_to_partition:
                fname=os.path.basename(f.source)
                subprocess.run('partit '+str(self.n_procs_x)+' '+str(self.n_procs_y)+' '+fname,
                               cwd=local_path+'/input_datasets/ROMS/',shell=True)

        
        
class MARBLComponent(Component):
    def build(self,local_path):
        print('source code modifications to MARBL are not yet supported')

    def pre_run(self,local_path):
        print('no pre-run actions involving MARBL are currently supported')

class Case:
    """A combination of unique components that make up this Case"""

    def __init__(self, components: List[Component], caseroot: str):
        if not all(isinstance(comp, Component) for comp in components):
            raise TypeError("components must be a list of Component instances")
        self.components = components
        self.caseroot = caseroot
        self.is_setup = False
        
    @classmethod
    def from_blueprint(cls, blueprint: str, caseroot: str):
        with open(blueprint, "r") as file:
            bp_dict = yaml.safe_load(file)
        components = []
        for component_info in bp_dict["components"]:
            component_kwargs={}
            
            # Get base model information and choose corresponding subclasses:
            # QUESTION : is this the best way to handle the need for different subclasses here?
            base_model_info = component_info["component"]["base_model"]

            match base_model_info["name"].casefold():
                case "roms":
                    ThisComponent=ROMSComponent
                    ThisBaseModel=ROMSBaseModel
                case "marbl":
                    ThisComponent=MARBLComponent                    
                    ThisBaseModel=MARBLBaseModel
            
            # Construct the BaseModel instance
            base_model = ThisBaseModel(
                base_model_info["source_repo"],
                base_model_info["checkout_target"]
                )
            component_kwargs['base_model']=base_model

            # Get discretization info:
            if "discretization" not in component_info["component"].keys():
                discretization=None
            else:
                discretization_info=component_info['component']['discretization']                
                for key in list(discretization_info.keys()):
                    component_kwargs[key]=discretization_info[key]
                
            # Construct any AdditionalCode instances
            if "additional_code" not in component_info["component"].keys():
                additional_code=None
            else:
                additional_code_list = component_info['component']['additional_code']
                additional_code=[]
                
                for additional_code_info in additional_code_list:
                    source_repo =              additional_code_info['source_repo']
                    checkout_target =          additional_code_info['checkout_target']
                    source_mods  = [f for f in additional_code_info['source_mods']] \
                           if 'source_mods' in additional_code_info.keys() else None

                    namelists    = [f for f in additional_code_info['namelists']] \
                           if 'namelists'   in additional_code_info.keys() else None

                    run_scripts  = [f for f in additional_code_info['scripts']['run']] \
                           if ('scripts'    in additional_code_info.keys()) and \
                              ('run'        in additional_code_info['scripts'].keys()) else None

                    proc_scripts = [f for f in additional_code_info['scripts']['processing']] \
                           if ('scripts'    in additional_code_info.keys()) and \
                              ('processing' in additional_code_info['scripts'].keys()) else None
                           
                
                additional_code.append(AdditionalCode(\
                                                 base_model      = base_model, \
                                                 source_repo     = source_repo,\
                                                 checkout_target = checkout_target,\
                                                 source_mods     = source_mods,\
                                                 namelists       = namelists,\
                                                 run_scripts     = run_scripts,\
                                          processing_scripts     = proc_scripts))

                if len(additional_code) == 1:
                    additional_code = additional_code[0]

                component_kwargs['additional_code']=additional_code
                
            # Construct any InputDataset instances:
            if 'input_datasets' not in component_info['component'].keys():
                input_datasets=None
            else:
                input_datasets=[]
                input_dataset_info = component_info['component']['input_datasets']
                # ModelGrid
                if "model_grid" not in input_dataset_info.keys():
                    model_grid = None
                else:
                    model_grid = [ModelGrid(base_model=base_model,source=f['source'],file_hash=f['hash'])
                                  for f in input_dataset_info['model_grid']['files']]
                    input_datasets+=model_grid
                # InitialConditions
                if "initial_conditions" not in input_dataset_info.keys():
                    initial_conditions=None
                else:
                    initial_conditions = \
                        [InitialConditions(base_model=base_model,source=f['source'],file_hash=f['hash'])
                         for f in input_dataset_info['initial_conditions']['files']]
                    input_datasets+=initial_conditions
                    
                # TidalForcing
                if "tidal_forcing" not in input_dataset_info.keys():
                    tidal_forcing = None
                else:
                    tidal_forcing = \
                        [TidalForcing(base_model=base_model,source=f['source'],file_hash=f['hash'])
                         for f in input_dataset_info['tidal_forcing']['files']]
                    input_datasets+=tidal_forcing

                # BoundaryForcing
                if "boundary_forcing" not in input_dataset_info.keys():
                    boundary_forcing = None
                else:
                    boundary_forcing = \
                        [BoundaryForcing(base_model=base_model,source=f['source'],file_hash=f['hash'])
                         for f in input_dataset_info['boundary_forcing']['files']]
                    input_datasets+=boundary_forcing

                # SurfaceForcing
                if "surface_forcing" not in input_dataset_info.keys():
                    surface_forcing = None
                else:
                    surface_forcing = \
                        [SurfaceForcing(base_model=base_model,source=f['source'],file_hash=f['hash'])
                         for f in input_dataset_info['surface_forcing']['files']]

                    input_datasets+=surface_forcing

                component_kwargs['input_datasets']=input_datasets
            print(component_kwargs)
            components.append(ThisComponent(**component_kwargs))
            
        if len(components) == 1:
            components = components[0]
            
        return cls(components=components, caseroot=caseroot)
    
    def setup(self):
        print("configuring this Case on this machine")

        for component in self.components:

            # Check BaseModel
            component.base_model.check()

            # Get AdditionalCode
            if isinstance(component.additional_code,list):
                [ac.get(self.caseroot) for ac in component.additional_code]
            elif isinstance(component.additional_code,AdditionalCode):
                component.additional_code.get(self.caseroot)

            #Get InputDatasets
            tgt_dir=self.caseroot+'/input_datasets/'+component.base_model.name            
            if isinstance(component.input_datasets,list):
                [inp.get(tgt_dir) for inp in component.input_datasets]
            elif isinstance(component.input_datasets,InputDataset):
                #os.makedirs(tgt_dir,exist_ok=True) 
                component.input_dataset.get(tgt_dir)

        
            #self.is_setup = True
            # Add a marker somewhere to avoid repeating this process

    def build(self):
        '''build this case on this machine'''
        for component in self.components:
            component.build(local_path=self.caseroot)

    def pre_run(self):
        ''' carry out pre-run actions '''
        for component in self.components:
            component.pre_run(self.caseroot)
        
        
# Example Usage
# config = cs.Case.from_blueprint('example.yaml')


# Steps:
# Most of these classes need a "get" option
# - Source repo on the basemodel should be optional and default to the right repo

#
