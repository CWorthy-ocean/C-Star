# Still todo list:
# Parse yaml input_datasets section
# issues: right now code lacks flexibility for optional Component items from yaml
# e.g. MARBL doesn't have any input_datasets and we don't have logic to act for that

# InputDataset.get() [using pooch]
# AdditionalCode.get() [requires rme repo revamp for logic]

import os
import re
import yaml
import pooch
import shutil
import subprocess
from abc import ABC,abstractmethod
from typing import List,Union,Optional
from . import _CSTAR_ROOT,_CSTAR_COMPILER#,_CONFIG_FILE
################################################################################
# Methods of use:
def _get_hash_from_checkout_target(repo_url,checkout_target):
    # First check if the checkout target is a 7 or 40 digit hexadecimal string
    is_potential_hash= (bool(re.match(r'^[0-9a-f]{7}$',checkout_target)) \
                     or bool(re.match(r'^[0-9a-f]{40}$',checkout_target)))
    
    # Then try ls-remote to see if there is a match
    # (no match if either invalid target or a valid hash):
    ls_remote=subprocess.run('git ls-remote '+repo_url+' '+checkout_target,\
                             shell=True,capture_output=True,text=True).stdout

    if len(ls_remote)==0:
        if is_potential_hash:
            # just return the input target assuming a hash, but can't validate
            return checkout_target 
        else:
            raise ValueError("supplied checkout_target does not appear "+\
                             "to be a valid reference for this repository")
    else:
        return ls_remote.split()[0]


################################################################################
# Ingredients that go into a component

class BaseModel(ABC):
    '''The model from which this model component is derived,
    incl. source code and commit/tag (e.g. MARBL v0.45.0) '''
    def __init__(self, source_repo=None, \
                   checkout_target=None):

        # Type check here
        self.source_repo     = source_repo     if source_repo     is not None \
                                else self.default_source_repo
        self.checkout_target = checkout_target if checkout_target is not None \
                                else self.default_checkout_target
        self.checkout_hash   = _get_hash_from_checkout_target(\
                                                self.source_repo,self.checkout_target)
        self.repo_basename   = os.path.basename(self.source_repo).replace('.git','')

    @property
    @abstractmethod
    def name(self):
        '''The name of the base model'''
        
    @property
    @abstractmethod
    def default_source_repo(self):
        '''Default source repository, defined in subclasses'''

    @property
    @abstractmethod
    def default_checkout_target(self):
        '''Default checkout target, defined in subclasses'''

    @property
    @abstractmethod
    def expected_env_var(self):
        '''X_ROOT environment variable associated with the base model'''

    @abstractmethod
    def _base_model_adjustments(self):
        '''If there are any adjustments we need to make to the base model
        after a clean checkout, do them here. For instance, we would like
        to replace the Makefiles that are bundled with ROMS with
        machine-agnostic equivalents'''

    def check(self):
        '''Check if we already have the BaseModel installed on this system'''

        #check 1: X_ROOT variable is in user's env
        env_var_exists=self.expected_env_var in os.environ

        #check 2: X_ROOT points to the correct repository
        if env_var_exists:
            
            local_root=os.environ[self.expected_env_var]
            env_var_repo_remote=subprocess.run(\
                            f"git -C {local_root} remote get-url origin",shell=True,\
                            capture_output=True,text=True).stdout.replace('\n','')
            env_var_matches_repo=(self.source_repo==env_var_repo_remote)

            if not env_var_matches_repo:
                raise EnvironmentError("System environment variable "+\
                                       f"'{self.expected_env_var}' points to"+\
                                       "a github repository whose "+\
                                       f"remote: \n '{env_var_repo_remote}' \n"+\
                                       "does not match that expected by C-Star: \n"+\
                                       f"{self.source_repo}."+\
                                       "Your environment may be misconfigured.")
            else:
        #check 3: local basemodel repo HEAD matches correct checkout hash:
                head_hash=subprocess.run(\
                            f"git -C {local_root} rev-parse HEAD",shell=True,\
                            capture_output=True,text=True).stdout.replace('\n','')
                head_hash_matches_checkout_hash=(head_hash==self.checkout_hash)
                if head_hash_matches_checkout_hash:
                    print(f"PLACEHOLDER MESSAGE: {self.expected_env_var}"+\
                          f"points to the correct repo {self.source_repo}"+\
                          f"at the correct hash {self.checkout_hash}. Proceeding")
                else:
                    print(f"{self.expected_env_var} points to the correct repo "+\
                          f"{self.source_repo} but HEAD is at: \n"+\
                          f"{head_hash}, rather than the hash associated with "+\
                          f"checkout_target {self.checkout_target}:\n"+\
                          f"{self.checkout_hash}")
                    yn=input("Would you like to checkout this target now?")
                    if yn.casefold() in ['y','yes']:
                        subprocess.run(\
                            f"git -C {local_root} checkout {self.checkout_target}",\
                                       shell=True)
                        self._base_model_adjustments()
                    else:
                        raise EnvironmentError()
        else: #env_var_exists False (e.g. ROMS_ROOT not defined)
            ext_dir=_CSTAR_ROOT+'/externals/'+self.repo_basename
            print(self.expected_env_var+" not found in current environment. " +\
                  "if this is your first time running a C-Star case that " +\
                  f"uses {self.name}, you will need to set it up." +\
                  f"It is recommended that you install {self.name} in " +\
                  f"{ext_dir}" )
            yn=input("Would you like to do this now?"+\
                   "('y', or 'n' to install elsewhere or quit)")
            if yn.casefold() in ['y','yes','ok']:
                self.get(ext_dir)
            else:
                custom_path=input("Would you like to install somewhere else?"+\
                                  "(enter path or 'N' to quit)")
                if custom_path.casefold() in ['n','no']:
                    raise EnvironmentError()
                else:
                    self.get(os.path.abspath(custom_path))

@abstractmethod
def get(self,target):
    '''clone the basemodel code to your local machine'''
        
class ROMSBaseModel(BaseModel):
    @property
    def name(self):
        return 'ROMS'
    @property
    def default_source_repo(self):
        return 'https://github.com/CESR-lab/ucla-roms.git'
    @property
    def default_checkout_target(self):
        return 'main'
    @property
    def expected_env_var(self):
        return 'ROMS_ROOT'
    
    def _base_model_adjustments(self):
        shutil.copytree(\
            _CSTAR_ROOT+"/additional_files/ROMS_Makefiles/",
                        os.environ[self.expected_env_var],\
                        dirs_exist_ok=True)

    def get(self,target):
        # Get the REPO and checkout the right version
        subprocess.run(f"git clone {self.source_repo} {target}",shell=True)
        subprocess.run(f"git -C {target} checkout {self.checkout_target}",shell=True)
        
        # Set environment variables for this session:
        os.environ["ROMS_ROOT"]=target
        os.environ["PATH"     ]+=':'+target+'/Tools-Roms/'
        print(_CSTAR_ROOT)
        
        # Set the configuration file to be read by __init__.py for future sessions:
        #                            === TODO ===
        #config_file_str=\
            #f'os.environ["ROMS_ROOT"]="{target}"\nos.environ["PATH"]+=":"+\
            #"{target}/Tools-Roms"\n'
        #if not os.path.exists(_CONFIG_FILE):
        # config_file_str='import os\n'+config_file_str                    
        #with open(_CONFIG_FILE,'w') as f:
        #f.write(config_file_str)
        
        # Distribute custom makefiles for ROMS
        self._base_model_adjustments()
                
        # Make things
        subprocess.run(f"make nhmg COMPILER={_CSTAR_COMPILER}",\
                       cwd=target+"/Work",shell=True)
        subprocess.run(f"make COMPILER={_CSTAR_COMPILER}",\
                       cwd=target+"/Tools-Roms",shell=True)

        
class MARBLBaseModel(BaseModel):
    @property
    def name(self):
        return 'MARBL'    
    @property
    def default_source_repo(self):
        return 'https://github.com/marbl-ecosys/MARBL.git'                
    @property
    def default_checkout_target(self):
        return 'v0.45.0'
    @property
    def expected_env_var(self):
        return 'MARBL_ROOT'
    
    def _base_model_adjustments(self):
        pass

    def get(self):
        # TODO: this is copypasta from the ROMSBaseModel get method
        subprocess.run(f"git clone {self.source_repo} {target}",shell=True)
        subprocess.run(f"git -C {target} checkout {self.checkout_target}",shell=True)

        # Set environment variables for this session:
        os.environ["MARBL_ROOT"]=target
        print(_CSTAR_ROOT)
                
        # Set the configuration file to be read by __init__.py for future sessions:
        #                              ===TODO===
        #config_file_str=f'os.environ["MARBL_ROOT"]="{target}"\n'
        #if not os.path.exists(_CONFIG_FILE):
        #    config_file_str='import os\n'+config_file_str                    
        #with open(_CONFIG_FILE,'w') as f:
        #        f.write(config_file_str)
                
            # Make things
        subprocess.run(f"make {_CSTAR_COMPILER} USEMPI=TRUE",cwd=target+"/src",shell=True)
        
        
class AdditionalCode:
    '''Additional code contributing to a unique instance of the BaseModel,
    e.g. source code modifications, namelists, etc.'''
    
    def __init__(self, source_repo: str, checkout_target: str):
        # Type check here
        self.source_repo = source_repo
        self.checkout_target = checkout_target

    def get(self,target):
        #TODO:
        # e.g. git clone roms_marbl_example and distribute files based on tree
        pass
        
class InputDataset:
    '''Any spatiotemporal data needed by the model.
    For now this will be NetCDF only,
    but we can imagine interfacing with equivalent ROMS Tools classes'''

    def __init__(self, source: str, file_hash: str):
        self.source = source
        self.file_hash = file_hash

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
    '''A model component of this Case, e.g. ROMS as the ocean physics component'''
    
    def __init__(self, base_model: BaseModel, \
                  additional_code: Optional[Union[AdditionalCode, List[AdditionalCode]]]=None,\
                   input_datasets: Optional[Union[InputDataset  , List[InputDataset]]]=None
                 ):

        # Do Type checking here
        self.base_model = base_model
        self.additional_code = additional_code
        self.input_datasets = input_datasets


class ROMSComponent(Component):    
    pass

class MARBLComponent(Component):
    pass

class Case:
    ''' A combination of unique components that make up this Case'''
    def __init__(self, components: List[Component],caseroot: str):
        if not all(isinstance(comp, Component) for comp in components):
            raise TypeError("components must be a list of Component instances")
        self.components = components
        self.caseroot = caseroot

    @classmethod
    def from_blueprint(cls, blueprint: str, caseroot: str):
        with open(blueprint, 'r') as file:
            bp_dict = yaml.safe_load(file)
            components = []
            for component_info in bp_dict['components']:
                # Construct the BaseModel instance
                base_model_info = component_info['component']['base_model']
                match base_model_info['name'].casefold():
                    case 'roms':
                        base_model=ROMSBaseModel( base_model_info['source_repo'],
                                                  base_model_info['checkout_target'])
                    case 'marbl':
                        base_model=MARBLBaseModel(base_model_info['source_repo'],
                                                  base_model_info['checkout_target'])

                
                # Construct any AdditionalCode instances
                if 'additional_code' in component_info['component'].keys():
                    additional_code = [AdditionalCode(ac['source_repo'], \
                                                  ac['checkout_target'])
                        for ac in component_info['component']['additional_code']]

                    if len(additional_code)==1: additional_code=additional_code[0]
                else:
                    additional_code=None
                
                # Construct any InputDataset instances:
                # if 'input_datasets' in component_info['component'].keys():
                #     input_dataset_info = component_info['component']['input_datasets']
                #     if "model_grid" in input_dataset_info.keys():
                #         model_grid = [ModelGrid(source=f['source'],file_hash=f['hash'])
                #                   for f in input_dataset_info['model_grid']['files']]
                #         if len(model_grid)==1:
                #             model_grid=model_grid[0]
                            
                #     if "initial_conditions" in input_dataset_info.keys():
                #         initial_conditions = \
                #            [InitialConditions(source=f['source'],file_hash=f['hash'])
                #           for f in input_dataset_info['initial_conditions']['files']]
                #         if len(initial_conditions)==1:
                #             initial_conditions=initial_conditions[0]
                            
                #     if "tidal_forcing" in input_dataset_info.keys():
                #         tidal_forcing = \
                #            [TidalForcing(source=f['source'],file_hash=f['hash'])
                #           for f in input_dataset_info['tidal_forcing']['files']]
                #         if len(tidal_forcing)==1:
                #             tidal_forcing=tidal_forcing[0]
                        
                #     if "boundary_forcing" in input_dataset_info.keys():
                #         boundary_forcing = \
                #            [BoundaryForcing(source=f['source'],file_hash=f['hash'])
                #           for f in input_dataset_info['boundary_forcing']['files']]
                #         if len(boundary_forcing)==1:
                #             boundary_forcing=boundary_forcing[0]
                        
                #     if "surface_forcing" in input_dataset_info.keys():
                #         surface_forcing = \
                #            [SurfaceForcing(source=f['source'],file_hash=f['hash'])
                #           for f in input_dataset_info['surface_forcing']['files']]

                #         if len(surface_forcing)==1:
                #             surface_forcing=surface_forcing[0]
                    
                    
                # else:
                #     input_datasets=None
                            
                            
                components.append(Component(base_model, additional_code))   
                #components.append(Component(base_model, additional_code, input_datasets))
                
            if len(components)==1: components=components[0]
            return cls(components=components, caseroot=caseroot)

# Example Usage
#config = cs.Case.from_blueprint('example.yaml')


# Steps:
# Most of these classes need a "get" option
# - Source repo on the basemodel should be optional and default to the right repo

# 
