import pooch
import numpy as np
import xarray as xr
import subprocess
import importlib.util
from dataclasses import dataclass, field
from cftime import datetime #cftime has more robust datetime objects for modelling
from datetime import timedelta #... but no timedelta object
import shutil
import os

from . import _cstar_root,_cstar_compiler,_config_file

@dataclass(kw_only=True)
class _input_files:

    source:     pooch.core.Pooch
    grid:       str

    ## Others wanted:

    #variables:  dict?
    #times:      xr.cftime_range
    #timesteps:  np.ndarray

    #n_entries:  int = field(init=False)
    #start_time: datetime = field(init=False)
    #end_time:   datetime = field(init=False)
    #start_step: int = field(init=False)
    #end_step:   int = field(init=False)
    #frequency:  timedelta  = field(init=False)
    #n_steps:    int = field(init=False)

    # def __post_init__(self):
            
    #     self.start_step   = self.timesteps[0]
    #     self.end_step     = self.timesteps[-1]
    #     self.start_time   = self.times[0]
    #     self.end_time     = self.times[-1]
    #     self.n_steps      = self.end_step - self.start_step
    #     self.n_entries    = len(self.timesteps)
    #     self.frequency    = (self.end_time - self.start_time)/(self.n_entries-1)
    
    def get(self):
        """Downloads or copies each file from source to the specified path."""

        [self.source.fetch(f) for f in self.source.registry.keys()]
        

@dataclass(kw_only=True)
class InitialConditions(_input_files):

#    is_restart: bool = field(init=False)

    # def __post_init__(self):
    #     super().__post_init__()
    #     # Automatically determine if this is a restart based on timestep
    #     self.is_restart = self.timesteps[0] > 0 
        
    def plot(self):
        '''Plot the initial conditions data.'''
        raise NotImplementedError()

@dataclass(kw_only=True)
class BoundaryConditions(_input_files):

    def plot(self):
        """Plot the boundary conditions data."""
        raise NotImplementedError("This method is not yet implemented.")

@dataclass(kw_only=True)
class SurfaceForcing(_input_files):

    def plot(self):
        '''plot the surface forcing data'''
        raise NotImplementedError("This method is not yet implemented.")

class ModelCode:
    ''' Source code modifications, namelists, etc.'''
    def __init__(self,source_repo,checkout_target=None,target_path='.',retrieval_commands=None):
        self.source_repo = source_repo
        self.checkout_target = checkout_target #either a commit hash or a tag
        self.target_path = target_path
        self.retrieval_commands = retrieval_commands
        
        if retrieval_commands is None:
            retrieval_commands=[f"git clone {self.source_repo} {self.target_path}",]
            if checkout_target is not None:
                retrieval_commands.append([\
                                f"cd {self.target_path}",\
                                f"git checkout {self.checkout_target}" ])
        else:
            # Reformat commands that use, e.g. {self.source_repo} by replacing those placeholders
            # with the actual values attributed to the class:
            self.retrieval_commands=[cmd.format(self=self) for cmd in self.retrieval_commands]
                
                            

    def get(self):
        # See /Users/dafyddstephenson/Code/tmp/rme_tmp/checkout_code_only.sh for commands to run
        # with subprocess
        for c in self.retrieval_commands:
            subprocess.run(c,shell=True)
        
class Component:
    '''C-Star components, e.g. ROMS'''
    def __init__(self,component_name,checkout_target=None,source_repo=None):
        self.component_name = component_name
        self.checkout_target = checkout_target
        self.source_repo = source_repo

        if self.source_repo is None:
            match self.component_name:
                case "ROMS":
                    self.source_repo='https://github.com/CESR-lab/ucla-roms.git'
                case "MARBL":
                    self.source_repo='https://github.com/marbl-ecosys/MARBL.git'

        if self.checkout_target is None:
            self.checkout_target='main'

    @property
    def expected_env_var(self):
        match self.component_name:
            case "ROMS":
                return "ROMS_ROOT"
            case "MARBL":
                return "MARBL_ROOT"
    @property
    def local_root(self):
        return os.environ[self.expected_env_var]
            

    def check(self):
        '''Check if we already have the component on this system'''
                   
        #Check 1: Check the X_ROOT variable is in the environment
        env_var_exists=self.expected_env_var in os.environ

        # Check 2: X_ROOT points to the correct repository
        if env_var_exists:
            # Check X_ROOT env var points to the right repo...
            env_var_repo_remote=subprocess.run(f"git -C "+os.environ[self.expected_env_var]+\
                                              " remote get-url origin",\
                            shell=True,capture_output=True,text=True).stdout.replace('\n','')
            # <TODO: catch errors from this subprocess>
            
            env_var_matches_repo=(self.source_repo==env_var_repo_remote)
            # ... and fail if it doesn't:
            if env_var_matches_repo:
                print(f"PLACEHOLDER MESSAGE: {self.expected_env_var} points to the correct repo {self.source_repo}. Proceeding")
                #TODO expand this                
            else:
                raise EnvironmentError(f"System environment variable '{self.expected_env_var}' points to a github repository whose "+\
                                       f"remote: \n '{env_var_repo_remote}' \n does not match that expected by C-Star: \n"+\
                                       f"{self.source_repo}. Your environment may be misconfigured.")

        else: #env_var_exists False, i.e. X_ROOT not defined 
            ext_dir=cstar_root+'/externals/'
            print(self.expected_env_var+" not found in current environment. " +\
                  "if this is your first time running a C-Star instance that " +\
                  f"uses {self.component_name}, you will need to set it up." +\
                  f"It is recommended that you install {self.component_name} in " +\
                  f"{ext_dir}" )
                
            yn=input("Would you like to do this now? (to install at a custom path or quit, enter 'N')")
            if yn.casefold() in ['y','yes','ok']:
                self.get(ext_dir)
            else:
                custom_path=input("Would you like to install somewhere else? (enter path or 'N' to quit)")
            if custom_path.casefold() not in ['n','no']:
                raise EnvironmentError()
            else:
                self.get(custom_path)
            
            # Check expected ROOT directory and offer to install or throw an error if it's there:
            
            
    def get(self,target):
        ''' Get the component'''

        # Get the REPO and checkout the right version
        subprocess.run(f"git clone {self.source_repo} {target}",shell=True)
        subprocess.run(f"git -C {target} checkout {self.checkout_target}",shell=True)

        # Set environment and compile
        match self.component_name:
            case "ROMS":
                # Set environment variables for this session:
                os.environ["ROMS_ROOT"]=target
                os.environ["PATH"     ]+=':'+target+'/Tools-Roms/'
                print(_cstar_root)
                # Set the configuration file to be read by __init__.py for future sessions:
                with open(_config_file,'w') as f:
                    f.write(f'os.environ["ROMS_ROOT"]={target}\n')
                    f.write(f'os.environ["PATH"]+=":"+{target}+"/Tools-Roms"\n')
                    
                # Distribute custom makefiles for ROMS
                subprocess.run(f"rsync -av {_cstar_root}/additional_files/ROMS_Makefiles/ {target}",shell=True)

                # Make things
                

            
    
            
@dataclass(kw_only=True)    
class Blueprint:
    name: str
    components: list
    grid: str # should eventually be a grid object from setup_tools
    initial_conditions: InitialConditions
    boundary_conditions: BoundaryConditions
    surface_forcing: SurfaceForcing
    model_code: ModelCode
    
    def create_instance(self, instance_name, path):
        return Instance(instance_name, self, path)

    
    
class Instance:
    def __init__(self, name, blueprint, path):
        # Matts proposed attributes: name,blueprint,machine,path_all_instances,path_scratch
        self.name = name
        self.blueprint = blueprint # blueprint object (or None?)
        self.path = path
        self.is_setup = False
    # Matt's proposed properties: path_blueprint, path_instance_root,path_run,path_self,
    #                             registry_id, history, status

    # Matt's proposed methods: persist,setup,build,pre_run,run,post_run
    
    def persist(self):
        print('Saving this instance to disk')                        
        
    def setup(self):
        print("configuring this instance on this machine")

        # Check and get components:
        for c in self.blueprint.components:
            c.check() # check() should prompt user to get() if needed

            # checkout the correct version
            subprocess.run(f"git checkout -C {c.local_root} {c.checkout_target}")
            
                
        
        print('Obtaining model source code modifications and namelist files')
        self.blueprint.model_code.get()
        print('Obtaining initial condition data')
        self.blueprint.initial_conditions.get()
        print('Obtaining boundary condition data')
        self.blueprint.boundary_conditions.get()
        print('Obtaining surface forcing data')
        self.blueprint.surface_forcing.get()
        
        self.is_setup = True
        # Also get source code modifications
        
        
    def build(self):
        '''
        
        '''
        print('Compiling the code')

    def run(self):
        print(f"Running the instance using blueprint {self.blueprint.name} on machine {self.machine}")

    def post_run(self):
        print('Carrying out post-run actions')
    

################################################################################
