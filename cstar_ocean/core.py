import pooch
import numpy as np
import xarray as xr
from dataclasses import dataclass, field
from cftime import datetime #cftime has more robust datetime objects for modelling
from datetime import timedelta #... but no timedelta object
import shutil
import os



class Blueprint:
    def __init__(self, name, components, grid_path, InitialConditions,\
                 BoundaryConditions,SurfaceForcing,code):
        '''
        What defines a "blueprint?"
        - grid (this is already an object in cstar)
        - IBCs & surface forcing (perhaps should be their own objects?)
        - Runtime parameters and settings incl n_timesteps & dt
        '''
        
        #Matt's proposed attributes: registry_data (name,child_blueprint,parent_blueprint),
        #                            components, grid, inputdata (forcing,BCs,ICs),
        #                            model_settings_inputs (e.g. roms.in), run_defaults
        
        self.name = name
        self.components = components
        self.grid = 'grid_string'
        self.inputdata = inputdata
        
        # Matt's proposed methods: create_sandbox,create_instance,clone_instance
        
    def create_instance(self, instance_name, path):
        return Instance(instance_name, self, path)

@dataclass(kw_only=True)
class _input_files:
    source:     pooch.core.Pooch
    grid:       str    
    times:      xr.cftime_range
    timesteps:  np.ndarray

    n_entries:  int = field(init=False)
    start_time: datetime = field(init=False)
    end_time:   datetime = field(init=False)
    start_step: int = field(init=False)
    end_step:   int = field(init=False)
    frequency:  timedelta  = field(init=False)
    n_steps:    int = field(init=False)

    def __post_init__(self):
            
        self.start_step   = self.timesteps[0]
        self.end_step     = self.timesteps[-1]
        self.start_time   = self.times[0]
        self.end_time     = self.times[-1]
        self.n_steps      = self.end_step - self.start_step
        self.n_entries    = len(self.timesteps)
        self.frequency    = (self.end_time - self.start_time)/(self.n_entries-1)
    
    def get(self):
        """Downloads or copies each file from source to the specified path."""

        [self.source.fetch(f) for f in self.source.registry.keys()]
        

@dataclass(kw_only=True)
class InitialConditions(_input_files):

    is_restart: bool = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        # Automatically determine if this is a restart based on timestep
        self.is_restart = self.timesteps[0] > 0 
        
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
    
    
class Instance:
    def __init__(self, name, blueprint, path):
        # Matts proposed attributes: name,blueprint,machine,path_all_instances,path_scratch
        self.name = name
        self.blueprint = blueprint # blueprint object (or None?)
        self.path = path
    # Matt's proposed properties: path_blueprint, path_instance_root,path_run,path_self,
    #                             registry_id, history, status

    # Matt's proposed methods: persist,setup,build,pre_run,run,post_run
    
    def persist(self,path):
        print('Saving this instance to disk')
        
    def setup(self):
        print("configuring this instance on this machine")
        #blueprint.InitialConditions.get()
        #blueprint.BoundaryConditions.get()
        #blueprint.SurfaceForcing.get()

        # Also get source code modifications
        
        
    def build(self):
        print('Compiling the code')

    def run(self):
        print(f"Running the instance using blueprint {self.blueprint.name} on machine {self.machine}")

    def post_run(self):
        print('Carrying out post-run actions')
    

################################################################################
