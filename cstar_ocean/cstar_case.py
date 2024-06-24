import yaml
from typing import List

from .cstar_component import Component,MARBLComponent,ROMSComponent
from .cstar_base_model import BaseModel, MARBLBaseModel, ROMSBaseModel
from .cstar_additional_code import AdditionalCode
from .cstar_input_dataset import InputDataset,ModelGrid,InitialConditions,TidalForcing,BoundaryForcing,SurfaceForcing




class Case:
    """A combination of unique components that make up this Case"""

    def __init__(self, components: List[Component],
                           name: str,
                       caseroot: str):
                 
        if not all(isinstance(comp, Component) for comp in components):
            raise TypeError("components must be a list of Component instances")
        self.components = components
        self.caseroot = caseroot
        self.is_setup = False
        self.name = name
        
    @classmethod
    def from_blueprint(cls, blueprint: str, caseroot: str):
        with open(blueprint, "r") as file:
            bp_dict = yaml.safe_load(file)

        # Primary metadata
        casename=bp_dict["registry_attrs"]["name"]
            
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

            components.append(ThisComponent(**component_kwargs))
            
        if len(components) == 1:
            components = components[0]
            
        return cls(components=components,name=casename,caseroot=caseroot)
    
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
        

    def run(self):
        '''execute the model'''

        # Assuming for now that ROMS presence implies it is the master program
        
        for component in self.components:
            if component.base_model.name=='ROMS':
                component.run(self.caseroot)
            
    def post_run(self):
        ''' carry out pre-run actions '''
        for component in self.components:
            component.post_run()
        
