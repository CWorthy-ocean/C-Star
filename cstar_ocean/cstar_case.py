import yaml
from typing import List

from .cstar_component import Component,MARBLComponent,ROMSComponent
from .cstar_base_model import BaseModel, MARBLBaseModel, ROMSBaseModel
from .cstar_additional_code import AdditionalCode
from .cstar_input_dataset import InputDataset,ModelGrid,InitialConditions,TidalForcing,BoundaryForcing,SurfaceForcing


class Case:
    """
    A unique combination of Components that defines a C-Star simulation.

    Attributes
    ---------
    components: Component or list of Components
        The unique model component(s) that make up this case
    name: str
        The name of this case
    caseroot: str
        The local directory in which this case will be set up

    Methods
    -------
    from_blueprint(blueprint,caseroot)
        Instantiate a Case from a "blueprint" yaml file
    
    setup()
        Fetch all code and files necessary to run this case in the local caseroot folder
    build()
        Compile any case-specific code on this machine
    pre_run()
        Execute any pre-processing actions necessary to run this case
    run()
        Run this case
    post_run()
        Execute any post-processing actions associated with this case
    
    """

    def __init__(self, components: List[Component],
                           name: str,
                       caseroot: str):

        """
        Initialize a Case object manually from components, name, and caseroot path.

        Parameters:
        ----------
        components: Component or list of Components
            The unique model component(s) that make up this case        
        name: str
            The name of this case
        caseroot: str
            The local directory in which this case will be set up

        Returns:
        -------
        Case
            An initialized Case object
        """
        
        if not all(isinstance(comp, Component) for comp in components):
            raise TypeError("components must be a list of Component instances")
        self.components = components
        self.caseroot = caseroot
        self.name = name
        
    @classmethod
    def from_blueprint(cls, blueprint: str, caseroot: str):
        """
        Initialize a Case object from a blueprint.

        This method reads a YAML file containing the blueprint for a case
        and initializes a Case object based on the provided specifications.        

        A blueprint YAML file should be structured as follows:

        - registry_attrs: overall case metadata, including "name"
        - components: A list of components, containing, e.g.
            - base_model: containing ["name","source_repo",and "checkout_target"]
            - additional_code: optional, containing ["source_repo","checkout_target","source_mods","namelists"]
            - input_datasets: with options like "model_grid","initial_conditions","tidal_forcing","boundary_forcing","surface_forcing"
                              each containing "source" (a URL) and "hash" (a SHA-256 sum)
            - discretization: containing e.g. grid "nx","ny","n_levels" and parallelization "n_procs_x","n_procs_y" information
        
        The blueprint MUST contain a name and at least one component with a base_model
        
        Parameters:
        -----------
        blueprint: str
            Path to a yaml file containing the blueprint for the case
        caseroot: str
            Path to the local directory where the case will be curated and run

        Returns:
        --------
        Case
            An initalized Case object based on the provided blueprint

        """
        
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

                    # run_scripts  = [f for f in additional_code_info['scripts']['run']] \
                    #        if ('scripts'    in additional_code_info.keys()) and \
                    #           ('run'        in additional_code_info['scripts'].keys()) else None

                    # proc_scripts = [f for f in additional_code_info['scripts']['processing']] \
                    #        if ('scripts'    in additional_code_info.keys()) and \
                    #           ('processing' in additional_code_info['scripts'].keys()) else None
                           
                
                additional_code.append(AdditionalCode(\
                                                 base_model      = base_model, \
                                                 source_repo     = source_repo,\
                                                 checkout_target = checkout_target,\
                                                 source_mods     = source_mods,\
                                                 namelists       = namelists))

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
        """
        Fetch all code and files necessary to run this case in the local `caseroot` folder

        This method loops over each Component object making up the case, and performs three operations
        1. Checks the component's base model is present in the environment
           and checked out to the correct point in the history by calling component.base_model.check()
        2. Retrieves any additional code associated with the component by calling the get() method
           on each AdditionalCode object in component.additional_code. Depending on the nature of the additional code, these
           are saved to `caseroot/namelists/component.name` or `caseroot/source_mods/component.base_model.name`
        3. Fetches any input datasets necessary to run the component by calling the get() method
           on each InputDataset object in component.input_datasets. These are saved to `caseroot`/input_datasets/`component.base_model.name`
        """
        

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

        
        #TODO: Add a marker somewhere to avoid repeating this process, e.g. self.is_setup=True

    def build(self):
        """Compile any necessary additional code associated with this case
        by calling component.build() on each Component object making up this case"""
        
        for component in self.components:
            component.build()

    def pre_run(self):
        """For each Component associated with this case, execute
        pre-processing actions by calling component.pre_run()"""
        
        for component in self.components:
            component.pre_run(self.caseroot)
        

    def run(self):
        """Run the case by calling `component.run(caseroot)`
        on the primary component (to which others are coupled)."""

        # Assuming for now that ROMS presence implies it is the master program
        # TODO add more advanced logic for this
        for component in self.components:
            if component.base_model.name=='ROMS':
                component.run(self.caseroot)

                
    def post_run(self):
        """For each Component associated with this case, execute
        post-processing actions by calling component.post_run()"""
        
        for component in self.components:
            component.post_run()
        
