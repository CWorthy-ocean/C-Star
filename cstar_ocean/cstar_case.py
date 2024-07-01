import yaml
from typing import List, Type, Union

from .cstar_component import Component, MARBLComponent, ROMSComponent, _ComponentKwargs
from .cstar_base_model import MARBLBaseModel, ROMSBaseModel, BaseModel
from .cstar_additional_code import AdditionalCode
from .cstar_input_dataset import (
    InputDataset,
    ModelGrid,
    InitialConditions,
    TidalForcing,
    BoundaryForcing,
    SurfaceForcing,
)


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
    is_from_blueprint: bool
        Whether this Case was instantiated from a blueprint yaml file

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

    def __init__(
        self, components: Union[Component, List[Component]], name: str, caseroot: str
    ):
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

        self.components: Union[Component, List[Component]] = components
        self.caseroot: str = caseroot
        self.name: str = name
        self.is_from_blueprint: bool = False
        self.blueprint: Union[str, None] = None
        self.is_setup: bool = self.check_is_setup()

        # self.is_setup=self.check_is_setup()

    def __str__(self):
        base_str = "------------------"
        base_str += "\nC-Star case object "
        base_str += "\n------------------"

        base_str += f"\nName: {self.name}"
        base_str += f"\nLocal caseroot: {self.caseroot}"
        base_str += f"\nIs setup: {self.is_setup}"
        base_str += "\n"

        if self.is_from_blueprint:
            base_str += "\nThis case was instantiated from the blueprint file:"
            base_str += f"\n   {self.blueprint}"

        base_str += "\n"
        base_str += "\nIt is built from the following Component base models (query using Case.components): "

        for C in self.components:
            base_str += "\n   " + C.base_model.name

        return base_str

    def __repr__(self):
        return self.__str__()

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
        casename = bp_dict["registry_attrs"]["name"]
        components: Union[Component, List[Component]]
        components = []

        for component_info in bp_dict["components"]:
            component_kwargs: _ComponentKwargs = {}

            # Use "ThisComponent" as a reference that is set here and used throughout
            # QUESTION : is this the best way to handle the need for different subclasses here?

            base_model_info = component_info["component"]["base_model"]
            ThisComponent: Type[Component]
            ThisBaseModel: Type[BaseModel]

            match base_model_info["name"].casefold():
                case "roms":
                    ThisComponent = ROMSComponent
                    ThisBaseModel = ROMSBaseModel
                case "marbl":
                    ThisComponent = MARBLComponent
                    ThisBaseModel = MARBLBaseModel

            # Construct the BaseModel instance
            base_model = ThisBaseModel(
                base_model_info["source_repo"], base_model_info["checkout_target"]
            )
            component_kwargs["base_model"] = base_model

            # Get discretization info:
            if "discretization" not in component_info["component"].keys():
                discretization=None
            else:
                discretization_info=component_info['component']['discretization']                
                for key in list(discretization_info.keys()):
                    component_kwargs[key]=discretization_info[key]

            
            # Construct any AdditionalCode instances
            additional_code: Union[AdditionalCode, List[AdditionalCode], None]
            if "additional_code" not in component_info["component"].keys():
                additional_code = None
            else:
                additional_code_list = component_info["component"]["additional_code"]
                additional_code = []

                for additional_code_info in additional_code_list:
                    source_repo = additional_code_info["source_repo"]
                    checkout_target = additional_code_info["checkout_target"]
                    source_mods = (
                        [f for f in additional_code_info["source_mods"]]
                        if "source_mods" in additional_code_info.keys()
                        else None
                    )

                    namelists = (
                        [f for f in additional_code_info["namelists"]]
                        if "namelists" in additional_code_info.keys()
                        else None
                    )

                    # run_scripts  = [f for f in additional_code_info['scripts']['run']] \
                    #        if ('scripts'    in additional_code_info.keys()) and \
                    #           ('run'        in additional_code_info['scripts'].keys()) else None

                    # proc_scripts = [f for f in additional_code_info['scripts']['processing']] \
                    #        if ('scripts'    in additional_code_info.keys()) and \
                    #           ('processing' in additional_code_info['scripts'].keys()) else None

                additional_code.append(
                    AdditionalCode(
                        base_model=base_model,
                        source_repo=source_repo,
                        checkout_target=checkout_target,
                        source_mods=source_mods,
                        namelists=namelists,
                    )
                )

                if len(additional_code) == 1:
                    additional_code = additional_code[0]

                component_kwargs["additional_code"] = additional_code

            # Construct any InputDataset instances:
            input_datasets: Union[InputDataset, List[InputDataset], None]
            if "input_datasets" not in component_info["component"].keys():
                input_datasets = None
            else:
                input_datasets = []
                input_dataset_info = component_info["component"]["input_datasets"]
                # ModelGrid
                if "model_grid" not in input_dataset_info.keys():
                    model_grid = None
                else:
                    model_grid = [
                        ModelGrid(
                            base_model=base_model,
                            source=f["source"],
                            file_hash=f["hash"],
                        )
                        for f in input_dataset_info["model_grid"]["files"]
                    ]
                    input_datasets += model_grid
                # InitialConditions
                if "initial_conditions" not in input_dataset_info.keys():
                    initial_conditions = None
                else:
                    initial_conditions = [
                        InitialConditions(
                            base_model=base_model,
                            source=f["source"],
                            file_hash=f["hash"],
                        )
                        for f in input_dataset_info["initial_conditions"]["files"]
                    ]
                    input_datasets += initial_conditions

                # TidalForcing
                if "tidal_forcing" not in input_dataset_info.keys():
                    tidal_forcing = None
                else:
                    tidal_forcing = [
                        TidalForcing(
                            base_model=base_model,
                            source=f["source"],
                            file_hash=f["hash"],
                        )
                        for f in input_dataset_info["tidal_forcing"]["files"]
                    ]
                    input_datasets += tidal_forcing

                # BoundaryForcing
                if "boundary_forcing" not in input_dataset_info.keys():
                    boundary_forcing = None
                else:
                    boundary_forcing = [
                        BoundaryForcing(
                            base_model=base_model,
                            source=f["source"],
                            file_hash=f["hash"],
                        )
                        for f in input_dataset_info["boundary_forcing"]["files"]
                    ]
                    input_datasets += boundary_forcing

                # SurfaceForcing
                if "surface_forcing" not in input_dataset_info.keys():
                    surface_forcing = None
                else:
                    surface_forcing = [
                        SurfaceForcing(
                            base_model=base_model,
                            source=f["source"],
                            file_hash=f["hash"],
                        )
                        for f in input_dataset_info["surface_forcing"]["files"]
                    ]

                    input_datasets += surface_forcing

                component_kwargs["input_datasets"] = input_datasets

            components.append(ThisComponent(**component_kwargs))

        if len(components) == 1:
            components = components[0]

        caseinstance = cls(components=components, name=casename, caseroot=caseroot)
        caseinstance.is_from_blueprint = True
        caseinstance.blueprint = blueprint

        return caseinstance

    def check_is_setup(self):
        """
        Check whether all code and files necessary to run this case exist in the local `caseroot` folder

        This method is called by Case.__init__() and sets the Case.is_setup attribute.

        The method loops over each Component object makng up the case and
        1. Checks for any issues withe the component's base model (using BaseModel.local_config_status)
        2. Loops over AdditionalCode instances in the component calling AdditionalCode.check_exists_locally(caseroot) on each
        3. Loops over InputDataset instances in the component calling InputDataset.check_exists_locally(caseroot) on each

        Returns:
        --------
        is_setup: bool
            True if all components are correctly set up in the caseroot directory

        """
        for component in self.components:
            if component.base_model.local_config_status != 0:
                # print(f'{component.base_model.name} does not appear to be configured properly.'+\
                #'\nRun Case.setup() or BaseModel.handle_config_status()')
                return False

            # Check AdditionalCode
            if isinstance(component.additional_code, list):
                for ac in component.additional_code:
                    if not ac.check_exists_locally(self.caseroot):
                        return False
            elif isinstance(component.additional_code, AdditionalCode):
                if not component.additional_code.check_exists_locally(self.caseroot):
                    return False

            # Check InputDatasets
            if isinstance(component.input_datasets, list):
                for id in component.input_datasets:
                    if not id.check_exists_locally(self.caseroot):
                        return False
            elif isinstance(component.input_datasets, InputDataset):
                if not component.input_dataset.check_exists_locally(self.caseroot):
                    return False

        return True

    def setup(self):
        """
        Fetch all code and files necessary to run this case in the local `caseroot` folder

        This method loops over each Component object making up the case, and performs three operations
        1. Ensures the component's base model is present in the environment
           and checked out to the correct point in the history by calling component.base_model.handle_config_status()
        2. Retrieves any additional code associated with the component by calling the get() method
           on each AdditionalCode object in component.additional_code. Depending on the nature of the additional code, these
           are saved to `caseroot/namelists/component.name` or `caseroot/source_mods/component.base_model.name`
        3. Fetches any input datasets necessary to run the component by calling the get() method
           on each InputDataset object in component.input_datasets. These are saved to `caseroot`/input_datasets/`component.base_model.name`
        """

        if self.is_setup:
            print(f"This case appears to have already been set up at {self.caseroot}")
            return

        for component in self.components:
            # Check BaseModel
            component.base_model.handle_config_status()

            # Get AdditionalCode
            if isinstance(component.additional_code, list):
                [ac.get(self.caseroot) for ac in component.additional_code]
            elif isinstance(component.additional_code, AdditionalCode):
                component.additional_code.get(self.caseroot)

            # Get InputDatasets
            # tgt_dir=self.caseroot+'/input_datasets/'+component.base_model.name
            if isinstance(component.input_datasets, list):
                [inp.get(self.caseroot) for inp in component.input_datasets]
            elif isinstance(component.input_datasets, InputDataset):
                component.input_dataset.get(self.caseroot)

        self.is_setup = True
        # TODO: Add a marker somewhere to avoid repeating this process, e.g. self.is_setup=True

    def build(self):
        """Compile any necessary additional code associated with this case
        by calling component.build() on each Component object making up this case"""

        for component in self.components:
            component.build()

    def pre_run(self):
        """For each Component associated with this case, execute
        pre-processing actions by calling component.pre_run()"""

        for component in self.components:
            component.pre_run()

    def run(self):
        """Run the case by calling `component.run(caseroot)`
        on the primary component (to which others are coupled)."""

        # Assuming for now that ROMS presence implies it is the master program
        # TODO add more advanced logic for this
        for component in self.components:
            if component.base_model.name == "ROMS":
                component.run(self.caseroot)

    def post_run(self):
        """For each Component associated with this case, execute
        post-processing actions by calling component.post_run()"""

        for component in self.components:
            component.post_run()
