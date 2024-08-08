import os
import yaml
import warnings
import datetime as dt
import dateutil.parser
from typing import List, Type, Any, Optional

from cstar_ocean.component import Component, MARBLComponent, ROMSComponent
from cstar_ocean.base_model import MARBLBaseModel, ROMSBaseModel, BaseModel
from cstar_ocean.additional_code import AdditionalCode
from cstar_ocean.environment import _CSTAR_SYSTEM_MAX_WALLTIME
from cstar_ocean.input_dataset import (
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
    valid_start_date: str or datetime.datetime, Optional, default=None
        The earliest start date at which this Case is considered valid
    valid_end_date: str or datetime.datetime, Optional, default=None
        The latest end date up to which this Case is considered valid
    start_date: str or datetime, Optional, default=valid_start_date
        The date from which to begin running this Case.
    end_date: str or datetime.datetime, Optional, default=valid_end_date
        The date at which to cease running this Case.

    is_from_blueprint: bool
        Whether this Case was instantiated from a blueprint yaml file

    Methods
    -------
    from_blueprint(blueprint,caseroot)
        Instantiate a Case from a "blueprint" yaml file
    persist(filename)
        Create a "blueprint" yaml file for this Case object
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
        self,
        components: Component | List[Component],
        name: str,
        caseroot: str,
        start_date: Optional[str | dt.datetime] = None,
        end_date: Optional[str | dt.datetime] = None,
        valid_start_date: Optional[str | dt.datetime] = None,
        valid_end_date: Optional[str | dt.datetime] = None,
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

        self.components: Component | List[Component] = components
        self.caseroot: str = os.path.abspath(caseroot)
        self.name: str = name
        self.is_from_blueprint: bool = False
        self.blueprint: Optional[str] = None

        # Make sure valid dates are datetime objects if present:
        if valid_start_date is not None:
            self.valid_start_date: Optional[dt.datetime] = (
                valid_start_date
                if isinstance(valid_start_date, dt.datetime)
                else dateutil.parser.parse(valid_start_date)
            )
        if valid_end_date is not None:
            self.valid_end_date: Optional[dt.datetime] = (
                valid_end_date
                if isinstance(valid_end_date, dt.datetime)
                else dateutil.parser.parse(valid_end_date)
            )
        # Warn user if valid dates are not present:
        if valid_end_date is None or valid_start_date is None:
            warnings.warn(
                "Range of valid dates not provided."
                + " Unable to check if simulation dates are out of range. "
                + "Case objects should be initialized with valid_start_date "
                + "and valid_end_date attributes.",
                RuntimeWarning,
            )

        # Make sure Case start_date is set and is a datetime object:
        if start_date is not None:
            # Set if provided
            self.start_date: Optional[dt.datetime] = (
                start_date
                if isinstance(start_date, dt.datetime)
                else dateutil.parser.parse(start_date)
            )
            # Set to earliest valid date if not provided and warn
        elif valid_start_date is not None:
            self.start_date = self.valid_start_date
            warnings.warn(
                "start_date not provided. "
                + f"Defaulting to earliest valid start date: {valid_start_date}."
            )
        else:
            # Raise error if no way to set
            raise ValueError(
                "Neither start_date nor valid_start_date provided."
                + " Unable to establish a simulation date range"
            )
        assert isinstance(
            self.start_date, dt.datetime
        ), "At this point either the code has failed or start_date is a datetime object"

        # Make sure Case end_date is set and is a datetime object:
        if end_date is not None:
            # Set if provided
            self.end_date: Optional[dt.datetime] = (
                end_date
                if isinstance(end_date, dt.datetime)
                else dateutil.parser.parse(end_date)
            )
        elif valid_end_date is not None:
            # Set to latest valid date if not provided and warn
            self.end_date = self.valid_end_date
            warnings.warn(
                "end_date not provided."
                + f"Defaulting to latest valid end date: {valid_end_date}"
            )

        else:
            # Raise error if no way to set
            raise ValueError(
                "Neither end_date nor valid_end_date provided."
                + " Unable to establish a simulation date range"
            )

        assert isinstance(
            self.end_date, dt.datetime
        ), "At this point either the code has failed or end_date is a datetime object"

        # Check provded dates are valid
        if (self.valid_start_date is not None) and (
            self.start_date < self.valid_start_date
        ):
            raise ValueError(
                f"start_date {self.start_date} is before the earliest valid start date {self.valid_start_date}."
            )
        if (self.valid_end_date is not None) and (self.end_date > self.valid_end_date):
            raise ValueError(
                f"end_date {self.end_date} is after the latest valid end date {self.valid_end_date}."
            )
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date {self.start_date} is after end_date {self.end_date}."
            )
        # Lastly, check if everything is set up
        self.is_setup: bool = self.check_is_setup()

    def __str__(self):
        base_str = "------------------"
        base_str += "\nC-Star case object "
        base_str += "\n------------------"

        base_str += f"\nName: {self.name}"
        base_str += f"\ncaseroot: {self.caseroot}"
        base_str += f"\nstart_date: {self.start_date}"
        base_str += f"\nend_date: {self.end_date}"
        base_str += f"\nIs setup: {self.is_setup}"
        base_str += "\nValid date range:"
        base_str += f"\nvalid_start_date: {self.valid_start_date}"
        base_str += f"\nvalid_end_date: {self.valid_end_date}"
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
    def from_blueprint(
        cls,
        blueprint: str,
        caseroot: str,
        start_date: Optional[str | dt.datetime],
        end_date: Optional[str | dt.datetime],
    ):
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
        start_date: str or datetime, Optional, default=valid_start_date
           The date from which to begin running this Case.
        end_date: str or datetime.datetime, Optional, default=valid_end_date
           The date at which to cease running this Case.

        Returns:
        --------
        Case
            An initalized Case object based on the provided blueprint

        """

        with open(blueprint, "r") as file:
            bp_dict = yaml.safe_load(file)

        # Top-level metadata
        casename = bp_dict["registry_attrs"]["name"]

        valid_start_date: dt.datetime
        valid_end_date: dt.datetime
        valid_start_date = bp_dict["registry_attrs"]["valid_date_range"]["start_date"]
        valid_end_date = bp_dict["registry_attrs"]["valid_date_range"]["end_date"]
        if isinstance(start_date, str):
            start_date = dateutil.parser.parse(start_date)
        if isinstance(end_date, str):
            end_date = dateutil.parser.parse(end_date)

        components: Component | List[Component]
        components = []

        for component_info in bp_dict["components"]:
            component_kwargs: dict[str, Any] = {}

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
                case _:
                    raise ValueError(
                        f'Base model name {base_model_info["name"]} in blueprint '
                        + f"{blueprint}  does not match a value that is supported by C-Star. "
                        + 'Currently supported values are "ROMS" and "MARBL"'
                    )

            # Construct the BaseModel instance
            base_model = ThisBaseModel(
                base_model_info["source_repo"], base_model_info["checkout_target"]
            )
            component_kwargs["base_model"] = base_model

            # Get discretization info:
            if "discretization" not in component_info["component"].keys():
                discretization_info = None
            else:
                discretization_info = component_info["component"]["discretization"]
                for key in list(discretization_info.keys()):
                    component_kwargs[key] = discretization_info[key]

            # Construct any AdditionalCode instances
            additional_code: Optional[AdditionalCode]
            if "additional_code" not in component_info["component"].keys():
                additional_code = None
            else:
                additional_code_info = component_info["component"]["additional_code"]

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

                additional_code = AdditionalCode(
                    base_model=base_model,
                    source_repo=source_repo,
                    checkout_target=checkout_target,
                    source_mods=source_mods,
                    namelists=namelists,
                )

                component_kwargs["additional_code"] = additional_code

            # Construct any InputDataset instances:
            input_datasets: InputDataset | List[InputDataset] | None
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
                            start_date=f["start_date"],
                            end_date=f["end_date"],
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
                            start_date=f["start_date"],
                            end_date=f["end_date"],
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
                            start_date=f["start_date"],
                            end_date=f["end_date"],
                        )
                        for f in input_dataset_info["surface_forcing"]["files"]
                    ]

                    input_datasets += surface_forcing

                component_kwargs["input_datasets"] = input_datasets

            components.append(ThisComponent(**component_kwargs))

        if len(components) == 1:
            components = components[0]

        caseinstance = cls(
            components=components,
            name=casename,
            caseroot=caseroot,
            start_date=start_date,
            end_date=end_date,
            valid_start_date=valid_start_date,
            valid_end_date=valid_end_date,
        )

        caseinstance.is_from_blueprint = True
        caseinstance.blueprint = blueprint

        return caseinstance

    def persist(self, filename: str):
        """
        Write this case to a yaml file.

        This effectively performs the actions of Case.from_blueprint(), but in reverse,
        populating a dictionary from a Case object and its components and their attributes,
        then writing that dictionary to a yaml file.

        Parameters:
        ----------
        filename (str):
            The yaml file created and written to by the method
        """

        bp_dict: dict = {}

        # Add metadata to dictionary
        bp_dict["registry_attrs"] = {"name": self.name}
        if self.valid_start_date is not None:
            bp_dict["registry_attrs"]["valid_date_range"] = {
                "start_date": str(self.valid_start_date)
            }
        if self.valid_end_date is not None:
            bp_dict["registry_attrs"]["valid_date_range"] = {
                "end_date": str(self.valid_end_date)
            }

        bp_dict["components"] = []

        if isinstance(self.components, Component):
            component_list = [
                self.components,
            ]
        elif isinstance(self.components, list):
            component_list = self.components

        for component in component_list:
            component_info: dict = {}
            # This will be bp_dict["components"]["component"]=component_info

            base_model_info: dict = {}
            # This will be component_info["base_model"] = base_model_info
            base_model_info = {}
            base_model_info["name"] = component.base_model.name
            base_model_info["source_repo"] = component.base_model.source_repo
            base_model_info["checkout_target"] = component.base_model.checkout_target

            component_info["base_model"] = base_model_info

            # discretization info (if present)
            discretization_info = {}
            if hasattr(component, "nx"):
                discretization_info["nx"] = component.nx
            if hasattr(component, "ny"):
                discretization_info["ny"] = component.ny
            if hasattr(component, "n_levels"):
                discretization_info["n_levels"] = component.n_levels
            if hasattr(component, "n_procs_x"):
                discretization_info["n_procs_x"] = component.n_procs_x
            if hasattr(component, "n_procs_y"):
                discretization_info["n_procs_y"] = component.n_procs_y
            if hasattr(component, "time_step"):
                discretization_info["time_step"] = component.time_step

            if len(discretization_info) > 0:
                component_info["discretization"] = discretization_info

            # AdditionalCode instances - can also be None
            # Loop over additional code
            additional_code = component.additional_code

            if additional_code is not None:
                additional_code_info: dict = {}
                # This will be component_info["component"]["additional_code"]=additional_code_info
                additional_code_info["source_repo"] = additional_code.source_repo
                additional_code_info["checkout_target"] = (
                    additional_code.checkout_target
                )
                if additional_code.source_mods is not None:
                    additional_code_info["source_mods"] = (
                        additional_code.source_mods
                    )  # this is a list
                if additional_code.namelists is not None:
                    additional_code_info["namelists"] = additional_code.namelists

                component_info["additional_code"] = additional_code_info

            # InputDataset
            input_datasets = component.input_datasets
            if isinstance(input_datasets, InputDataset):
                input_datasets = [
                    input_datasets,
                ]
            if isinstance(input_datasets, list):
                input_dataset_info: dict = {}
                for ind in input_datasets:
                    if isinstance(ind, ModelGrid):
                        dct_key = "model_grid"
                    elif isinstance(ind, InitialConditions):
                        dct_key = "initial_conditions"
                    elif isinstance(ind, TidalForcing):
                        dct_key = "tidal_forcing"
                    elif isinstance(ind, BoundaryForcing):
                        dct_key = "boundary_forcing"
                    elif isinstance(ind, SurfaceForcing):
                        dct_key = "surface_forcing"
                    if dct_key not in input_dataset_info.keys():
                        input_dataset_info[dct_key] = {}
                    if "files" not in input_dataset_info[dct_key].keys():
                        input_dataset_info[dct_key]["files"] = []
                    input_dataset_info[dct_key]["files"].append(
                        {"source": ind.source, "file_hash": ind.file_hash}
                    )

                component_info["input_datasets"] = input_dataset_info

            bp_dict["components"].append({"component": component_info})

        with open(filename, "w") as yaml_file:
            yaml.dump(bp_dict, yaml_file, default_flow_style=False, sort_keys=False)

    def check_is_setup(self) -> bool:
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
        if isinstance(self.components, Component):
            component_list = [
                self.components,
            ]
        elif isinstance(self.components, list):
            component_list = self.components

        for component in component_list:
            if component.base_model.local_config_status != 0:
                return False

            # Check AdditionalCode
            if (component.additional_code is not None) and (
                component.additional_code.check_exists_locally(self.caseroot)
            ):
                return False

            # Check InputDatasets
            if isinstance(component.input_datasets, InputDataset):
                dataset_list = [
                    component.input_datasets,
                ]
            elif isinstance(component.input_datasets, list):
                dataset_list = component.input_datasets
            else:
                dataset_list = []

            for inp in dataset_list:
                if not inp.check_exists_locally(self.caseroot):
                    # If it can't be found locally, check whether it should by matching dataset dates with simulation dates:
                    if (not isinstance(inp.start_date, dt.datetime)) or (
                        not isinstance(inp.end_date, dt.datetime)
                    ):
                        return False
                    elif (not isinstance(self.start_date, dt.datetime)) or (
                        not isinstance(self.end_date, dt.datetime)
                    ):
                        return False
                    elif (inp.start_date <= self.end_date) and (
                        inp.end_date >= self.start_date
                    ):
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
            if isinstance(component.input_datasets, InputDataset):
                dataset_list = [
                    component.input_datasets,
                ]
            elif isinstance(component.input_datasets, list):
                dataset_list = component.input_datasets
            else:
                dataset_list = []

            # Verify dates line up before running .get():
            for inp in dataset_list:
                # Download input dataset if its date range overlaps Case's date range
                if ((inp.start_date is None) or (inp.end_date is None)) or (
                    (inp.start_date <= self.end_date)
                    and (inp.end_date >= self.start_date)
                ):
                    inp.get(self.caseroot)

        self.is_setup = True

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

    def run(
        self,
        account_key=None,
        walltime=_CSTAR_SYSTEM_MAX_WALLTIME,
        job_name="my_case_run",
    ):
        """Run the case by calling `component.run(caseroot)`
        on the primary component (to which others are coupled)."""

        # Assuming for now that ROMS presence implies it is the master program
        # TODO add more advanced logic for this
        # 20240807 - TN - set first component as main?
        for component in self.components:
            if component.base_model.name == "ROMS":
                # Calculate number of time steps:
                run_length_seconds = int(
                    (self.end_date - self.start_date).total_seconds()
                )

                # After that you need to run some verification stuff on the downloaded files
                component.run(
                    n_time_steps=(run_length_seconds // component.time_step),
                    account_key=account_key,
                    walltime=walltime,
                    job_name=job_name,
                )

    def post_run(self):
        """For each Component associated with this case, execute
        post-processing actions by calling component.post_run()"""

        for component in self.components:
            component.post_run()
