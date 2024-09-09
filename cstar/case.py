import yaml
import warnings
import datetime as dt
import dateutil.parser
from pathlib import Path
from typing import List, Type, Any, Optional, TYPE_CHECKING

from cstar.base.component import Component, Discretization
from cstar.base.additional_code import AdditionalCode
from cstar.base.environment import _CSTAR_SYSTEM_MAX_WALLTIME
from cstar.base.utils import _dict_to_tree
from cstar.base.input_dataset import InputDataset
from cstar.roms.input_dataset import (
    ROMSModelGrid,
    ROMSInitialConditions,
    ROMSTidalForcing,
    ROMSBoundaryForcing,
    ROMSSurfaceForcing,
)
from cstar.roms.component import ROMSComponent, ROMSDiscretization

if TYPE_CHECKING:
    from cstar.base import BaseModel


class Case:
    """
    A unique combination of Components that defines a C-Star simulation.

    Attributes
    ---------
    components: list of Components
        The unique model component(s) that make up this case
    name: str
        The name of this case
    caseroot: Path
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
        components: List["Component"],
        name: str,
        caseroot: str | Path,
        start_date: Optional[str | dt.datetime] = None,
        end_date: Optional[str | dt.datetime] = None,
        valid_start_date: Optional[str | dt.datetime] = None,
        valid_end_date: Optional[str | dt.datetime] = None,
    ):
        """
        Initialize a Case object manually from components, name, and caseroot path.

        Parameters:
        ----------
        components: list of Components
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

        self.components: List["Component"] = components
        self.caseroot: Path = Path(caseroot).resolve()
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
        else:
            warnings.warn(
                "Valid start date not provided."
                + " Unable to check if simulation dates are out of range. "
                + "Case objects should be initialized with valid_start_date "
                + "and valid_end_date attributes.",
                RuntimeWarning,
            )
            self.valid_start_date = None

        if valid_end_date is not None:
            self.valid_end_date: Optional[dt.datetime] = (
                valid_end_date
                if isinstance(valid_end_date, dt.datetime)
                else dateutil.parser.parse(valid_end_date)
            )
        else:
            warnings.warn(
                "Valid end date not provided."
                + " Unable to check if simulation dates are out of range. "
                + "Case objects should be initialized with valid_start_date "
                + "and valid_end_date attributes.",
                RuntimeWarning,
            )
            self.valid_end_date = None

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

    def __str__(self) -> str:
        base_str = "C-Star Case\n"
        base_str += "-" * (len(base_str) - 1)

        base_str += f"\nName: {self.name}"
        base_str += f"\ncaseroot: {self.caseroot}"
        base_str += f"\nstart_date: {self.start_date}"
        base_str += f"\nend_date: {self.end_date}"
        base_str += f"\nIs setup: {self.is_setup}"
        base_str += "\nValid date range:"
        base_str += f"\nvalid_start_date: {self.valid_start_date}"
        base_str += f"\nvalid_end_date: {self.valid_end_date}"

        if self.is_from_blueprint:
            base_str += "\nThis case was instantiated from the blueprint file:"
            base_str += f"\n   {self.blueprint}"

        base_str += "\n"
        base_str += "\nIt is built from the following Components (query using Case.components): "

        for component in self.components:
            base_str += f"\n   <{component.__class__.__name__} instance>"

        return base_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nname = {self.name}, "
        repr_str += f"\ncaseroot = {self.caseroot}, "
        repr_str += f"\nstart_date = {self.start_date}, "
        repr_str += f"\nend_date = {self.end_date}, "
        repr_str += f"\nvalid_start_date = {self.valid_start_date}, "
        repr_str += f"\nvalid_end_date = {self.valid_end_date}, "
        repr_str += "\ncomponents = ["
        for component in self.components:
            repr_str += f"\n{component.__repr__()}, "
        repr_str = repr_str.strip(", ")
        repr_str += "\n]"
        repr_str += ")"

        return repr_str

    def tree(self):
        """
        Represent this Case using a `tree`-style visualisation

        This function prints a representation of the Case to stdout.
        It represents the directory structure that Case.caseroot takes after calling Case.setup(),
        but does not require the user to have already called Case.setup().

        """
        # Build a dictionary of files connected to this case
        case_tree_dict = {}
        for component in self.components:
            if len(component.input_datasets) > 0:
                case_tree_dict.setdefault("input_datasets", {})
                case_tree_dict["input_datasets"][component.base_model.name] = [
                    dataset.source.basename for dataset in component.input_datasets
                ]
            if hasattr(component, "additional_code") and (
                component.additional_code is not None
            ):
                if component.additional_code.namelists is not None:
                    case_tree_dict.setdefault("namelists", {})
                    case_tree_dict["namelists"][component.base_model.name] = [
                        namelist.split("/")[-1]
                        for namelist in component.additional_code.namelists
                    ]
                if component.additional_code.modified_namelists is not None:
                    case_tree_dict.setdefault("namelists", {})
                    case_tree_dict["namelists"][component.base_model.name] += [
                        namelist.split("/")[-1]
                        for namelist in component.additional_code.modified_namelists
                    ]
                if component.additional_code.source_mods is not None:
                    case_tree_dict.setdefault("source_mods", {})
                    case_tree_dict["source_mods"][component.base_model.name] = [
                        sourcemod.split("/")[-1]
                        for sourcemod in component.additional_code.source_mods
                    ]

        print(f"{self.caseroot}\n{_dict_to_tree(case_tree_dict)}")

    @classmethod
    def from_blueprint(
        cls,
        blueprint: str,
        caseroot: Path,
        start_date: Optional[str | dt.datetime] = None,
        end_date: Optional[str | dt.datetime] = None,
    ) -> "Case":
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
        registry_attrs = bp_dict.get("registry_attrs")
        if registry_attrs is None:
            raise ValueError(
                f"No top-level metadata found in blueprint {blueprint}."
                + "Ensure there is a 'registry_attrs' section containing 'name',and 'valid_date_range' entries"
            )

        casename = registry_attrs.get("name")
        if casename is None:
            raise ValueError(
                f"'name' entry not found in 'registry_attrs' section of blueprint {blueprint}"
            )

        valid_date_range = registry_attrs.get("valid_date_range")
        valid_start_date: dt.datetime
        valid_end_date: dt.datetime
        valid_start_date = valid_date_range.get("start_date")
        valid_end_date = valid_date_range.get("end_date")

        if isinstance(start_date, str):
            start_date = dateutil.parser.parse(start_date)
        if isinstance(end_date, str):
            end_date = dateutil.parser.parse(end_date)

        components: List["Component"]
        components = []

        if "components" not in bp_dict.keys():
            raise ValueError(
                f"No 'components' entry found in blueprint {blueprint}. "
                + "Cannot create a Case with no components!"
            )
        for component in bp_dict["components"]:
            component_info = component.get("component")
            component_kwargs: dict[str, Any] = {}

            ThisComponent: Type["Component"]
            ThisBaseModel: Type["BaseModel"]
            ThisDiscretization: Type["Discretization"]

            # Construct the BaseModel instance
            base_model_info = component_info.get("base_model")
            match base_model_info["name"].casefold():
                case "roms":
                    from cstar.roms import ROMSBaseModel, ROMSComponent

                    ThisComponent = ROMSComponent
                    ThisBaseModel = ROMSBaseModel
                    ThisDiscretization = ROMSDiscretization
                case "marbl":
                    from cstar.marbl import MARBLBaseModel, MARBLComponent

                    ThisComponent = MARBLComponent
                    ThisBaseModel = MARBLBaseModel
                case _:
                    raise ValueError(
                        f'Base model name {base_model_info["name"]} in blueprint '
                        + f"{blueprint}  does not match a value that is supported by C-Star. "
                        + 'Currently supported values are "ROMS" and "MARBL"'
                    )

            base_model = ThisBaseModel(
                base_model_info["source_repo"], base_model_info["checkout_target"]
            )
            component_kwargs["base_model"] = base_model

            # Construct the Discretization instance
            discretization: Optional[Discretization] = None
            if "discretization" in component_info.keys():
                discretization_info = component_info.get("discretization")
                discretization = ThisDiscretization(**discretization_info)
                component_kwargs["discretization"] = discretization

            # Construct any AdditionalCode instances
            additional_code: Optional[AdditionalCode] = None
            if "additional_code" in component_info.keys():
                additional_code_info = component_info.get("additional_code")
                additional_code = AdditionalCode(
                    base_model=base_model, **additional_code_info
                )
                component_kwargs["additional_code"] = additional_code

            # Construct any InputDataset instances:
            input_datasets: List[InputDataset] | None
            input_dataset_info = component_info.get("input_datasets", {})
            input_datasets = []

            if base_model.name.casefold() == "roms":
                idtype_class_map = {
                    "model_grid": ROMSModelGrid,
                    "initial_conditions": ROMSInitialConditions,
                    "tidal_forcing": ROMSTidalForcing,
                    "boundary_forcing": ROMSBoundaryForcing,
                    "surface_forcing": ROMSSurfaceForcing,
                }

                ## Loop over input_datasets entries,initialise appropriate class, append to list
                for idtype, dataset_info in input_dataset_info.items():
                    if idtype in idtype_class_map:
                        # Get the class to be instantiated
                        ThisInputDataset = idtype_class_map[idtype]

                        for file_info in dataset_info["files"]:
                            input_datasets.append(
                                ThisInputDataset(base_model=base_model, **file_info)
                            )
                    else:
                        raise ValueError(
                            f"InputDataset type {idtype} in {blueprint} not recognized"
                        )
            if len(input_datasets) > 0:
                component_kwargs["input_datasets"] = input_datasets

            components.append(ThisComponent(**component_kwargs))

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

    def persist(self, filename: str) -> None:
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

        # Add start date to valid_date_range if it exists
        if self.valid_start_date is not None:
            bp_dict["registry_attrs"].setdefault("valid_date_range", {})[
                "start_date"
            ] = str(self.valid_start_date)
        if self.valid_end_date is not None:
            bp_dict["registry_attrs"].setdefault("valid_date_range", {})["end_date"] = (
                str(self.valid_end_date)
            )

        bp_dict["components"] = []

        for component in self.components:
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
            if (
                hasattr(component, "discretization")
                and component.discretization is not None
            ):
                for thisattr in vars(component.discretization).keys():
                    discretization_info[thisattr] = getattr(
                        component.discretization, thisattr
                    )
            if len(discretization_info) > 0:
                component_info["discretization"] = discretization_info

            # AdditionalCode instances - can also be None
            # Loop over additional code
            additional_code = component.additional_code

            if additional_code is not None:
                additional_code_info: dict = {}
                # This will be component_info["component"]["additional_code"]=additional_code_info
                additional_code_info["location"] = additional_code.source.location
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

            input_dataset_info: dict = {}
            for ind in input_datasets:
                # Determine what kind of input dataset we are adding
                if isinstance(ind, ROMSModelGrid):
                    dct_key = "model_grid"
                elif isinstance(ind, ROMSInitialConditions):
                    dct_key = "initial_conditions"
                elif isinstance(ind, ROMSTidalForcing):
                    dct_key = "tidal_forcing"
                elif isinstance(ind, ROMSBoundaryForcing):
                    dct_key = "boundary_forcing"
                elif isinstance(ind, ROMSSurfaceForcing):
                    dct_key = "surface_forcing"
                else:
                    raise ValueError(f"Unknown dataset type: {type(ind)}")

                # If there is not already an instance of this input dataset type,
                # add an empty dict as the key so we can access/build it
                if dct_key not in input_dataset_info.keys():
                    input_dataset_info[dct_key] = {}

                # Create a dictionary of file_info for each dataset file:
                if "files" not in input_dataset_info[dct_key].keys():
                    input_dataset_info[dct_key]["files"] = []
                file_info = {}
                file_info["location"] = ind.source.location
                if hasattr(ind, "file_hash") and (ind.file_hash is not None):
                    file_info["file_hash"] = ind.file_hash
                if hasattr(ind, "start_date") and (ind.start_date is not None):
                    file_info["start_date"] = str(ind.start_date)
                if hasattr(ind, "end_date") and (ind.end_date is not None):
                    file_info["end_date"] = str(ind.end_date)

                input_dataset_info[dct_key]["files"].append(file_info)

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

        for component in self.components:
            if component.base_model.local_config_status != 0:
                return False

            # Check AdditionalCode
            if (component.additional_code is None) or not (
                component.additional_code.check_exists_locally(self.caseroot)
            ):
                return False

            # Check InputDatasets
            if component.input_datasets is None:
                continue
            for inp in component.input_datasets:
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

    def setup(self) -> None:
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
            # Verify dates line up before running .get():
            if component.input_datasets is None:
                continue
            for inp in component.input_datasets:
                # Download input dataset if its date range overlaps Case's date range
                if (
                    ((inp.start_date is None) or (inp.end_date is None))
                    or ((self.start_date is None) or (self.end_date is None))
                    or (inp.start_date <= self.end_date)
                    and (self.end_date >= self.start_date)
                ):
                    inp.get(self.caseroot)

        self.is_setup = True

    def build(self) -> None:
        """Compile any necessary additional code associated with this case
        by calling component.build() on each Component object making up this case"""
        for component in self.components:
            component.build()

    def pre_run(self) -> None:
        """For each Component associated with this case, execute
        pre-processing actions by calling component.pre_run()"""
        for component in self.components:
            component.pre_run()

    def run(
        self,
        account_key=None,
        walltime=_CSTAR_SYSTEM_MAX_WALLTIME,
        job_name="my_case_run",
    ) -> None:
        """Run the case by calling `component.run(caseroot)`
        on the primary component (to which others are coupled)."""

        # Assuming for now that ROMS presence implies it is the master program
        # TODO add more advanced logic for this
        # 20240807 - TN - set first component as main?

        for component in self.components:
            if isinstance(component, ROMSComponent):
                # Calculate number of time steps:
                if (self.end_date is not None) and (self.start_date is not None):
                    run_length_seconds = int(
                        (self.end_date - self.start_date).total_seconds()
                    )
                    ntimesteps = (
                        run_length_seconds // component.discretization.time_step
                    )
                else:
                    ntimesteps = None

                # After that you need to run some verification stuff on the downloaded files
                component.run(
                    n_time_steps=ntimesteps,
                    account_key=account_key,
                    walltime=walltime,
                    job_name=job_name,
                )

    def post_run(self) -> None:
        """For each Component associated with this case, execute
        post-processing actions by calling component.post_run()"""
        for component in self.components:
            component.post_run()
