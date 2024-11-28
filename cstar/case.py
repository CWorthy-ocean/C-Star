import yaml
import warnings

import dateutil.parser
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING
from datetime import datetime

from cstar.base.component import Component

# from cstar.base.environment import environment
from cstar.base.system import cstar_system
from cstar.base.utils import _dict_to_tree
from cstar.roms.component import ROMSComponent
from cstar.marbl.component import MARBLComponent

if TYPE_CHECKING:
    pass


class Case:
    """A unique combination of Components that defines a C-Star simulation.

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
    from_blueprint(blueprint,caseroot,start_date,end_date)
        Instantiate a Case from a "blueprint" yaml file
    to_blueprint(filename)
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
        start_date: Optional[str | datetime] = None,
        end_date: Optional[str | datetime] = None,
        valid_start_date: Optional[str | datetime] = None,
        valid_end_date: Optional[str | datetime] = None,
    ):
        """Initialize a Case object manually from components, name, and caseroot path.

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
            self.valid_start_date: Optional[datetime] = (
                valid_start_date
                if isinstance(valid_start_date, datetime)
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
            self.valid_end_date: Optional[datetime] = (
                valid_end_date
                if isinstance(valid_end_date, datetime)
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
            self.start_date: Optional[datetime] = (
                start_date
                if isinstance(start_date, datetime)
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
            self.start_date, datetime
        ), "At this point either the code has failed or start_date is a datetime object"

        # Make sure Case end_date is set and is a datetime object:
        if end_date is not None:
            # Set if provided
            self.end_date: Optional[datetime] = (
                end_date
                if isinstance(end_date, datetime)
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
            self.end_date, datetime
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

    @property
    def is_setup(self):
        """Check whether all code and files necessary to run this case exist in the
        local `caseroot` folder.

        The method loops over each Component object makng up the case and
        1. Checks for any issues withe the component's base model (using BaseModel.local_config_status)
        2. Loops over AdditionalCode instances in the component calling AdditionalCode.check_exists_locally(caseroot) on each
        3. Loops over InputDataset instances in the component checking if InputDataset.working_path exists

        Returns:
        --------
        is_setup: bool
            True if all components are correctly set up in the caseroot directory
        """

        for component in self.components:
            if component.base_model.local_config_status != 0:
                return False

            # Check AdditionalCode
            if (
                (hasattr(component, "namelists"))
                and (component.namelists is not None)
                and (not component.namelists.exists_locally)
            ):
                return False
            if (component.additional_source_code is not None) and (
                not component.additional_source_code.exists_locally
            ):
                return False

            # Check InputDatasets
            if (not hasattr(component, "input_datasets")) or (
                component.input_datasets is None
            ):
                continue
            for inp in component.input_datasets:
                if (inp.working_path is None) or (not inp.working_path.exists()):
                    # If it can't be found locally, check whether it should by matching dataset dates with simulation dates:
                    # If no start or end date, it should be found locally:
                    if (not isinstance(inp.start_date, datetime)) or (
                        not isinstance(inp.end_date, datetime)
                    ):
                        return False
                    # If no start or end date for case, all files should be found locally:
                    elif (not isinstance(self.start_date, datetime)) or (
                        not isinstance(self.end_date, datetime)
                    ):
                        return False
                    # If inp and case start and end dates overlap, should be found locally:
                    elif (inp.start_date <= self.end_date) and (
                        inp.end_date >= self.start_date
                    ):
                        return False
        return True

    def tree(self):
        """Represent this Case using a `tree`-style visualisation.

        This function prints a representation of the Case to stdout. It represents the
        directory structure that Case.caseroot takes after calling Case.setup(), but
        does not require the user to have already called Case.setup().
        """
        # Build a dictionary of files connected to this case
        case_tree_dict = {}
        for component in self.components:
            if hasattr(component, "input_datasets") and (
                len(component.input_datasets) > 0
            ):
                case_tree_dict.setdefault("input_datasets", {})
                case_tree_dict["input_datasets"][component.component_type] = [
                    dataset.source.basename for dataset in component.input_datasets
                ]
            if hasattr(component, "namelists") and (component.namelists is not None):
                case_tree_dict.setdefault("namelists", {})
                case_tree_dict["namelists"].setdefault(component.component_type, {})
                case_tree_dict["namelists"][component.component_type] = [
                    namelist.split("/")[-1] for namelist in component.namelists.files
                ]

                # TODO return here and add any modified namelists that aren't temporary
            if hasattr(component, "additional_source_code") and (
                component.additional_source_code is not None
            ):
                case_tree_dict.setdefault("additional_source_code", {})
                case_tree_dict["additional_source_code"].setdefault(
                    component.component_type, {}
                )

                case_tree_dict["additional_source_code"][component.component_type] = [
                    namelist.split("/")[-1]
                    for namelist in component.additional_source_code.files
                ]

        print(f"{self.caseroot}\n{_dict_to_tree(case_tree_dict)}")

    @classmethod
    def from_blueprint(
        cls,
        blueprint: str,
        caseroot: Path,
        start_date: Optional[str | datetime] = None,
        end_date: Optional[str | datetime] = None,
    ) -> "Case":
        """Initialize a Case object from a blueprint.

        This method reads a YAML file containing the blueprint for a case
        and initializes a Case object based on the provided specifications.

        A blueprint YAML file should be structured as follows:

        - registry_attrs: overall case metadata, including "name"
        - components: A list of components, containing, e.g.
            - base_model: containing ["name","source_repo",and "checkout_target"]
            - namelists: optional, containing ["source_repo","checkout_target","source_mods","files"]
            - additional_source_code: optional, containing ["source_repo","checkout_target","source_mods","files"]
            - <input dataset>: taking values like "model_grid","initial_conditions","tidal_forcing","boundary_forcing","surface_forcing"
                              each containing "location" and "file_hash" (a SHA-256 sum) if the location is a URL
            - discretization: containing e.g. time step "time_step"  and parallelization "n_procs_x","n_procs_y" information


        The blueprint MUST contain a name and at least one component with a base_model

        Parameters:
        -----------
        blueprint: str | Path
            Path to a yaml file containing the blueprint for the case
        caseroot: str | Path
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
        valid_start_date: datetime
        valid_end_date: datetime
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
            component_type = component_info.get("component_type")

            if component_type is None:
                raise ValueError(
                    f"'component_type' not found for component entry in blueprint {blueprint}"
                )
            component_info.pop("component_type")
            match component_type.casefold():
                case "roms":
                    components.append(ROMSComponent.from_dict(component_info))
                case "marbl":
                    components.append(MARBLComponent.from_dict(component_info))
                case _:
                    raise ValueError(
                        f"component_type {component_type} in blueprint "
                        + f"{blueprint}  does not match a value that is supported by C-Star. "
                        + 'Currently supported values are "ROMS" and "MARBL"'
                    )

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

    def to_blueprint(self, filename: str) -> None:
        """Write this case to a yaml 'blueprint' file.

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
            component_info = component.to_dict()
            bp_dict["components"].append({"component": component_info})

        with open(filename, "w") as yaml_file:
            yaml.dump(bp_dict, yaml_file, default_flow_style=False, sort_keys=False)

    def setup(self) -> None:
        """Fetch all code and files necessary to run this case in the local `caseroot`
        folder.

        This method loops over each Component object making up the case, and calls
        Component.setup()
        """

        if self.is_setup:
            print(f"This case appears to have already been set up at {self.caseroot}")
            return

        for component in self.components:
            infostr = f"\nSetting up {component.__class__.__name__}"
            print(infostr + "\n" + "-" * len(infostr))
            if isinstance(component, ROMSComponent):
                component.setup(
                    namelist_dir=self.caseroot / "namelists/ROMS",
                    additional_source_code_dir=self.caseroot
                    / "additional_source_code/ROMS",
                    input_datasets_target_dir=self.caseroot / "input_datasets/ROMS",
                    start_date=self.start_date,
                    end_date=self.end_date,
                )
            elif isinstance(component, MARBLComponent):
                component.setup()

    def build(self) -> None:
        """Compile any necessary additional code associated with this case by calling
        component.build() on each Component object making up this case."""
        for component in self.components:
            infostr = f"\nCompiling {component.__class__.__name__}"
            print(infostr + "\n" + "-" * len(infostr))
            component.build()

    def pre_run(self) -> None:
        """For each Component associated with this case, execute pre-processing actions
        by calling component.pre_run()"""
        for component in self.components:
            infostr = (
                f"\nCompleting pre-processing steps for {component.__class__.__name__}"
            )
            print(infostr + "\n" + "-" * len(infostr))
            component.pre_run()

    def run(
        self,
        account_key=None,
        walltime=cstar_system.environment.max_walltime,
        queue=cstar_system.environment.primary_queue,
        job_name="my_case_run",
    ) -> None:
        """Run the case by calling `component.run(caseroot)` on the primary component
        (to which others are coupled)."""

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
                print("\nRunning ROMS: " + "\n------------")
                component.run(
                    output_dir=self.caseroot / "output",
                    n_time_steps=ntimesteps,
                    account_key=account_key,
                    walltime=walltime,
                    queue=queue,
                    job_name=job_name,
                )

    def post_run(self) -> None:
        """For each Component associated with this case, execute post-processing actions
        by calling component.post_run()"""
        for component in self.components:
            if isinstance(component, ROMSComponent):
                infostr = f"\nCompleting post-processing steps for {component.__class__.__name__}"
                print(infostr + "\n" + "-" * len(infostr))
                component.post_run(output_dir=self.caseroot / "output")

    def restart(self, new_end_date: str | datetime) -> "Case":
        """Returns a new Case instance beginning at the end date of this Case.

        This method creates a deep copy of the current Case and replaces
        any components that need to be updated by calling Component.restart()
        on them.

        Parameters:
        -----------
        new_end_date (str or datetime):
           The end date for the restarted Case. The start date corresponds
           to the end date of the existing Case.

        Returns:
        --------
        new_case (cstar.Case):
           The new Case instance with updated components and attributes
           allowing the simulation to continue.
        """
        import copy

        new_case = copy.deepcopy(self)
        new_case.start_date = self.end_date
        if isinstance(new_end_date, str):
            new_case.end_date = dateutil.parser.parse(new_end_date)
        elif isinstance(new_end_date, datetime):
            new_case.end_date = new_end_date
        else:
            raise ValueError(
                f"Expected str or datetime for `new_end_date`, got {type(new_end_date)}"
            )

        # Go through components and call restart() on them
        new_components = []
        for component in self.components:
            if hasattr(component, "restart"):
                if component.component_type.lower() == "roms":
                    new_component = component.restart(
                        # restart_dir is just output_dir from Case.run()
                        restart_dir=self.caseroot / "output",
                        new_start_date=new_case.start_date,
                    )
            else:
                new_component = component
            new_components.append(new_component)
        new_case.components = new_components

        # TODO: need handling of ROMSComponent.initial_conditions:
        # - Somehow need to calculate what the restart file will be
        #   and replace the IC with this

        return new_case
