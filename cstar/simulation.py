import copy
import pickle
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import dateutil

from cstar.base import AdditionalCode, Discretization, ExternalCodeBase
from cstar.base.log import LoggingMixin
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.execution.local_process import LocalProcess


class Simulation(ABC, LoggingMixin):
    """An abstract base class representing a C-Star simulation.

    Attributes
    ----------
    name : str
        The name of this simulation.
    directory : Path
        The local directory in which this simulation will be prepared and executed.
    start_date : str or datetime
        The starting date of the simulation.
    end_date : str or datetime
        The ending date of the simulation.
    valid_start_date : str or datetime
        The earliest allowed start date, based on, e.g. the availability of input data.
    valid_end_date : str or datetime
        The latest allowed end date, based on, e.g., the availability of input data.
    codebase : ExternalCodeBase
        The repository containing the base source code for this simulation.
    runtime_code : AdditionalCode
        Runtime configuration files.
    compile_time_code : AdditionalCode
        Additional source code modifications and compile-time configuration files.
    discretization : Discretization
        Numerical discretization parameters for this simulation.

    Methods
    -------
    from_dict(simulation_dict, directory)
        Construct a Simulation instance from a dictionary.
    to_dict()
        Convert this Simulation instance into a dictionary.
    from_blueprint(blueprint, directory)
        Initialize a Simulation from a YAML blueprint.
    to_blueprint(filename)
        Save this simulation's configuration as a YAML blueprint.
    setup()
        Prepare all necessary files and configurations for the simulation locally.
    build(rebuild=False)
        Compile any necessary additional code associated with this simulation.
    pre_run()
        Execute any pre-processing actions required before running the simulation.
    run()
        Execute the simulation.
    post_run()
        Execute any post-processing actions required after running the simulation.
    persist()
        Save the state of this Simulation instance to disk.
    restore(directory)
        Restore a previously saved Simulation instance from disk.
    restart(new_end_date)
        Create a new Simulation instance starting from the end of this one.
    """

    def __init__(
        self,
        name: str,
        directory: str | Path,
        discretization: "Discretization",
        runtime_code: Optional["AdditionalCode"] = None,
        compile_time_code: Optional["AdditionalCode"] = None,
        codebase: Optional["ExternalCodeBase"] = None,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        valid_start_date: str | datetime | None = None,
        valid_end_date: str | datetime | None = None,
    ):
        """Initialize a Simulation object with a given name, directory, codebase, and
        configuration parameters.

        Parameters
        ----------
        name : str
            The name of this simulation.
        directory : str or Path
            The directory where the simulation will be prepared and executed.
        discretization : Discretization
            The numerical discretization settings for this simulation.
        runtime_code : AdditionalCode, optional
            Runtime configuration files.
        compile_time_code : AdditionalCode, optional
            Additional source code modifications and compile-time configuration files.
        codebase : ExternalCodeBase, optional
            The repository containing the base source code for this simulation.
        start_date : str or datetime, optional
            The starting date of the simulation.
        end_date : str or datetime, optional
            The ending date of the simulation.
        valid_start_date : str or datetime, optional
            The earliest allowed start date, based on, e.g. the availability of input data.
        valid_end_date : str or datetime, optional
            The latest allowed end date, based on, e.g., the availability of input data.
        """
        self.directory = Path(directory).resolve()
        self.name = name

        # Process valid date ranges
        self.valid_start_date = self._parse_date(
            date=valid_start_date, field_name="Valid start date"
        )
        self.valid_end_date = self._parse_date(
            date=valid_end_date, field_name="Valid end date"
        )
        if self.valid_start_date is None or self.valid_end_date is None:
            self.log.warning(
                "Cannot enforce date range validation: Missing `valid_start_date` or `valid_end_date`.",
            )

        # Set start and end dates, using defaults where needed
        self.start_date = self._get_date_or_fallback(
            date=start_date, fallback=self.valid_start_date, field_name="start_date"
        )
        self.end_date = self._get_date_or_fallback(
            date=end_date, fallback=self.valid_end_date, field_name="end_date"
        )

        # Ensure start_date and end_date are within valid range
        self._validate_date_range()

        if codebase is None:
            self.codebase = self.default_codebase
            self.log.warning(
                f"Creating {self.__class__.__name__} instance without a specified "
                "ExternalCodeBase, default codebase will be used:\n"
                f"          • Source location: {self.codebase._default_source_repo}\n"
                f"          • Checkout target: {self.codebase._default_checkout_target}\n"
            )
        else:
            self.codebase = codebase

        self.runtime_code = runtime_code or None
        self.compile_time_code = compile_time_code or None
        self.discretization = discretization

    @staticmethod
    def _parse_date(date: str | datetime | None, field_name: str) -> datetime | None:
        """Converts a date string to a datetime object if it's not None.

        If the input is a string, it attempts to parse it into a `datetime` object.
        If the input is already a `datetime` object, it is returned as is.
        If the input is `None`, a warning is issued, and `None` is returned.

        Parameters
        ----------
        date : str, datetime, or None
            The date to be parsed. Can be a string representation of a date,
            a `datetime` object, or `None`.
        field_name : str
            The name of the field being parsed, used for warning messages.

        Returns
        -------
        datetime or None
            The parsed `datetime` object if the input is valid, otherwise `None`.

        Warns
        -----
        RuntimeWarning
            If the date is `None`, a warning is issued indicating that
            validation of simulation dates may be incomplete.
        """
        if date is None:
            return None
        return date if isinstance(date, datetime) else dateutil.parser.parse(date)

    def _get_date_or_fallback(
        self,
        date: str | datetime | None,
        fallback: datetime | None,
        field_name: str,
    ) -> datetime:
        """Ensures a date is set, using a fallback if needed.

        If a valid date is provided, it is parsed and returned. If `date` is `None`,
        the fallback is used instead. If both `date` and `fallback` are `None`,
        an error is raised.

        Parameters
        ----------
        date : str, datetime, or None
            The date to be parsed. Can be a string representation of a date,
            a `datetime` object, or `None`.
        fallback : datetime or None
            The fallback value to use if `date` is not provided.
        field_name : str
            The name of the field being processed, used for warning messages.

        Returns
        -------
        datetime
            The parsed `datetime` object.

        Raises
        ------
        ValueError
            If both `date` and `fallback` are `None`, indicating that no valid date is available.

        Warns
        -----
        UserWarning
            If `date` is `None`, a warning is issued indicating that the fallback value is being used.
        """
        parsed_date = self._parse_date(date=date, field_name=field_name)

        if parsed_date is None:  # If no date is provided, use the fallback
            if fallback is not None:
                warn_msg = f"{field_name} not provided. Defaulting to {fallback}."
                self.log.warning(warn_msg)
                return fallback
            raise ValueError(f"Neither {field_name} nor a valid fallback was provided.")

        return parsed_date  # Always returns a valid datetime

    def _validate_date_range(self):
        """Checks that `start_date` and `end_date` fall within the valid date range.

        Ensures that the simulation's `start_date` is not earlier than `valid_start_date`,
        and that `end_date` is not later than `valid_end_date`. Also checks that `start_date`
        is not after `end_date`.

        Raises
        ------
        ValueError
            If `start_date` is earlier than `valid_start_date`.
        ValueError
            If `end_date` is later than `valid_end_date`.
        ValueError
            If `start_date` is later than `end_date`.
        """
        if self.valid_start_date and self.start_date < self.valid_start_date:
            raise ValueError(
                f"start_date {self.start_date} is before the earliest valid start date {self.valid_start_date}."
            )
        if self.valid_end_date and self.end_date > self.valid_end_date:
            raise ValueError(
                f"end_date {self.end_date} is after the latest valid end date {self.valid_end_date}."
            )
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date {self.start_date} is after end_date {self.end_date}."
            )

    def __str__(self) -> str:
        """Returns a string representation of the simulation.

        The representation includes the simulation's name, directory,
        start and end dates, valid date range, discretization settings,
        and code-related information.

        Returns
        -------
        str
            A formatted string summarizing the simulation's attributes.
        """
        class_name = self.__class__.__name__
        base_str = f"{class_name}\n" + ("-" * len(class_name)) + "\n"

        base_str += f"Name: {self.name}\n"
        base_str += f"Directory: {self.directory}\n"

        # Dates
        base_str += f"Start date: {self.start_date}\n"
        base_str += f"End date: {self.end_date}\n"
        base_str += f"Valid start date: {self.valid_start_date}\n"
        base_str += f"Valid end date: {self.valid_end_date}\n"

        if self.discretization is not None:
            base_str += "\nDiscretization: "
            base_str += self.discretization.__repr__() + "\n"

        # Codebase
        base_str += "\nCode:"
        base_str += f"\nCodebase: {self.codebase.__class__.__name__} instance (query using {class_name}.codebase)\n"

        # Runtime code:
        if self.runtime_code is not None:
            NN = len(self.runtime_code.source)
            base_str += f"Runtime code: {self.runtime_code.__class__.__name__} instance with {NN} files (query using {class_name}.runtime_code)\n"

        # Compile-time code:
        if self.compile_time_code is not None:
            NN = len(self.compile_time_code.source)
            base_str += f"Compile-time code: {self.compile_time_code.__class__.__name__} instance with {NN} files (query using {class_name}.compile_time_code)"

        exe_path = getattr(self, "exe_path", None)
        if exe_path is not None:
            base_str += "\nIs compiled: True"
            base_str += "\nExecutable path: " + str(exe_path)

        return base_str

    def __repr__(self) -> str:
        """Returns a detailed string representation of the simulation.

        The representation includes all relevant attributes, such as
        name, directory, start and end dates, valid date range,
        discretization settings, and code-related components.

        Returns
        -------
        str
            A string representation of the simulation suitable for debugging.
        """
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nname = {self.name},"
        repr_str += f"\ndirectory = {self.directory},"
        repr_str += f"\nstart_date = {self.start_date},"
        repr_str += f"\nend_date = {self.end_date},"
        repr_str += f"\nvalid_start_date = {self.valid_start_date},"
        repr_str += f"\nvalid_end_date = {self.valid_end_date},"
        if self.discretization is not None:
            repr_str += f"\ndiscretization = {self.discretization.__repr__()},"

        repr_str += f"\ncodebase = <{self.codebase.__class__.__name__} instance>,"
        if self.runtime_code is not None:
            repr_str += (
                "\nruntime_code = "
                + f"<{self.runtime_code.__class__.__name__} instance>,"
            )
        if self.compile_time_code is not None:
            repr_str += (
                "\ncompile_time_code = "
                + f"<{self.compile_time_code.__class__.__name__} instance>,"
            )
        repr_str = repr_str.rstrip(",")
        repr_str += ")"

        return repr_str

    @property
    @abstractmethod
    def default_codebase(self) -> ExternalCodeBase:
        """Abstract property that must be implemented by subclasses to provide a default
        codebase.

        This property ensures that each subclass of `Simulation` defines a default
        `ExternalCodeBase` instance to be used when no specific codebase is provided.

        Returns
        -------
        ExternalCodeBase
            The default codebase instance for the simulation.
        """
        pass

    @classmethod
    @abstractmethod
    def from_dict(self, simulation_dict: dict, directory: str | Path):
        """Abstract method to create a Simulation instance from a dictionary.

        This method must be implemented by subclasses to construct a simulation
        object using a dictionary representation, typically generated by `to_dict()`.

        Parameters
        ----------
        simulation_dict : dict
            A dictionary containing the attributes required to initialize a Simulation instance.
        directory : str or Path
            The directory where the simulation will be set up.

        Returns
        -------
        Simulation
            An initialized simulation instance.

        See Also
        --------
        to_dict : Converts an existing Simulation instance into a dictionary representation.
        from_blueprint: Reads an equivalent representation from a yaml file.
        """
        pass

    def to_dict(self) -> dict:
        """Convert the Simulation instance into a dictionary representation.

        This method serializes the attributes of the Simulation instance into a
        dictionary format, including metadata, codebase details, discretization
        settings, runtime and compile-time code information.

        Returns
        -------
        dict
            A dictionary containing all the attributes of the Simulation instance.

        See Also
        --------
        from_dict : Constructs a Simulation instance from a dictionary.
        to_blueprint: Writes an equivalent representation to a yaml file.
        """
        simulation_dict: dict[Any, Any] = {}

        # Top-level information
        simulation_dict["name"] = self.name
        simulation_dict["valid_start_date"] = self.valid_start_date
        simulation_dict["valid_end_date"] = self.valid_end_date

        # ExternalCodeBases:
        simulation_dict["codebase"] = self.codebase.to_dict()

        # discretization
        simulation_dict["discretization"] = self.discretization.__dict__

        # runtime code
        if hasattr(self, "runtime_code") and self.runtime_code is not None:
            simulation_dict["runtime_code"] = self.runtime_code.to_dict()

        # compile-time code
        if hasattr(self, "compile_time_code") and self.compile_time_code is not None:
            simulation_dict["compile_time_code"] = self.compile_time_code.to_dict()

        return simulation_dict

    @classmethod
    @abstractmethod
    def from_blueprint(
        cls,
        blueprint: str,
    ) -> "Simulation":
        """Abstract method to create a Simulation instance from a blueprint file.

        This method should be implemented in subclasses to read a YAML file containing
        a structured blueprint for a simulation and initialize a Simulation instance
        accordingly.

        Parameters
        ----------
        blueprint : str
            The path or URL of a YAML file containing the blueprint for the simulation.

        Returns
        -------
        Simulation
            An initialized Simulation instance based on the provided blueprint.

        See Also
        --------
        to_blueprint : Saves the Simulation instance to a YAML blueprint file.
        from_dict : Creates a Simulation instance from a dictionary.
        """
        pass

    @abstractmethod
    def setup(self) -> None:
        """Abstract method to set up the Simulation.

        This method should be implemented in subclasses to handle tasks such as
        configuring the simulation directory, retrieving necessary files, and preparing
        the simulation for execution.

        See Also
        --------
        run : Executes the simulation.
        """
        pass

    def persist(self) -> None:
        """Save the current state of the simulation to a file.

        This method serializes the simulation object and writes it to a file named
        `simulation_state.pkl` within the simulation directory, allowing the exact state
        to be restored later.

        Raises
        ------
        RuntimeError
            If the simulation is currently running in a local process, as a running
            LocalProcess instance cannot be serialized.

        See Also
        --------
        restore : Restores a previously saved simulation state.
        """
        if (
            (hasattr(self, "_execution_handler"))
            and (isinstance(self._execution_handler, LocalProcess))
            and (self._execution_handler.status == ExecutionStatus.RUNNING)
        ):
            raise RuntimeError(
                "Simulation.persist() was called, but at least one "
                "local process is currently running in. Await "
                "completion or use LocalProcess.cancel(), then try again"
            )

        # Loggers do not survive roundtrip
        if hasattr(self, "_log"):
            del self._log

        with open(f"{self.directory}/simulation_state.pkl", "wb") as state_file:
            pickle.dump(self, state_file)

    @classmethod
    def restore(cls, directory: str | Path) -> "Simulation":
        """Restore a previously saved simulation state.

        This method loads a serialized simulation object from the `simulation_state.pkl`
        file located in the specified directory and returns the restored instance.

        Parameters
        ----------
        directory : str or Path
            The directory containing the saved simulation state.

        Returns
        -------
        Simulation
            The restored simulation instance.

        Raises
        ------
        FileNotFoundError
            If the `simulation_state.pkl` file is not found in the specified directory.
        pickle.UnpicklingError
            If the file cannot be deserialized, indicating possible corruption.

        See Also
        --------
        persist : Saves the current simulation state.
        """
        directory = Path(directory)
        with open(f"{directory}/simulation_state.pkl", "rb") as state_file:
            simulation_instance = pickle.load(state_file)
        return simulation_instance

    @abstractmethod
    def build(self, rebuild=False) -> None:
        """Abstract method to compile any necessary code for this simulation.

        This method should be implemented by subclasses to handle steps
        specific to that Simulation type.

        Parameters
        ----------
        rebuild : bool, optional, default=False
            If True, forces recompilation or reconfiguration even if an existing
            build is detected.

        See Also
        --------
        setup : Prepares the Simulation locally.
        run : Executes the Simulation.
        """
        pass

    @abstractmethod
    def pre_run(self) -> None:
        """Abstract method to perform any necessary pre-processing steps before running
        the simulation.

        This method should be implemented by subclasses to handle setup tasks such as
        preparing input datasets or configuration files.

        Raises
        ------
        NotImplementedError
            If the subclass does not implement this method.

        See Also
        --------
        build : Compiles or prepares necessary components.
        run : Executes the simulation.
        post_run : Handles post-processing steps after execution.
        """
        pass

    @abstractmethod
    def run(self) -> "ExecutionHandler":
        """Abstract method to begin execution of the simulation.

        This method should be implemented by subclasses to handle the execution
        of the simulation, whether locally or through a job scheduler. The
        method should return an `ExecutionHandler` instance that tracks the
        execution status.

        Returns
        -------
        ExecutionHandler
            An object that manages and tracks the execution of the simulation.

        Raises
        ------
        NotImplementedError
            If the subclass does not implement this method.

        See Also
        --------
        pre_run : Prepares the simulation before execution.
        post_run : Handles post-processing steps after execution.
        """
        pass

    @abstractmethod
    def post_run(self) -> None:
        """Abstract method to perform post-processing actions after the simulation run.

        This method should be implemented by subclasses to handle any necessary
        post-processing steps after the simulation has completed, such as
        processing output files.

        See Also
        --------
        run : Executes the simulation.
        pre_run : Performs preprocessing before execution.
        """
        pass

    def restart(self, new_end_date: str | datetime) -> "Simulation":
        """Create a new Simulation instance starting from the end date of the current
        simulation.

        This method generates a deep copy of the current simulation and updates its
        start date to match the current simulation's end date. The new simulation
        may require additional modifications, such as setting restart files, which
        should be implemented in subclasses.

        Parameters
        ----------
        new_end_date : str or datetime
            The end date for the restarted simulation.

        Returns
        -------
        Simulation
            A new simulation instance with updated parameters for continuing the
            previous simulation.

        Raises
        ------
        ValueError
            If `new_end_date` is not of type str or datetime.

        See Also
        --------
        persist : Saves the state of the current simulation.
        restore : Restores a saved simulation instance.
        """
        new_sim = copy.deepcopy(self)
        new_sim.start_date = self.end_date
        new_sim.directory = (
            new_sim.directory
            / f"RESTART_{new_sim.start_date.strftime('%Y%m%d_%H%M%S')}"
        )
        if isinstance(new_end_date, str):
            new_sim.end_date = dateutil.parser.parse(new_end_date)
        elif isinstance(new_end_date, datetime):
            new_sim.end_date = new_end_date
        else:
            raise ValueError(
                f"Expected str or datetime for `new_end_date`, got {type(new_end_date)}"
            )

        return new_sim
