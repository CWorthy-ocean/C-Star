import shutil
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, cast

import requests
import yaml

from cstar import Simulation
from cstar.base.additional_code import AdditionalCode
from cstar.base.datasource import DataSource
from cstar.base.utils import (
    _dict_to_tree,
    _get_sha256_hash,
    _replace_text_in_file,
    _run_cmd,
)
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.execution.local_process import LocalProcess
from cstar.execution.scheduler_job import create_scheduler_job
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.input_dataset import (
    ROMSBoundaryForcing,
    ROMSForcingCorrections,
    ROMSInitialConditions,
    ROMSInputDataset,
    ROMSModelGrid,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)
from cstar.system.manager import cstar_sysmgr


class ROMSSimulation(Simulation):
    """A specialized `Simulation` subclass for configuring and running ROMS (Regional
    Ocean Modeling System) simulations.

    This class extends `Simulation` to provide ROMS-specific functionality, including managing
    model grids, forcing files, and discretization parameters. It also facilitates the setup,
    execution, and post-processing of ROMS simulations.

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
    marbl_codebase : MARBLExternalCodeBase
        The repository containing the base source code for MARBL (Marine Biogeochemistry Library).
    runtime_code : AdditionalCode
        Additional code needed by ROMS at runtime (e.g. a `.in` file)
    compile_time_code : AdditionalCode
        Additional ROMS source code to be included at compile time (e.g. `.opt` files)
    model_grid : ROMSModelGrid
        The model grid used in the simulation.
    initial_conditions : ROMSInitialConditions
        Initial conditions for the simulation.
    tidal_forcing : ROMSTidalForcing
        Tidal forcing data used in the simulation.
    surface_forcing : list[ROMSSurfaceForcing]
        List of surface forcing datasets.
    boundary_forcing : list[ROMSBoundaryForcing]
        List of boundary forcing datasets.
    forcing_corrections : list[ROMSForcingCorrections]
        List of forcing correction datasets.
    exe_path : Path
        Path to the compiled ROMS executable.
    partitioned_files : List[Path]
        Paths to partitioned input files for distributed ROMS runs.
    _execution_handler : ExecutionHandler
        Object handling execution status and control.

    Methods
    -------
    setup()
        Configures and prepares the ROMS simulation environment.
    build(rebuild=False)
        Compiles the ROMS executable if necessary.
    pre_run()
        Performs pre-processing steps before execution.
    run(account_key=None, walltime=None, queue_name=None, job_name=None)
        Submits the ROMS simulation for execution.
    post_run()
        Processes model outputs after execution.
    restart(new_end_date)
        Creates a new ROMS simulation instance from a restart file.

    See Also
    --------
    Simulation : Base class providing general simulation functionalities.
    """

    discretization: ROMSDiscretization

    def __init__(
        self,
        name: str,
        directory: str | Path,
        discretization: "ROMSDiscretization",
        runtime_code: Optional["AdditionalCode"] = None,
        compile_time_code: Optional["AdditionalCode"] = None,
        codebase: Optional["ROMSExternalCodeBase"] = None,
        start_date: Optional[str | datetime] = None,
        end_date: Optional[str | datetime] = None,
        valid_start_date: Optional[str | datetime] = None,
        valid_end_date: Optional[str | datetime] = None,
        marbl_codebase: Optional["MARBLExternalCodeBase"] = None,
        model_grid: Optional["ROMSModelGrid"] = None,
        initial_conditions: Optional["ROMSInitialConditions"] = None,
        tidal_forcing: Optional["ROMSTidalForcing"] = None,
        river_forcing: Optional["ROMSRiverForcing"] = None,
        boundary_forcing: Optional[list["ROMSBoundaryForcing"]] = None,
        surface_forcing: Optional[list["ROMSSurfaceForcing"]] = None,
        forcing_corrections: Optional[list["ROMSForcingCorrections"]] = None,
    ):
        """Initializes a `ROMSSimulation` instance.

        This constructor defines a ROMS simulation via its parameters, codebase,
        input datasets, and discretization settings. It validates inputs and establishes
        attributes required for simulation setup, execution, and post-processing.

        Parameters
        ----------
        name : str
            The name of the simulation.
        directory : str or Path
            Path to the directory where simulation files and outputs will be stored.
        discretization : ROMSDiscretization
            The discretization settings defining the ROMS grid resolution and time step.
        runtime_code : AdditionalCode, optional
            ROMS runtime configuration files (e.g. a `.in` file) required for execution.
        compile_time_code : AdditionalCode, optional
            Additional source code or modifications needed for compiling ROMS (e.g. `.opt` files).
        codebase : ROMSExternalCodeBase, optional
            The ROMS source code repository to be used. If not provided, a default ROMS
            codebase will be used.
        start_date : str or datetime, optional
            The start date of the simulation. If not provided, it defaults to the valid start date.
        end_date : str or datetime, optional
            The end date of the simulation. If not provided, it defaults to the valid end date.
        valid_start_date : str or datetime
            The earliest allowed start date, based on, e.g. the availability of input data.
        valid_end_date : str or datetime
            The latest allowed end date, based on, e.g., the availability of input data.
        marbl_codebase : MARBLExternalCodeBase, optional
            External codebase for MARBL (Marine Biogeochemistry Library) integration.
            If not provided, a default MARBL codebase is used.
        model_grid : ROMSModelGrid
            The grid used by the ROMS simulation.
        initial_conditions : ROMSInitialConditions
            Initial conditions dataset specifying the starting ocean state.
        tidal_forcing : ROMSTidalForcing
            Tidal forcing dataset providing tidal components for the simulation.
        river_forcing: ROMSRiverForcing
            River forcing dataset providing river location and flux information.
        boundary_forcing : list[ROMSBoundaryForcing]
            List of datasets specifying boundary conditions for ROMS.
        surface_forcing : list[ROMSBoundaryForcing]
            List of surface forcing datasets (e.g., wind stress, heat flux).
        forcing_corrections : list[ROMSForcingCorrections], optional
            List of surface forcing correction datasets.

        Raises
        ------
        TypeError
            If `surface_forcing` is not a list of `ROMSSurfaceForcing` instances.
            If `boundary_forcing` is not a list of `ROMSBoundaryForcing` instances.
            If `forcing_corrections` is not a list of `ROMSForcingCorrections` instances.
        """
        if discretization is None:
            raise ValueError(
                "Cannot construct a ROMSSimulation instance without a "
                + "ROMSDiscretization object, but could not find 'discretization' entry"
            )
        if runtime_code is None:
            raise ValueError(
                "Cannot construct a ROMSSimulation instance without runtime "
                + "code, but could not find 'runtime_code' entry"
            )
        if compile_time_code is None:
            raise NotImplementedError(
                "This version of C-Star does not support ROMSSimulation instances "
                + "without code to be included at compile time (.opt files, etc.), but "
                + "could not find a 'compile_time_code' entry."
            )

        super().__init__(
            name=name,
            directory=directory,
            discretization=discretization,
            codebase=codebase,
            runtime_code=runtime_code,
            compile_time_code=compile_time_code,
            start_date=start_date,
            end_date=end_date,
            valid_start_date=valid_start_date,
            valid_end_date=valid_end_date,
        )

        self.model_grid = model_grid
        self.initial_conditions = initial_conditions
        self.tidal_forcing = tidal_forcing
        self.river_forcing = river_forcing
        self.surface_forcing = [] if surface_forcing is None else surface_forcing
        self._check_forcing_collection_types(self.surface_forcing, ROMSSurfaceForcing)

        self.boundary_forcing = [] if boundary_forcing is None else boundary_forcing
        self._check_forcing_collection_types(self.boundary_forcing, ROMSBoundaryForcing)
        self.forcing_corrections = (
            [] if forcing_corrections is None else forcing_corrections
        )
        self._check_forcing_collection_types(
            self.forcing_corrections, ROMSForcingCorrections
        )

        self._check_inputdataset_dates()

        if marbl_codebase is None:
            self.marbl_codebase = MARBLExternalCodeBase()
            self.log.warning(
                "Creating MARBLSimulation instance without a specified "
                + "MARBLExternalCodeBase, default codebase will be used:\n"
                + f"          • Source location: {self.marbl_codebase.source_repo}\n"
                + f"          • Checkout target: {self.marbl_codebase.checkout_target}\n"
            )
        else:
            self.marbl_codebase = marbl_codebase

        # roms-specific
        self.exe_path: Optional[Path] = None
        self._exe_hash: Optional[str] = None
        self.partitioned_files: List[Path] | None = None

        self._execution_handler: Optional["ExecutionHandler"] = None

    def _check_forcing_collection_types(self, collection, expected_class):
        """For forcing types that may correspond to multiple InputDataset instances
        (ROMSSurfaceForcing, ROMSBoundaryForcing, ROMSForcingCorrections), ensure that
        the corresponding attribute is a list of instances of the correct type."""
        if not all([isinstance(ind, expected_class) for ind in collection]):
            raise TypeError(
                f"ROMSSimulation.{collection} must be a list of {expected_class} instances"
            )

    def _check_inputdataset_dates(self) -> None:
        """For ROMSInputDatasets whose source type is `yaml`, ensure that any set
        'start_date' and/or 'end_date' attributes of the ROMSInputDataset match the
        'start_date' and 'end_date' of the ROMSSimulation, and override them, warning
        the user, if not."""

        for inp in [
            self.initial_conditions,
            self.river_forcing,
            *self.surface_forcing,
            *self.boundary_forcing,
        ]:
            if (inp is not None) and (inp.source.source_type == "yaml"):
                if (
                    hasattr(inp, "start_date")
                    and (inp.start_date is not None)
                    and (inp.start_date != self.start_date)
                ):
                    self.log.warning(
                        f"{inp.__class__.__name__} has start date attribute {inp.start_date} "
                        + f"that does not match ROMSSimulation.start_date {self.start_date}. "
                        f"C-Star will enforce {self.start_date} as the start date"
                    )
                inp.start_date = self.start_date

                if isinstance(inp, ROMSInitialConditions):
                    continue

                if (
                    hasattr(inp, "end_date")
                    and (inp.end_date is not None)
                    and (inp.end_date != self.end_date)
                ):
                    self.log.warning(
                        f"{inp.__class__.__name__} has end date attribute {inp.end_date} "
                        + f"that does not match ROMSSimulation.end_date {self.end_date}. "
                        f"C-Star will enforce {self.end_date} as the end date"
                    )
                inp.end_date = self.end_date

    def __str__(self) -> str:
        """Returns a string representation of the simulation.

        Returns
        -------
        str
            A formatted string summarizing the simulation's attributes.
        """

        class_name = self.__class__.__name__
        base_str = super().__str__()

        # MARBL Codebase
        if self.marbl_codebase is not None:
            base_str += f"\nMARBL Codebase: {self.marbl_codebase.__class__.__name__} instance (query using {class_name}.marbl_codebase)\n"

        # Input Datasets:
        base_str += "\nInput Datasets:\n"
        if self.model_grid is not None:
            base_str += f"Model grid: <{self.model_grid.__class__.__name__} instance>"
        if self.initial_conditions is not None:
            base_str += f"\nInitial conditions: <{self.initial_conditions.__class__.__name__} instance>"
        if self.tidal_forcing is not None:
            base_str += (
                f"\nTidal forcing: <{self.tidal_forcing.__class__.__name__} instance>"
            )
        if self.river_forcing is not None:
            base_str += (
                f"\nRiver forcing: <{self.river_forcing.__class__.__name__} instance>"
            )
        if len(self.surface_forcing) > 0:
            base_str += (
                f"\nSurface forcing: <list of {len(self.surface_forcing)} "
                + f"{self.surface_forcing[0].__class__.__name__} instances>"
            )
        if len(self.boundary_forcing) > 0:
            base_str += (
                f"\nBoundary forcing: <list of {len(self.boundary_forcing)} "
                + f"{self.boundary_forcing[0].__class__.__name__} instances>"
            )
        if len(self.forcing_corrections) > 0:
            base_str += (
                f"\nForcing corrections: <list of {len(self.forcing_corrections)} "
                + f"{self.forcing_corrections[0].__class__.__name__} instances>\n"
            )

        base_str += f"\nIs setup: {self.is_setup}"

        return base_str

    def __repr__(self) -> str:
        """Returns a detailed string representation of the simulation.

        Returns
        -------
        str
            A string representation of the simulation suitable for debugging.
        """

        repr_str = super().__repr__().rstrip(")")

        if hasattr(self, "model_grid") and self.model_grid is not None:
            repr_str += (
                f"\nmodel_grid = <{self.model_grid.__class__.__name__} instance>,"
            )
        if hasattr(self, "initial_conditions") and self.initial_conditions is not None:
            repr_str += f"\ninitial_conditions = <{self.initial_conditions.__class__.__name__} instance>,"
        if hasattr(self, "tidal_forcing") and self.tidal_forcing is not None:
            repr_str += (
                f"\ntidal_forcing = <{self.tidal_forcing.__class__.__name__} instance>,"
            )
        if hasattr(self, "river_forcing") and self.river_forcing is not None:
            repr_str += (
                f"\nriver_forcing = <{self.river_forcing.__class__.__name__} instance>,"
            )
        if hasattr(self, "surface_forcing") and len(self.surface_forcing) > 0:
            repr_str += (
                f"\nsurface_forcing = <list of {len(self.surface_forcing)} "
                + f"{self.surface_forcing[0].__class__.__name__} instances>,"
            )
        if hasattr(self, "boundary_forcing") and len(self.boundary_forcing) > 0:
            repr_str += (
                f"\nboundary_forcing = <list of {len(self.boundary_forcing)} "
                + f"{self.boundary_forcing[0].__class__.__name__} instances>,"
            )
        if hasattr(self, "forcing_corrections") and len(self.forcing_corrections) > 0:
            repr_str += (
                f"\nforcing_corrections = <list of {len(self.forcing_corrections)} "
                + f"{self.forcing_corrections[0].__class__.__name__} instances>"
            )

        repr_str += "\n)"

        return repr_str

    @property
    def default_codebase(self) -> ROMSExternalCodeBase:
        """Returns the default ROMS external codebase.

        This property provides a default instance of `ROMSExternalCodeBase` to be used
        if no codebase is explicitly provided during initialization.

        Returns
        -------
        ROMSExternalCodeBase
            A default instance of the ROMS external codebase.
        """

        return ROMSExternalCodeBase()

    @property
    def codebases(self) -> list:
        """Returns a list of external codebases associated with this ROMS simulation.

        This property includes both the primary ROMS external codebase and the
        MARBL external codebase (if applicable).

        Returns
        -------
        list
            A list containing the ROMS external codebase and MARBL external codebase.
        """

        return [self.codebase, self.marbl_codebase]

    @property
    def in_file(self) -> Path:
        """Retrieves the ROMS runtime input file (.in) associated with this simulation.

        ROMS requires a text file containing runtime options to run. This file is
        typically called `roms.in`, but variations occur. This property finds the
        appropriate `.in` or `.in_TEMPLATE` file in the `runtime_code` files and
        returns its path.

        Returns
        -------
        Path
            The path to the ROMS runtime input file.

        Raises
        ------
        ValueError
            - If `runtime_code` is not set.
            - If multiple `.in` files are found, making the selection ambiguous.
            - If no `.in` file is found in `runtime_code.files`.
        """

        in_files = []
        if self.runtime_code is None:
            raise ValueError(
                "ROMSSimulation.runtime_code not set."
                + " ROMS requires a runtime options file "
                + "(typically roms.in)"
            )

        in_files = [
            fname.replace(".in_TEMPLATE", ".in")
            for fname in self.runtime_code.files
            if (fname.endswith(".in") or fname.endswith(".in_TEMPLATE"))
        ]
        if len(in_files) > 1:
            raise ValueError(
                "Multiple '.in' files found:"
                + "\n{in_files}"
                + "\nROMS runtime file choice ambiguous"
            )
        elif len(in_files) == 0:
            raise ValueError(
                "No '.in' file found in ROMSSimulation.runtime_code."
                + "ROMS expects a runtime options file with the '.in'"
                + "extension, e.g. roms.in"
            )
        else:
            if self.runtime_code.working_path is not None:
                return self.runtime_code.working_path / in_files[0]
            else:
                return Path(in_files[0])

    @property
    def input_datasets(self) -> list[ROMSInputDataset]:
        """Retrieves all input datasets associated with this ROMS simulation.

        This property compiles a list of `ROMSInputDataset` instances that are used
        in the simulation, including model grids, initial conditions, tidal forcing,
        surface forcing, and boundary forcing datasets.

        Returns
        -------
        list of ROMSInputDataset
            A list containing all input datasets used in the simulation.
        """

        input_datasets: List[ROMSInputDataset] = []
        if self.model_grid is not None:
            input_datasets.append(self.model_grid)
        if self.initial_conditions is not None:
            input_datasets.append(self.initial_conditions)
        if self.tidal_forcing is not None:
            input_datasets.append(self.tidal_forcing)
        if self.river_forcing is not None:
            input_datasets.append(self.river_forcing)
        if len(self.boundary_forcing) > 0:
            input_datasets.extend(self.boundary_forcing)
        if len(self.surface_forcing) > 0:
            input_datasets.extend(self.surface_forcing)
        if len(self.forcing_corrections) > 0:
            input_datasets.extend(self.forcing_corrections)
        return input_datasets

    @classmethod
    def from_dict(
        cls,
        simulation_dict: dict,
        directory: str | Path,
        start_date: Optional[str | datetime] = None,
        end_date: Optional[str | datetime] = None,
    ):
        """Create a `ROMSSimulation` instance from a dictionary representation.

        Parameters
        ----------
        simulation_dict : dict
            A dictionary containing the serialized attributes of a `ROMSSimulation`.
        directory : str or Path
            The directory where the simulation data should be stored.
        start_date : str or datetime, optional
            The start date of the simulation. If not provided, it will be attempted to be inferred.
        end_date : str or datetime, optional
            The end date of the simulation. If not provided, it will be attempted to be inferred.

        Returns
        -------
        ROMSSimulation
            An initialized `ROMSSimulation` instance.

        Raises
        ------
        ValueError
            If essential keys such as 'discretization' or 'runtime_code' are missing.
        NotImplementedError
            If `compile_time_code` is not provided, as it is currently required.

        See Also
        --------
        to_dict : Serializes a `ROMSSimulation` instance into a dictionary.
        from_blueprint : Creates a `ROMSSimulation` from a YAML blueprint file.
        """

        # Initialise keyword argument dictionary to create ROMSSimulation
        simulation_kwargs: dict[Any, Any] = {}

        # Pass method parameters in
        simulation_kwargs["directory"] = directory
        simulation_kwargs["start_date"] = start_date
        simulation_kwargs["end_date"] = end_date

        # Get other direct entries from dictionary
        simulation_kwargs["name"] = simulation_dict.get("name")
        simulation_kwargs["valid_start_date"] = simulation_dict.get("valid_start_date")
        simulation_kwargs["valid_end_date"] = simulation_dict.get("valid_end_date")

        # Construct the ExternalCodeBase instance
        codebase_kwargs = simulation_dict.get("codebase", {})
        codebase = ROMSExternalCodeBase(**codebase_kwargs)

        simulation_kwargs["codebase"] = codebase

        marbl_codebase_kwargs = simulation_dict.get("marbl_codebase", {})
        marbl_codebase = MARBLExternalCodeBase(**marbl_codebase_kwargs)

        simulation_kwargs["marbl_codebase"] = marbl_codebase

        # Construct the Discretization instance
        discretization_kwargs = simulation_dict.get("discretization")
        if discretization_kwargs is not None:
            discretization = ROMSDiscretization(**discretization_kwargs)

            simulation_kwargs["discretization"] = discretization

        # Construct any AdditionalCode instance associated with namelists
        runtime_code_kwargs = simulation_dict.get("runtime_code")
        if runtime_code_kwargs is not None:
            runtime_code = AdditionalCode(**runtime_code_kwargs)
            simulation_kwargs["runtime_code"] = runtime_code

        # Construct any AdditionalCode instance associated with source mods
        compile_time_code_kwargs = simulation_dict.get("compile_time_code")
        if compile_time_code_kwargs is not None:
            compile_time_code = AdditionalCode(**compile_time_code_kwargs)
            simulation_kwargs["compile_time_code"] = compile_time_code

        # Construct any ROMSModelGrid instance:
        model_grid_kwargs = simulation_dict.get("model_grid")
        if model_grid_kwargs is not None:
            simulation_kwargs["model_grid"] = ROMSModelGrid(**model_grid_kwargs)

        # Construct any ROMSInitialConditions instance:
        initial_conditions_kwargs = simulation_dict.get("initial_conditions")
        if initial_conditions_kwargs is not None:
            simulation_kwargs["initial_conditions"] = ROMSInitialConditions(
                **initial_conditions_kwargs
            )

        # Construct any ROMSTidalForcing instance:
        tidal_forcing_kwargs = simulation_dict.get("tidal_forcing")
        if tidal_forcing_kwargs is not None:
            simulation_kwargs["tidal_forcing"] = ROMSTidalForcing(
                **tidal_forcing_kwargs
            )

        # Construct any ROMSRiverForcing instance:
        river_forcing_kwargs = simulation_dict.get("river_forcing")
        if river_forcing_kwargs is not None:
            simulation_kwargs["river_forcing"] = ROMSRiverForcing(
                **river_forcing_kwargs
            )

        # Construct any ROMSBoundaryForcing instances:
        boundary_forcing_entries = simulation_dict.get("boundary_forcing", [])
        if len(boundary_forcing_entries) > 0:
            simulation_kwargs["boundary_forcing"] = []
        if isinstance(boundary_forcing_entries, dict):
            boundary_forcing_entries = [
                boundary_forcing_entries,
            ]
        for bf_kwargs in boundary_forcing_entries:
            simulation_kwargs["boundary_forcing"].append(
                ROMSBoundaryForcing(**bf_kwargs)
            )

        # Construct any ROMSSurfaceForcing instances:
        surface_forcing_entries = simulation_dict.get("surface_forcing", [])
        if len(surface_forcing_entries) > 0:
            simulation_kwargs["surface_forcing"] = []
        if isinstance(surface_forcing_entries, dict):
            surface_forcing_entries = [
                surface_forcing_entries,
            ]
        for sf_kwargs in surface_forcing_entries:
            simulation_kwargs["surface_forcing"].append(ROMSSurfaceForcing(**sf_kwargs))

        # Construct any ROMSForcingCorrections instances:
        forcing_corrections_entries = simulation_dict.get("forcing_corrections", [])
        if len(forcing_corrections_entries) > 0:
            simulation_kwargs["forcing_corrections"] = []
        if isinstance(forcing_corrections_entries, dict):
            forcing_corrections_entries = [
                forcing_corrections_entries,
            ]
        for fc_kwargs in forcing_corrections_entries:
            simulation_kwargs["forcing_corrections"].append(
                ROMSForcingCorrections(**fc_kwargs)
            )

        return cls(**simulation_kwargs)

    def to_dict(self) -> dict:
        """Convert the `ROMSSimulation` instance into a dictionary representation.

        This method serializes the attributes of the `ROMSSimulation` instance into
        a dictionary, allowing it to be stored, transferred, or reconstructed later.

        Returns
        -------
        dict
            A dictionary representation of the `ROMSSimulation`, including its
            name, codebases, discretization, runtime and compile-time code,
            input datasets, and metadata.

        Notes
        -----
        - This method ensures that all relevant attributes are properly formatted
          for serialization.
        - Input datasets and additional code attributes are stored as nested dictionaries.
        - The dictionary produced by this method can be used with `from_dict` to
          reconstruct the `ROMSSimulation` instance.

        See Also
        --------
        from_dict : Reconstructs a `ROMSSimulation` instance from a dictionary.
        to_blueprint : Saves the dictionary representation as a YAML blueprint.
        """

        simulation_dict = super().to_dict()

        # MARBLExternalCodeBase
        marbl_codebase_info = {}
        marbl_codebase_info["source_repo"] = self.marbl_codebase.source_repo
        marbl_codebase_info["checkout_target"] = self.marbl_codebase.checkout_target
        simulation_dict["marbl_codebase"] = marbl_codebase_info

        # InputDatasets:
        if self.model_grid is not None:
            simulation_dict["model_grid"] = self.model_grid.to_dict()
        if self.initial_conditions is not None:
            simulation_dict["initial_conditions"] = self.initial_conditions.to_dict()
        if self.tidal_forcing is not None:
            simulation_dict["tidal_forcing"] = self.tidal_forcing.to_dict()
        if self.river_forcing is not None:
            simulation_dict["river_forcing"] = self.river_forcing.to_dict()
        if len(self.surface_forcing) > 0:
            simulation_dict["surface_forcing"] = [
                sf.to_dict() for sf in self.surface_forcing
            ]
        if len(self.boundary_forcing) > 0:
            simulation_dict["boundary_forcing"] = [
                bf.to_dict() for bf in self.boundary_forcing
            ]
        if len(self.forcing_corrections) > 0:
            simulation_dict["forcing_corrections"] = [
                fc.to_dict() for fc in self.forcing_corrections
            ]

        return simulation_dict

    @classmethod
    def from_blueprint(
        cls,
        blueprint: str,
        directory: str | Path,
        start_date: Optional[str | datetime] = None,
        end_date: Optional[str | datetime] = None,
    ):
        """Create a `ROMSSimulation` instance from a YAML blueprint.

        This method reads a YAML blueprint file, extracts the relevant configuration
        details, and initializes a `ROMSSimulation` object using the extracted data.

        Parameters
        ----------
        blueprint : str
            The path or URL to the YAML blueprint file.
        directory : str or Path
            The directory where the simulation will be stored.
        start_date : str or datetime, optional
            The start date for the simulation. If not provided, defaults to the
            `valid_start_date` specified in the blueprint.
        end_date : str or datetime, optional
            The end date for the simulation. If not provided, defaults to the
            `valid_end_date` specified in the blueprint.

        Returns
        -------
        ROMSSimulation
            An initialized `ROMSSimulation` instance based on the blueprint data.

        Raises
        ------
        ValueError
            If the blueprint file is not in YAML format or if required fields are missing.

        See Also
        --------
        to_blueprint : Saves the simulation as a YAML blueprint.
        from_dict : Creates an instance from a dictionary representation.
        """

        source = DataSource(location=blueprint)
        if source.source_type != "yaml":
            raise ValueError(
                f"C-Star expects blueprint in '.yaml' format, but got {blueprint}"
            )
        if source.location_type == "path":
            with open(blueprint, "r") as file:
                bp_dict = yaml.safe_load(file)
        elif source.location_type == "url":
            bp_dict = yaml.safe_load(requests.get(source.location).text)

        return cls.from_dict(
            bp_dict,
            directory=directory,
            start_date=start_date,
            end_date=end_date,
        )

    def to_blueprint(self, filename: str) -> None:
        """Save the `ROMSSimulation` instance as a YAML blueprint.

        This method converts the simulation instance into a dictionary representation
        and writes it to a YAML file in a structured format.

        Parameters
        ----------
        filename : str
            The name of the YAML file where the blueprint will be saved.

        Raises
        ------
        OSError
            If an issue occurs while writing to the file.

        See Also
        --------
        from_blueprint : Creates a `ROMSSimulation` instance from a YAML blueprint.
        to_dict : Converts the instance into a dictionary representation.
        """

        with open(filename, "w") as yaml_file:
            yaml.dump(
                self.to_dict(), yaml_file, default_flow_style=False, sort_keys=False
            )

    def update_runtime_code(self):
        """Update the runtime code files for the ROMS simulation.

        This method modifies the runtime code files by replacing template placeholders
        with the corresponding values based on the current state of the `ROMSSimulation`
        instance. If no template files are found, a warning is issued.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If `runtime_code` is not set or does not have a `working_path`.

        Warnings
        --------
        UserWarning
            If no template runtime code files (`*_TEMPLATE`) are found, indicating
            that runtime parameters may not be applied.
        """

        no_template_found = True
        for nl_idx, nl_fname in enumerate(self.runtime_code.files):
            nl_path = self.runtime_code.working_path / nl_fname
            if str(nl_fname)[-9:] == "_TEMPLATE":
                no_template_found = False
                mod_nl_path = Path(str(nl_path)[:-9])
                shutil.copy(nl_path, mod_nl_path)
                for placeholder, replacement in self._runtime_code_modifications[
                    nl_idx
                ].items():
                    _replace_text_in_file(mod_nl_path, placeholder, str(replacement))
                self.runtime_code.modified_files[nl_idx] = mod_nl_path

        if no_template_found:
            warnings.warn(
                "WARNING: No editable runtime code found to set ROMS runtime parameters. "
                + "Expected to find a template in ROMSSimulation.runtime_code"
                + " with the suffix '_TEMPLATE' on which to base the file."
                + "\n********************************************************"
                + "\nANY MODEL PARAMETERS SET IN C-STAR WILL NOT BE APPLIED."
                + "\n********************************************************"
            )

    @property
    def _runtime_code_modifications(self) -> list[dict]:
        """Generate a list of modifications to be applied to runtime code template
        files.

        This method constructs a list of dictionaries (one per file), where each
        dictionary contains placeholder keys and their corresponding replacement values.
        The placeholders are replaced based on the current state of this `ROMSSimulation`.

        Returns
        -------
        list of dict
            A list of dictionaries, each containing placeholder keys and replacement values
            for runtime code modifications.

        Raises
        ------
        ValueError
            If `runtime_code` is not set, or if required input files (e.g., `roms.in_TEMPLATE`)
            are missing.

        See Also
        --------
        update_runtime_code : Uses this method to apply modifications to the runtime code.
        """
        self.runtime_code = cast(AdditionalCode, self.runtime_code)

        if self.runtime_code.working_path is None:
            raise ValueError(
                "ROMSSimulation.runtime_code does not have a "
                + "'working_path' attribute. "
                + "Run ROMSSimulation.runtime_code.get() and try again"
            )

        # Helper function for formatting:
        def partitioned_files_to_runtime_code_string(input_dataset):
            """Take a ROMSInputDataset that has been partitioned and return a ROMS in-
            file-compatible string pointing to it e.g. path/to/roms_file.232.nc -> '
            path/to/roms_file.nc'."""

            unique_paths = {
                str(Path(f).parent / (Path(Path(f).stem).stem + ".nc"))
                for f in input_dataset.partitioned_files
            }
            return "\n     ".join(sorted(list(unique_paths)))

        runtime_code_modifications: list[dict] = [{} for f in self.runtime_code.files]

        ################################################################################
        # 'roms.in' file modifications (the only file to modify as of 2024-10-01):
        ################################################################################

        # First figure out which namelist is the one to modify
        nl_template = self.in_file.name + "_TEMPLATE"
        if nl_template in self.runtime_code.files:
            nl_idx = self.runtime_code.files.index(nl_template)
        else:
            raise ValueError(
                "could not find expected template namelist file "
                + "roms.in_TEMPLATE to modify. "
                + "ROMS requires a namelist file to run."
            )

        # Time step entry
        formatting = runtime_code_modifications[nl_idx]
        formatting["__TIMESTEP_PLACEHOLDER__"] = self.discretization.time_step

        # Grid file entry
        if self.model_grid is not None:
            if len(self.model_grid.partitioned_files) == 0:
                raise ValueError(
                    "could not find a local path to a partitioned "
                    + "ROMS grid file. Run ROMSSimulation.pre_run() to partition "
                    + "ROMS input datasets and try again."
                )

            runtime_code_modifications[nl_idx]["__GRID_FILE_PLACEHOLDER__"] = (
                partitioned_files_to_runtime_code_string(self.model_grid)
            )

        # Initial conditions entry
        if self.initial_conditions is not None:
            if len(self.initial_conditions.partitioned_files) == 0:
                raise ValueError(
                    "could not find a local path to a partitioned "
                    + "ROMS initial file. Run ROMSSimulation.pre_run() to partition "
                    + "ROMS input datasets and try again."
                )

            runtime_code_modifications[nl_idx][
                "__INITIAL_CONDITION_FILE_PLACEHOLDER__"
            ] = partitioned_files_to_runtime_code_string(self.initial_conditions)

        # Forcing files entry
        runtime_code_forcing_str = ""
        for sf in self.surface_forcing:
            if len(sf.partitioned_files) > 0:
                runtime_code_forcing_str += (
                    "\n     " + partitioned_files_to_runtime_code_string(sf)
                )
        for bf in self.boundary_forcing:
            if len(bf.partitioned_files) > 0:
                runtime_code_forcing_str += (
                    "\n     " + partitioned_files_to_runtime_code_string(bf)
                )
        if self.tidal_forcing is not None:
            if len(self.tidal_forcing.partitioned_files) == 0:
                raise ValueError(
                    "ROMSSimulation has tidal_forcing attribute "
                    + "but could not find a local path to a partitioned ROMS "
                    + "tidal forcing file. Run ROMSSimulation.pre_run() "
                    + " to partition ROMS input datasets and try again."
                )
            runtime_code_forcing_str += (
                "\n     " + partitioned_files_to_runtime_code_string(self.tidal_forcing)
            )

        if self.river_forcing is not None:
            if len(self.river_forcing.partitioned_files) == 0:
                raise ValueError(
                    "ROMSSimulation has river_forcing attribute "
                    + "but could not find a local path to a partitioned ROMS "
                    + "river forcing file. Run ROMSSimulation.pre_run() "
                    + " to partition ROMS input datasets and try again."
                )
            runtime_code_forcing_str += (
                "\n     " + partitioned_files_to_runtime_code_string(self.river_forcing)
            )
        for fc in self.forcing_corrections:
            if len(fc.partitioned_files) > 0:
                runtime_code_forcing_str += (
                    "\n     " + partitioned_files_to_runtime_code_string(fc)
                )

        formatting = runtime_code_modifications[nl_idx]
        formatting["__FORCING_FILES_PLACEHOLDER__"] = runtime_code_forcing_str.lstrip()

        # MARBL settings filepaths entries
        ## NOTE: WANT TO RAISE IF PLACEHOLDER IS IN NAMELIST BUT not Path(marbl_file.exists())
        if "marbl_in" in self.runtime_code.files:
            runtime_code_modifications[nl_idx][
                "__MARBL_SETTINGS_FILE_PLACEHOLDER__"
            ] = str(self.runtime_code.working_path / "marbl_in")

        if "marbl_tracer_output_list" in self.runtime_code.files:
            runtime_code_modifications[nl_idx][
                "__MARBL_TRACER_LIST_FILE_PLACEHOLDER__"
            ] = str(self.runtime_code.working_path / "marbl_tracer_output_list")

        if "marbl_diagnostic_output_list" in self.runtime_code.files:
            runtime_code_modifications[nl_idx][
                "__MARBL_DIAG_LIST_FILE_PLACEHOLDER__"
            ] = str(self.runtime_code.working_path / "marbl_diagnostic_output_list")

        return runtime_code_modifications

    def tree(self):
        """Display a tree-style representation of the ROMS simulation structure.

        This method prints a hierarchical representation of the `ROMSSimulation` instance,
        showing the organization of runtime code, compile-time code, and input datasets.
        It mimics the directory structure that will exist after calling `setup()`,
        but does not require `setup()` to have been run.

        Examples
        --------
        >>> simulation.tree()
        /path/to/simulation
        ├── ROMS
        │   ├── runtime_code
        │   │   ├── namelist_file.in
        │   │   ├── another_file.in
        │   ├── compile_time_code
        │   │   ├── some_source_file.F90
        │   ├── input_datasets
        │   │   ├── grid_file.nc
        │   │   ├── forcing_file.nc

        Notes
        -----
        - The displayed tree is based on the files tracked by the `ROMSSimulation` instance.
        - It does not necessarily reflect the actual state of the filesystem.
        """

        # Build a dictionary of files connected to this case
        simulation_tree_dict = {}

        if hasattr(self, "input_datasets") and (len(self.input_datasets) > 0):
            simulation_tree_dict.setdefault("input_datasets", {})
            simulation_tree_dict["input_datasets"] = [
                dataset.source.basename for dataset in self.input_datasets
            ]
        if hasattr(self, "runtime_code") and (self.runtime_code is not None):
            simulation_tree_dict.setdefault("runtime_code", {})
            simulation_tree_dict["runtime_code"] = [
                runtime_code.split("/")[-1] for runtime_code in self.runtime_code.files
            ]

        if hasattr(self, "compile_time_code") and (self.compile_time_code is not None):
            simulation_tree_dict.setdefault("compile_time_code", {})
            simulation_tree_dict["compile_time_code"] = [
                compile_time_code.split("/")[-1]
                for compile_time_code in self.compile_time_code.files
            ]
        print_dict = {}
        print_dict["ROMS"] = simulation_tree_dict
        return f"{self.directory}\n{_dict_to_tree(print_dict)}"

    def setup(
        self,
    ) -> None:
        """Prepare this ROMSSimulation locally by fetching necessary files and compiling
        any external codebases.

        This method ensures that all required components (codebases, runtime code,
        compile-time code, and input datasets) are correctly retrieved and configured
        in the simulation directory.

        The method performs the following steps:
        1. Configures the ROMS and MARBL external codebases.
        2. Fetches and organizes compile-time code.
        3. Fetches and organizes runtime code (e.g., namelists).
        4. Fetches and prepares input datasets.

        Raises
        ------
        ValueError
            If any required component (e.g., runtime code, compile-time code) is missing.
        RuntimeError
            If there is an issue configuring the external codebases.

        Notes
        -----
        - Input datasets are only fetched if their date range overlaps the
          simulation's start and end dates.

        See Also
        --------
        build : Compiles the ROMS model.
        is_setup : Checks if the simulation has been properly configured.
        """

        compile_time_code_dir = self.directory / "ROMS/compile_time_code"
        runtime_code_dir = self.directory / "ROMS/runtime_code"
        input_datasets_dir = self.directory / "ROMS/input_datasets"

        # Setup ExternalCodeBase
        infostr = f"🛠️  Configuring {self.__class__.__name__}"
        self.log.info(infostr + "\n" + "-" * len(infostr))
        self.log.info(f"🔧 Setting up {self.codebase.__class__.__name__}...")

        for codebase in self.codebases:
            codebase.interactive = self.interactive

        self.codebase.handle_config_status()

        if self.marbl_codebase is not None:
            self.log.info(f"🔧 Setting up {self.marbl_codebase.__class__.__name__}...")
            self.marbl_codebase.handle_config_status()

        # Compile-time code
        self.log.info("📦 Fetching compile-time code...")
        if self.compile_time_code is not None:
            self.compile_time_code.get(compile_time_code_dir)

        # Runtime code
        self.log.info("📦 Fetching runtime code... ")
        if self.runtime_code is not None:
            self.runtime_code.get(runtime_code_dir)

        # InputDatasets
        self.log.info("📦 Fetching input datasets...")
        for inp in self.input_datasets:
            # Download input dataset if its date range overlaps Simulation's date range
            if (
                ((inp.start_date is None) or (inp.end_date is None))
                or ((self.start_date is None) or (self.end_date is None))
                or (inp.start_date <= self.end_date)
                and (self.end_date >= self.start_date)
            ):
                inp.get(local_dir=input_datasets_dir)

    @property
    def is_setup(self) -> bool:
        """Check whether the ROMSSimulation is fully configured locally.

        This property verifies that all required components (codebases, runtime code,
        compile-time code, and input datasets) are correctly retrieved and available.

        The following criteria are checked:
        - The ROMS external codebase is correctly configured.
        - The MARBL external codebase is correctly configured (if applicable).
        - Runtime code (e.g., namelists) exists locally.
        - Compile-time code (e.g., build scripts) exists locally.
        - Input datasets that overlap with the simulation's date range exist locally.

        Returns
        -------
        bool
            True if all necessary components are present and properly configured,
            False otherwise.

        Raises
        ------
        AttributeError
            If any required attributes are missing from the instance.

        Notes
        -----
        - If this method returns `False`, `setup()` should be called.
        - Input datasets are only required if their date range overlaps the
          simulation's start and end dates.

        See Also
        --------
        setup : Fetches and organizes necessary files for the simulation.
        """

        if self.codebase.local_config_status != 0:
            return False
        if self.marbl_codebase.local_config_status != 0:
            return False
        if (self.runtime_code is not None) and (not self.runtime_code.exists_locally):
            return False
        if (self.compile_time_code is not None) and (
            not self.compile_time_code.exists_locally
        ):
            return False
        for inp in self.input_datasets:
            if not (inp.exists_locally):
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

    def build(self, rebuild: bool = False) -> None:
        """Compile the ROMS executable from source code.

        This method compiles the ROMS simulation based on the retrieved
        compile-time code and source files. The compilation process occurs
        in the working directory of `compile_time_code`. If the executable
        already exists and has not changed, it is not rebuilt unless explicitly
        requested with `rebuild=True`.

        Parameters
        ----------
        rebuild : bool, optional
            If True, forces recompilation even if the executable already exists
            and the source code has not changed. Default is False.

        Raises
        ------
        ValueError
            If `compile_time_code` is None or its working path is not set.
        RuntimeError
            If an error occurs during the compilation process.

        Notes
        -----
        - This method first attempts to clean the build directory before compilation.
        - The compiled executable is stored in the `exe_path` attribute.
        - Compilation uses the system's default compiler, which can be configured
          through `cstar_sysmgr.environment.compiler`.

        Examples
        --------
        >>> simulation.build()
        Compiling UCLA-ROMS configuration...
        UCLA-ROMS compiled at /path/to/build/directory

        >>> simulation.build(rebuild=True)
        Recompiling UCLA-ROMS...
        Compilation complete.

        See Also
        --------
        setup : Ensures necessary files are available before compilation.
        run : Executes the compiled ROMS model.
        """
        self.compile_time_code = cast(AdditionalCode, self.compile_time_code)
        build_dir = self.compile_time_code.working_path
        if build_dir is None:
            raise ValueError(
                "Unable to compile ROMSSimulation: "
                + "\nROMSSimulation.compile_time_code.working_path is None."
                + "\n Call ROMSSimulation.compile_time_code.get() and try again"
            )
        exe_path = build_dir / "roms"
        if (
            (exe_path.exists())
            and (self.compile_time_code.exists_locally)
            and (self._exe_hash is not None)
            and (_get_sha256_hash(exe_path) == self._exe_hash)
            and not rebuild
        ):
            self.log.info(
                f"ROMS has already been built at {exe_path}, and "
                "the source code appears not to have changed. "
                "If you would like to recompile, call "
                "ROMSSimulation.build(rebuild = True)"
            )
            return

        if (build_dir / "Compile").is_dir():
            _run_cmd(
                "make compile_clean",
                cwd=build_dir,
                msg_err="Error when compiling ROMS.",
                raise_on_error=True,
            )

        _run_cmd(
            f"make COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=build_dir,
            msg_pre="Compiling UCLA-ROMS configuration...",
            msg_post=f"UCLA-ROMS compiled at {build_dir}",
            msg_err="Error when compiling ROMS.",
            raise_on_error=True,
        )

        self.exe_path = exe_path
        self._exe_hash = _get_sha256_hash(exe_path)

        self.persist()

    def pre_run(self) -> None:
        """Perform pre-processing steps needed to run the ROMS simulation.

        This method partitions any required input datasets according to
        the computational domain decomposition specified in the discretization
        settings. Each dataset is divided into smaller files so that they
        can be processed in parallel by ROMS during execution.

        Raises
        ------
        ValueError
            If any input dataset exists but has not been partitioned correctly.

        Notes
        -----
        - Partitioning is based on `n_procs_x` and `n_procs_y` defined in the
          `discretization` attribute.
        - This method must be called before `run()` to ensure that ROMS can
          correctly access its input data.

        See Also
        --------
        run : Executes the compiled ROMS model.
        post_run : Performs post-processing steps after execution.
        """

        # Partition input datasets and add their paths to namelist
        if self.input_datasets is not None and all(
            [isinstance(a, ROMSInputDataset) for a in self.input_datasets]
        ):
            datasets_to_partition = [d for d in self.input_datasets if d.exists_locally]

            for f in datasets_to_partition:
                f.partition(
                    np_xi=self.discretization.n_procs_x,
                    np_eta=self.discretization.n_procs_y,
                )

        self.persist()

    def run(
        self,
        account_key: Optional[str] = None,
        walltime: Optional[str] = None,
        queue_name: Optional[str] = None,
        job_name: Optional[str] = None,
    ) -> "ExecutionHandler":
        """Execute the ROMS simulation.

        This method runs the compiled ROMS executable using the configured
        environment. If a job scheduler is available, the simulation is
        submitted as a scheduled job; otherwise, it runs as a local process.

        Parameters
        ----------
        account_key : str, optional
            The user's account key on the system (required if using a job scheduler).
        queue_name : str, optional
            The name of the scheduler queue to submit the job to. Defaults to the
            system's primary queue.
        walltime : str, optional
            The maximum allowed execution time for a scheduler job in HH:MM:SS format.
            Defaults to the queue's max walltime if a scheduler is used.
        job_name : str, optional
            The name of the job submitted to the scheduler, which also sets
            the output file name `job_name.out`.

        Returns
        -------
        ExecutionHandler
            An execution handler object tracking the simulation's execution
            status, logs, and completion.

        Raises
        ------
        ValueError
            - If the ROMS executable path is not set (`self.exe_path` is None).
            - If `account_key` is required but not provided for scheduled jobs.
        RuntimeError
            If ROMS fails to start or encounters an execution error.

        Notes
        -----
        - If a job scheduler is available, this method generates a job script and
          submits it using the appropriate scheduler command.
        - If no scheduler is available, ROMS runs as a local process using
          MPI (`mpiexec` or equivalent).
        - The number of time steps is computed based on `start_date` and `end_date`
          if they are set; otherwise, a default of 1 time step is used.

        Examples
        --------
        Running locally:
        >>> execution = simulation.run()
        >>> execution.status
        'RUNNING'

        Running with a scheduler:
        >>> execution = simulation.run(account_key="ABC123", job_name="roms_simulation")
        >>> execution.status
        'QUEUED'

        See Also
        --------
        pre_run : Prepares the input data before running.
        post_run : Handles output processing after execution.
        """

        if self.exe_path is None:
            raise ValueError(
                "C-STAR: ROMSSimulation.exe_path is None; unable to find ROMS executable."
                + "\nRun Simulation.build() first. "
                + "\n If you have already run Simulation.build(), either run it again or "
                + " add the executable path manually using Simulation.exe_path='YOUR/PATH'."
            )
        if self.discretization.n_procs_tot is None:
            raise ValueError(
                "Unable to calculate node distribution for this Simulation. "
                + "Simulation.n_procs_tot is not set"
            )

        run_length_seconds = int((self.end_date - self.start_date).total_seconds())
        n_time_steps = run_length_seconds // self.discretization.time_step

        if (queue_name is None) and (cstar_sysmgr.scheduler is not None):
            queue_name = cstar_sysmgr.scheduler.primary_queue_name
        if (walltime is None) and (cstar_sysmgr.scheduler is not None):
            walltime = cstar_sysmgr.scheduler.get_queue(queue_name).max_walltime

        output_dir = self.directory / "output"

        # Set run path to output dir for clarity: we are running in the output dir but
        # these are conceptually different:
        run_path = output_dir

        # 1. MODIFY NAMELIST
        # Copy template namelist and add all current known information
        # from this Simulation instance:
        self.update_runtime_code()

        # Now need to manually update number of time steps as it is unknown
        # outside of the context of this function:

        _replace_text_in_file(
            self.in_file,
            "__NTIMES_PLACEHOLDER__",
            str(n_time_steps),
        )

        output_dir.mkdir(parents=True, exist_ok=True)

        ## 2: RUN ROMS

        roms_exec_cmd = (
            f"{cstar_sysmgr.environment.mpi_exec_prefix} -n {self.discretization.n_procs_tot} {self.exe_path} "
            + f"{self.in_file}"
        )

        if cstar_sysmgr.scheduler is not None:
            if account_key is None:
                raise ValueError(
                    "please call Simulation.run() with a value for account_key"
                )

            job_instance = create_scheduler_job(
                commands=roms_exec_cmd,
                job_name=job_name,
                cpus=self.discretization.n_procs_tot,
                account_key=account_key,
                run_path=run_path,
                queue_name=queue_name,
                walltime=walltime,
            )

            job_instance.submit()
            self._execution_handler = job_instance
            return job_instance

        else:  # cstar_sysmgr.scheduler is None
            romsprocess = LocalProcess(commands=roms_exec_cmd, run_path=run_path)
            self._execution_handler = romsprocess
            romsprocess.start()
            return romsprocess

        self.persist()

    def post_run(self) -> None:
        """Perform post-processing steps after the ROMS simulation run.

        This method processes the output files generated by ROMS, including
        joining NetCDF output files that were produced separately
        by each processor.

        Raises
        ------
        RuntimeError
            - If `post_run` is called before `run`.
            - If the ROMS execution is not yet completed.

        Notes
        -----
        - This method searches for NetCDF files with a timestamped pattern
          (`*.??????????????.*.nc`) and merges them into unified files.
        - Partitioned files are moved to a `PARTITIONED` subdirectory
          within the output directory after merging.
        - Uses the `ncjoin` command-line tool for file merging.

        Examples
        --------
        >>> simulation.run()
        >>> simulation.post_run()
        Joining netCDF files ocean_his.*.nc...
        Joining netCDF files ocean_rst.*.nc...

        See Also
        --------
        run : Executes the ROMS simulation.
        pre_run : Prepares the simulation before execution.
        """

        if self._execution_handler is None:
            raise RuntimeError(
                "Cannot call 'ROMSSimulation.post_run()' before calling 'ROMSSimulation.run()'"
            )
        elif self._execution_handler.status != ExecutionStatus.COMPLETED:
            raise RuntimeError(
                "Cannot call 'ROMSSimulation.post_run()' until the ROMS run is completed, "
                + f"but current execution status is '{self._execution_handler.status}'"
            )

        output_dir = self.directory / "output"
        files = list(output_dir.glob("*.??????????????.*.nc"))
        unique_wildcards = {Path(fname.stem).stem + ".*.nc" for fname in files}
        if not files:
            self.log.warning("No suitable output found")
        else:
            (output_dir / "PARTITIONED").mkdir(exist_ok=True)
            for wildcard_pattern in unique_wildcards:
                # Want to go from, e.g. myfile.001.nc to myfile.*.nc, so we apply stem twice:
                self.log.info(f"Joining netCDF files {wildcard_pattern}...")
                _run_cmd(
                    f"ncjoin {wildcard_pattern}",
                    cwd=output_dir,
                    raise_on_error=True,
                )

                for F in output_dir.glob(wildcard_pattern):
                    F.rename(output_dir / "PARTITIONED" / F.name)

        self.persist()

    def restart(self, new_end_date: str | datetime) -> "ROMSSimulation":
        """Restart the ROMS simulation from the end of the current simulation, if
        possible.

        This method creates a new `ROMSSimulation` instance that continues from
        the date specified by `end_date` in the current ROMSSimulation. The
        method searches for a restart file generated by the simulation corresponding
        to this date, and proceeds if one is found.
        The new instance inherits the configuration of the current simulation but updates the
        initial conditions based on the restart file.

        Parameters
        ----------
        new_end_date : str or datetime
            The new end date for the restarted simulation. If given as a string,
            it will be parsed into a `datetime` object.

        Returns
        -------
        ROMSSimulation
            A new instance of `ROMSSimulation` that starts from `end_date`.
            file and runs until `new_end_date`.

        Raises
        ------
        FileNotFoundError
            If no restart file corresponding to the new start date is found in
            the output directory.
        ValueError
            If multiple distinct restart files match the expected restart pattern.

        Notes
        -----
        - This method searches the output directory for restart files that
          match the timestamped pattern `*_rst.YYYYMMDDHHMMSS.nc`.
        - The new simulation instance will have its `initial_conditions`
          set to the detected restart file.
        - Cached dataset information is reset for the new instance.

        Examples
        --------
        >>> new_sim = simulation.restart(new_end_date="2025-06-01")
        >>> print(new_sim.initial_conditions.location)
        /path/to/output/restart_rst.20250601000000.nc

        See Also
        --------
        post_run : Handles post-processing of ROMS output files.
        run : Executes the ROMS simulation.
        """

        new_sim = cast(ROMSSimulation, super().restart(new_end_date=new_end_date))

        restart_dir = self.directory / "output"

        new_start_date = new_sim.start_date
        restart_date_string = new_start_date.strftime("%Y%m%d%H%M%S")
        restart_wildcard = f"*_rst.{restart_date_string}.nc"
        restart_files = list(restart_dir.glob(restart_wildcard))
        if len(restart_files) == 0:
            raise FileNotFoundError(
                f"No files in {restart_dir} match the pattern "
                + f"'*_rst.{restart_date_string}.nc"
            )

        unique_restarts = {fname for fname in restart_files}

        if len(unique_restarts) > 1:
            raise ValueError(
                "Found multiple distinct restart files corresponding to "
                + f"{restart_date_string}: "
                + "\n ".join([str(rst) for rst in list(unique_restarts)])
            )

        restart_file = restart_dir / list(unique_restarts)[0]
        new_ic = ROMSInitialConditions(
            location=str(restart_file.resolve()), start_date=new_start_date
        )
        new_sim.initial_conditions = new_ic

        # Reset cached data for input datasets
        for inp in new_sim.input_datasets:
            inp._local_file_hash_cache = None
            inp._local_file_stat_cache = None
            inp.working_path = None

        return new_sim
