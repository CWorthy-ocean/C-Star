import warnings
import subprocess
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional, TYPE_CHECKING, List

from cstar.execution.scheduler_job import create_scheduler_job
from cstar.execution.local_process import LocalProcess
from cstar.execution.handler import ExecutionStatus
from cstar.base.utils import _replace_text_in_file, _get_sha256_hash
from cstar.base.component import Component
from cstar.roms.base_model import ROMSBaseModel
from cstar.roms.input_dataset import (
    ROMSInputDataset,
    ROMSInitialConditions,
    ROMSModelGrid,
    ROMSSurfaceForcing,
    ROMSBoundaryForcing,
    ROMSTidalForcing,
)
from cstar.roms.discretization import ROMSDiscretization
from cstar.base.additional_code import AdditionalCode

from cstar.system.manager import cstar_sysmgr

if TYPE_CHECKING:
    from cstar.roms import ROMSBaseModel
    from cstar.execution.handler import ExecutionHandler


class ROMSComponent(Component):
    """An implementation of the Component class for the UCLA Regional Ocean Modeling
    System.

    This subclass contains ROMS-specific implementations of the build(), pre_run(), run(), and post_run() methods.

    Attributes:
    -----------
    base_model: ROMSBaseModel
        An object pointing to the unmodified source code of ROMS at a specific commit
    namelists: AdditionalCode (Optional, default None)
        Namelist files contributing to a unique instance of the base model,
        to be used at runtime
    additional_source_code: AdditionalCode (Optional, default None)
        Additional source code contributing to a unique instance of a base model,
        to be included at compile time
    discretization: ROMSDiscretization
        Any information related to discretization of this ROMSComponent
        e.g. time step, number of levels, number of CPUs following each direction, etc.
    model_grid: ROMSModelGrid, optional
        The model grid InputDataset associated with this ROMSComponent
    initial_conditions: ROMSInitialConditions, optional
        The initial conditions InputDataset associated with this ROMSComponent
    tidal_forcing: ROMSTidalForcing, optional
        The tidal forcing InputDataset associated with this ROMSComponent
    surface_forcing: (list of) ROMSSurfaceForcing, optional
        list of surface forcing InputDataset objects associated with this ROMSComponent
    boundary_forcing: (list of) ROMSBoundaryForcing, optional
        list of boundary forcing InputDataset objects associated with this ROMSComponent


    Properties:
    -----------
    component_type: str
       The type of Component, in this case "ROMS"
    input_datasets: list
       A list of any input datasets associated with this instance of ROMSComponent

    Methods:
    --------
    build()
        Compiles any code associated with this configuration of ROMS
    pre_run()
        Performs pre-processing steps, such as partitioning input netcdf datasets into one file per core
    run()
        Runs the executable created by `build()`
    post_run()
        Performs post-processing steps, such as joining output netcdf files that are produced one-per-core
    """

    base_model: "ROMSBaseModel"
    additional_code: "AdditionalCode"
    discretization: "ROMSDiscretization"

    def __init__(
        self,
        base_model: "ROMSBaseModel",
        discretization: "ROMSDiscretization",
        namelists: "AdditionalCode",
        additional_source_code: "AdditionalCode",
        model_grid: Optional["ROMSModelGrid"] = None,
        initial_conditions: Optional["ROMSInitialConditions"] = None,
        tidal_forcing: Optional["ROMSTidalForcing"] = None,
        boundary_forcing: Optional[list["ROMSBoundaryForcing"]] = None,
        surface_forcing: Optional[list["ROMSSurfaceForcing"]] = None,
    ):
        """Initialize a ROMSComponent object from a ROMSBaseModel object, additional
        code, input datasets, and discretization information.

        Parameters:
        -----------
        base_model: ROMSBaseModel
            An object pointing to the unmodified source code of a model handling an individual
            aspect of the simulation such as biogeochemistry or ocean circulation
        namelists: AdditionalCode (Optional, default None)
            Namelist files contributing to a unique instance of the base model,
            to be used at runtime
        additional_source_code: AdditionalCode (Optional, default None)
            Additional source code contributing to a unique instance of a base model,
            to be included at compile time
        discretization: ROMSDiscretization
            Any information related to discretization of this ROMSComponent
            e.g. time step, number of levels, number of CPUs following each direction, etc.
        model_grid: ROMSModelGrid, optional
            The model grid InputDataset associated with this ROMSComponent
        initial_conditions: ROMSInitialConditions, optional
            The initial conditions InputDataset associated with this ROMSComponent
        tidal_forcing: ROMSTidalForcing, optional
            The tidal forcing InputDataset associated with this ROMSComponent
        surface_forcing: (list of) ROMSSurfaceForcing, optional
            list of surface forcing InputDataset objects associated with this ROMSComponent
        boundary_forcing: (list of) ROMSBoundaryForcing, optional
            list of boundary forcing InputDataset objects associated with this ROMSComponent

        Returns:
        --------
        ROMSComponent:
            An intialized ROMSComponent object
        """

        self.base_model = base_model
        self.namelists = namelists
        self.additional_source_code = additional_source_code
        self.discretization = discretization
        self.model_grid = model_grid
        self.initial_conditions = initial_conditions
        self.tidal_forcing = tidal_forcing
        self.surface_forcing = [] if surface_forcing is None else surface_forcing
        if not all([isinstance(sf, ROMSSurfaceForcing) for sf in self.surface_forcing]):
            raise TypeError(
                "ROMSComponent.surface_forcing must be a list of ROMSSurfaceForcing instances"
            )
        self.boundary_forcing = [] if boundary_forcing is None else boundary_forcing
        if not all(
            [isinstance(bf, ROMSBoundaryForcing) for bf in self.boundary_forcing]
        ):
            raise TypeError(
                "ROMSComponent.boundary_forcing must be a list of ROMSBoundaryForcing instances"
            )

        # roms-specific
        self.exe_path: Optional[Path] = None
        self._exe_hash: Optional[str] = None
        self.partitioned_files: List[Path] | None = None

        self._execution_handler: Optional["ExecutionHandler"] = None

    @property
    def in_file(self) -> Path:
        """Find the .in file associated with this ROMSComponent in
        ROMSComponent.namelists.

        ROMS requires a text file containing runtime options to run. This file is typically
        called `roms.in`, but variations occur and C-Star only enforces that
        the file has a `.in` extension.

        This property finds any ".in" or ".in_TEMPLATE" files in ROMSComponent.namelists.files
        and assigns the result (less any _TEMPLATE suffix) to ROMSComponent.in_file.

        If there are multiple `.in` files, or none, errors are raised.
        This property is used by ROMSComponent.run()
        """
        in_files = []
        if self.namelists is None:
            raise ValueError(
                "ROMSComponent.namelists not set."
                + " ROMS reuires a runtime options file "
                + "(typically roms.in)"
            )

        in_files = [
            fname.replace(".in_TEMPLATE", ".in")
            for fname in self.namelists.files
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
                "No '.in' file found in ROMSComponent.namelists."
                + "ROMS expects a runtime options file with the '.in'"
                + "extension, e.g. roms.in"
            )
        else:
            if self.namelists.working_path is not None:
                return self.namelists.working_path / in_files[0]
            else:
                return Path(in_files[0])

    def __str__(self) -> str:
        base_str = super().__str__()
        if hasattr(self, "namelists") and self.namelists is not None:
            NN = len(self.namelists.files)
        else:
            NN = 0
        base_str += f"\nnamelists: {self.namelists.__class__.__name__} instance with {NN} files (query using Component.namelists)"
        if hasattr(self, "exe_path") and self.exe_path is not None:
            base_str += "\n\nIs compiled: True"
            base_str += "\n exe_path: " + str(self.exe_path)
        if hasattr(self, "model_grid") and self.model_grid is not None:
            base_str += (
                f"\nmodel_grid = <{self.model_grid.__class__.__name__} instance>"
            )
        if hasattr(self, "initial_conditions") and self.initial_conditions is not None:
            base_str += f"\ninitial_conditions = <{self.initial_conditions.__class__.__name__} instance>"
        if hasattr(self, "tidal_forcing") and self.tidal_forcing is not None:
            base_str += (
                f"\ntidal_forcing = <{self.tidal_forcing.__class__.__name__} instance>"
            )
        if hasattr(self, "surface_forcing") and len(self.surface_forcing) > 0:
            base_str += (
                f"\nsurface_forcing = <list of {len(self.surface_forcing)} "
                + f"{self.surface_forcing[0].__class__.__name__} instances>"
            )
        if hasattr(self, "boundary_forcing") and len(self.boundary_forcing) > 0:
            base_str += (
                f"\nboundary_forcing = <list of {len(self.boundary_forcing)} "
                + f"{self.boundary_forcing[0].__class__.__name__} instances>"
            )

        if hasattr(self, "discretization") and self.discretization is not None:
            base_str += "\n\nDiscretization:\n"
            base_str += self.discretization.__str__()

        return base_str

    def __repr__(self) -> str:
        repr_str = super().__repr__().rstrip(")")

        if hasattr(self, "namelists") and self.namelists is not None:
            repr_str += (
                f"\nnamelists = <{self.namelists.__class__.__name__} instance>, "
            )
        if hasattr(self, "discretization") and self.discretization is not None:
            repr_str += f"\ndiscretization = {self.discretization.__repr__()}"

        if hasattr(self, "model_grid") and self.model_grid is not None:
            repr_str += (
                f"\nmodel_grid = <{self.model_grid.__class__.__name__} instance>"
            )
        if hasattr(self, "initial_conditions") and self.initial_conditions is not None:
            repr_str += f"\ninitial_conditions = <{self.initial_conditions.__class__.__name__} instance>"
        if hasattr(self, "tidal_forcing") and self.tidal_forcing is not None:
            repr_str += (
                f"\ntidal_forcing = <{self.tidal_forcing.__class__.__name__} instance>"
            )
        if hasattr(self, "surface_forcing") and len(self.surface_forcing) > 0:
            repr_str += (
                f"\nsurface_forcing = <list of {len(self.surface_forcing)} "
                + f"{self.surface_forcing[0].__class__.__name__} instances>"
            )
        if hasattr(self, "boundary_forcing") and len(self.boundary_forcing) > 0:
            repr_str += (
                f"\nboundary_forcing = <list of {len(self.boundary_forcing)} "
                + f"{self.boundary_forcing[0].__class__.__name__} instances>"
            )
        repr_str += "\n)"

        return repr_str

    @classmethod
    def from_dict(cls, component_dict):
        """Construct a ROMSComponent instance from a dictionary of kwargs.

        Parameters:
        -----------
        component_dict (dict):
           A dictionary of keyword arguments used to construct this component.

        Returns:
        --------
        ROMSComponent
           An initialized ROMSComponent object
        """

        component_kwargs = {}
        # Construct the BaseModel instance
        base_model_kwargs = component_dict.get("base_model")
        if base_model_kwargs is None:
            raise ValueError(
                "Cannot construct a ROMSComponent instance without a "
                + "ROMSBaseModel object, but could not find 'base_model' entry"
            )
        base_model = ROMSBaseModel(**base_model_kwargs)

        component_kwargs["base_model"] = base_model

        # Construct the Discretization instance
        discretization_kwargs = component_dict.get("discretization")
        if discretization_kwargs is None:
            raise ValueError(
                "Cannot construct a ROMSComponent instance without a "
                + "ROMSDiscretization object, but could not find 'discretization' entry"
            )
        discretization = ROMSDiscretization(**discretization_kwargs)

        component_kwargs["discretization"] = discretization

        # Construct any AdditionalCode instance associated with namelists
        namelists_kwargs = component_dict.get("namelists")
        if namelists_kwargs is None:
            raise ValueError(
                "Cannot construct a ROMSComponent instance without a runtime "
                + "namelist, but could not find 'namelists' entry"
            )
        namelists = AdditionalCode(**namelists_kwargs)
        component_kwargs["namelists"] = namelists

        # Construct any AdditionalCode instance associated with source mods
        additional_source_code_kwargs = component_dict.get("additional_source_code")
        if additional_source_code_kwargs is None:
            raise NotImplementedError(
                "This version of C-Star does not support ROMSComponent instances "
                + "without code to be included at compile time (.opt files, etc.), but "
                + "could not find an 'additional_source_code' entry."
            )

        additional_source_code = AdditionalCode(**additional_source_code_kwargs)
        component_kwargs["additional_source_code"] = additional_source_code

        # Construct any ROMSModelGrid instance:
        model_grid_kwargs = component_dict.get("model_grid")
        if model_grid_kwargs is not None:
            component_kwargs["model_grid"] = ROMSModelGrid(**model_grid_kwargs)

        # Construct any ROMSInitialConditions instance:
        initial_conditions_kwargs = component_dict.get("initial_conditions")
        if initial_conditions_kwargs is not None:
            component_kwargs["initial_conditions"] = ROMSInitialConditions(
                **initial_conditions_kwargs
            )

        # Construct any ROMSTidalForcing instance:
        tidal_forcing_kwargs = component_dict.get("tidal_forcing")
        if tidal_forcing_kwargs is not None:
            component_kwargs["tidal_forcing"] = ROMSTidalForcing(**tidal_forcing_kwargs)

        # Construct any ROMSBoundaryForcing instances:
        boundary_forcing_entries = component_dict.get("boundary_forcing", [])
        if len(boundary_forcing_entries) > 0:
            component_kwargs["boundary_forcing"] = []
        if isinstance(boundary_forcing_entries, dict):
            boundary_forcing_entries = [
                boundary_forcing_entries,
            ]
        for bf_kwargs in boundary_forcing_entries:
            component_kwargs["boundary_forcing"].append(
                ROMSBoundaryForcing(**bf_kwargs)
            )

        # Construct any ROMSSurfaceForcing instances:
        surface_forcing_entries = component_dict.get("surface_forcing", [])
        if len(surface_forcing_entries) > 0:
            component_kwargs["surface_forcing"] = []
        if isinstance(surface_forcing_entries, dict):
            surface_forcing_entries = [
                surface_forcing_entries,
            ]
        for sf_kwargs in surface_forcing_entries:
            component_kwargs["surface_forcing"].append(ROMSSurfaceForcing(**sf_kwargs))

        return cls(**component_kwargs)

    @property
    def component_type(self) -> str:
        return "ROMS"

    @property
    def _namelist_modifications(self) -> list[dict]:
        """List of modifications to be made to template namelist files.

        This property takes the current state of ROMSComponent and returns a
        list of dictionaries (one dictionary per namelist file) whose keys
        are placeholder strings to replace in the template namelist,
        and whose values are their replacements.

        The property uses the `partitioned_files_to_namelist_string` local
        helper function to format input dataset paths such that ROMS can
        recognise them.

        Related:
        --------
        ROMSComponent.update_namelists():
           uses this property to create a modified namelist from a template
        """

        # Helper function for formatting:
        def partitioned_files_to_namelist_string(input_dataset):
            """Take a ROMSInputDataset that has been partitioned and return a ROMS
            namelist-compatible string pointing to it e.g. path/to/roms_file.232.nc -> '
            path/to/roms_file.nc'."""

            unique_paths = {
                str(Path(f).parent / (Path(Path(f).stem).stem + ".nc"))
                for f in input_dataset.partitioned_files
            }
            return "\n     ".join(sorted(list(unique_paths)))

        # Initialise the list of dictionaries:
        if self.namelists is None:
            raise ValueError(
                "attempted to access "
                + "ROMSComponent._namelist_modifications, but "
                + "ROMSComponent.namelists is None"
            )

        namelist_modifications: list[dict] = [{} for f in self.namelists.files]

        ################################################################################
        # 'roms.in' file modifications (the only namelist to modify as of 2024-10-01):
        ################################################################################
        # First figure out which namelist is the one to modify
        if "roms.in_TEMPLATE" in self.namelists.files:
            nl_idx = self.namelists.files.index("roms.in_TEMPLATE")
        else:
            raise ValueError(
                "could not find expected template namelist file "
                + "roms.in_TEMPLATE to modify. "
                + "ROMS requires a namelist file to run."
            )

        # Time step entry
        namelist_modifications[nl_idx]["__TIMESTEP_PLACEHOLDER__"] = (
            self.discretization.time_step
        )

        # Grid file entry
        if self.model_grid is not None:
            if len(self.model_grid.partitioned_files) == 0:
                raise ValueError(
                    "could not find a local path to a partitioned"
                    + "ROMS grid file. Run ROMSComponent.pre_run() [or "
                    + "Case.pre_run() if running a Case] to partition "
                    + "ROMS input datasets and try again."
                )

            namelist_modifications[nl_idx]["__GRID_FILE_PLACEHOLDER__"] = (
                partitioned_files_to_namelist_string(self.model_grid)
            )

        # Initial conditions entry
        if self.initial_conditions is not None:
            if len(self.initial_conditions.partitioned_files) == 0:
                raise ValueError(
                    "could not find a local path to a partitioned"
                    + "ROMS initial file. Run ROMSComponent.pre_run() [or "
                    + "Case.pre_run() if running a Case] to partition "
                    + "ROMS input datasets and try again."
                )

            namelist_modifications[nl_idx]["__INITIAL_CONDITION_FILE_PLACEHOLDER__"] = (
                partitioned_files_to_namelist_string(self.initial_conditions)
            )

        # Forcing files entry
        namelist_forcing_str = ""
        for sf in self.surface_forcing:
            if len(sf.partitioned_files) > 0:
                namelist_forcing_str += (
                    "\n     " + partitioned_files_to_namelist_string(sf)
                )
        for bf in self.boundary_forcing:
            if len(bf.partitioned_files) > 0:
                namelist_forcing_str += (
                    "\n     " + partitioned_files_to_namelist_string(bf)
                )
        if self.tidal_forcing is not None:
            if len(self.tidal_forcing.partitioned_files) == 0:
                raise ValueError(
                    "ROMSComponent has tidal_forcing attribute "
                    + "but could not find a local path to a partitioned "
                    + "tidal forcing file. Run ROMSComponent.pre_run() "
                    + "[or Case.pre_run() if building a Case] "
                    + " to partition ROMS input datasets and try again."
                )
            namelist_forcing_str += "\n     " + partitioned_files_to_namelist_string(
                self.tidal_forcing
            )

        namelist_modifications[nl_idx]["__FORCING_FILES_PLACEHOLDER__"] = (
            namelist_forcing_str.lstrip()
        )

        # MARBL settings filepaths entries
        ## NOTE: WANT TO RAISE IF PLACEHOLDER IS IN NAMELIST BUT not Path(marbl_file.exists())
        if "marbl_in" in self.namelists.files:
            if self.namelists.working_path is None:
                raise ValueError(
                    "ROMSComponent.namelists does not have a "
                    + "'working_path' attribute. "
                    + "Run ROMSComponent.namelists.get() and try again"
                )
            namelist_modifications[nl_idx]["__MARBL_SETTINGS_FILE_PLACEHOLDER__"] = str(
                self.namelists.working_path / "marbl_in"
            )

        if "marbl_tracer_output_list" in self.namelists.files:
            if self.namelists.working_path is None:
                raise ValueError(
                    "ROMSComponent.namelists does not have a "
                    + "'working_path' attribute. "
                    + "Run ROMSComponent.namelists.get() and try again"
                )

            namelist_modifications[nl_idx]["__MARBL_TRACER_LIST_FILE_PLACEHOLDER__"] = (
                str(self.namelists.working_path / "marbl_tracer_output_list")
            )

        if "marbl_diagnostic_output_list" in self.namelists.files:
            if self.namelists.working_path is None:
                raise ValueError(
                    "ROMSComponent.namelists does not have a "
                    + "'working_path' attribute. "
                    + "Run ROMSComponent.namelists.get() and try again"
                )

            namelist_modifications[nl_idx]["__MARBL_DIAG_LIST_FILE_PLACEHOLDER__"] = (
                str(self.namelists.working_path / "marbl_diagnostic_output_list")
            )

        return namelist_modifications

    def update_namelists(self):
        """Update ROMSComponent.namelists.modified_files based on current state.

        This method loops over the ROMSComponent.namelists.files list, and:
        1. Creates modifiable copies of any template namelist files
        2. Replaces placeholder strings in the modifiable namelists based on
           the current ROMSComponent state
        3. Updates ROMSComponent.namelists.modified_files
        """

        no_template_found = True
        for nl_idx, nl_fname in enumerate(self.namelists.files):
            nl_path = self.namelists.working_path / nl_fname
            if str(nl_fname)[-9:] == "_TEMPLATE":
                no_template_found = False
                mod_nl_path = Path(str(nl_path)[:-9])
                shutil.copy(nl_path, mod_nl_path)
                for placeholder, replacement in self._namelist_modifications[
                    nl_idx
                ].items():
                    _replace_text_in_file(mod_nl_path, placeholder, str(replacement))
                self.namelists.modified_files[nl_idx] = mod_nl_path
        if no_template_found:
            warnings.warn(
                "WARNING: No editable namelist found to set ROMS runtime parameters. "
                + "Expected to find a file in ROMSComponent.namelists"
                + " with the suffix '_TEMPLATE' on which to base the ROMS namelist."
                + "\n********************************************************"
                + "\nANY MODEL PARAMETERS SET IN C-STAR WILL NOT BE APPLIED."
                + "\n********************************************************"
            )

    @property
    def input_datasets(self) -> list:
        """List all ROMSInputDataset objects associated with this ROMSComponent."""

        input_datasets: List[ROMSInputDataset] = []
        if self.model_grid is not None:
            input_datasets.append(self.model_grid)
        if self.initial_conditions is not None:
            input_datasets.append(self.initial_conditions)
        if self.tidal_forcing is not None:
            input_datasets.append(self.tidal_forcing)
        if len(self.boundary_forcing) > 0:
            input_datasets.extend(self.boundary_forcing)
        if len(self.surface_forcing) > 0:
            input_datasets.extend(self.surface_forcing)
        return input_datasets

    def to_dict(self) -> dict:
        # Docstring is inherited

        component_dict = super().to_dict()
        # additional source code
        namelists = getattr(self, "namelists")
        if namelists is not None:
            namelists_info = {}
            namelists_info["location"] = namelists.source.location
            if namelists.subdir is not None:
                namelists_info["subdir"] = namelists.subdir
            if namelists.checkout_target is not None:
                namelists_info["checkout_target"] = namelists.checkout_target
            if namelists.files is not None:
                namelists_info["files"] = namelists.files

            component_dict["namelists"] = namelists_info

        # Discretization
        component_dict["discretization"] = self.discretization.__dict__

        # InputDatasets:
        if self.model_grid is not None:
            component_dict["model_grid"] = self.model_grid.to_dict()
        if self.initial_conditions is not None:
            component_dict["initial_conditions"] = self.initial_conditions.to_dict()
        if self.tidal_forcing is not None:
            component_dict["tidal_forcing"] = self.tidal_forcing.to_dict()
        if len(self.surface_forcing) > 0:
            component_dict["surface_forcing"] = [
                sf.to_dict() for sf in self.surface_forcing
            ]
        if len(self.boundary_forcing) > 0:
            component_dict["boundary_forcing"] = [
                bf.to_dict() for bf in self.boundary_forcing
            ]

        return component_dict

    def setup(
        self,
        additional_source_code_dir: str | Path,
        namelist_dir: str | Path,
        input_datasets_target_dir: Optional[str | Path] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> None:
        """Set up this ROMSComponent instance locally.

        This method ensures the ROMSBaseModel is correctly configured, and
        that any additional code and input datasets corresponding to the
        chosen simulation period (defined by `start_date` and `end_date`)
        are made available in the chosen `additional_code_target_dir` and
        `input_datasets_target_dir` directories

        Parameters:
        -----------
        additional_source_code_dir (str or Path):
           The directory in which to save local copies of the files described by
           ROMSComponent.additional_source_code
        namelist_dir (str or Path):
           The directory in which to save local copies of the files described by
           ROMSComponent.namelists
        input_datasets_target_dir (str or Path):
           The directory in which to make locally accessible the input datasets
           described by ROMSComponent.input_datasets
        start_date (datetime.datetime):
           The date from which the ROMSComponent is expected to be run. Used to
           determine which input datasets are needed as part of this setup call.
        end_date (datetime.datetime):
           The date until which the ROMSComponent is expected to be run. Used to
           determine which input datasets are needed as part of this setup call.
        """
        # Setup BaseModel
        infostr = f"Configuring {self.__class__.__name__}"
        print(infostr + "\n" + "-" * len(infostr))
        self.base_model.handle_config_status()

        # Additional source code
        print(
            "\nFetching additional source code..."
            + "\n----------------------------------"
        )
        if self.additional_source_code is not None:
            self.additional_source_code.get(additional_source_code_dir)

        # Namelists
        print("\nFetching namelists... " + "\n----------------------")
        if self.namelists is not None:
            self.namelists.get(namelist_dir)

        # InputDatasets
        print("\nFetching input datasets..." + "\n--------------------------")
        for inp in self.input_datasets:
            # Download input dataset if its date range overlaps Case's date range
            if (
                ((inp.start_date is None) or (inp.end_date is None))
                or ((start_date is None) or (end_date is None))
                or (inp.start_date <= end_date)
                and (end_date >= start_date)
            ):
                if input_datasets_target_dir is None:
                    raise ValueError(
                        "ROMSComponent.input_datasets has entries "
                        + f" in the specified date range {start_date},{end_date}, "
                        + "but ROMSComponent.setup() did not receive "
                        + "an input_datasets_target_dir argument"
                    )

                inp.get(
                    local_dir=input_datasets_target_dir,
                    start_date=start_date,
                    end_date=end_date,
                )

    def build(self, rebuild: bool = False) -> None:
        """Compiles any code associated with this configuration of ROMS.

        Compilation occurs in the directory
        `ROMSComponent.additional_source_code.working_path
        This method sets the ROMSComponent `exe_path` attribute.

        Parameters
        ----------
        rebuild (bool, default False):
            Will force the recompilation of ROMS even if the executable
            already exists and there is no apparent reason to recompile.
        """

        if self.additional_source_code is None:
            raise ValueError(
                "Unable to compile ROMSComponent: "
                + "\nROMSComponent.additional_source_code is None."
                + "\n Compile-time files are needed to build ROMS"
            )

        build_dir = self.additional_source_code.working_path
        if build_dir is None:
            raise ValueError(
                "Unable to compile ROMSComponent: "
                + "\nROMSComponent.additional_source_code.working_path is None."
                + "\n Call ROMSComponent.additional_source_code.get() and try again"
            )

        exe_path = build_dir / "roms"
        if (
            (exe_path.exists())
            and (self.additional_source_code.exists_locally)
            and (self._exe_hash is not None)
            and (_get_sha256_hash(exe_path) == self._exe_hash)
            and not rebuild
        ):
            print(
                f"ROMS has already been built at {exe_path}, and "
                "the source code appears not to have changed. "
                "If you would like to recompile, call "
                "ROMSComponent.build(rebuild = True)"
            )
            return

        if (build_dir / "Compile").is_dir():
            make_clean_result = subprocess.run(
                "make compile_clean",
                cwd=build_dir,
                shell=True,
                capture_output=True,
                text=True,
            )
            if make_clean_result.returncode != 0:
                raise RuntimeError(
                    f"Error {make_clean_result.returncode} when compiling ROMS. STDERR stream: "
                    + f"\n {make_clean_result.stderr}"
                )

        print("Compiling UCLA-ROMS configuration...")
        make_roms_result = subprocess.run(
            f"make COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=build_dir,
            shell=True,
            capture_output=True,
            text=True,
        )
        if make_roms_result.returncode != 0:
            raise RuntimeError(
                f"Error {make_roms_result.returncode} when compiling ROMS. STDERR stream: "
                + f"\n {make_roms_result.stderr}"
            )

        print(f"UCLA-ROMS compiled at {build_dir}")

        self.exe_path = exe_path
        self._exe_hash = _get_sha256_hash(exe_path)

    def pre_run(self) -> None:
        """Performs pre-processing steps associated with this ROMSComponent object.

        At present the pre-run method exclusively calls ROMSInputDataset.partition() on
        any locally present datasets.
        """

        # Partition input datasets and add their paths to namelist
        if self.input_datasets is not None and all(
            [isinstance(a, ROMSInputDataset) for a in self.input_datasets]
        ):
            datasets_to_partition = [d for d in self.input_datasets if d.exists_locally]

            for f in datasets_to_partition:
                # fname = f.source.basename
                f.partition(
                    np_xi=self.discretization.n_procs_x,
                    np_eta=self.discretization.n_procs_y,
                )

    def run(
        self,
        n_time_steps: Optional[int] = None,
        account_key: Optional[str] = None,
        output_dir: Optional[str | Path] = None,
        walltime: Optional[str] = None,
        queue_name: Optional[str] = None,
        job_name: Optional[str] = None,
    ) -> "ExecutionHandler":
        """Runs the executable created by `build()`

        This method creates a temporary file to be submitted to the job scheduler (if any)
        on the calling machine, then submits it. By default the job requests the maximum
        walltime. It calculates the number of nodes and cores-per-node to request based on
        the number of cores required by the job, `ROMSComponent.discretization.n_procs_tot`.

        Parameters:
        -----------
        account_key: str, default None
            The users account key on the system
        output_dir: str or Path:
            The path to the directory in which model output will be saved. This is by default
            the directory from which the ROMS executable will be called.
        walltime: str, default cstar.system.manager.environment.environment.max_walltime
            The requested length of the job, HH:MM:SS
        job_name: str, default 'my_roms_run'
            The name of the job submitted to the scheduler, which also sets the output file name
            `job_name.out`
        """
        if self.exe_path is None:
            raise ValueError(
                "C-STAR: ROMSComponent.exe_path is None; unable to find ROMS executable."
                + "\nRun Component.build() first. "
                + "\n If you have already run Component.build(), either run it again or "
                + " add the executable path manually using Component.exe_path='YOUR/PATH'."
            )
        if (queue_name is None) and (cstar_sysmgr.scheduler is not None):
            queue_name = cstar_sysmgr.scheduler.primary_queue_name
        if (walltime is None) and (cstar_sysmgr.scheduler is not None):
            walltime = cstar_sysmgr.scheduler.get_queue(queue_name).max_walltime

        if output_dir is None:
            output_dir = self.exe_path.parent
        output_dir = Path(output_dir)

        # Set run path to output dir for clarity: we are running in the output dir but
        # these are conceptually different:
        run_path = output_dir

        # 1. MODIFY NAMELIST
        # Copy template namelist and add all current known information
        # from this Component instance:
        self.update_namelists()

        # Now need to manually update number of time steps as it is unknown
        # outside of the context of this function:

        # Check if n_time_steps is None, indicating it was not explicitly set
        if n_time_steps is None:
            n_time_steps = 1
            warnings.warn(
                "n_time_steps not explicitly set, using default value of 1. "
                "Please call ROMSComponent.run() with the n_time_steps argument "
                "to specify the length of the run.",
                UserWarning,
            )
        assert isinstance(n_time_steps, int)

        # run_infile = self.namelists.working_path / self.in_file
        _replace_text_in_file(
            # run_infile,
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

        if self.discretization.n_procs_tot is None:
            raise ValueError(
                "Unable to calculate node distribution for this Component. "
                + "Component.n_procs_tot is not set"
            )
        if cstar_sysmgr.scheduler is not None:
            if account_key is None:
                raise ValueError(
                    "please call Component.run() with a value for account_key"
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

    def post_run(self, output_dir: Optional[str | Path] = None) -> None:
        """Performs post-processing steps associated with this ROMSComponent object.

        This method goes through any netcdf files produced by the model in
        `output_dir` and joins netcdf files that are produced separately by each processor.

        Parameters:
        -----------
        output_dir: str | Path
            The directory in which output was produced by the run
        """

        if self._execution_handler is None:
            raise RuntimeError(
                "Cannot call 'ROMSComponent.post_run()' before calling 'ROMSComponent.run()'"
            )
        elif self._execution_handler.status != ExecutionStatus.COMPLETED:
            raise RuntimeError(
                "Cannot call 'ROMSComponent.post_run()' until the ROMS run is completed, "
                + f"but current execution status is '{self._execution_handler.status}'"
            )

        if output_dir is None:
            # This should not be necessary, it allows output_dir to appear
            # optional for signature compatibility in linting, see
            # https://github.com/CWorthy-ocean/C-Star/issues/115
            # https://github.com/CWorthy-ocean/C-Star/issues/116
            raise ValueError("ROMSComponent.post_run() expects an output_dir parameter")

        output_dir = Path(output_dir)
        files = list(output_dir.glob("*.??????????????.*.nc"))
        unique_wildcards = {Path(fname.stem).stem + ".*.nc" for fname in files}
        if not files:
            print("no suitable output found")
        else:
            (output_dir / "PARTITIONED").mkdir(exist_ok=True)
            for wildcard_pattern in unique_wildcards:
                # Want to go from, e.g. myfile.001.nc to myfile.*.nc, so we apply stem twice:

                print(f"Joining netCDF files {wildcard_pattern}...")
                ncjoin_result = subprocess.run(
                    f"ncjoin {wildcard_pattern}",
                    cwd=output_dir,
                    capture_output=True,
                    text=True,
                    shell=True,
                )
                if ncjoin_result.returncode != 0:
                    raise RuntimeError(
                        f"Error {ncjoin_result.returncode} while joining ROMS output. "
                        + f"STDERR stream:\n {ncjoin_result.stderr}"
                    )
                for F in output_dir.glob(wildcard_pattern):
                    F.rename(output_dir / "PARTITIONED" / F.name)

    def restart(self, new_start_date: datetime, restart_dir: str | Path):
        """Returns a new ROMSComponent instance initialized from a restart file.

        This method searches `restart_dir` for a ROMS restart file
        corresponding to `new_start_date`, and returns a new ROMSComponent
        instance whose initial_conditions attribute points to the
        restart file.

        Parameters:
        -----------
        new_start_date (datetime):
           The desired start date of the restarted ROMSComponent
        restart_dir (str or Path):
           The directory in which to find a restart file
        Returns:
        --------
        new_component (ROMSComponent):
           The new ROMSComponent instance with initial conditions
           specified by the restart file.
        """

        restart_dir = Path(restart_dir)

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

        import copy

        new_component = copy.deepcopy(self)
        new_component.initial_conditions = new_ic

        # Reset cached data for input datasets
        for inp in new_component.input_datasets:
            inp._local_file_hash_cache = None
            inp._local_file_stat_cache = None
            inp.working_path = None

        return new_component
