from pathlib import Path
from typing import Optional
from cstar.base import ExternalCodeBase, AdditionalCode, Discretization
from datetime import datetime
import copy
import warnings
import dateutil
from abc import ABC, abstractmethod
from cstar.execution.local_process import LocalProcess
from cstar.execution.handler import ExecutionStatus, ExecutionHandler
import pickle


class Simulation(ABC):
    def __init__(
        self,
        name: str,
        directory: str | Path,
        runtime_code: Optional["AdditionalCode"],
        compile_time_code: Optional["AdditionalCode"],
        discretization: Optional["Discretization"],
        codebase: Optional["ExternalCodeBase"] = None,
        start_date: Optional[str | datetime] = None,
        end_date: Optional[str | datetime] = None,
        valid_start_date: Optional[str | datetime] = None,
        valid_end_date: Optional[str | datetime] = None,
    ):
        self.directory: Path = Path(directory).resolve()
        self.name = name

        # Process valid date ranges
        self.valid_start_date = self._parse_date(
            date=valid_start_date, field_name="Valid start date"
        )
        self.valid_end_date = self._parse_date(
            date=valid_end_date, field_name="Valid end date"
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

        self.codebase = codebase if codebase is not None else self.default_codebase
        self.runtime_code = runtime_code or None
        self.compile_time_code = compile_time_code or None
        self.discretization = discretization or None

    def _validate_caseroot(self, caseroot: str | Path) -> Path:
        """Validates and resolves the caseroot directory."""
        resolved_caseroot = Path(caseroot).resolve()
        if resolved_caseroot.exists() and (
            not resolved_caseroot.is_dir() or any(resolved_caseroot.iterdir())
        ):
            raise FileExistsError(
                f"Your chosen caseroot {caseroot} exists and is not an empty directory."
                "\nIf you have previously created this case, use "
                f"\nmy_case = Case.restore(caseroot={caseroot!r})"
                "\n to restore it"
            )
        return resolved_caseroot

    def _parse_date(
        self, date: Optional[str | datetime], field_name: str
    ) -> Optional[datetime]:
        """Converts a date string to a datetime object if it's not None."""
        if date is None:
            warnings.warn(
                f"{field_name} not provided. Unable to check if simulation dates are out of range.",
                RuntimeWarning,
            )
            return None
        return date if isinstance(date, datetime) else dateutil.parser.parse(date)

    def _get_date_or_fallback(
        self,
        date: Optional[str | datetime],
        fallback: Optional[datetime],
        field_name: str,
    ) -> datetime:
        """Ensures a date is set, using a fallback if needed."""
        parsed_date = self._parse_date(date=date, field_name=field_name)

        if parsed_date is None:  # If no date is provided, use the fallback
            if fallback is not None:
                warnings.warn(f"{field_name} not provided. Defaulting to {fallback}.")
                return fallback
            raise ValueError(f"Neither {field_name} nor a valid fallback was provided.")

        return parsed_date  # Always returns a valid datetime

    def _validate_date_range(self):
        """Checks that start_date and end_date are within valid ranges."""
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

    @property
    @abstractmethod
    def default_codebase(self) -> ExternalCodeBase:
        """Each subclass must provide a default CodeBase instance."""
        pass

    @classmethod
    @abstractmethod
    def from_dict(self, arg_dict):
        pass

    @abstractmethod
    def to_dict(self) -> dict:
        pass

    @classmethod
    @abstractmethod
    def from_blueprint(self):
        """Construct this component instance from a dictionary of kwargs.

        This method is implemented separately for different subclasses of Component.
        """
        # This should basically be `from_dict` +
        pass

    @abstractmethod
    def to_blueprint(self) -> None:
        # This should be `to_dict`+
        # i.e. subclass calls to_dict which calls super.to_dict to get a basic
        # dictionary to fill in more thoroughly
        # then to_blueprint just dumps the dict
        pass

    @abstractmethod
    def setup(self) -> None:
        pass

    def persist(self) -> None:
        if (
            (hasattr(self, "_execution_handler"))
            and (isinstance(self._execution_handler, LocalProcess))
            and (self._execution_handler.status == ExecutionStatus.RUNNING)
        ):
            raise RuntimeError(
                "Simulation.persist() was called, but at least one "
                "component is currently running in a local process. Await "
                "completion or use LocalProcess.cancel(), then try again"
            )

        with open(f"{self.directory}/simulation_state.pkl", "wb") as state_file:
            pickle.dump(self, state_file)

    @classmethod
    def restore(cls, directory: str | Path) -> "Simulation":
        directory = Path(directory)
        with open(f"{directory}/simulation_state.pkl", "rb") as state_file:
            simulation_instance = pickle.load(state_file)
        return simulation_instance

    @abstractmethod
    def build(self, rebuild=False) -> None:
        pass

    @abstractmethod
    def pre_run(self) -> None:
        pass

    @abstractmethod
    def run(
        self,
        account_key: Optional[str] = None,
        walltime: Optional[str] = None,
        queue_name: Optional[str] = None,
        job_name: Optional[str] = None,
    ) -> "ExecutionHandler":
        pass

    @abstractmethod
    def post_run(self) -> None:
        pass

    def restart(self, new_end_date: str | datetime) -> "Simulation":
        # This just sets dates, restart files etc. should be set in
        # subclasses
        new_sim = copy.deepcopy(self)
        new_sim.start_date = self.end_date
        new_sim.directory = (
            new_sim.directory
            / f"RESTART_{new_sim.start_date.strftime(format='%Y%m%d_%H%M%S')}"
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
