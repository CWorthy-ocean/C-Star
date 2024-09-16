from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

from cstar.base.base_model import BaseModel
from cstar.base.input_dataset import InputDataset


if TYPE_CHECKING:
    from cstar.base.additional_code import AdditionalCode


class Component(ABC):
    """
    A model component that contributes to a unique Case instance.

    Attributes:
    ----------
    base_model: BaseModel
        An object pointing to the unmodified source code of a model handling an individual
        aspect of the simulation such as biogeochemistry or ocean circulation
    additional_code: AdditionalCode or list of AdditionalCodes
        Additional code contributing to a unique instance of a base model,
        e.g. namelists, source modifications, etc.
    input_datasets: list of InputDatasets
        Any spatiotemporal data needed to run this instance of the base model
        e.g. initial conditions, surface forcing, etc.
    discretization: Discretization
        Any information related to the discretization of this Component
        e.g. time step, number of vertical levels, etc.

    Methods:
    -------
    build()
        Compile any component-specific code on this machine
    pre_run()
        Execute any pre-processing actions necessary to run this component
    run()
        Run this component
    post_run()
        Execute any post-processing actions associated with this component
    """

    def __init__(
        self,
        base_model: BaseModel,
        additional_code: Optional["AdditionalCode"],
        input_datasets: list[InputDataset] | None = None,
        discretization: Optional["Discretization"] = None,
    ):
        """
        Initialize a Component object from a base model and any additional_code or input_datasets

        Parameters:
        -----------
        base_model: BaseModel
            An object pointing to the unmodified source code of a model handling an individual
            aspect of the simulation such as biogeochemistry or ocean circulation
        additional_code: AdditionalCode (Optional, default None)
            Additional code contributing to a unique instance of a base model,
            e.g. namelists, source modifications, etc.
        input_datasets: list of InputDatasets (Optional, default [])
            Any spatiotemporal data needed to run this instance of the base model
            e.g. initial conditions, surface forcing, etc.
        discretization: Discretization (Optional, default None)
            Any information related to the discretization of this Component (e.g. time step)


        Returns:
        --------
        Component:
            An intialized Component object
        """
        if not isinstance(base_model, BaseModel):
            raise ValueError(
                "base_model must be provided and must be an instance of BaseModel"
            )
        self.base_model = base_model
        self.additional_code = additional_code or None
        self.input_datasets = [] if input_datasets is None else input_datasets
        self.discretization = discretization or None

    def __str__(self) -> str:
        # Header
        name = self.__class__.__name__
        base_str = f"{name}"
        # base_str = "-" * len(name) + "\n" + base_str
        base_str += "\n" + "-" * len(name)

        # Attrs
        base_str += "\nBuilt from: "

        NAC = 0 if self.additional_code is None else 1

        NID = len(self.input_datasets)

        base_str += (
            f"\n{NAC} AdditionalCode instances (query using Component.additional_code)"
        )
        base_str += f"\n{NID} Input datasets (query using Component.input_datasets)"
        if hasattr(self, "discretization") and self.discretization is not None:
            base_str += "\n\nDiscretization:\n"
            base_str += self.discretization.__str__()
        if hasattr(self, "exe_path") and self.exe_path is not None:
            base_str += "\n\nIs compiled: True"
            base_str += "\n exe_path: " + self.exe_path
        return base_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nbase_model = <{self.base_model.__class__.__name__} instance>, "
        repr_str += f"\nadditional_code = <{self.additional_code.__class__.__name__} instance>, "
        repr_str += "\ninput_datasets = ["
        for i, inp in enumerate(self.input_datasets):
            repr_str += f"\n    <{inp.__class__.__name__} from {inp.source.basename}>, "
        repr_str = repr_str.strip(", ")
        repr_str += "],"
        repr_str += f"\ndiscretization = {self.discretization.__repr__()}"
        repr_str += "\n)"

        return repr_str

    @abstractmethod
    def build(self) -> None:
        """
        Compile any Component-specific code on this machine

        This abstract method will be implemented differently by different Component types.
        """

    @abstractmethod
    def pre_run(self) -> None:
        """
        Execute any pre-processing actions necessary to run this component.

        This abstract method will be implemented differently by different Component types.
        """

    @abstractmethod
    def run(self) -> None:
        """
        Run this component

        This abstract method will be implemented differently by different Component types.
        """
        pass

    @abstractmethod
    def post_run(self) -> None:
        """
        Execute any pre-processing actions associated with this component.

        This abstract method will be implemented differently by different Component types.
        """
        pass


class Discretization(ABC):
    """
    Holds discretization information about a Component.

    Attributes:
    -----------

    time_step: int
        The time step with which to run the Component
    """

    def __init__(
        self,
        time_step: int,
    ):
        """
        Initialize a Discretization object from basic discretization parameters

        Parameters:
        -----------
        time_step: int
            The time step with which to run the Component

        Returns:
        --------
        Discretization:
            An initialized Discretization object

        """

        self.time_step: int = time_step

    def __str__(self) -> str:
        # Discretisation
        disc_str = ""

        if hasattr(self, "time_step") and self.time_step is not None:
            disc_str += "\ntime_step: " + str(self.time_step) + "s"
        if len(disc_str) > 0:
            classname = self.__class__.__name__
            header = classname
            disc_str = header + "\n" + "-" * len(classname) + disc_str

        return disc_str

    def __repr__(self) -> str:
        repr_str = ""
        repr_str = f"{self.__class__.__name__}("
        if hasattr(self, "time_step") and self.time_step is not None:
            repr_str += f"time_step = {self.time_step}, "
        if hasattr(self, "n_levels") and self.n_levels is not None:
            repr_str += f"n_levels = {self.n_levels}, "
        if hasattr(self, "nx") and self.nx is not None:
            repr_str += f"nx = {str(self.nx)}, "
        if hasattr(self, "ny") and self.ny is not None:
            repr_str += f"ny = {self.ny}"
        repr_str += ")"
        return repr_str
