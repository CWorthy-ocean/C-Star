from abc import ABC, abstractmethod
from typing import List, Optional, TYPE_CHECKING

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
        input_datasets: List[InputDataset] = [],
    ):
        """
        Initialize a Component object from a base model and any additional_code or input_datasets

        Parameters:
        -----------
        base_model: BaseModel
            An object pointing to the unmodified source code of a model handling an individual
            aspect of the simulation such as biogeochemistry or ocean circulation
        additional_code: AdditionalCode or list of AdditionalCodes
            Additional code contributing to a unique instance of a base model,
            e.g. namelists, source modifications, etc.
        input_datasets: list of InputDatasets
            Any spatiotemporal data needed to run this instance of the base model
            e.g. initial conditions, surface forcing, etc.

        Returns:
        --------
        Component:
            An intialized Component object
        """

        # if "base_model" not in kwargs or not isinstance(
        #     kwargs["base_model"], BaseModel
        # ):
        #     raise ValueError(
        #         "base_model must be provided and must be an instance of BaseModel"
        #     )
        # self.base_model: BaseModel = kwargs["base_model"]
        self.base_model = base_model

        # self.additional_code: Optional["AdditionalCode"] = kwargs.get(
        #     "additional_code", None
        # )
        self.additional_code: Optional["AdditionalCode"] = additional_code or None
        # self.input_datasets: List[InputDataset] = kwargs.get("input_datasets") or []
        self.input_datasets: List[InputDataset] = list(input_datasets)

    def __str__(self) -> str:
        # Header
        name = self.__class__.__name__
        base_str = f"{name} object "
        base_str = "-" * (len(name) + 7) + "\n" + base_str
        base_str += "\n" + "-" * (len(name) + 7)

        # Attrs
        base_str += "\nBuilt from: "

        NAC = 0 if self.additional_code is None else 1

        NID = len(self.input_datasets)

        base_str += f"\n{NAC} AdditionalCode repositories (query using Component.additional_code)"
        base_str += (
            f"\n{NID} InputDataset objects (query using Component.input_datasets"
        )

        # Discretisation
        disc_str = ""
        if hasattr(self, "time_step") and self.time_step is not None:
            disc_str += "\ntime_step: " + str(self.time_step) + "s"
        if hasattr(self, "n_procs_x") and self.n_procs_x is not None:
            disc_str += (
                "\nn_procs_x: "
                + str(self.n_procs_x)
                + " (Number of x-direction processors)"
            )
        if hasattr(self, "n_procs_y") and self.n_procs_y is not None:
            disc_str += (
                "\nn_procs_y:"
                + str(self.n_procs_y)
                + " (Number of y-direction processors)"
            )
        if hasattr(self, "n_levels") and self.n_levels is not None:
            disc_str += "\nn_levels:" + str(self.n_levels)
        if hasattr(self, "nx") and self.nx is not None:
            disc_str += "\nnx:" + str(self.nx)
        if hasattr(self, "ny") and self.ny is not None:
            disc_str += "\nny:" + str(self.ny)
        if hasattr(self, "exe_path") and self.exe_path is not None:
            disc_str += "\n\nIs compiled: True"
            disc_str += "\n exe_path: " + self.exe_path
        if len(disc_str) > 0:
            disc_str = "\n\nDiscretization info:" + disc_str
        base_str += disc_str
        return base_str

    def __repr__(self) -> str:
        return self.__str__()

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
    Hold discretization information about a Component.
    """

    def __init__(
        self,
        time_step: int,
        nx: Optional[int] = None,
        ny: Optional[int] = None,
        n_levels: Optional[int] = None,
    ):
        self.time_step = time_step
        self.nx = nx
        self.ny = ny
        self.n_levels = n_levels
