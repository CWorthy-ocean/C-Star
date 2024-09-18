from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING, get_type_hints

from cstar.base.base_model import BaseModel

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
    discretization: Discretization
         Any information related to the discretization of this Component
         e.g. time step, number of vertical levels, etc.
     additional_code: AdditionalCode
         Additional code contributing to a unique instance of a base model,
         e.g. namelists, source modifications, etc.

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

    base_model: BaseModel
    discretization: Optional["Discretization"]
    additional_code: Optional["AdditionalCode"]

    def __init__(
        self,
        base_model: BaseModel,
        additional_code: Optional["AdditionalCode"] = None,
        discretization: Optional["Discretization"] = None,
    ):
        """
        Initialize a Component object from a base model and any discretization information and additional_code

        Parameters:
        -----------
        base_model: BaseModel
            An object pointing to the unmodified source code of a model handling an individual
            aspect of the simulation such as biogeochemistry or ocean circulation
        discretization: Discretization (Optional, default None)
            Any information related to the discretization of this Component (e.g. time step)
        additional_code: AdditionalCode (Optional, default None)
            Additional code contributing to a unique instance of a base model,
            e.g. namelists, source modifications, etc.

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
        self.discretization = discretization or None

    @classmethod
    def from_dict(cls, component_dict):
        """docstring"""

        base_model_entry = component_dict.get("base_model", None)
        if base_model_entry is None:
            raise ValueError("Component 'base_model' entry is missing.")

        # BaseModel
        base_model_class = get_type_hints(cls).get("base_model")
        if isinstance(base_model_entry, base_model_class):
            base_model = base_model_entry
        elif isinstance(base_model_entry, dict):
            base_model = base_model_class(**base_model_entry)

        # AdditionalCode
        additional_code_entry = component_dict.get("additional_code", None)
        if isinstance(additional_code_entry, AdditionalCode) or (
            additional_code_entry is None
        ):
            additional_code = additional_code_entry
        elif isinstance(additional_code_entry, dict):
            additional_code = AdditionalCode(**additional_code_entry)

        # Discretization
        discretization_entry = component_dict.get("discretization", None)
        discretization_class = get_type_hints(cls).get("discretization")
        if isinstance(discretization_entry, discretization_class) or (
            discretization_entry is None
        ):
            discretization = discretization_entry
        elif isinstance(discretization_entry, dict):
            discretization = discretization_class(**discretization_entry)

        return cls(
            base_model=base_model,
            additional_code=additional_code,
            discretization=discretization,
        )

    def to_dict(self) -> dict:
        component_dict = {}

        # BaseModel
        base_model_info = {}
        base_model_info["name"] = self.base_model.name
        base_model_info["source_repo"] = self.base_model.source_repo
        base_model_info["checkout_target"] = self.base_model.checkout_target

        component_dict["base_model"] = base_model_info

        # Discretization
        if self.discretization is not None:
            discretization_info = {}
            for thisattr in vars(self.discretization).keys():
                discretization_info[thisattr] = getattr(self.discretization, thisattr)
            component_dict["discretization"] = discretization_info

        # AdditionalCode
        if self.additional_code is not None:
            additional_code_info: dict = {}
            additional_code_info["location"] = self.additional_code.source.location
            additional_code_info["subdir"] = self.additional_code.subdir
            additional_code_info["checkout_target"] = (
                self.additional_code.checkout_target
            )
            if self.additional_code.source_mods is not None:
                additional_code_info["source_mods"] = self.additional_code.source_mods
            if self.additional_code.namelists is not None:
                additional_code_info["namelists"] = self.additional_code.namelists

            component_dict["additional_code"] = additional_code_info
        return component_dict

    def __str__(self) -> str:
        # Header
        name = self.__class__.__name__
        base_str = f"{name}"
        # base_str = "-" * len(name) + "\n" + base_str
        base_str += "\n" + "-" * len(name)

        # Attrs
        base_str += "\nBuilt from: "

        NAC = 0 if self.additional_code is None else 1

        base_str += (
            f"\n{NAC} AdditionalCode instances (query using Component.additional_code)"
        )
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
        if self.additional_code is not None:
            repr_str += f"\nadditional_code = <{self.additional_code.__class__.__name__} instance>, "
        else:
            repr_str += "\n additional_code = None"
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
        repr_str += ")"
        return repr_str
