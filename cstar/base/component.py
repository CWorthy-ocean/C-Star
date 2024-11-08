from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

from cstar.base.base_model import BaseModel

if TYPE_CHECKING:
    from cstar.base.additional_code import AdditionalCode
    from cstar.base.discretization import Discretization


class Component(ABC):
    """A model component that contributes to a unique Case instance.

    Attributes:
    ----------
    base_model: BaseModel
        An object pointing to the unmodified source code of a model handling an individual
        aspect of the simulation such as biogeochemistry or ocean circulation
    additional_source_code: AdditionalCode (Optional, default None)
        Additional source code contributing to a unique instance of a base model,
        to be included at compile time
    namelists: AdditionalCode (Optional, default None)
        Namelist files contributing to a unique instance of the base model,
        to be used at runtime
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

    base_model: BaseModel
    namelists: Optional["AdditionalCode"]
    additional_source_code: Optional["AdditionalCode"]
    discretization: Optional["Discretization"]

    def __init__(
        self,
        base_model: BaseModel,
        namelists: Optional["AdditionalCode"] = None,
        additional_source_code: Optional["AdditionalCode"] = None,
        discretization: Optional["Discretization"] = None,
    ):
        """Initialize a Component object from a base model and any additional_code.

        Parameters:
        -----------
        base_model: BaseModel
            An object pointing to the unmodified source code of a model handling an individual
            aspect of the simulation such as biogeochemistry or ocean circulation
        namelists: AdditionalCode (Optional, default None)
            Namelist files contributing to a unique instance of the base model,
            to be used at runtime
        additional_source_code: AdditionalCode (Optional, default None)
            Additional source code contributing to a unique instance of a base model,
            to be included at compile time
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
        self.additional_source_code = additional_source_code or None
        self.discretization = discretization or None

    @classmethod
    @abstractmethod
    def from_dict(self):
        """Construct this component instance from a dictionary of kwargs.

        This method is implemented separately for different subclasses of Component.
        """
        pass

    def to_dict(self):
        """Create a dictionary representation of this Component object.

        Returns:
        --------
        component_dict (dict):
           A dictionary representation of this Component.
        """
        component_dict = {}

        component_dict["component_type"] = self.component_type

        # BaseModel:
        base_model_info = {}
        base_model_info["source_repo"] = self.base_model.source_repo
        base_model_info["checkout_target"] = self.base_model.checkout_target
        component_dict["base_model"] = base_model_info

        # additional source code
        additional_src = getattr(self, "additional_source_code")
        if additional_src is not None:
            additional_src_info = {}
            additional_src_info["location"] = additional_src.source.location
            if additional_src.subdir is not None:
                additional_src_info["subdir"] = additional_src.subdir
            if additional_src.checkout_target is not None:
                additional_src_info["checkout_target"] = additional_src.checkout_target
            if additional_src.files is not None:
                additional_src_info["files"] = additional_src.files

            component_dict["additional_source_code"] = additional_src_info

        return component_dict

    def __str__(self) -> str:
        # Header
        name = self.__class__.__name__
        base_str = f"{name}"
        # base_str = "-" * len(name) + "\n" + base_str
        base_str += "\n" + "-" * len(name)

        # Attrs
        base_str += f"\nbase_model: {self.base_model.__class__.__name__} instance (query using Component.base_model)"

        if (
            hasattr(self, "additional_source_code")
            and self.additional_source_code is not None
        ):
            NS = len(self.additional_source_code.files)
            base_str += (
                f"\nadditional_source_code: {self.additional_source_code.__class__.__name__} instance with {NS} files "
                + "(query using Component.additional_source_code)"
            )

        return base_str

    def __repr__(self) -> str:
        repr_str = f"{self.__class__.__name__}("
        repr_str += f"\nbase_model = <{self.base_model.__class__.__name__} instance>, "
        if self.additional_source_code is not None:
            repr_str += (
                "\nadditional_source_code = "
                + f"<{self.additional_source_code.__class__.__name__} instance>, "
            )
        repr_str += "\n)"

        return repr_str

    @property
    @abstractmethod
    def component_type(self) -> str:
        pass

    @abstractmethod
    def build(self) -> None:
        """Compile any Component-specific code on this machine.

        This abstract method will be implemented differently by different Component
        types.
        """

    @abstractmethod
    def pre_run(self) -> None:
        """Execute any pre-processing actions necessary to run this component.

        This abstract method will be implemented differently by different Component
        types.
        """

    @abstractmethod
    def run(self) -> None:
        """Run this component.

        This abstract method will be implemented differently by different Component
        types.
        """
        pass

    @abstractmethod
    def post_run(self) -> None:
        """Execute any pre-processing actions associated with this component.

        This abstract method will be implemented differently by different Component
        types.
        """
        pass
