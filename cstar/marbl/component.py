from typing import Optional
from cstar.base import Component


from cstar.marbl.base_model import MARBLBaseModel
from cstar.base.additional_code import AdditionalCode


class MARBLComponent(Component):
    # base_model: "MARBLBaseModel"
    # additional_code: Optional["AdditionalCode"]
    # Inherits its docstring from Component

    def __init__(
        self,
        base_model: "MARBLBaseModel",
        additional_source_code: Optional["AdditionalCode"] = None,
    ):
        self.base_model = base_model
        if additional_source_code is not None:
            raise NotImplementedError(
                "Source code modifications to MARBL " + "are not yet supported"
            )
        self.additional_source_code = additional_source_code

    @classmethod
    def from_dict(cls, component_info):
        component_kwargs = {}
        # Construct the BaseModel instance
        base_model_info = component_info.get("base_model")
        if base_model_info is None:
            raise ValueError(
                "Cannot construct a MARBLComponent instance without a "
                + "MARBLBaseModel object, but could not find 'base_model' entry"
            )
        base_model = MARBLBaseModel(**base_model_info)
        component_kwargs["base_model"] = base_model

        # Construct any AdditionalCode instances
        additional_code_info = component_info.get("additional_source_code")
        if additional_code_info is not None:
            additional_source_code = AdditionalCode(**additional_code_info)
            component_kwargs["additional_source_code"] = additional_source_code
        return cls(**component_kwargs)

    @property
    def component_type(self) -> str:
        return "MARBL"

    def setup(self) -> None:
        # Setup BaseModel
        infostr = f"Configuring {self.__class__.__name__}"
        print(infostr + "\n" + "-" * len(infostr))
        self.base_model.handle_config_status()

    def build(self) -> None:
        print("No build steps to be completed for MARBLComponent")
        pass

    def pre_run(self) -> None:
        print("No pre-processing steps to be completed for MARBLComponent")
        pass

    def run(self) -> None:
        print("MARBL must be run in the context of a parent model")

    def post_run(self) -> None:
        print("No post-processing steps to be completed for MARBLComponent")
        pass
