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
        additional_code: Optional["AdditionalCode"] = None,
    ):
        self.base_model = base_model
        self.additional_code = additional_code

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
        additional_code_info = component_info.get("additional_code")
        if additional_code_info is not None:
            additional_code = AdditionalCode(**additional_code_info)
            component_kwargs["additional_code"] = additional_code
        return cls(**component_kwargs)

    @property
    def component_type(self) -> str:
        return "MARBL"

    # def to_dict(self):
    #     # TODO add this to the component baseclass and call super in subclass
    #     component_info = {}
    #     component_info["component_type"] = "MARBL"

    #     # BaseModel:
    #     base_model_info = {}
    #     base_model_info["source_repo"] = self.base_model.source_repo
    #     base_model_info["checkout_target"] = self.base_model.checkout_target

    #     component_info["base_model"] = base_model_info

    #     # AdditionalCode
    #     additional_code = self.additional_code

    #     if additional_code is not None:
    #         additional_code_info = {}

    #         additional_code_info["location"] = additional_code.source.location
    #         additional_code_info["subdir"] = additional_code.subdir
    #         additional_code_info["checkout_target"] = additional_code.checkout_target

    #         if additional_code.source_mods is not None:
    #             additional_code_info["source_mods"] = additional_code.source_mods
    #         if additional_code.namelists is not None:
    #             additional_code_info["namelists"] = additional_code.namelists

    #         component_info["additional_code"] = additional_code_info

    #     return component_info

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
