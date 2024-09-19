from typing import TYPE_CHECKING
from cstar.base import Component


from cstar.marbl.base_model import MARBLBaseModel
from cstar.base.additional_code import AdditionalCode
    
class MARBLComponent(Component):
    base_model : "MARBLBaseModel"
    additional_code: "AdditionalCode"
    # Inherits its docstring from Component

    def __init__(self,
                 base_model: "MARBLBaseModel",
                 additional_code: "AdditionalCode" = None):

        self.base_model = base_model
        self.additional_code = additional_code
    
    @classmethod
    def from_dict(cls, component_dict):
        """docstring"""
        return super().from_dict(component_dict)
        
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
