from typing import Optional
from cstar.base import Component


from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.base.additional_code import AdditionalCode


class MARBLComponent(Component):
    # additional_code: Optional["AdditionalCode"]
    # Inherits its docstring from Component

    def __init__(
        self,
        codebase: "MARBLExternalCodeBase",
        additional_source_code: Optional["AdditionalCode"] = None,
    ):
        self.codebase = codebase
        if additional_source_code is not None:
            raise NotImplementedError(
                "Source code modifications to MARBL " + "are not yet supported"
            )
        self.additional_source_code = additional_source_code

    @classmethod
    def from_dict(cls, component_info):
        component_kwargs = {}
        # Construct the ExternalCodeBase instance
        codebase_info = component_info.get("codebase")
        if codebase_info is None:
            raise ValueError(
                "Cannot construct a MARBLComponent instance without a "
                + "MARBLExternalCodeBase object, but could not find 'codebase' entry"
            )
        codebase = MARBLExternalCodeBase(**codebase_info)
        component_kwargs["codebase"] = codebase

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
        # Setup ExternalCodeBase
        infostr = f"Configuring {self.__class__.__name__}"
        print(infostr + "\n" + "-" * len(infostr))
        self.codebase.handle_config_status()

    def build(self, rebuild: bool = False) -> None:
        print("No build steps to be completed for MARBLComponent")
        pass

    def pre_run(self) -> None:
        print("No pre-processing steps to be completed for MARBLComponent")
        pass

    def run(self):
        raise ValueError("MARBL must be run in the context of a parent model")

    def post_run(self) -> None:
        print("No post-processing steps to be completed for MARBLComponent")
        pass
