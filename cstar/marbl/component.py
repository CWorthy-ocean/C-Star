from cstar.base import Component


class MARBLComponent(Component):
    # Inherits its docstring from Component

    @property
    def component_type(self) -> str:
        return "MARBL"

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
