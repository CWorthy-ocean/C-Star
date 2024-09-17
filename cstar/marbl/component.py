from cstar.base import Component


class MARBLComponent(Component):
    # Inherits its docstring from Component

    def build(self) -> None:
        pass

    def pre_run(self) -> None:
        pass

    def run(self) -> None:
        print("MARBL must be run in the context of a parent model")

    def post_run(self) -> None:
        pass
