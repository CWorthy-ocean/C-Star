from cstar.base import Component


class MARBLComponent(Component):
    # Inherits its docstring from Component

    def build(self) -> None:
        print("source code modifications to MARBL are not yet supported")

    def pre_run(self) -> None:
        print("no pre-run actions involving MARBL are currently supported")

    def run(self) -> None:
        print("MARBL must be run in the context of a parent model")

    def post_run(self) -> None:
        print("no post-run actions involving MARBL are currently supported")
