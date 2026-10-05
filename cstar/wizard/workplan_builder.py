"""The wizard's Workplan page.

Scaffold: another work package replaces the body of
:class:`WorkplanBuilderPage`; the constructor signature and the ``.widget`` /
``.blueprint_app`` attributes are the contract with the shell
(:func:`cstar.wizard.ui.shell.app`).
"""

from __future__ import annotations

from typing import Any


class WorkplanBuilderPage:
    """The Workplan page: build a workplan from catalog blueprints and steps.

    Parameters
    ----------
    blueprint_app
        The Blueprint page's app, shared so the two pages see the same catalog.
    W
        The ``ipywidgets`` module; imported lazily when omitted.
    """

    def __init__(self, blueprint_app: Any, *, W: Any = None) -> None:
        """Build the page's widget tree."""
        if W is None:
            import ipywidgets

            W = ipywidgets
        self.W = W
        self.blueprint_app = blueprint_app
        self.widget = W.VBox([W.HTML("<p>Workplan builder: under construction.</p>")])
        self.widget.add_class("forge-app")
