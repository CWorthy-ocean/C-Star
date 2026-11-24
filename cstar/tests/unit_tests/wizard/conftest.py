"""Fixtures shared by the wizard's test modules."""

import pytest

from cstar.wizard.forcing_editor import _ForcingEditor


@pytest.fixture
def editor():
    import ipywidgets as W

    return _ForcingEditor(W, {}, on_change=lambda: None)
