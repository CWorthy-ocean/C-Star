"""The ipywidgets/Voila wizard for assembling and reviewing a ForgeBlueprint.

This package is kept import-light on purpose: ``ipywidgets`` (and the rest of
the wizard's UI stack) is an optional dependency imported lazily inside the
functions and methods that need it, not at module scope, so importing
:mod:`cstar.wizard` or its submodules never requires those packages to be
installed.
"""
