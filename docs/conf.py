# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import os
import pathlib
import sys
import logging

logging.basicConfig(level=logging.DEBUG)

root = pathlib.Path(__file__).parent.parent.absolute()
os.environ["PYTHONPATH"] = str(root)
# cstar will look for os.environ["CONDA_PREFIX"] but this is not available on RTD; let's fill it with dummy
os.environ["CONDA_PREFIX"] = str(root)
sys.path.insert(0, str(root))
os.environ["OMP_DISPLAY_ENV"] = "FALSE"
os.environ["OMP_DISPLAY_AFFINITY"] = "FALSE"

import cstar  # isort:skip


project = "C-Star"
copyright = "2026, [C]Worthy"
author = "C-Star developers"

# -- Early stage of development Warning banner -----------------------------------------------------

rst_prolog = """.. attention::
    **This project is still in an early phase of development.**

    The :doc:`python API </api>` is not yet stable, and some aspects of the schema for the :doc:`blueprint </blueprints>` and :doc:`workplan </workplans>` will likely evolve.
    Therefore whilst you are welcome to try out using the package, we cannot yet guarantee backwards compatibility. 
    We expect to reach a more stable version in 2026.
"""

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.napoleon",
    "nbsphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_design",
]

numpydoc_show_class_members = True
napolean_google_docstring = False
napolean_numpy_docstring = True

templates_path = ["_templates"]
exclude_patterns = []
# exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

napoleon_custom_sections = [
    ("Returns", "params_style"),
    ("Sets Attributes", "params_style"),
    ("Required Parameter Sections", "params_style"),
    ("Assumptions", "notes_style"),
    ("Example Config YAML File", "example"),
]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
# html_theme = 'alabaster'
html_static_path = ["_static"]


html_theme_options = {
    "repository_url": "https://github.com/CWorthy-ocean/C-Star",
    "use_repository_button": True,
}
