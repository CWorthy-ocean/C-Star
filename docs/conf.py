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

import importlib
import logging
import os
import pathlib
import sys
import textwrap
import typing as t

from docutils import nodes  # noqa: F401
from docutils.parsers.rst import Directive

logging.basicConfig(level=logging.DEBUG)

root = pathlib.Path(__file__).parent.parent.absolute()
os.environ["PYTHONPATH"] = str(root)
# cstar will look for os.environ["CONDA_PREFIX"] but this is not available on RTD; let's fill it with dummy
os.environ["CONDA_PREFIX"] = str(root)
sys.path.insert(0, str(root))
os.environ["OMP_DISPLAY_ENV"] = "FALSE"
os.environ["OMP_DISPLAY_AFFINITY"] = "FALSE"

import cstar  # isort:skip


log = logging.getLogger(__name__)


class EnvVarTableDirective(Directive):
    """A custom Sphinx directive to generate a table of environment variables from a module."""

    required_arguments = 1

    def run(self) -> list:
        module_name = self.arguments[0]
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            log.exception("Could not import module %s", module_name)
            return []

        from cstar.base.utils import EnvVar

        hints = t.get_type_hints(module, include_extras=True)

        # Group items by their group name
        groups = {}
        for name, hint in hints.items():
            if name.startswith("ENV_"):
                metadata = getattr(hint, "__metadata__", None)
                if metadata and isinstance(metadata[0], EnvVar):
                    meta = metadata[0]
                    var_name = getattr(module, name)

                    default = meta.default
                    if meta.default_factory:
                        default = (
                            f"{default} or <generated>" if default else "<generated>"
                        )

                    if meta.group not in groups:
                        groups[meta.group] = []
                    groups[meta.group].append(
                        {
                            "name": var_name,
                            "default": str(default) if default else "(no default)",
                            "effect": meta.description,
                        },
                    )

        if not groups:
            return []

        output = []
        for group_name, items in groups.items():
            output.append(group_name)
            output.append("^" * len(group_name))
            output.append("")
            output.append(".. list-table::")
            output.append("   :header-rows: 1")
            output.append("   :widths: 30 20 50")
            output.append("")
            output.append("   * - Variable")
            output.append("     - Default")
            output.append("     - Effect")

            for item in items:
                output.append(f"   * - ``{item['name']}``")
                output.append(f"     - {item['default']}")
                output.append(f"     - {item['effect']}")

            output.append("")

        self.state_machine.insert_input(output, module_name)
        return []


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
    "nbsphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx_design",
]


def get_pydantic_exclusions() -> set[str]:
    return {
        "Config",
        "construct",
        "copy",
        "dict",
        "json",
        "from_orm",
        "model_computed_fields",
        "model_config",
        "model_construct",
        "model_copy",
        "model_dump",
        "model_dump_json",
        "model_extra",
        "model_fields",
        "model_fields_set",
        "model_json",
        "model_json_schema",
        "model_parametrized_name",
        "model_post_init",
        "model_rebuild",
        "model_validate",
        "model_validate_json",
        "model_validate_strings",
        "parse_file",
        "parse_obj",
        "parse_raw",
        "schema",
        "schema_json",
        "update_forward_refs",
        "validate",
    }


def get_enum_exclusions() -> set[str]:
    return {
        "as_integer_ratio",
        "bit_length",
        "bit_count",
        "conjugate",
        "denominator",
        "from_bytes",
        "imag",
        "is_integer",
        "numerator",
        "real",
        "to_bytes",
    }


def autodoc_skip_member(app, what, name, obj, skip, options) -> bool:
    """Return `True` if class member should be skipped by autodoc."""
    exclusions = get_pydantic_exclusions() | get_enum_exclusions()

    if name.startswith("_") or name in exclusions:
        return True
    return skip


def setup(app) -> None:
    """Configure the Sphinx app."""
    app.connect("autodoc-skip-member", autodoc_skip_member)
    app.add_directive("envvar-table", EnvVarTableDirective)


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
