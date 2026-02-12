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
import typing as t
from collections import defaultdict
from types import ModuleType

from docutils import nodes  # noqa: F401
from docutils.parsers.rst import Directive
from sphinx.application import Sphinx

from cstar.base.env import EnvVar, EnvItem, discover_env_vars

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


class EnvVarRow(t.NamedTuple):
    env_var: EnvItem

    @property
    def name(self) -> str:
        return self.env_var.name

    @property
    def default(self) -> str:
        """Convert the default / default_factory into a docs-ready value."""
        env_var = self.env_var

        if env_var.default_factory and env_var.default:
            docs_default = f"{env_var.default} or <generated>"
        elif env_var.default_factory:
            docs_default = "<generated>"
        elif env_var.default:
            docs_default = env_var.default
        else:
            docs_default = "(no default)"

        return docs_default

    @property
    def effect(self) -> str:
        return self.env_var.description


class EnvVarTableDirective(Directive):
    """A custom Sphinx directive to generate a table of environment variables from a module."""

    required_arguments = 1
    optional_arguments = 100

    def _load_variable_groups(
        self,
        module: ModuleType,
        groups: dict[str, list[EnvVarRow]],
    ) -> None:
        """Reflect through the supplied module to discover all environment variables."""
        env_vars = discover_env_vars([module])

        for var in env_vars:
            groups[var.group].append(EnvVarRow(var))

    def _render_table(self, groups: dict[str, list[EnvVarRow]]) -> list[str]:
        """Render restructuredText for all discovered environment variables."""
        input_lines: list[str] = []

        if not groups:
            return input_lines

        for group_name, items in sorted(groups.items()):
            input_lines.append(group_name)
            input_lines.append("^" * len(group_name))
            input_lines.append("")
            input_lines.append(".. list-table::")
            input_lines.append("   :header-rows: 1")
            input_lines.append("   :widths: 30 20 50")
            input_lines.append("")
            input_lines.append("   * - Variable")
            input_lines.append("     - Default")
            input_lines.append("     - Effect")

            for item in sorted(items, key=lambda x: x.name):
                input_lines.append(f"   * - ``{item.name}``")
                input_lines.append(f"     - {item.default}")
                input_lines.append(f"     - {item.effect}")

            input_lines.append("")

        return input_lines

    def run(self) -> list:
        """Build the content for the environment variable table."""
        all_groups: dict[str, list[EnvVarRow]] = defaultdict(list)

        try:
            for module_name in self.arguments:
                module = importlib.import_module(module_name)
                self._load_variable_groups(module, all_groups)
        except ImportError:
            msg = f"Unable to import all modules from: {self.arguments}"
            log.exception(msg)

        if content := self._render_table(all_groups):
            self.state_machine.insert_input(content, "envvar-table")

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


def setup(app: Sphinx) -> None:
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
