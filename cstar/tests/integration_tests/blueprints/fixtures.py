from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def modify_template_blueprint(
    tmpdir: str,
) -> Callable[[Path | str, dict[str, str]], str]:
    """Fixture that provides a factory function for modifying template blueprint files.

    This fixture returns a function that can returns a path to a modified a blueprint
    template file based on specified string replacements.

    Parameters
    ----------
    tmpdir:
       Pytest fixture used to create a temporary directory in which to hold a modified version
       of the template file during the test.

    Returns
    -------
    _modify_template_blueprint, Callable[[Path | str, dict[str, str]], Path]:
       The factory function

    Raises
    ------
    ValueError
    """

    def _modify_template_blueprint(
        template_blueprint_path: Path | str,
        strs_to_replace: dict,
        **kwargs: dict[str, Any],  # noqa: ARG001
    ) -> str:
        """Create a temporary, customized blueprint file from a template.

        This function reads a blueprint template file, performs string replacements
        as specified by `strs_to_replace`, saves the modified content to a temporary
        file within the `tmpdir` provided by pytest.

        Parameters
        ----------
        template_blueprint_path : Path | str
           The path to the blueprint template file.
        strs_to_replace : dict[str, str]
           A dictionary where keys are substrings to find in the template,
           and values are the replacements.
        kwargs : dict[str, Any]
            keyword arguments used to modify the template

        Returns
        -------
        modified_blueprint_path:
           A temporary path to a modified version of the template blueprint file.
        """
        template_blueprint_path = Path(template_blueprint_path)

        with open(template_blueprint_path) as template_file:  # noqa: PTH123
            template_content = template_file.read()

        modified_template_content = template_content
        for oldstr, newstr in strs_to_replace.items():
            modified_template_content = modified_template_content.replace(
                oldstr, newstr
            )
        temp_path = tmpdir.join(template_blueprint_path.name)
        with open(temp_path, "w") as temp_file:  # noqa: PTH123
            temp_file.write(modified_template_content)

        return temp_path

    return _modify_template_blueprint
