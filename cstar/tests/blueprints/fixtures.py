import pytest
from pathlib import Path
from typing import Callable

from cstar import Case


@pytest.fixture
def template_blueprint_to_case(tmpdir) -> Callable[[Path | str, dict[str, str]], Case]:
    """
    Fixture that provides a factory function for creating `Case` objects from a blueprint template.

    This fixture returns a function that can generate `Case` instances by modifying a blueprint
    template file based on specified string replacements. When `template_blueprint_to_case` is
    used in a test, this function is then available to call at a user-specified point in that test.

    Parameters:
    -----
    tmpdir:
       Pytest fixture used to create a temporary directory in which to hold a modified version
       of the template file during the test.

    Returns:
    --------
    _template_blueprint_to_case, Callable[[Path | str, dict[str, str]], Case]:
       The factory function

    Examples:
    ------
        def test_case_creation(template_blueprint_to_case):
            case = template_blueprint_to_case(
                "template.yaml",
                {"PLACEHOLDER": "actual_value"}
            )
            assert isinstance(case, Case)
    """

    def _template_blueprint_to_case(
        template_blueprint_path: Path | str, strs_to_replace: dict, **kwargs
    ) -> Case:
        """
        Creates a `Case` object from a modified blueprint template.

        This function reads a blueprint template file, performs string replacements as specified
        by `strs_to_replace`, saves the modified content to a temporary file within the `tmpdir`
        provided by pytest, and then uses this temporary blueprint file to create a `Case` instance.
        Additional keyword arguments can be passed to the `Case.from_blueprint` method.

        Parameters:
        -----------
        template_blueprint_path (Path | str):
           The path to the blueprint template file.
        strs_to_replace (dict[str, str]):
           A dictionary where keys are substrings to find in the template,
           and values are the replacements.
        **kwargs:
           Additional keyword arguments passed to `Case.from_blueprint`.

        Returns:
        --------
            Case: An instance of the `Case` class created from the modified blueprint.
        """

        template_blueprint_path = Path(template_blueprint_path)

        with open(template_blueprint_path, "r") as template_file:
            template_content = template_file.read()

        modified_template_content = template_content
        for oldstr, newstr in strs_to_replace.items():
            modified_template_content = modified_template_content.replace(
                oldstr, newstr
            )
        temp_path = tmpdir.join(template_blueprint_path.name)
        with open(temp_path, "w") as temp_file:
            temp_file.write(modified_template_content)

        return Case.from_blueprint(temp_path, **kwargs)

    return _template_blueprint_to_case
