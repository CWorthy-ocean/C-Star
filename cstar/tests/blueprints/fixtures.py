import pytest
from pathlib import Path
from cstar import Case


@pytest.fixture
def template_blueprint_to_case(tmpdir):
    def _template_blueprint_to_case(
        template_blueprint_path: Path | str, strs_to_replace: dict, **kwargs
    ):
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
