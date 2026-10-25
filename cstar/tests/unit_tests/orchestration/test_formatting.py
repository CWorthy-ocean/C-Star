import pytest
from pydantic import BaseModel, ConfigDict, ValidationError

from cstar.orchestration.formatting import format_validation_errors


class _Doc(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    size: int = 0


def _error(data: dict[str, object]) -> ValidationError:
    with pytest.raises(ValidationError) as caught:
        _Doc.model_validate(data)
    return caught.value


def test_format_validation_errors_names_the_input() -> None:
    """Verify an invalid value is reported with the value supplied."""
    ex = _error({"name": "a", "size": "big"})

    assert format_validation_errors(ex).startswith("`Invalid size value (big): ")


def test_format_validation_errors_omits_the_document_of_a_missing_field() -> None:
    """Verify a missing field is reported without echoing the whole document,
    which is the input pydantic records for it.
    """
    ex = _error({"size": 12345})

    assert format_validation_errors(ex) == "`Invalid name value: Field required`"
