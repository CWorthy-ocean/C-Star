from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import yaml
from pydantic import ValidationError

from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.orchestration.adapter import PIOAdapter
from cstar.pio.external_codebase import PIOExternalCodeBase


@pytest.fixture
def complete_blueprint_dict(complete_blueprint_path: Path) -> dict[str, Any]:
    """A dictionary representation of the complete example blueprint."""
    with complete_blueprint_path.open() as fp:
        return yaml.safe_load(fp)


class TestUsePIOSchema:
    """Tests for the `use_pio` model parameter and `code.pio` blueprint section."""

    def test_use_pio_defaults_false(self, complete_blueprint_dict):
        """Test that `model_params.use_pio` defaults to False when absent."""
        complete_blueprint_dict["model_params"].pop("use_pio", None)
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.model_params.use_pio is False
        assert bp.code.pio is None

    def test_code_pio_requires_use_pio(self, complete_blueprint_dict):
        """Test that supplying `code.pio` without `use_pio: true` raises."""
        complete_blueprint_dict["code"]["pio"] = {
            "location": "https://github.com/NCAR/ParallelIO.git",
            "branch": "pio2_7_0",
        }
        complete_blueprint_dict["model_params"]["use_pio"] = False
        with pytest.raises(
            ValidationError,
            match="code.pio was supplied but model_params.use_pio is false",
        ):
            RomsMarblBlueprint.model_validate(complete_blueprint_dict)

    def test_code_pio_with_use_pio_validates(self, complete_blueprint_dict):
        """Test that `code.pio` together with `use_pio: true` validates."""
        complete_blueprint_dict["code"]["pio"] = {
            "location": "https://github.com/NCAR/ParallelIO.git",
            "branch": "pio2_7_0",
        }
        complete_blueprint_dict["model_params"]["use_pio"] = True
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.model_params.use_pio is True
        assert bp.code.pio is not None
        assert bp.code.pio.checkout_target == "pio2_7_0"

    def test_use_pio_without_code_pio_validates(self, complete_blueprint_dict):
        """Test that `use_pio: true` is valid without a `code.pio` section (the
        default ParallelIO source is used).
        """
        complete_blueprint_dict["model_params"]["use_pio"] = True
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.model_params.use_pio is True
        assert bp.code.pio is None


class TestPIOAdapter:
    """Tests for the `PIOAdapter` converting blueprint models to
    `PIOExternalCodeBase` instances.
    """

    def test_adapt_defaults_when_code_pio_absent(
        self, complete_blueprint_dict, mocksourcedata_remote_repo
    ):
        """Test that `adapt` returns a default `PIOExternalCodeBase` when the
        blueprint has no `code.pio` section.
        """
        complete_blueprint_dict["model_params"]["use_pio"] = True
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)

        source_data = mocksourcedata_remote_repo(
            location="https://github.com/NCAR/ParallelIO.git", identifier="pio2_7_0"
        )
        with mock.patch(
            "cstar.base.external_codebase.SourceData", return_value=source_data
        ) as mock_source:
            pio_codebase = PIOAdapter(bp).adapt()

        assert isinstance(pio_codebase, PIOExternalCodeBase)
        mock_source.assert_called_once_with(
            location="https://github.com/NCAR/ParallelIO.git",
            identifier="pio2_7_0",
        )

    def test_adapt_honors_code_pio(
        self, complete_blueprint_dict, mocksourcedata_remote_repo
    ):
        """Test that `adapt` uses the location and checkout target from `code.pio`
        when supplied.
        """
        complete_blueprint_dict["code"]["pio"] = {
            "location": "https://github.com/my-fork/ParallelIO.git",
            "commit": "abc1234",
        }
        complete_blueprint_dict["model_params"]["use_pio"] = True
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)

        source_data = mocksourcedata_remote_repo(
            location="https://github.com/my-fork/ParallelIO.git",
            identifier="abc1234",
        )
        with mock.patch(
            "cstar.base.external_codebase.SourceData", return_value=source_data
        ) as mock_source:
            pio_codebase = PIOAdapter(bp).adapt()

        assert isinstance(pio_codebase, PIOExternalCodeBase)
        mock_source.assert_called_once_with(
            location="https://github.com/my-fork/ParallelIO.git",
            identifier="abc1234",
        )
