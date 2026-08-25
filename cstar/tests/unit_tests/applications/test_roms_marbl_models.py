from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import yaml
from pydantic import ValidationError

from cstar.applications.roms_marbl.adapter import PIOAdapter
from cstar.applications.roms_marbl.models import (
    PartitioningParameterSet,
    RomsMarblBlueprint,
    RuntimeParameterSet,
)
from cstar.pio.external_codebase import PIOExternalCodeBase


@pytest.fixture
def complete_blueprint_dict(complete_blueprint_path: Path) -> dict[str, Any]:
    """A dictionary representation of the complete example blueprint."""
    with complete_blueprint_path.open() as fp:
        return yaml.safe_load(fp)


class TestUsePIOSchema:
    """Tests for the `use_pio` model parameter and `code.pio` blueprint section."""

    def test_use_pio_defaults_false(self, complete_blueprint_dict):
        """Test that `partitioning.use_pio` defaults to False when absent."""
        complete_blueprint_dict["partitioning"].pop("use_pio", None)
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.partitioning.use_pio is False
        assert bp.code.pio is None

    def test_code_pio_requires_use_pio(self, complete_blueprint_dict):
        """Test that supplying `code.pio` without `use_pio: true` raises."""
        complete_blueprint_dict["code"]["pio"] = {
            "location": "https://github.com/NCAR/ParallelIO.git",
            "branch": "pio2_7_0",
        }
        complete_blueprint_dict["partitioning"]["use_pio"] = False
        with pytest.raises(
            ValidationError,
            match="code.pio was supplied but partitioning.use_pio is false",
        ):
            RomsMarblBlueprint.model_validate(complete_blueprint_dict)

    def test_code_pio_with_use_pio_validates(self, complete_blueprint_dict):
        """Test that `code.pio` together with `use_pio: true` validates."""
        complete_blueprint_dict["code"]["pio"] = {
            "location": "https://github.com/NCAR/ParallelIO.git",
            "branch": "pio2_7_0",
        }
        complete_blueprint_dict["partitioning"]["use_pio"] = True
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.partitioning.use_pio is True
        assert bp.code.pio is not None
        assert bp.code.pio.checkout_target == "pio2_7_0"

    def test_use_pio_without_code_pio_validates(self, complete_blueprint_dict):
        """Test that `use_pio: true` is valid without a `code.pio` section (the
        default ParallelIO source is used).
        """
        complete_blueprint_dict["partitioning"]["use_pio"] = True
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.partitioning.use_pio is True
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
        complete_blueprint_dict["partitioning"]["use_pio"] = True
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
        complete_blueprint_dict["partitioning"]["use_pio"] = True
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


class TestNamelistOverrides:
    """Tests for the `namelist_overrides` field on `RomsMarblBlueprint`."""

    def test_defaults_to_empty_dict(self, complete_blueprint_dict):
        """Test that `namelist_overrides` defaults to `{}` when absent."""
        complete_blueprint_dict.pop("namelist_overrides", None)
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.namelist_overrides == {}

    def test_accepts_nested_partial_mapping(self, complete_blueprint_dict):
        """Test that a nested, partial mapping of namelist groups validates."""
        complete_blueprint_dict["namelist_overrides"] = {
            "time_stepping": {"dt": 30},
            "param_settings": {"np_xi": 3},
        }
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.namelist_overrides == {
            "time_stepping": {"dt": 30},
            "param_settings": {"np_xi": 3},
        }

    def test_model_params_raises(self, complete_blueprint_dict):
        """Test that the removed `model_params` field is rejected as extra."""
        complete_blueprint_dict["model_params"] = {"time_step": 60}
        with pytest.raises(ValidationError, match="model_params"):
            RomsMarblBlueprint.model_validate(complete_blueprint_dict)


class TestPartitioningParameterSet:
    """Tests for the `PartitioningParameterSet` validation rules."""

    def test_locked_without_hash_rejected(self):
        """Test that the inherited `ParameterSet` locked/hash rule still applies."""
        with pytest.raises(
            ValidationError, match="A locked parameter set must include a hash"
        ):
            PartitioningParameterSet(locked=True, n_procs_x=2, n_procs_y=2)

    def test_runtime_parameter_set_locked_without_hash_rejected(self):
        """Test that `RuntimeParameterSet` no longer shadows the inherited
        `ParameterSet` locked/hash rule with its own same-named validator.
        """
        with pytest.raises(
            ValidationError, match="A locked parameter set must include a hash"
        ):
            RuntimeParameterSet(
                locked=True,
                start_date="2020-01-01",
                end_date="2020-01-02",
            )

    def test_auto_tiling_with_use_pio_and_n_cores_valid(self):
        """Test that `auto_tiling` with `use_pio` and `n_cores` validates."""
        pps = PartitioningParameterSet(use_pio=True, auto_tiling=True, n_cores=16)
        assert pps.n_cores == 16

    def test_auto_tiling_without_use_pio_rejected(self):
        """Test that `auto_tiling` without `use_pio` is rejected."""
        with pytest.raises(ValidationError, match="auto_tiling requires use_pio"):
            PartitioningParameterSet(auto_tiling=True, n_cores=16)

    def test_missing_procs_without_auto_tiling_rejected(self):
        """Test that omitting `n_procs_x`/`n_procs_y` without `auto_tiling`
        is rejected.
        """
        with pytest.raises(
            ValidationError, match="n_procs_x and n_procs_y are required"
        ):
            PartitioningParameterSet()

    def test_n_cores_without_auto_tiling_rejected(self):
        """Test that supplying `n_cores` without `auto_tiling` is rejected."""
        with pytest.raises(
            ValidationError, match="n_cores is only accepted with auto_tiling"
        ):
            PartitioningParameterSet(n_procs_x=2, n_procs_y=2, n_cores=4)

    def test_all_three_consistent_accepted(self):
        """Test that `n_cores` consistent with `n_procs_x * n_procs_y` validates."""
        pps = PartitioningParameterSet(
            use_pio=True,
            auto_tiling=True,
            n_procs_x=4,
            n_procs_y=4,
            n_cores=16,
        )
        assert pps.n_cores == 16

    def test_all_three_inconsistent_rejected(self):
        """Test that `n_cores` inconsistent with `n_procs_x * n_procs_y` is rejected."""
        with pytest.raises(ValidationError, match="n_cores must equal"):
            PartitioningParameterSet(
                use_pio=True,
                auto_tiling=True,
                n_procs_x=4,
                n_procs_y=4,
                n_cores=99,
            )

    def test_multiple_violations_all_reported(self):
        """Test that simultaneous violations are all included in a single error."""
        with pytest.raises(ValidationError) as exc_info:
            PartitioningParameterSet(
                auto_tiling=True,
                n_procs_x=4,
                n_procs_y=4,
                n_cores=99,
            )
        msg = str(exc_info.value)
        assert "auto_tiling requires use_pio" in msg
        assert "n_cores must equal" in msg


class TestCpusNeeded:
    """Tests for the `cpus_needed` property on `RomsMarblBlueprint`."""

    def test_returns_product_of_procs_by_default(self, complete_blueprint_dict):
        """Test that `cpus_needed` returns `n_procs_x * n_procs_y` by default."""
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.cpus_needed == (bp.partitioning.n_procs_x * bp.partitioning.n_procs_y)

    def test_returns_n_cores_when_set(self, complete_blueprint_dict):
        """Test that `cpus_needed` returns `n_cores` when it is set."""
        complete_blueprint_dict["partitioning"] = {
            "use_pio": True,
            "auto_tiling": True,
            "n_cores": 16,
        }
        bp = RomsMarblBlueprint.model_validate(complete_blueprint_dict)
        assert bp.cpus_needed == 16
