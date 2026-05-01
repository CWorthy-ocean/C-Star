import random
import typing as t
from unittest import mock

import pytest

from cstar.base.exceptions import CstarExpectationFailed
from cstar.system.migration import (
    # BlueprintVersionAdapter2025v1,
    # ConversionPlan,
    RawModelVersionAdapterType,
    RomsBlueprintMigration,
    SchemaMigration,
)


def test_migration_simple_plan() -> None:
    """Verify a simple, one-step migration is planned."""
    src_version_exp = "2025.1"
    tgt_version_exp = "2026.1"

    bp0 = {"version": src_version_exp}

    migrator = RomsBlueprintMigration()

    # static target avoids the test planning multiple steps as migration count grows.
    with mock.patch.object(
        RomsBlueprintMigration,
        "LATEST",
        mock.PropertyMock(return_value=tgt_version_exp),
    ):
        src_version, tgt_version, plan = migrator.plan(bp0)

    assert src_version == src_version_exp
    assert tgt_version == tgt_version_exp
    assert plan


def test_migration_to_unknown_target() -> None:
    """Verify that an incomplete migration path raises an exception."""
    src_version_exp = "2025.1"
    tgt_version_exp = "2026.UNK"

    bp0 = {"version": src_version_exp}

    migrator = RomsBlueprintMigration()

    with (
        # simulate failure to add the latest target to `BlueprintMigrationManager`.
        mock.patch.object(
            RomsBlueprintMigration,
            "LATEST",
            mock.PropertyMock(return_value=tgt_version_exp),
        ),
        pytest.raises(
            CstarExpectationFailed,
            match="migration adapter",
        ),
    ):
        _ = migrator.plan(bp0)


def test_migration_from_unknown_source() -> None:
    """Verify that no migration path found raises an exception."""
    src_version_exp = "2025.UNK"
    # tgt_version_exp = BlueprintMigrationManager.latest

    bp0 = {"version": src_version_exp}

    migrator = RomsBlueprintMigration()

    # simulate failure to add the correct source & target to a migration.
    with pytest.raises(
        CstarExpectationFailed,
        match="migration adapter",
    ):
        _ = migrator.plan(bp0)


class BPTestAdapterV0(SchemaMigration):
    """V0 adapter for testing."""

    @property
    def source(self) -> str:
        """The supported version of the input model."""
        return "test.0"

    @property
    def target(self) -> str:
        """The version of the output model."""
        return "test.1"

    @t.override
    def adapt(self) -> dict[str, str]:
        return {**self.model}


class BPTestAdapterV1(SchemaMigration):
    """V1 adapter for testing."""

    @property
    def source(self) -> str:
        """The supported version of the input model."""
        return "test.1"

    @property
    def target(self) -> str:
        """The version of the output model."""
        return "test.2"

    @t.override
    def adapt(self) -> dict[str, str]:
        return {**self.model}


class BPTestAdapterV2(SchemaMigration):
    """V2 adapter for testing."""

    @property
    def source(self) -> str:
        """The supported version of the input model."""
        return "test.2"

    @property
    def target(self) -> str:
        """The version of the output model."""
        return "test.3"

    @t.override
    def adapt(self) -> dict[str, str]:
        return {**self.model}


class BPTestAdapterV3(SchemaMigration):
    """V2 adapter for testing."""

    @property
    def source(self) -> str:
        """The supported version of the input model."""
        return "test.3"

    @property
    def target(self) -> str:
        """The version of the output model."""
        return "test.4"

    @t.override
    def adapt(self) -> dict[str, str]:
        return {**self.model}


@pytest.mark.parametrize(
    ("src_version_exp", "exp_num_steps"),
    [
        ("test.0", 4),
        ("test.1", 3),
        ("test.2", 2),
        ("test.3", 1),
    ],
)
def test_migration_intermediate_multistep(
    src_version_exp: str,
    exp_num_steps: int,
) -> None:
    """Verify that multi-step migration from any version in the history completes.

    Parameters
    ----------
    src_version_exp : str
        The version to use as the starting point for the migration.
    exp_num_steps : str
        The number of migration steps expected to complete the migration.
    """
    tgt_version_exp = "test.4"

    bp0 = {"version": src_version_exp}

    converters: list[RawModelVersionAdapterType] = [
        BPTestAdapterV0,
        BPTestAdapterV1,
        BPTestAdapterV2,
        BPTestAdapterV3,
    ]
    # shuffle to ensure order in the converters list doesn't matter
    random.shuffle(converters)

    migrator = RomsBlueprintMigration(converters)

    with (
        # simulate failure to add the latest target to `BlueprintMigrationManager`.
        mock.patch.object(
            RomsBlueprintMigration,
            "LATEST",
            mock.PropertyMock(return_value=tgt_version_exp),
        ),
    ):
        src_version, tgt_version, plan = migrator.plan(bp0)

    assert src_version == src_version_exp
    assert tgt_version == tgt_version_exp
    assert len(plan) == exp_num_steps
