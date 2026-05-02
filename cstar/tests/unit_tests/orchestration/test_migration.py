import random
import typing as t
from unittest import mock

import pytest

from cstar.system.migration import (
    APP_ROMS_MARBL_SCHEMA_2025_1,
    APP_ROMS_MARBL_SCHEMA_2026_1,
    BlueprintMigration,
    CstarMigrationError,
    CstarUnsupportedMigrationError,
    RawModelVersionAdapterType,
    SchemaAdapter,
    SchemaBounds,
)

APP_SLEEP: t.Literal["sleep"] = "sleep"
APP_ROMS: t.Literal["roms_marbl"] = "roms_marbl"

KEY_VERSION: t.Literal["version"] = "version"
KEY_APP: t.Literal["application"] = "application"


def test_migration_simple_plan() -> None:
    """Verify a simple, one-step migration is planned when using
    the default migration adapters registered in `BlueprintMigration`
    for `roms_marbl` blueprints.
    """
    src_version_exp = APP_ROMS_MARBL_SCHEMA_2025_1
    tgt_version_exp = APP_ROMS_MARBL_SCHEMA_2026_1

    bp0 = {KEY_VERSION: src_version_exp, KEY_APP: APP_ROMS}
    migrator = BlueprintMigration()

    # static target avoids the test planning multiple steps as migration count grows.
    src_version, tgt_version, plan = migrator.plan(bp0)

    assert src_version == src_version_exp
    assert tgt_version == tgt_version_exp
    assert plan


def test_migration_with_unregistered_application() -> None:
    """Verify that a migration error is raised when a migration of
    an unknown schema type is attempted.
    """
    src_version_exp = BPTestAdapterV0.source()
    tgt_version_exp = BPTestAdapterV3.target()

    bp0 = {KEY_VERSION: src_version_exp, KEY_APP: APP_SLEEP}

    converters: list[RawModelVersionAdapterType] = [
        BPTestAdapterV0,
        BPTestAdapterV1,
        BPTestAdapterV2,
        BPTestAdapterV3,
    ]
    bounds: dict[str, SchemaBounds] = {
        "not-sleep": {
            "min": BPTestAdapterV0.source(),
            "max": BPTestAdapterV3.target(),
        },
    }

    with (
        # simulate failure to add a migration for latest version (or
        # an incorrect update to BlueprintMigration.application).
        mock.patch.object(
            BPTestAdapterV0, "target", mock.Mock(return_value=tgt_version_exp)
        ),
    ):
        migrator = BlueprintMigration(converters, bounds)

    with pytest.raises(
        CstarUnsupportedMigrationError,
        match="schema bounds",
    ):
        _ = migrator.plan(bp0)


def test_migration_to_unknown_target() -> None:
    """Verify that an incomplete migration path raises an exception."""
    src_version_exp = BPTestAdapterV0.source()
    tgt_version_exp = "2026.UNK"

    bp0 = {KEY_VERSION: src_version_exp, KEY_APP: APP_SLEEP}

    converters: list[RawModelVersionAdapterType] = [
        BPTestAdapterV0,
        BPTestAdapterV1,
        BPTestAdapterV2,
        BPTestAdapterV3,
    ]
    bounds: dict[str, SchemaBounds] = {
        APP_SLEEP: {
            "min": BPTestAdapterV0.source(),
            "max": BPTestAdapterV3.target(),
        },
    }

    with (
        # simulate failure to add a migration for latest version (or
        # an incorrect update to BlueprintMigration.application).
        mock.patch.object(
            BPTestAdapterV0, "target", mock.Mock(return_value=tgt_version_exp)
        ),
    ):
        migrator = BlueprintMigration(converters, bounds)

    with pytest.raises(
        CstarMigrationError,
        match="migration adapter",
    ):
        _ = migrator.plan(bp0)


def test_migration_from_unknown_source() -> None:
    """Verify that no migration path found raises an exception."""
    src_version_exp = "2025.UNK"

    bp0 = {KEY_VERSION: src_version_exp, KEY_APP: APP_SLEEP}
    converters: list[RawModelVersionAdapterType] = [
        BPTestAdapterV0,
        BPTestAdapterV1,
        BPTestAdapterV2,
        BPTestAdapterV3,
    ]
    bounds: dict[str, SchemaBounds] = {
        APP_SLEEP: {
            "min": BPTestAdapterV0.source(),
            "max": BPTestAdapterV3.target(),
        },
    }
    migrator = BlueprintMigration(converters, bounds)

    # simulate failure to add a migration adapter for any version
    with pytest.raises(
        CstarUnsupportedMigrationError,
        match="migration adapter",
    ):
        _ = migrator.plan(bp0)


class BPTestAdapterV0(SchemaAdapter):
    """V0 adapter for testing."""

    @classmethod
    def application(cls) -> str:
        """The supported version of the input model."""
        return "sleep"

    @classmethod
    def source(cls) -> str:
        """The supported version of the input model."""
        return "test.0"

    @classmethod
    def target(cls) -> str:
        """The version of the output model."""
        return "test.1"

    @t.override
    def adapt(self) -> dict[str, str]:
        return {**self.model}


class BPTestAdapterV1(SchemaAdapter):
    """V1 adapter for testing."""

    @classmethod
    def application(cls) -> str:
        """The supported version of the input model."""
        return "sleep"

    @classmethod
    def source(cls) -> str:
        """The supported version of the input model."""
        return "test.1"

    @classmethod
    def target(cls) -> str:
        """The version of the output model."""
        return "test.2"

    @t.override
    def adapt(self) -> dict[str, str]:
        return {**self.model}


class BPTestAdapterV2(SchemaAdapter):
    """V2 adapter for testing."""

    @classmethod
    def application(cls) -> str:
        """The supported version of the input model."""
        return "sleep"

    @classmethod
    def source(cls) -> str:
        """The supported version of the input model."""
        return "test.2"

    @classmethod
    def target(cls) -> str:
        """The version of the output model."""
        return "test.3"

    @t.override
    def adapt(self) -> dict[str, str]:
        return {**self.model}


class BPTestAdapterV3(SchemaAdapter):
    """V2 adapter for testing."""

    @classmethod
    def application(cls) -> str:
        """The supported version of the input model."""
        return "sleep"

    @classmethod
    def source(cls) -> str:
        """The supported version of the input model."""
        return "test.3"

    @classmethod
    def target(cls) -> str:
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

    bp0 = {KEY_VERSION: src_version_exp, KEY_APP: APP_SLEEP}

    # pass in the converters instead of relying on the default
    converters: list[RawModelVersionAdapterType] = [
        BPTestAdapterV0,
        BPTestAdapterV1,
        BPTestAdapterV2,
        BPTestAdapterV3,
    ]
    bounds: dict[str, SchemaBounds] = {
        APP_SLEEP: {
            "min": BPTestAdapterV0.source(),
            "max": BPTestAdapterV3.target(),
        },
    }
    # shuffle to confirm that order in the converters list doesn't matter
    random.shuffle(converters)

    migrator = BlueprintMigration(converters, bounds)
    src_version, tgt_version, plan = migrator.plan(bp0)

    assert src_version == src_version_exp
    assert tgt_version == tgt_version_exp
    assert len(plan) == exp_num_steps
