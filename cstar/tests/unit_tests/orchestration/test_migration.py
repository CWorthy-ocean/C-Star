import random
import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.applications.hello_world_app import HelloWorldBlueprint
from cstar.orchestration.serialization import deserialize
from cstar.system.migration import (
    APP_HW_SCHEMA_1_0_0,
    APP_ROMS_MARBL_SCHEMA_1_0_0,
    APP_ROMS_MARBL_SCHEMA_2_0_0,
    SCHEMA_VERSION_KEY,
    BlueprintMigration,
    CstarMigrationError,
    CstarUnsupportedMigrationError,
    SchemaAdapter,
    SchemaBounds,
)

APP_SLEEP: t.Literal["sleep"] = "sleep"
APP_ROMS: t.Literal["roms_marbl"] = "roms_marbl"

KEY_SV = SCHEMA_VERSION_KEY
KEY_APP: t.Literal["application"] = "application"


@pytest.mark.parametrize(
    ("model", "exp_version"),
    [
        pytest.param(
            {KEY_APP: APP_ROMS},
            APP_ROMS_MARBL_SCHEMA_2_0_0,
            id="rm::app-only",
        ),
        pytest.param(
            {KEY_APP: APP_ROMS, KEY_SV: APP_ROMS_MARBL_SCHEMA_1_0_0},
            APP_ROMS_MARBL_SCHEMA_2_0_0,
            id="rm::with source schema",
        ),
        pytest.param(
            {KEY_APP: "hello_world", KEY_SV: "1.0.0"},
            "1.0.0",
            id="hw::with source schema",
        ),
    ],
)
def test_migration_version(model: dict[str, t.Any], exp_version: str) -> None:
    """Verify that a migrated model contains the latest schema version."""
    bp0 = model
    migrator = BlueprintMigration()

    result = migrator.plan_and_migrate(bp0)

    assert "schema_version" in result.migrated
    assert exp_version == result.migrated["schema_version"]


def test_migration_simple_plan() -> None:
    """Verify a simple, one-step migration is planned when using
    the default migration adapters registered in `BlueprintMigration`
    for `roms_marbl` blueprints.
    """
    src_version_exp = APP_ROMS_MARBL_SCHEMA_1_0_0
    tgt_version_exp = APP_ROMS_MARBL_SCHEMA_2_0_0

    bp0 = {SCHEMA_VERSION_KEY: src_version_exp, KEY_APP: APP_ROMS}
    migrator = BlueprintMigration()

    # static target avoids the test planning multiple steps as migration count grows.
    src_version, tgt_version, plan = migrator.plan(bp0)

    assert src_version == src_version_exp
    assert tgt_version == tgt_version_exp
    assert plan


def test_migration_no_migration_needed(hello_world_bp_path: Path) -> None:
    """Verify a blueprint that is already at the latest schema version results
    in an empty migration plan.
    """
    src_version_exp = APP_HW_SCHEMA_1_0_0
    tgt_version_exp = src_version_exp

    bp = deserialize(hello_world_bp_path, HelloWorldBlueprint)
    migrator = BlueprintMigration()

    # static target avoids the test planning multiple steps as migration count grows.
    src_version, tgt_version, plan = migrator.plan(bp.model_dump())

    assert src_version == src_version_exp
    assert tgt_version == tgt_version_exp
    assert not plan


def test_migration_no_plan_no_changes(hello_world_bp_path: Path) -> None:
    """Verify that a blueprint with no plan (already the latest version) is not
    modified at all.
    """
    bp = deserialize(hello_world_bp_path, HelloWorldBlueprint)
    migrator = BlueprintMigration()

    dumped = bp.model_dump()
    source, target, adapters = migrator.plan(dumped)

    assert source == target
    assert not adapters

    dumped.pop(KEY_SV, None)  # remove version inserted by the fixture.
    plan = migrator.plan(dumped)
    migrated = migrator.migrate(dumped, plan)

    assert KEY_SV not in migrated


def test_migration_with_unregistered_application() -> None:
    """Verify that a migration error is raised when a migration of
    an unknown schema type is attempted.
    """
    src_version_exp = BPTestAdapterV0.source()
    tgt_version_exp = BPTestAdapterV3.target()

    bp0 = {SCHEMA_VERSION_KEY: src_version_exp, KEY_APP: APP_SLEEP}

    converters: list[type[SchemaAdapter]] = [
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
    tgt_version_exp = "1.0.42"

    bp0 = {SCHEMA_VERSION_KEY: src_version_exp, KEY_APP: APP_SLEEP}

    converters: list[type[SchemaAdapter]] = [
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
    src_version_exp = "1.0.UNK"

    bp0 = {SCHEMA_VERSION_KEY: src_version_exp, KEY_APP: APP_SLEEP}
    converters: list[type[SchemaAdapter]] = [
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

    @classmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        return {**model}


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

    @classmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        return {**model}


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

    @classmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        return {**model}


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

    @classmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        return {**model}


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

    bp0 = {SCHEMA_VERSION_KEY: src_version_exp, KEY_APP: APP_SLEEP}

    # pass in the converters instead of relying on the default
    converters: list[type[SchemaAdapter]] = [
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
