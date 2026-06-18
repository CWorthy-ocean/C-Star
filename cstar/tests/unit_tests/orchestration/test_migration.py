import random
import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.applications.hello_world_app import (
    APP_HW_SCHEMA_1_0_0,
    HelloWorldBlueprint,
    HelloWorldSchemaAdapterV1V1,
)
from cstar.applications.plotter_app import (
    APP_PLOTTER_SCHEMA_1_0_0,
    APP_PLOTTER_SCHEMA_2_0_0,
    PlotterSchemaAdapterV1V2,
)
from cstar.applications.roms_marbl.app import APP_NAME as APP_ROMS
from cstar.applications.roms_marbl.migration import (
    APP_ROMS_MARBL_SCHEMA_1_0_0,
    APP_ROMS_MARBL_SCHEMA_2_0_0,
    RomsMarblSchemaAdapter2025v1,
)
from cstar.orchestration.serialization import deserialize
from cstar.system.migration import (
    KEY_APP,
    KEY_SV,
    BlueprintMigration,
    CstarMigrationError,
    CstarUnsupportedMigrationError,
    SchemaAdapter,
    SchemaBounds,
)

APP_SLEEP: t.Final[str] = "sleep"


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
            {KEY_APP: "hello_world", KEY_SV: APP_HW_SCHEMA_1_0_0},
            APP_HW_SCHEMA_1_0_0,
            id="hw::with source schema",
        ),
    ],
)
def test_migration_version(model: dict[str, t.Any], exp_version: str) -> None:
    """Verify that a migrated model contains the latest schema version."""
    bp0 = model

    adapters: list[type[SchemaAdapter]] = [
        RomsMarblSchemaAdapter2025v1,
        HelloWorldSchemaAdapterV1V1,
    ]

    migrator = BlueprintMigration(adapters)

    result = migrator.plan_and_migrate(bp0)

    # we now expect the result to reflect the target schema version
    assert "schema_version" in result.migrated
    assert exp_version == result.migrated["schema_version"]


def test_migration_simple_plan() -> None:
    """Verify a simple, one-step migration is planned when using
    the default migration adapters registered in `BlueprintMigration`
    for `roms_marbl` blueprints.
    """
    src_version_exp = APP_ROMS_MARBL_SCHEMA_1_0_0
    tgt_version_exp = APP_ROMS_MARBL_SCHEMA_2_0_0

    # create a mock adapter that has the desired source and target in a single step
    mock_adapter = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter).application = lambda _cls: APP_ROMS
    type(mock_adapter).source = lambda _cls: APP_ROMS_MARBL_SCHEMA_1_0_0
    type(mock_adapter).target = lambda _cls: APP_ROMS_MARBL_SCHEMA_2_0_0

    adapters: list[type[SchemaAdapter]] = [mock_adapter]  # type: ignore  # noqa: PGH003

    bp0 = {KEY_SV: src_version_exp, KEY_APP: APP_ROMS}
    migrator = BlueprintMigration(adapters=adapters)  # , schema_bounds=schema_bounds)

    # static target avoids the test planning multiple steps as migration count grows.
    src_version, tgt_version, plan = migrator.plan(bp0)

    assert src_version == src_version_exp
    assert tgt_version == tgt_version_exp
    assert plan


def test_migration_identify_bounds() -> None:
    """Verify that bounds identification by `BlueprintMigration` uses any passed
    adapters to locate bounds if schema bounds are not explicitly provided.
    """
    global_min: t.Final[str] = "0.0.1"
    global_max: t.Final[str] = "3.0.0"

    # a mock adapter that precedes the min bound that will be manually passed.
    mock_adapter0 = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter0).application = lambda _cls: APP_ROMS
    type(mock_adapter0).source = lambda _cls: global_min
    type(mock_adapter0).target = lambda _cls: APP_ROMS_MARBL_SCHEMA_1_0_0

    # a mock adapter that exceeds the max bound that will be manually passed.
    mock_adapter1 = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter1).application = lambda _cls: APP_ROMS
    type(mock_adapter1).source = lambda _cls: APP_ROMS_MARBL_SCHEMA_2_0_0
    type(mock_adapter1).target = lambda _cls: global_max

    # a Goldilocks adapter (it's juuuuust right)
    mock_adapter2 = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter2).application = lambda _cls: APP_ROMS
    type(mock_adapter2).source = lambda _cls: APP_ROMS_MARBL_SCHEMA_1_0_0
    type(mock_adapter2).target = lambda _cls: APP_ROMS_MARBL_SCHEMA_2_0_0

    adapters: list[type[SchemaAdapter]] = [
        mock_adapter0,
        mock_adapter1,
        mock_adapter2,
    ]  # type: ignore  # noqa: PGH003

    # do not pass bounds. BlueprintMigration should auto-discover them from adapters.
    migrator = BlueprintMigration(adapters=adapters)

    # verify global min/max are found in the discovered bounds
    bounds = migrator.schema_bounds[APP_ROMS]
    assert bounds["min"] == global_min
    assert bounds["max"] == global_max

    manual_bounds: dict[str, SchemaBounds] = {
        APP_ROMS: {
            "min": mock_adapter2.source(),
            "max": mock_adapter2.target(),
        },
    }
    # BlueprintMigration should not use discovery with manually-passed bounds
    migrator = BlueprintMigration(adapters=adapters, schema_bounds=manual_bounds)

    # confirm min/max do not reflect all available adapters
    bounds = migrator.schema_bounds.get(APP_ROMS, {"min": "", "max": ""})
    assert bounds["min"] != global_min
    assert bounds["max"] != global_max


@pytest.mark.parametrize(
    ("dumped", "exp_min", "exp_max"),
    [
        pytest.param({KEY_APP: "A"}, "1.0.0", "4.0.0", id="many adapters (>2)"),
        pytest.param({KEY_APP: "B"}, "1.0.0", "5.0.0", id="multi adapter (2)"),
        pytest.param({KEY_APP: "C"}, "0.0.1", "1.0.0", id="single adapter"),
    ],
)
def test_migration_bounds_with_multiple_apps_in_adapters(
    dumped: dict[str, t.Any],
    exp_min: str,
    exp_max: str,
) -> None:
    """Verify that the `BlueprintMigration` correctly groups adapters using the
    application name when identifying schema bounds.
    """
    # app A, min version.
    mock_adapter0 = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter0).application = lambda _cls: "A"
    type(mock_adapter0).source = lambda _cls: "1.0.0"
    type(mock_adapter0).target = lambda _cls: "2.0.0"

    # app A, intermediate version.
    mock_adapter1 = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter1).application = lambda _cls: "A"
    type(mock_adapter1).source = lambda _cls: "2.0.0"
    type(mock_adapter1).target = lambda _cls: "3.0.0"

    # app A, max version.
    mock_adapter2 = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter2).application = lambda _cls: "A"
    type(mock_adapter2).source = lambda _cls: "3.0.0"
    type(mock_adapter2).target = lambda _cls: "4.0.0"

    # app B, min version.
    mock_adapter3 = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter3).application = lambda _cls: "B"
    type(mock_adapter3).source = lambda _cls: "1.0.0"
    type(mock_adapter3).target = lambda _cls: "2.0.0"

    # app B, max version, "global" maximum version
    mock_adapter4 = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter4).application = lambda _cls: "B"
    type(mock_adapter4).source = lambda _cls: "2.0.0"
    type(mock_adapter4).target = lambda _cls: "5.0.0"

    # app C, min/max, "global" minimum version...
    mock_adapter5 = mock.MagicMock(spec=type[SchemaAdapter])
    type(mock_adapter5).application = lambda _cls: "C"
    type(mock_adapter5).source = lambda _cls: "0.0.1"
    type(mock_adapter5).target = lambda _cls: "1.0.0"

    adapters: list[type[SchemaAdapter]] = [
        mock_adapter0,
        mock_adapter1,
        mock_adapter2,
        mock_adapter3,
        mock_adapter4,
        mock_adapter5,
    ]  # type: ignore  # noqa: PGH003

    # do not pass bounds. BlueprintMigration should auto-discover them from adapters.
    migrator = BlueprintMigration(adapters=adapters)

    # verify expected min/max bounds for each app
    bounds = migrator.schema_bounds[dumped[KEY_APP]]
    assert bounds["min"] == exp_min
    assert bounds["max"] == exp_max


def test_migration_no_migration_needed(hello_world_bp_path: Path) -> None:
    """Verify a blueprint that is already at the latest schema version results
    in an empty migration plan.
    """
    src_version_exp = HelloWorldSchemaAdapterV1V1.source()
    tgt_version_exp = HelloWorldSchemaAdapterV1V1.target()

    adapters = [HelloWorldSchemaAdapterV1V1]
    bp = deserialize(hello_world_bp_path, HelloWorldBlueprint)
    migrator = BlueprintMigration(adapters)

    # HW adapter has source == target
    src_version, tgt_version, plan = migrator.plan(bp.model_dump())

    # we expect the source and target to be equal and come from the adapter
    assert src_version == src_version_exp
    assert src_version == tgt_version_exp
    assert src_version == tgt_version

    # we expect no plan to be provided since it's up-to-date
    assert not plan


def test_migration_no_plan_no_changes(hello_world_bp_path: Path) -> None:
    """Verify that a blueprint with no plan (already the latest version) is not
    modified when migrate is called.
    """
    adapters = [HelloWorldSchemaAdapterV1V1]
    bp = deserialize(hello_world_bp_path, HelloWorldBlueprint)
    migrator = BlueprintMigration(adapters)

    dumped = bp.model_dump()

    # remove SV. migrator should assume input is @ min version
    del dumped[KEY_SV]
    original_keys = set(dumped.keys())

    result = migrator.plan_and_migrate(dumped)

    migrated = result.migrated
    migrated_keys = set(migrated.keys())

    # the migration would update schema_version if it ran. confirm it didn't
    assert KEY_SV not in migrated
    assert original_keys == migrated_keys


def test_migration_with_unregistered_application() -> None:
    """Verify that a migration error is raised when a migration of
    an unknown schema type is attempted.
    """
    src_version_exp = BPTestAdapterV0.source()
    tgt_version_exp = BPTestAdapterV3.target()

    bp0 = {KEY_SV: src_version_exp, KEY_APP: APP_SLEEP}

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
        migrator = BlueprintMigration(converters, schema_bounds=bounds)

    with pytest.raises(
        CstarUnsupportedMigrationError,
        match="schema bounds",
    ):
        _ = migrator.plan(bp0)


def test_migration_to_unknown_target() -> None:
    """Verify that an incomplete migration path raises an exception."""
    src_version_exp = BPTestAdapterV0.source()
    tgt_version_exp = "1.0.42"

    bp0 = {KEY_SV: src_version_exp, KEY_APP: APP_SLEEP}

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

    bp0 = {KEY_SV: src_version_exp, KEY_APP: APP_SLEEP}
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

    bp0 = {KEY_SV: src_version_exp, KEY_APP: APP_SLEEP}

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


def test_migrate_plotter(plotter_v1_0_0_model: dict[str, t.Any]) -> None:
    """Verify a simple, one-step migration is planned when using
    the default migration adapters registered in `BlueprintMigration`
    for `roms_marbl` blueprints.
    """
    src_version_exp = APP_PLOTTER_SCHEMA_1_0_0
    tgt_version_exp = APP_PLOTTER_SCHEMA_2_0_0

    adapters = [PlotterSchemaAdapterV1V2]
    assert PlotterSchemaAdapterV1V2.source() != PlotterSchemaAdapterV1V2.target()

    migrator = BlueprintMigration(adapters)

    # static target avoids the test planning multiple steps as migration count grows.
    src_version, tgt_version, plan = migrator.plan(plotter_v1_0_0_model)

    assert src_version == src_version_exp
    assert tgt_version == tgt_version_exp
    assert plan
