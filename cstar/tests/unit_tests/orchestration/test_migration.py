import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.applications.core import get_application
from cstar.applications.hello_world import APP_NAME as APP_HW
from cstar.applications.hello_world import HelloWorldBlueprint
from cstar.applications.plotter import (
    APP_NAME as APP_PLOTTER,
)
from cstar.applications.plotter import (
    APP_PLOTTER_SCHEMA_1_0_0,
    PlotterSchemaAdapterV1V2,
)
from cstar.applications.roms_marbl.app import APP_NAME as APP_ROMS
from cstar.applications.roms_marbl.migration import (
    APP_ROMS_MARBL_SCHEMA_1_0_0,
    APP_ROMS_MARBL_SCHEMA_2_1_0,
    APP_ROMS_MARBL_SCHEMA_3_0_0,
    RomsMarblSchemaAdapter2025v1,
    RomsMarblSchemaAdapterV2V21,
    RomsMarblSchemaAdapterV21V3,
)
from cstar.base.adapter import SchemaAdapter, SchemaBreak
from cstar.orchestration.serialization import deserialize
from cstar.system.migration import (
    KEY_APP,
    KEY_SV,
    BlueprintMigration,
    CstarManualMigrationError,
    CstarMigrationError,
    CstarSchemaTooNewError,
    CstarUnsupportedMigrationError,
)

APP_NAME: t.Final[str] = "fake_app"
"""Application name used by the in-file fake adapters."""

ROMS_ADAPTERS: t.Final[list[type[SchemaAdapter]]] = [
    RomsMarblSchemaAdapter2025v1,
    RomsMarblSchemaAdapterV2V21,
    RomsMarblSchemaAdapterV21V3,
]
"""The real roms_marbl adapter chain."""

ROMS_TARGET: t.Final[str] = "3.1.0"
"""A build version one minor ahead of the last roms_marbl adapter target."""


def _fake_adapter(
    source: str,
    target: str,
    application: str = APP_NAME,
    base: type = SchemaAdapter,
    **attrs: t.Any,
) -> type[SchemaAdapter]:
    """Create an adapter class that maps `source` to `target`."""
    namespace: dict[str, t.Any] = {
        "application": classmethod(lambda cls: application),
        "source": classmethod(lambda cls: source),
        "target": classmethod(lambda cls: target),
        "_migrate_schema": classmethod(lambda cls, model: {**model}),
        **attrs,
    }
    return type(f"Fake_{application}_{source}_{target}", (base,), namespace)


@pytest.mark.parametrize(
    ("model", "target", "exp_version"),
    [
        pytest.param(
            {KEY_APP: APP_ROMS},
            APP_ROMS_MARBL_SCHEMA_3_0_0,
            APP_ROMS_MARBL_SCHEMA_3_0_0,
            id="rm::app-only",
        ),
        pytest.param(
            {KEY_APP: APP_ROMS, KEY_SV: APP_ROMS_MARBL_SCHEMA_1_0_0},
            APP_ROMS_MARBL_SCHEMA_3_0_0,
            APP_ROMS_MARBL_SCHEMA_3_0_0,
            id="rm::with source schema",
        ),
        pytest.param(
            {KEY_APP: APP_ROMS, KEY_SV: APP_ROMS_MARBL_SCHEMA_1_0_0},
            ROMS_TARGET,
            ROMS_TARGET,
            id="rm::stamped with the build version",
        ),
        pytest.param(
            {KEY_APP: APP_HW, KEY_SV: "1.0.0"},
            "1.0.0",
            "1.0.0",
            id="hw::with source schema",
        ),
    ],
)
def test_migration_version(
    model: dict[str, t.Any], target: str, exp_version: str
) -> None:
    """Verify that a migrated model contains the build's schema version."""
    migrator = BlueprintMigration(
        ROMS_ADAPTERS, targets={APP_ROMS: target, APP_HW: "1.0.0"}
    )

    plan = migrator.plan(model)
    result = migrator.migrate(model, plan)

    assert exp_version == result.migrated["schema_version"]


def test_migrate_v21_to_v3_moves_time_step_and_use_pio() -> None:
    """Verify `time_step` moves to `namelist_overrides.time_stepping.dt`,
    `use_pio` moves to `partitioning.use_pio`, and `model_params` is removed.
    """
    model = {
        KEY_APP: APP_ROMS,
        KEY_SV: APP_ROMS_MARBL_SCHEMA_2_1_0,
        "model_params": {"time_step": 360, "use_pio": True},
        "partitioning": {"n_procs_x": 2, "n_procs_y": 2},
    }

    adapted = RomsMarblSchemaAdapterV21V3(model).adapt()

    assert adapted["namelist_overrides"]["time_stepping"]["dt"] == 360
    assert adapted["partitioning"]["use_pio"] is True
    assert "model_params" not in adapted
    assert adapted[KEY_SV] == APP_ROMS_MARBL_SCHEMA_3_0_0

    # source model is not mutated
    assert model["model_params"] == {"time_step": 360, "use_pio": True}


def test_migrate_v21_to_v3_without_model_params_passes_through() -> None:
    """Verify a model with no `model_params` migrates cleanly."""
    model = {
        KEY_APP: APP_ROMS,
        KEY_SV: APP_ROMS_MARBL_SCHEMA_2_1_0,
        "partitioning": {"n_procs_x": 2, "n_procs_y": 2},
    }

    adapted = RomsMarblSchemaAdapterV21V3(model).adapt()

    assert "model_params" not in adapted
    assert "namelist_overrides" not in adapted
    assert adapted["partitioning"] == {"n_procs_x": 2, "n_procs_y": 2}
    assert adapted[KEY_SV] == APP_ROMS_MARBL_SCHEMA_3_0_0


def test_migrate_v21_to_v3_existing_namelist_override_wins() -> None:
    """Verify a pre-existing `namelist_overrides.time_stepping.dt` is not
    overwritten by the value seeded from `model_params.time_step`.
    """
    model = {
        KEY_APP: APP_ROMS,
        KEY_SV: APP_ROMS_MARBL_SCHEMA_2_1_0,
        "model_params": {"time_step": 360},
        "namelist_overrides": {"time_stepping": {"dt": 999}},
    }

    adapted = RomsMarblSchemaAdapterV21V3(model).adapt()

    assert adapted["namelist_overrides"]["time_stepping"]["dt"] == 999


def test_migration_simple_plan() -> None:
    """Verify a simple, one-step migration is planned."""
    adapter = _fake_adapter("1.0.0", "2.0.0")
    migrator = BlueprintMigration([adapter], targets={APP_NAME: "2.0.0"})

    src_version, tgt_version, plan = migrator.plan({KEY_SV: "1.0.0", KEY_APP: APP_NAME})

    assert src_version == "1.0.0"
    assert tgt_version == "2.0.0"
    assert list(plan) == [adapter]


@pytest.mark.parametrize(
    ("found", "target"),
    [
        pytest.param("3.0.0", "3.0.0", id="same version"),
        pytest.param("3.0.0", "3.1.0", id="older minor"),
        pytest.param("3.0.0", "3.0.7", id="older patch"),
    ],
)
def test_plan_compatible_has_no_adapters(found: str, target: str) -> None:
    """Verify documents of the build's major version plan with no adapters and
    do not report a plan to the callback.
    """
    on_planned = mock.Mock()
    migrator = BlueprintMigration(
        ROMS_ADAPTERS, targets={APP_ROMS: target}, on_planned=on_planned
    )

    plan = migrator.plan({KEY_APP: APP_ROMS, KEY_SV: found})

    assert (plan.source, plan.target) == (found, target)
    assert not plan.adapters
    assert plan.is_compatible
    on_planned.assert_not_called()


@pytest.mark.parametrize("found", ["9.9.9", "3.0.1", "3.1.0"])
def test_plan_too_new(found: str) -> None:
    """Verify a document newer than the build in any component is refused."""
    migrator = BlueprintMigration(ROMS_ADAPTERS, targets={APP_ROMS: "3.0.0"})

    with pytest.raises(CstarSchemaTooNewError, match="Upgrade cstar-ocean") as ex:
        migrator.plan({KEY_APP: APP_ROMS, KEY_SV: found})

    assert (ex.value.application, ex.value.found, ex.value.target) == (
        APP_ROMS,
        found,
        "3.0.0",
    )


def test_plan_missing_version_is_1_0_0() -> None:
    """Verify a document with no schema version walks the chain from 1.0.0."""
    migrator = BlueprintMigration(ROMS_ADAPTERS, targets={APP_ROMS: ROMS_TARGET})

    plan = migrator.plan({KEY_APP: APP_ROMS})

    assert plan.source == "1.0.0"
    assert list(plan.adapters) == ROMS_ADAPTERS


@pytest.mark.parametrize(
    ("found", "exp_adapters"),
    [
        pytest.param("1.0.0", ROMS_ADAPTERS, id="1.0.0"),
        pytest.param("2.0.0", ROMS_ADAPTERS[1:], id="2.0.0"),
        pytest.param("2.0.5", ROMS_ADAPTERS[1:], id="2.0.5 uses the 2.0.0 adapter"),
        pytest.param("2.1.0", ROMS_ADAPTERS[2:], id="2.1.0"),
        pytest.param("2.2.0", ROMS_ADAPTERS[2:], id="2.2.0 uses the 2.1.0 adapter"),
    ],
)
def test_plan_roms_marbl_chain(
    found: str, exp_adapters: list[type[SchemaAdapter]]
) -> None:
    """Verify older-major documents walk the adapters by major version."""
    migrator = BlueprintMigration(ROMS_ADAPTERS, targets={APP_ROMS: ROMS_TARGET})

    plan = migrator.plan({KEY_APP: APP_ROMS, KEY_SV: found})

    assert list(plan.adapters) == exp_adapters
    assert (plan.source, plan.target) == (found, ROMS_TARGET)
    assert not plan.is_compatible


def test_plan_older_major_without_adapters_needs_manual_migration() -> None:
    """Verify an older major with nothing registered is refused with generic guidance."""
    migrator = BlueprintMigration([], targets={APP_NAME: "3.0.0"})

    with pytest.raises(CstarManualMigrationError) as ex:
        migrator.plan({KEY_APP: APP_NAME, KEY_SV: "1.0.0"})

    msg = str(ex.value)
    assert "no automatic migration to 3.0.0 from 1.x" in msg
    assert "cstar blueprint check" in msg


def test_plan_stuck_chain_needs_manual_migration() -> None:
    """Verify a chain that stops short of the build's major is refused."""
    adapters = [_fake_adapter("1.0.0", "2.0.0")]
    migrator = BlueprintMigration(adapters, targets={APP_NAME: "4.0.0"})

    with pytest.raises(CstarManualMigrationError):
        migrator.plan({KEY_APP: APP_NAME, KEY_SV: "1.0.0"})


def test_plan_past_last_adapter_of_major_needs_manual_migration() -> None:
    """Verify a document beyond an adapter's target, with no later adapter, cannot
    be walked (it would otherwise re-select the same adapter forever).
    """
    adapters = [_fake_adapter("2.0.0", "2.1.0")]
    migrator = BlueprintMigration(adapters, targets={APP_NAME: "3.0.0"})

    with pytest.raises(CstarManualMigrationError):
        migrator.plan({KEY_APP: APP_NAME, KEY_SV: "2.1.5"})


def test_plan_schema_break_refuses_with_guidance() -> None:
    """Verify a registered `SchemaBreak` refuses with its guidance, without
    running any adapter.
    """
    migrate = mock.Mock(side_effect=AssertionError("adapter must not run"))
    schema_break = _fake_adapter(
        "3.0.0",
        "4.0.0",
        base=SchemaBreak,
        guidance=classmethod(lambda cls: "move x to y"),
        _migrate_schema=classmethod(lambda cls, model: migrate(model)),
    )
    migrator = BlueprintMigration([schema_break], targets={APP_NAME: "4.0.0"})

    with pytest.raises(CstarManualMigrationError, match="move x to y"):
        migrator.plan({KEY_APP: APP_NAME, KEY_SV: "3.0.0"})

    migrate.assert_not_called()


@pytest.mark.parametrize("found", ["2.0.0", "2.0.5", "2.1.0", "2.1.5"])
def test_plan_schema_break_guides_every_file_of_its_major(found: str) -> None:
    """Verify a break registered at the last version of a retired major guides
    files older than that version too, not only files at or past it.
    """
    schema_break = _fake_adapter(
        "2.1.0",
        "3.0.0",
        base=SchemaBreak,
        guidance=classmethod(lambda cls: "move x to y"),
    )
    migrator = BlueprintMigration([schema_break], targets={APP_NAME: "3.0.0"})

    with pytest.raises(CstarManualMigrationError, match="move x to y"):
        migrator.plan({KEY_APP: APP_NAME, KEY_SV: found})


def test_migrate_stamps_plan_target() -> None:
    """Verify the migrated document is written at the build's version even when
    the last adapter stops at an older minor of the same major.
    """
    adapters = [_fake_adapter("1.0.0", "2.0.0")]
    migrator = BlueprintMigration(adapters, targets={APP_NAME: "2.1.0"})
    dumped = {KEY_APP: APP_NAME, KEY_SV: "1.0.0"}

    # the walk stops at 2.0.0, in the build's major; no 2.0.0 -> 2.1.0 adapter is needed
    plan = migrator.plan(dumped)
    assert plan.target == "2.1.0"
    assert plan.adapters[-1].target() == "2.0.0"

    result = migrator.migrate(dumped, plan)

    assert result.migrated[KEY_SV] == "2.1.0"
    assert result.original[KEY_SV] == "1.0.0"


def test_migrate_calls_on_migrated() -> None:
    """Verify the migrated callback receives the executed plan."""
    on_migrated = mock.Mock()
    adapters = [_fake_adapter("1.0.0", "2.0.0")]
    migrator = BlueprintMigration(
        adapters, targets={APP_NAME: "2.0.0"}, on_migrated=on_migrated
    )
    dumped = {KEY_APP: APP_NAME, KEY_SV: "1.0.0"}

    plan = migrator.plan(dumped)
    migrator.migrate(dumped, plan)

    on_migrated.assert_called_once_with(plan)


def test_migrate_failing_adapter_raises() -> None:
    """Verify an adapter that produces no document fails the migration."""
    adapter = _fake_adapter(
        "1.0.0", "2.0.0", _migrate_schema=classmethod(lambda cls, model: {})
    )
    migrator = BlueprintMigration([adapter], targets={APP_NAME: "2.0.0"})
    dumped = {KEY_APP: APP_NAME, KEY_SV: "1.0.0"}

    with pytest.raises(CstarMigrationError, match="failed"):
        migrator.migrate(dumped, migrator.plan(dumped))


def test_construction_rejects_non_advancing_adapter() -> None:
    """Verify an adapter whose target does not exceed its source is rejected."""
    for source, target in [("2.0.0", "2.0.0"), ("2.0.0", "1.0.0")]:
        with pytest.raises(ValueError, match="advance"):
            BlueprintMigration(
                [_fake_adapter(source, target)], targets={APP_NAME: "3.0.0"}
            )


def test_construction_rejects_duplicate_source() -> None:
    """Verify two adapters for the same (application, source) are rejected."""
    adapters = [_fake_adapter("1.0.0", "2.0.0"), _fake_adapter("1.0.0", "3.0.0")]

    with pytest.raises(ValueError, match="share"):
        BlueprintMigration(adapters, targets={APP_NAME: "3.0.0"})


def test_construction_allows_same_source_across_applications() -> None:
    """Verify adapters of different applications may share a source version."""
    adapters = [
        _fake_adapter("1.0.0", "2.0.0", application="A"),
        _fake_adapter("1.0.0", "2.0.0", application="B"),
    ]

    migrator = BlueprintMigration(adapters, targets={"A": "2.0.0", "B": "2.0.0"})

    assert len(migrator.adapter_lookup) == 2


def test_plan_multiple_apps_are_independent() -> None:
    """Verify the planner groups adapters by application."""
    a_adapters = [
        _fake_adapter("1.0.0", "2.0.0", "A"),
        _fake_adapter("2.0.0", "3.0.0", "A"),
    ]
    b_adapters = [_fake_adapter("1.0.0", "2.0.0", "B")]
    migrator = BlueprintMigration(
        [*a_adapters, *b_adapters], targets={"A": "3.0.0", "B": "2.0.0"}
    )

    assert list(migrator.plan({KEY_APP: "A", KEY_SV: "1.0.0"}).adapters) == a_adapters
    assert list(migrator.plan({KEY_APP: "B", KEY_SV: "1.0.0"}).adapters) == b_adapters


def test_migration_orders_versions_numerically() -> None:
    """Verify the planner compares versions numerically per component.

    Lexicographic string comparison would order "10.0.0" before "2.0.0".
    """
    adapters = [_fake_adapter("2.0.0", "10.0.0")]
    migrator = BlueprintMigration(adapters, targets={APP_NAME: "10.0.0"})

    assert (
        list(migrator.plan({KEY_APP: APP_NAME, KEY_SV: "2.0.0"}).adapters) == adapters
    )
    with pytest.raises(CstarSchemaTooNewError):
        BlueprintMigration([], targets={APP_NAME: "2.0.0"}).plan(
            {KEY_APP: APP_NAME, KEY_SV: "10.0.0"}
        )


def test_migration_no_migration_needed(hello_world_bp_path: Path) -> None:
    """Verify a blueprint that is already at the build's schema version results
    in an empty migration plan.
    """
    target = get_application(APP_HW).schema_version
    bp = deserialize(hello_world_bp_path, HelloWorldBlueprint)
    migrator = BlueprintMigration([], targets={APP_HW: target})

    src_version, tgt_version, adapters = migrator.plan(bp.model_dump())

    # the template is the first schema version; the build reads it without adapters
    assert src_version == "1.0.0"
    assert tgt_version == target
    assert not adapters


def test_plan_malformed_version_is_unsupported() -> None:
    """Verify a schema_version that is not dotted integers is refused as unsupported."""
    migrator = BlueprintMigration(adapters=[], targets={"app": "1.0.0"})

    with pytest.raises(
        CstarUnsupportedMigrationError, match="Unrecognized schema_version"
    ):
        migrator.plan({KEY_APP: "app", KEY_SV: "v1"})


def test_migration_with_unregistered_application() -> None:
    """Verify that an error is raised when planning for an application the
    migrator has no target version for.
    """
    migrator = BlueprintMigration([], targets={"not-sleep": "1.0.0"})

    with pytest.raises(CstarUnsupportedMigrationError, match="No schema version"):
        migrator.plan({KEY_SV: "1.0.0", KEY_APP: APP_NAME})


FAKE_CHAIN: t.Final[list[type[SchemaAdapter]]] = [
    _fake_adapter(f"{i}.0.0", f"{i + 1}.0.0") for i in range(1, 5)
]


@pytest.mark.parametrize(
    ("src_version_exp", "exp_num_steps"),
    [("1.0.0", 4), ("2.0.0", 3), ("3.0.0", 2), ("4.0.0", 1)],
)
def test_migration_intermediate_multistep(
    src_version_exp: str,
    exp_num_steps: int,
) -> None:
    """Verify that multi-step migration from any version in the history completes."""
    migrator = BlueprintMigration(
        list(reversed(FAKE_CHAIN)), targets={APP_NAME: "5.0.0"}
    )

    src_version, tgt_version, plan = migrator.plan(
        {KEY_SV: src_version_exp, KEY_APP: APP_NAME}
    )

    assert src_version == src_version_exp
    assert tgt_version == "5.0.0"
    assert len(plan) == exp_num_steps


def test_migrate_plotter(plotter_v1_0_0_model: dict[str, t.Any]) -> None:
    """Verify a simple, one-step migration is planned for plotter blueprints."""
    migrator = BlueprintMigration(
        [PlotterSchemaAdapterV1V2],
        targets={APP_PLOTTER: get_application(APP_PLOTTER).schema_version},
    )

    src_version, tgt_version, plan = migrator.plan(plotter_v1_0_0_model)

    assert src_version == APP_PLOTTER_SCHEMA_1_0_0
    assert tgt_version == get_application(APP_PLOTTER).schema_version
    assert list(plan) == [PlotterSchemaAdapterV1V2]


@pytest.mark.parametrize(
    ("app_name", "exp_version"),
    [
        (APP_ROMS, "3.1.0"),
        (APP_PLOTTER, "2.1.0"),
        (APP_HW, "1.1.0"),
    ],
)
def test_application_schema_version(app_name: str, exp_version: str) -> None:
    """Verify an application reports the version its blueprint model defaults to."""
    assert get_application(app_name).schema_version == exp_version
