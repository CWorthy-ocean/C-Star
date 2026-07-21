import typing as t

from cstar.base.adapter import SchemaAdapter

APP_NAME: t.Final[str] = "roms_marbl"
APP_ROMS_MARBL_SCHEMA_1_0_0: t.Final[str] = "1.0.0"
APP_ROMS_MARBL_SCHEMA_2_0_0: t.Final[str] = "2.0.0"
APP_ROMS_MARBL_SCHEMA_2_1_0: t.Final[str] = "2.1.0"


rm_bounds = {
    "min": APP_ROMS_MARBL_SCHEMA_1_0_0,
    "max": APP_ROMS_MARBL_SCHEMA_2_1_0,
}
"""Schema bounds for the roms_marbl blueprint schema.

The schema bounds enable the migration tool to:
- automatically set version to  minimum version for a blueprint that predated versioning
- configure which version it will target for updates
"""


class RomsMarblSchemaAdapter2025v1(SchemaAdapter):
    """Schema migration from schema version `1.0.0` to `2.0.0`.

    Adapting `1.0.0` to `2.0.0`:
    - use `working_dir` attribute from `Blueprint` base class instead of `runtime_params.output_dir`
    """

    @classmethod
    def application(cls) -> str:
        return APP_NAME

    @classmethod
    def source(cls) -> str:
        return APP_ROMS_MARBL_SCHEMA_1_0_0

    @classmethod
    def target(cls) -> str:
        return APP_ROMS_MARBL_SCHEMA_2_0_0

    @classmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        runtime_params = t.cast(
            "dict[str, str | None]",
            model.get("runtime_params", {"output_dir": None}),
        )
        if working_dir := runtime_params.pop("output_dir", None):
            model["working_dir"] = working_dir
        return {**model}


class RomsMarblSchemaAdapterV2V21(SchemaAdapter):
    """Schema migration from schema version `2.0.0` to `2.1.0`.

    Adapting `2.0.0` to `2.1.0`:
    - adds optional `code.pio` and `model_params.use_pio`, both with safe
      defaults, so no structural change is needed. The base adapter stamps the
      new schema version and Pydantic supplies the field defaults on load.
    """

    @classmethod
    def application(cls) -> str:
        return APP_NAME

    @classmethod
    def source(cls) -> str:
        return APP_ROMS_MARBL_SCHEMA_2_0_0

    @classmethod
    def target(cls) -> str:
        return APP_ROMS_MARBL_SCHEMA_2_1_0

    @classmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        return {**model}
