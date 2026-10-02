"""Forge test cases shared by the integration-suite tiers.

All cases run the same one-hour window of the 2012-01-01 forcing files. ``DT`` is the
value the committed ``cstar_blueprint_test_case`` ran with: one hour at 60 s is 60
steps and two 1800 s restart records.

There is no tides case yet: the TPXO source needs a ``{grid, h, u}`` path mapping but
``SourceSpec.path`` is typed ``str | None`` (see ``catalog/README.md``).
"""

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

RUN_START = datetime(2012, 1, 1, 12)
RUN_END = datetime(2012, 1, 1, 13)
MODEL_REFERENCE_DATE = datetime(2000, 1, 1)
DT = 60.0
MODEL_SPEC = "roms-marbl-0.8-default"
PINNED_ROMS_REF = "0.8.0"
# the ucla-roms ref the bundled ``roms-marbl-0.8-default`` ModelSpec pins
ROMS_REF = os.environ.get("CSTAR_IT_ROMS_REF") or PINNED_ROMS_REF
"""The ucla-roms ref under test; ``CSTAR_IT_ROMS_REF`` is the weekly extended workflow's override."""


@dataclass(frozen=True)
class ForgeCase:
    """A domain + forcing pairing from the test catalog, with optional cppdefs overrides."""

    domain: str
    forcing: str
    compile_time_overrides: dict[str, Any] | None = None


FORGE_CASES = {
    "unified": ForgeCase("na-test-8x8", "test-glorys-era5-unified"),
    "constants": ForgeCase(
        "na-test-8x8",
        "test-glorys-era5-constants",
        compile_time_overrides={
            "cppdefs": {"nhy_forcing": False, "nox_forcing": False}
        },
    ),
}
