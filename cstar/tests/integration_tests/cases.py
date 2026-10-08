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

from cstar.applications.forge.namelist_model import BgcMode

RUN_START = datetime(2012, 1, 1, 12)
RUN_END = datetime(2012, 1, 1, 13)
MODEL_REFERENCE_DATE = datetime(2000, 1, 1)
DT = 60.0
MODEL_SPEC = "roms-marbl-0.9-default"
PINNED_ROMS_REF = "0.9.1"
# the ucla-roms ref the bundled ``roms-marbl-0.9-default`` ModelSpec pins
ROMS_REF = os.environ.get("CSTAR_IT_ROMS_REF") or PINNED_ROMS_REF
"""The ucla-roms ref under test; ``CSTAR_IT_ROMS_REF`` is the weekly extended workflow's override."""


@dataclass(frozen=True)
class ForgeCase:
    """A domain + forcing pairing from the test catalog, with optional cppdefs overrides.

    ``run_time_overrides``, ``bgc_mode`` and ``cdr`` are forwarded to
    ``build_forge_blueprint``; ``cdr`` is the ``CdrSpec`` dict a ``cdr_lite`` build
    requires.
    """

    domain: str
    forcing: str
    compile_time_overrides: dict[str, Any] | None = None
    run_time_overrides: dict[str, Any] | None = None
    bgc_mode: BgcMode = "marbl"
    cdr: dict[str, Any] | None = None


CDR_LITE_FORCING: dict[str, Any] = {
    "start_time": RUN_START.isoformat(),
    "end_time": RUN_END.isoformat(),
    "releases": [
        {
            "name": name,
            "lat": 60.0,
            "lon": -5.0,
            "depth": 10.0,
            "hsc": 10.0,
            "vsc": 10.0,
            "times": [RUN_START.isoformat(), RUN_END.isoformat()],
            "release_type": "tracer_perturbation",
            "tracer_fluxes": {role: [2.0e6, 2.0e6]},
        }
        for name, role in (("oae", "ALK"), ("dor", "DIC"))
    ],
}
"""A ``simple``-mode CDR forcing over the run window, at the ``na-test-8x8`` grid centre:
one OAE release (an ALK/DIC tracer pair) and one DOR release (one DIC tracer), so a
``cdr_lite`` build has ``nt_cdr_oae == nt_cdr_dor == 1`` and five tracers. The resolver
defaults each release's ``tracer_set`` to ``cdr_lite``."""


FORGE_CASES = {
    "unified": ForgeCase("na-test-8x8", "test-glorys-era5-unified"),
    "constants": ForgeCase(
        "na-test-8x8",
        "test-glorys-era5-constants",
        compile_time_overrides={
            "cppdefs": {"nhy_forcing": False, "nox_forcing": False}
        },
    ),
    "cdr_lite": ForgeCase(
        "na-test-8x8",
        "test-glorys-era5-physics",
        # a cdr_lite build forces the _cdrtrc stream on, and its rollover must divide
        # the 1800 s restart period: the OutputSpec's 24 x 3600 s would abort configure_build
        run_time_overrides={"cdr_lite_output": {"output_period": 1800, "nrpf": 1}},
        bgc_mode="cdr_lite",
        cdr={"mode": "simple", "cdr_forcing": CDR_LITE_FORCING},
    ),
}
