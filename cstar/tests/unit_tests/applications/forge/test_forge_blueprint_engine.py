"""
Tests for ``cstar.applications.forge.engine`` -- primarily
``sources_to_forcing_override``, the ForgeBlueprint -> executor forcing-override
conversion, and its interaction with ``ForgeBlueprint.content_hash()``.

Reuses ``test_forge_blueprint``'s ``_build`` fixture helper (a full,
resolver-built ``ForgeBlueprint`` off the bundled ``glorys-era5-unified``
ForcingSpec, which has exactly one bgc source on both initial_conditions and
forcing.boundary) rather than duplicating it here.
"""

from test_forge_blueprint import _build

from cstar.applications.forge.blueprint import (
    BgcInterpMethod,
    BgcSourceItem,
    SourceSpec,
)
from cstar.applications.forge.engine import sources_to_forcing_override


def test_bgc_source_item_fields_reach_forcing_override():
    """Regression for the dropped ``serialize_dask``: ``_bgc_section`` (inside
    ``sources_to_forcing_override``) used to hand-list which ``BgcSourceItem``
    fields it forwarded, so a field added to the model later (serialize_dask)
    was silently dropped. Introspect ``BgcSourceItem.model_fields`` here too, so
    this test itself can't silently go stale the same way.
    """
    cfg = _build()
    non_source_fields = set(BgcSourceItem.model_fields) - {"source"}
    overrides = {
        "use_vars": ["ALK", "DIC"],
        "bgc_interpolation_method": BgcInterpMethod.DENSITY,
        "serialize_dask": True,
    }
    # Guard: if BgcSourceItem gains/loses a field, this test must be updated too
    # rather than silently checking a stale subset.
    assert non_source_fields == set(overrides), (
        "BgcSourceItem's non-source fields changed -- update `overrides` above "
        "to cover every field."
    )

    ic = cfg.forcing.initial_conditions
    boundary = cfg.forcing.boundary
    cfg = cfg.model_copy(
        update={
            "forcing": cfg.forcing.model_copy(
                update={
                    "initial_conditions": ic.model_copy(
                        update={
                            "bgc_sources": [
                                ic.bgc_sources[0].model_copy(update=overrides)
                            ]
                        }
                    ),
                    "boundary": boundary.model_copy(
                        update={
                            "bgc_sources": [
                                boundary.bgc_sources[0].model_copy(update=overrides)
                            ]
                        }
                    ),
                }
            )
        }
    )

    ov = sources_to_forcing_override(cfg)
    for label, item in [
        ("initial_conditions", ov["initial_conditions"]["bgc_sources"][0]),
        ("forcing.boundary", ov["forcing"]["boundary"]["bgc_sources"][0]),
    ]:
        for field, value in overrides.items():
            expected = value.value if hasattr(value, "value") else value
            assert item.get(field) == expected, (
                f"{label}.bgc_sources[0] is missing/wrong for {field!r}: "
                f"got {item.get(field)!r}, expected {expected!r} (full item: {item!r})"
            )


def test_content_hash_ignores_bgc_serialize_dask_and_section_bypass_validation():
    """``serialize_dask`` (per bgc source) and ``bypass_validation`` (per
    section) are execution-environment knobs -- they must not perturb
    ``content_hash()``. Changing ``use_vars`` (a results-affecting choice of
    which tracers a source contributes) must.
    """
    cfg = _build()
    base_hash = cfg.content_hash()

    ic = cfg.forcing.initial_conditions
    boundary = cfg.forcing.boundary
    toggled = cfg.model_copy(
        update={
            "forcing": cfg.forcing.model_copy(
                update={
                    "initial_conditions": ic.model_copy(
                        update={
                            "bypass_validation": True,
                            "bgc_sources": [
                                ic.bgc_sources[0].model_copy(
                                    update={"serialize_dask": True}
                                )
                            ],
                        }
                    ),
                    "boundary": boundary.model_copy(
                        update={
                            "bypass_validation": True,
                            "bgc_sources": [
                                boundary.bgc_sources[0].model_copy(
                                    update={"serialize_dask": True}
                                )
                            ],
                        }
                    ),
                }
            )
        }
    )
    assert toggled.content_hash() == base_hash

    changed_use_vars = cfg.model_copy(
        update={
            "forcing": cfg.forcing.model_copy(
                update={
                    "initial_conditions": ic.model_copy(
                        update={
                            "bgc_sources": [
                                ic.bgc_sources[0].model_copy(
                                    update={"use_vars": ["ALK"]}
                                )
                            ]
                        }
                    ),
                }
            )
        }
    )
    assert changed_use_vars.content_hash() != base_hash


def test_esper_salinity_conditioning_reaches_forcing_override_as_a_switch():
    """``SourceSpec.esper_salinity_conditioning`` travels to roms-tools' source dict
    as ``salinity_conditioning: True`` (input_generator later swaps in the staged WOA23
    file's path); off/unset emits nothing, matching esper_method/esper_equation.
    """
    cfg = _build()
    boundary = cfg.forcing.boundary

    def _with(source):
        return cfg.model_copy(
            update={
                "forcing": cfg.forcing.model_copy(
                    update={
                        "boundary": boundary.model_copy(
                            update={
                                "bgc_sources": [
                                    boundary.bgc_sources[0].model_copy(
                                        update={"source": source}
                                    )
                                ]
                            }
                        )
                    }
                )
            }
        )

    on = sources_to_forcing_override(
        _with(SourceSpec(name="ESPER", esper_salinity_conditioning=True))
    )
    src = on["forcing"]["boundary"]["bgc_sources"][0]["source"]
    assert src["name"] == "ESPER"
    assert src["salinity_conditioning"] is True
    assert "esper_salinity_conditioning" not in src

    off = sources_to_forcing_override(_with(SourceSpec(name="ESPER")))
    assert (
        "salinity_conditioning"
        not in off["forcing"]["boundary"]["bgc_sources"][0]["source"]
    )


def test_section_groom_reaches_roms_tools_kwargs_only_when_set():
    """``groom``/``groom_settings`` on the IC/boundary sections become the
    roms-tools ``InitialConditions``/``BoundaryForcing`` kwargs of the same name
    (``RomsInputGenerator`` passes each section dict through as constructor kwargs);
    unset emits nothing, so roms-tools' own defaults apply. No source dict, ESPER
    or otherwise, ever carries a ``groom`` key.
    """
    cfg = _build()
    f = cfg.forcing
    esper = SourceSpec(name="ESPER")

    def _sec(sec, **update):
        bgc = [sec.bgc_sources[0].model_copy(update={"source": esper})]
        return sec.model_copy(update={"bgc_sources": bgc, **update})

    unset = sources_to_forcing_override(
        cfg.model_copy(
            update={
                "forcing": f.model_copy(
                    update={
                        "initial_conditions": _sec(f.initial_conditions),
                        "boundary": _sec(f.boundary),
                    }
                )
            }
        )
    )
    settings = {"some_threshold": 3.0}
    set_ = sources_to_forcing_override(
        cfg.model_copy(
            update={
                "forcing": f.model_copy(
                    update={
                        "initial_conditions": _sec(
                            f.initial_conditions, groom=False, groom_settings=settings
                        ),
                        "boundary": _sec(f.boundary, groom=True),
                    }
                )
            }
        )
    )
    for ov in (unset, set_):
        for sec in (ov["initial_conditions"], ov["forcing"]["boundary"]):
            assert sec["bgc_sources"][0]["source"]["name"] == "ESPER"
            assert "groom" not in sec["source"]
            assert all("groom" not in bs["source"] for bs in sec["bgc_sources"])
    for sec in (unset["initial_conditions"], unset["forcing"]["boundary"]):
        assert "groom" not in sec and "groom_settings" not in sec
    assert set_["initial_conditions"]["groom"] is False
    assert set_["initial_conditions"]["groom_settings"] == settings
    assert set_["forcing"]["boundary"]["groom"] is True
    assert "groom_settings" not in set_["forcing"]["boundary"]
