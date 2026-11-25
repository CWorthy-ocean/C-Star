"""Helpers and setting keys shared by the wizard's test modules."""

from datetime import date

from cstar.wizard.wizard import ForgeBlueprintWizard


def _display(widget) -> str:
    return getattr(widget.layout, "display", "") or ""


def _descendants(w):
    """Yield ``w`` and every widget nested under it (depth-first, pre-order)."""
    yield w
    for c in getattr(w, "children", []):
        yield from _descendants(c)


def _built_fields(editor) -> dict[str, list]:
    """Section -> every field the settings editor built for it, a section split
    across containers (bgc/marbl_bgc) listing the fields of both.
    """
    out: dict[str, list] = {}
    for fields_by_section in editor._group_fields.values():
        for section, fields in fields_by_section.items():
            out.setdefault(section, []).extend(fields)
    return out


# ``bottom_fill_blend_m`` (roms-tools' below-bottom blend depth) is a free-text box on
# the IC and boundary sections: blank = unset, a number (or ``inf``) = that float,
# anything else is gathered as typed so blueprint validation reports it.
_BLEND_SECTIONS = ("ic", "boundary")


_LITE_STREAM = ("cdr_lite_output", "do_cdr_lite_output")


_INTERP_KEY = ("cdr_lite", "cdr_carbonate_sensitivity_time_interpolation")


def _iter_widget_tree(widget):
    """Depth-first walk of a widget and its ``.children`` (Box/GridBox/HBox/VBox)."""
    yield widget
    for child in getattr(widget, "children", ()):
        yield from _iter_widget_tree(child)


def _is_in_tree(root, target) -> bool:
    return any(node is target for node in _descendants(root))


def _find_card(root, key):
    """Recursively find the ``components.card`` VBox tagged ``forge_key == key``
    under ``root`` (see ``cstar.wizard.ui.components.card``).
    """
    if getattr(root, "forge_key", None) == key:
        return root
    for c in getattr(root, "children", []):
        found = _find_card(c, key)
        if found is not None:
            return found
    return None


def _strip_bgc_forcing(wiz) -> None:
    """Remove every BGC signal from the bundled ForcingSpec's widgets, so
    bgc_mode "none"/"cdr_lite" (which reject BGC forcing) resolves.
    """
    fe = wiz._forcing_editor
    for ws in list(fe._rows["surface"]):
        if ws.get("type") is not None and ws["type"].value == "bgc":
            fe._remove("surface", ws)
    for section in ("boundary_bgc", "ic_bgc"):
        for ws in list(fe._rows[section]):
            fe._remove(section, ws)
    for ws in list(fe._rows["river"]):
        if "include_bgc" in ws:
            ws["include_bgc"].value = False


def _cdr_lite_wizard(catalog=None):
    """A wizard in bgc_mode "cdr_lite" with physics-only forcing and a simple CDR
    release (the mode requires a CDR forcing; "simple" targets the CDR-lite
    tracers by default).
    """
    wiz = ForgeBlueprintWizard() if catalog is None else ForgeBlueprintWizard(catalog)
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.use_pio_chk.value = False  # off: no CDF-5 conversion warning per rebuild
    _strip_bgc_forcing(wiz)
    wiz.model_card.bgc_dd.value = "cdr_lite"
    wiz.cdr_mode_dd.value = "simple"
    return wiz
