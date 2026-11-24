"""Helpers and setting keys shared by the wizard's test modules."""


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
