"""Helpers and setting keys shared by the wizard's test modules."""

from datetime import date
from pathlib import Path

import yaml

from cstar.applications.forge.blueprint import ForgeBlueprint
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
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz.run_card.use_pio_chk.value = (
        False  # off: no CDF-5 conversion warning per rebuild
    )
    _strip_bgc_forcing(wiz)
    wiz.model_card.bgc_dd.value = "cdr_lite"
    wiz.run_card.cdr_mode_dd.value = "simple"
    return wiz


_CDR_SAMPLE_YAML = (
    Path(__file__).parent.parent
    / "applications"
    / "forge"
    / "fixtures"
    / "cdr_forcing_sample.yaml"
)


def _upload_change(content: bytes):
    """Build an ipywidgets FileUpload-style ``change`` dict for a single file."""
    return {"new": ({"name": "cdr.yaml", "content": content},)}


def _fake_cdr_forcing_class(*, release_type="tracer_perturbation", names=("r1",)):
    """A stand-in for ``roms_tools.CDRForcing`` whose plot_* methods render a
    trivial figure (so plt.gcf()/savefig has something real to capture) without
    needing an actual grid/dataset. ``build_calls`` records each construction --
    tests assert on its length to prove the plot widget builds at most once per
    Generate click and never again on a plot-type/release switch.
    """
    import matplotlib.pyplot as plt

    build_calls: list[dict] = []

    class _FakeRelease:
        def __init__(self, name):
            self.name = name

    class _FakeCDRForcing:
        def __init__(self, **kwargs):
            build_calls.append(kwargs)
            self.release_type = release_type
            self.releases = [_FakeRelease(n) for n in names]
            self.release_tracers: dict[str, tuple[str, ...]] = {}  # all marbl

        def __getitem__(self, name):
            return next(r for r in self.releases if r.name == name)

        def plot_locations(self, *a, **k):
            plt.figure()

        def plot_distribution(self, release_name, *a, **k):
            plt.figure()

        def plot_tracer_flux(self, tracer_name, *a, **k):
            plt.figure()

    return _FakeCDRForcing, build_calls


def _new_wizard():
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    return wiz


class _FakeStdout:
    """Minimal async stand-in for asyncio.StreamReader's ``read(n)``.

    Serves a flat byte buffer in small, deliberately line-*un*aligned chunks (7
    bytes by default), so a test exercises the wizard's chunk-to-line reassembly
    rather than getting one whole line per read (which would prove nothing). Returns
    ``b""`` at EOF, like the real reader.

    Deliberately implements only ``read(n)`` and no ``__aiter__``: the wizard must
    not go back to ``async for line in proc.stdout`` (StreamReader.readline), whose
    64 KiB line limit is the bug this change removed -- doing so would fail here.
    """

    def __init__(self, data, chunk_size=7):
        self._data = data if isinstance(data, bytes) else b"".join(data)
        self._pos = 0
        self._chunk_size = chunk_size

    async def read(self, n=-1):
        if self._pos >= len(self._data):
            return b""
        take = len(self._data) - self._pos if n < 0 else min(n, self._chunk_size)
        chunk = self._data[self._pos : self._pos + take]
        self._pos += len(chunk)
        return chunk


class _FakeProcess:
    def __init__(self, data, returncode=0, chunk_size=7):
        self.stdout = _FakeStdout(data, chunk_size=chunk_size)
        self._returncode = returncode

    async def wait(self):
        return self._returncode


def _run_wiz_with_output(monkeypatch, tmp_path, data, *, returncode=0, chunk_size=7):
    """Drive _on_run with a fake process emitting ``data`` (bytes); return the wizard."""
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.review_card.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True  # not exercising boundary derivation here

    async def _fake(*args, **kwargs):
        return _FakeProcess(data, returncode=returncode, chunk_size=chunk_size)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake)
    wiz.review_card._on_run(None)
    return wiz


def _saving_wizard(path):
    """A wizard holding the default valid config, set to save to ``path``."""
    wiz = _new_wizard()
    wiz._boundaries_touched = True  # not exercising boundary derivation here
    wiz._rebuild()
    assert wiz.config is not None, wiz.review_card.derived.value
    wiz.review_card.save_path.value = str(path)
    return wiz


def _save(wiz):
    """Press Save and return the provenance of the file it wrote."""
    wiz.review_card._on_save(None)
    assert "Saved" in wiz.review_card.save_status.value, (
        wiz.review_card.save_status.value
    )
    return ForgeBlueprint.from_yaml(wiz.review_card.save_path.value).provenance


def _downloaded_yaml(link_html):
    """The parsed YAML a download link carries in its data URI."""
    import base64
    import re

    payload = re.search(r"base64,([A-Za-z0-9+/=]+)", link_html).group(1)
    return yaml.safe_load(base64.b64decode(payload))


def _write_tiny_netcdf(path: Path) -> Path:
    """A minimal real netCDF, just for ``hash_netcdf_contents`` to hash."""
    import numpy as np
    import xarray as xr

    ds = xr.Dataset(
        {"temp": (["y", "x"], np.zeros((2, 2), dtype=np.float64))},
        attrs={"title": "tiny test file"},
    )
    ds.to_netcdf(path)
    return path


def _nested_config_at_1800():
    wiz = _new_wizard()
    wiz.nest_enable.value = True
    wiz.nest_period.value = 1800.0
    wiz._rebuild()
    cfg = wiz.config
    assert cfg is not None, wiz.review_card.derived.value
    assert cfg.domain.metadata_child["period"] == 1800.0
    return cfg
