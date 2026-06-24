"""
An ``ipywidgets`` wizard for assembling and reviewing a :class:`SpecConfig`.

This is a thin UI shell over :func:`cstar_forge.spec_config_resolve.build_spec_config`:
the widgets only *collect inputs and display the resolved result* — all resolution
and validation stay in the resolver. That keeps the notebook UI interchangeable with
any future app/WASM front-end and lets the logic be tested without rendering.

Usage (in a Jupyter notebook)::

    from cstar_forge.spec_config_wizard import SpecConfigWizard
    wiz = SpecConfigWizard()
    wiz.display()
    # ... pick a model + domain, tweak fields, review the live YAML, Save ...
    cfg = wiz.config            # the current resolved SpecConfig (or None if invalid)

The wizard discovers Models from ``catalog/ModelSpec/`` and Domains from
``catalog/DomainSpec/``; selecting a cataloged Domain prefills the grid kwargs,
boundaries, partitioning, and dates. The live preview re-resolves on every change
(using the ``dt`` field, so it never builds a grid); the "Compute dt (CFL)" button
is the only action that builds a grid (needs ``roms_tools``).
"""

from __future__ import annotations

import base64
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .spec_config import SpecConfig
from .spec_config_resolve import build_spec_config

_GRID_INT = ("nx", "ny", "N")
_GRID_FLOAT = ("size_x", "size_y", "center_lon", "center_lat", "rot")
_SCOORD = ("theta_s", "theta_b", "hc")
_DEFAULT_GRID = dict(nx=6, ny=2, size_x=500.0, size_y=1000.0, center_lon=0.0,
                     center_lat=55.0, rot=10.0, N=3, theta_s=5.0, theta_b=2.0, hc=250.0)


def _get_catalog():
    """Return the bundled DomainCatalog (read-only discovery of pieces)."""
    from .domain_catalog import default_catalog
    return default_catalog


class SpecConfigWizard:
    """Build/curate a :class:`SpecConfig` interactively. ``self.config`` holds the
    latest successfully-resolved config (``None`` while inputs are invalid)."""

    def __init__(self, catalog: Any = None):
        import ipywidgets as W  # imported here so the package doesn't require ipywidgets

        self.W = W
        self.catalog = catalog or _get_catalog()
        self.config: Optional[SpecConfig] = None

        models = list(self.catalog.model_names)
        domains = list(self.catalog.domain_names)

        # --- piece selectors ---
        self.model_dd = W.Dropdown(options=models, description="Model:",
                                   value=(models[0] if models else None),
                                   style={"description_width": "110px"})
        self.domain_dd = W.Dropdown(options=["<custom>"] + domains, description="Domain:",
                                    value="<custom>", style={"description_width": "110px"})
        self.grid_name = W.Text(value="my-grid", description="Grid name:",
                                style={"description_width": "110px"})

        # --- grid kwargs ---
        self.grid_w: Dict[str, Any] = {}
        for k in _GRID_INT:
            self.grid_w[k] = W.IntText(value=int(_DEFAULT_GRID[k]), description=f"{k}:",
                                       style={"description_width": "90px"}, layout=W.Layout(width="200px"))
        for k in _GRID_FLOAT + _SCOORD:
            self.grid_w[k] = W.FloatText(value=float(_DEFAULT_GRID[k]), description=f"{k}:",
                                         style={"description_width": "90px"}, layout=W.Layout(width="200px"))
        self.scoord_chk = W.Checkbox(value=True, description="specify s-coord (theta_s/theta_b/hc)",
                                     indent=False)

        # --- boundaries / partitioning ---
        self.bnd = {d: W.Checkbox(value=(d in ("east", "north")), description=d, indent=False)
                    for d in ("north", "south", "east", "west")}
        self.npx = W.IntText(value=1, description="n_procs_x:", style={"description_width": "90px"},
                             layout=W.Layout(width="200px"))
        self.npy = W.IntText(value=1, description="n_procs_y:", style={"description_width": "90px"},
                             layout=W.Layout(width="200px"))

        # --- run window ---
        self.start = W.DatePicker(value=date(2012, 1, 1), description="Start:",
                                  style={"description_width": "110px"})
        self.end = W.DatePicker(value=date(2012, 1, 2), description="End:",
                                style={"description_width": "110px"})
        self.description = W.Text(value="Generated blueprint", description="Description:",
                                  style={"description_width": "110px"}, layout=W.Layout(width="420px"))
        self.ensemble = W.Text(value="", description="Ensemble id:", placeholder="(optional int)",
                               style={"description_width": "110px"}, layout=W.Layout(width="260px"))

        # --- timestep ---
        self.dt = W.FloatText(value=7200.0, description="dt (s):",
                              style={"description_width": "90px"}, layout=W.Layout(width="220px"))
        self.dt_btn = W.Button(description="Compute dt (CFL)", icon="calculator",
                               tooltip="Build the grid and compute dt from the CFL criterion (needs roms_tools)")
        self.dt_status = W.HTML("")

        # --- output / preview ---
        self.derived = W.HTML("")
        self.preview = W.Output(layout=W.Layout(border="1px solid #ccc", padding="6px",
                                                 max_height="380px", overflow="auto"))
        # Browser download (works in Voilà / JupyterLab without server file access)
        self.download_link = W.HTML("")
        # Save to the server/working-dir filesystem (handy for local or HPC use)
        self.save_path = W.Text(value="spec_config.yml", description="Save to:",
                                style={"description_width": "110px"}, layout=W.Layout(width="420px"))
        self.save_btn = W.Button(description="Save to disk", icon="save")
        self.save_status = W.HTML("")

        self._wire()
        self._rebuild()

    # ---- wiring --------------------------------------------------------------
    def _wire(self):
        self.domain_dd.observe(self._on_domain, names="value")
        self.dt_btn.on_click(self._on_compute_dt)
        self.save_btn.on_click(self._on_save)
        watched = [self.model_dd, self.grid_name, self.scoord_chk, self.npx, self.npy,
                   self.start, self.end, self.description, self.ensemble, self.dt,
                   *self.grid_w.values(), *self.bnd.values()]
        for w in watched:
            w.observe(self._rebuild, names="value")

    def _on_domain(self, _change):
        """Prefill from a cataloged Domain.yml when one is selected."""
        name = self.domain_dd.value
        if name == "<custom>":
            return
        data = self.catalog.domain_data(name)
        gk = data.get("grid_kwargs", {}) or {}
        with self._suspend():
            self.grid_name.value = data.get("grid_name", name)
            for k, w in self.grid_w.items():
                if k in gk:
                    w.value = gk[k]
            self.scoord_chk.value = any(k in gk for k in _SCOORD)
            for d, w in self.bnd.items():
                w.value = bool((data.get("open_boundaries", {}) or {}).get(d, False))
            part = data.get("partitioning", {}) or {}
            self.npx.value = int(part.get("n_procs_x", self.npx.value))
            self.npy.value = int(part.get("n_procs_y", self.npy.value))
            for key, picker in (("start_time", self.start), ("end_time", self.end)):
                if data.get(key):
                    picker.value = datetime.fromisoformat(str(data[key])).date()
            if data.get("model_name") in self.model_dd.options:
                self.model_dd.value = data["model_name"]
        self._rebuild()

    class _Suspender:
        def __init__(self, wiz): self.wiz = wiz
        def __enter__(self): self.wiz._suspended = True
        def __exit__(self, *a): self.wiz._suspended = False

    def _suspend(self):
        return SpecConfigWizard._Suspender(self)

    # ---- gather + resolve ----------------------------------------------------
    def _gather(self) -> Dict[str, Any]:
        gk: Dict[str, Any] = {}
        for k in _GRID_INT:
            gk[k] = int(self.grid_w[k].value)
        for k in _GRID_FLOAT:
            gk[k] = float(self.grid_w[k].value)
        if self.scoord_chk.value:
            for k in _SCOORD:
                gk[k] = float(self.grid_w[k].value)
        ens = self.ensemble.value.strip()
        return dict(
            model_dir=self.catalog.model_dir(self.model_dd.value),
            grid_name=self.grid_name.value,
            grid_kwargs=gk,
            open_boundaries={d: w.value for d, w in self.bnd.items()},
            partitioning={"n_procs_x": int(self.npx.value), "n_procs_y": int(self.npy.value)},
            start_date=datetime.combine(self.start.value, datetime.min.time()),
            end_date=datetime.combine(self.end.value, datetime.min.time()),
            description=self.description.value,
            ensemble_id=int(ens) if ens else None,
            dt=float(self.dt.value),
        )

    def _rebuild(self, *_):
        if getattr(self, "_suspended", False):
            return
        self.preview.clear_output(wait=True)
        if self.start.value is None or self.end.value is None:
            self.config = None
            self.derived.value = "<i>Set start and end dates…</i>"
            self.download_link.value = ""
            return
        try:
            cfg = build_spec_config(**self._gather())
        except Exception as exc:  # validation or input error → show, don't crash
            self.config = None
            self.derived.value = f"<b style='color:#b00'>Invalid:</b> {type(exc).__name__}"
            self.download_link.value = ""
            with self.preview:
                print(f"{type(exc).__name__}: {exc}")
            return
        self.config = cfg
        self.download_link.value = self._download_html(cfg)
        comp = cfg.composition
        self.derived.value = (
            f"<b>name</b>: <code>{cfg.name}</code> &nbsp; "
            f"<b>casename</b>: <code>{cfg.casename}</code><br>"
            f"<b>composition</b>: model=<code>{comp.model.name}</code> ({comp.model.origin}), "
            f"domain=<code>{comp.domain.name or '—'}</code> ({comp.domain.origin}), "
            f"forcing ({comp.forcing.origin})"
        )
        with self.preview:
            print(cfg.to_yaml_str())

    @staticmethod
    def _download_html(cfg: SpecConfig) -> str:
        """A data-URI download link for the resolved YAML — works in the browser
        (Voilà / JupyterLab) with no server-side file access."""
        payload = cfg.to_yaml_str().encode("utf-8")
        b64 = base64.b64encode(payload).decode("ascii")
        fname = f"{cfg.casename}.spec_config.yml"
        return (f'⬇ <a download="{fname}" href="data:text/yaml;base64,{b64}">'
                f"Download <code>{fname}</code></a>")

    # ---- actions -------------------------------------------------------------
    def _on_compute_dt(self, _):
        self.dt_status.value = "<i>computing…</i>"
        try:
            kw = self._gather()
            kw["dt"] = None  # force CFL computation (builds the grid via roms_tools)
            cfg = build_spec_config(**kw)
            self.dt.value = float(cfg.model_settings["time_stepping"]["dt"])
            self.dt_status.value = f"<span style='color:#080'>dt = {self.dt.value:g} s (CFL)</span>"
        except Exception as exc:
            self.dt_status.value = f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"

    def _on_save(self, _):
        if self.config is None:
            self.save_status.value = "<span style='color:#b00'>Nothing to save — config is invalid.</span>"
            return
        try:
            p = self.config.to_yaml(Path(self.save_path.value))
            self.save_status.value = f"<span style='color:#080'>Saved {p}</span>"
        except Exception as exc:
            self.save_status.value = f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"

    # ---- layout / display ----------------------------------------------------
    @property
    def widget(self):
        W = self.W
        def section(title, *rows):
            return W.VBox([W.HTML(f"<b>{title}</b>"), *rows],
                          layout=W.Layout(border="1px solid #e0e0e0", padding="8px", margin="4px 0"))
        grid_box = W.GridBox(
            [self.grid_w[k] for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)],
            layout=W.Layout(grid_template_columns="repeat(3, 210px)"))
        return W.VBox([
            W.HTML("<h3>SpecConfig wizard</h3>"
                   "<i>Pick a Model and (optionally) a Domain, tweak fields, review, save.</i>"),
            section("Pieces", self.model_dd, self.domain_dd, self.grid_name),
            section("Grid", grid_box, self.scoord_chk),
            section("Open boundaries", W.HBox(list(self.bnd.values()))),
            section("Partitioning", W.HBox([self.npx, self.npy])),
            section("Run window", self.start, self.end, self.description, self.ensemble),
            section("Timestep", W.HBox([self.dt, self.dt_btn]), self.dt_status),
            section("Review (resolved SpecConfig)", self.derived, self.preview),
            section("Export",
                    self.download_link,
                    W.HBox([self.save_path, self.save_btn]), self.save_status),
        ])

    def display(self):
        from IPython.display import display
        display(self.widget)
