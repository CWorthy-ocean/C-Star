"""The Forge blueprint wizard's Grid card (card 2): the Domain pick and the grid.

`DomainCard` owns the card's widgets: the Domain dropdown and grid name, the grid
geometry and vertical coordinate, the bathymetry (topography, hmin, channel closing,
land-mask shapefile), a pre-made grid file to attach instead, the properties derived
from the grid (open boundaries, sponge viscosity, time step), the grid previews, and
the nested child and parent grids. It owns their observers, its share of the
resolver kwargs and of loading a blueprint back, and the card's status chip.

`ForgeBlueprintWizard` places the card's rows, and keeps the reactions that reach
other cards: a catalog Domain pick (`DomainCard.load_picked_domain`, then the Run
card's layout and dates and the forcing defaults), a parent pick and the parent
toggle (the forcing a child grid does not take), and the domain snapshot and
DomainSpec, which hold other cards' values too. Otherwise it reads and sets the
card only through its methods and properties. The card reaches back only through
the callables it is given.
"""

from __future__ import annotations

import copy
import html
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

from cstar.applications.forge.blueprint import (
    SpecRef,
    grid_build_kwargs,
    split_child_nesting_metadata,
)
from cstar.applications.forge.resolve import cfl_dt
from cstar.base.digest import hash_netcdf_contents
from cstar.wizard.fields import (
    _PATH_PLACEHOLDER,
    _dd_options,
    _same_attached_file,
    _stage_uploaded_netcdf,
    _tip,
    _user_file_status_html,
)
from cstar.wizard.ui import components
from cstar.wizard.ui.labels import label_for, section_for

if TYPE_CHECKING:
    from collections.abc import Callable, Collection
    from contextlib import AbstractContextManager

    from cstar.applications.forge.blueprint import ForgeBlueprint

# A nested grid's topography file: blank follows its topography source dropdown
# (this grid's file when that reads "(same as this grid)").
_NESTED_TOPO_PATH_PLACEHOLDER = "Leave blank to use the source above"


_GRID_INT = ("nx", "ny", "N")
_GRID_FLOAT = ("size_x", "size_y", "center_lon", "center_lat", "rot")
_SCOORD = ("theta_s", "theta_b", "hc")
_DEFAULT_GRID = dict(
    nx=6,
    ny=2,
    size_x=500.0,
    size_y=1000.0,
    center_lon=0.0,
    center_lat=55.0,
    rot=10.0,
    N=3,
    theta_s=5.0,
    theta_b=2.0,
    hc=250.0,
)


# Nested-grid topography: the parent/child grid kwargs may carry their own
# ``topography_source``/``topography_path`` (Forge inputs, popped before rt.Grid --
# see ForgeExecutor._nested_topography_pair for the inheritance rule). The
# dropdown's first entry is the "inherit this grid's pair" sentinel.
_NESTED_TOPO_INHERIT = "(same as this grid)"
_TOPO_SOURCES = ["ETOPO5", "SRTM15", "EMOD"]


def _nested_topo_kwargs(source_value: str, path_value: str) -> dict[str, str]:
    """The ``topography_source``/``topography_path`` keys a nested grid's
    widgets contribute to its grid_kwargs dict (empty = inherit the domain's).
    """
    out: dict[str, str] = {}
    if source_value and source_value != _NESTED_TOPO_INHERIT:
        out["topography_source"] = source_value
    if path_value.strip():
        out["topography_path"] = path_value.strip()
    return out


# The grid_kwargs keys the main grid's widgets leave out, each with the value
# that leaves it out (``_base_grid_kwargs`` writes them only when they differ).
_GRID_KWARGS_LEFT_OUT: Final[dict[str, Any]] = {
    "hmin": 5.0,
    "close_narrow_channels": False,
    "mask_shapefile": None,
}
# The keys of a parent's or child's grid_kwargs that its topography widgets set
# (see _nested_topo_kwargs); the geometry keys come from its grid boxes.
_NESTED_TOPO_KEYS: Final = ("topography_source", "topography_path")


def _merge_loaded_kwargs(
    loaded: dict[str, Any],
    gathered: dict[str, Any],
    owned: Collection[str],
    left_out: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A grid_kwargs dict read from the widgets (``gathered``) laid over the one
    last loaded into them (``loaded``, from a blueprint or a DomainSpec).

    Widgets cannot hold every key a file may carry (a parent's ``hmin``, a
    ``verbose`` flag), so a key no widget sets (not in ``owned``) is kept as
    loaded. A key the widgets own but left out (``left_out`` maps it to the
    value that leaves it out) keeps its loaded spelling while the widget still
    shows that value -- a file's explicit ``hmin: 5.0`` -- and is dropped once
    the widget shows something else. Loaded keys keep their order.
    """
    left_out = left_out or {}
    out: dict[str, Any] = {}
    for k, v in loaded.items():
        if k in gathered:
            out[k] = gathered[k]
        elif k not in owned or (k in left_out and v == left_out[k]):
            out[k] = copy.deepcopy(v)
    for k, v in gathered.items():
        out.setdefault(k, v)
    return out


def _effective_nested_topo(
    source_value: str, path_value: str, domain_source: str, domain_path: str
) -> tuple[str, str]:
    """Mirror of ``ForgeExecutor._nested_topography_pair`` on widget values:
    ``(name, path)`` a nested grid resolves to, with ``""`` for no path.
    """
    path = path_value.strip()
    if source_value == _NESTED_TOPO_INHERIT:
        return domain_source, (path or domain_path.strip())
    return source_value, path


def _plot_topography_source(name: str, path: str) -> tuple[dict[str, str] | None, str]:
    """What a wizard-side grid build (plots / derive-from-grid) can use for
    ``rt.Grid(topography_source=...)`` on THIS machine, plus a status note.

    Returns ``(None, "")`` for default ETOPO5 (roms-tools fetches it itself) and
    ``({'name','path'}, "")`` when ``path`` exists locally. A non-default dataset
    whose file isn't available here (the common laptop case -- the path is an
    executor-side path) falls back to ETOPO5 with a visible note, rather than
    failing the plot: the executor is the hard gate, this is a preview.
    """
    path = (path or "").strip()
    if path:
        local = Path(path).expanduser()
        if local.exists():
            return {"name": name, "path": str(local)}, ""
    if name == "ETOPO5" and not path:
        return None, ""
    why = f"file not found here: {path}" if path else "no local file"
    return None, (
        " "
        + components.status_span(
            "warn", f"(plotted with default ETOPO5 topography; {name}: {why})"
        )
    )


def _display_round(value: float) -> float:
    """A derived number rounded for display: one decimal, or three significant
    digits below 1 (where one decimal could round it to zero).
    """
    return round(value, 1) if abs(value) >= 1 else float(f"{value:.3g}")


class DomainCard:
    """The wizard's Grid card: the Domain pick, the grid and its bathymetry or a
    pre-made grid file, the open boundaries, sponge viscosity and time step derived
    from it until set, the grid previews, and the nested child and parent grids.

    The callables are the wizard's: ``rebuild`` re-resolves the blueprint,
    ``suspend`` returns the wizard's suspend context manager, ``is_suspended`` says
    whether one is open, and ``show_summary`` sets the Save card's summary line
    (for a child spec the card refuses). Construction builds the widgets and sets
    their defaults but registers no observers: the wizard calls `wire` once it is
    built.
    """

    def __init__(
        self,
        W: Any,
        catalog: Any,
        *,
        rebuild: Callable[[], None],
        suspend: Callable[[], AbstractContextManager[Any]],
        is_suspended: Callable[[], bool],
        show_summary: Callable[[str], None],
    ) -> None:
        self.W = W
        self.catalog = catalog
        self._rebuild = rebuild
        self._suspend = suspend
        self._is_suspended = is_suspended
        self._show_summary = show_summary
        domains = list(self.catalog.domain_names)

        self.domain_dd = W.Dropdown(
            options=_dd_options(self.catalog, domains, "domain", prefix=["<custom>"]),
            description="Domain:",
            value="<custom>",
            style={"description_width": "110px"},
        )
        self.grid_name = W.Text(
            value="my-grid",
            description="Grid name:",
            style={"description_width": "110px"},
            tooltip="Short name for this grid configuration. Used in the domain name.",
        )

        # --- grid kwargs ---
        self.grid_w: dict[str, Any] = {}
        for k in _GRID_INT:
            self.grid_w[k] = W.IntText(
                value=int(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k),
            )
        for k in _GRID_FLOAT + _SCOORD:
            self.grid_w[k] = W.FloatText(
                value=float(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k),
            )
        self.scoord_chk = W.Checkbox(
            value=True,
            description=label_for(
                "scoord_chk", "specify s-coord (theta_s/theta_b/hc)"
            ).label,
            indent=False,
            tooltip="When checked, theta_s, theta_b, and hc are passed to roms-tools "
            "to set the vertical stretching. Required for ROMS simulations.",
        )

        # --- boundaries / partitioning ---
        self.bnd = {
            d: W.Checkbox(
                value=(d in ("east", "north")),
                description=d.capitalize(),
                indent=False,
                tooltip=f"Enable the {d} open boundary for ocean exchange.",
            )
            for d in ("north", "south", "east", "west")
        }
        # --- domain-derived properties: v_sponge + (the above) open boundaries.
        # Both auto-derive from the grid unless touched by a manual edit or
        # restored from a saved DomainSpec -- see _on_derive_domain_properties /
        # _v_sponge_touched / _boundaries_touched / _boundaries_derived.
        self.v_sponge = W.FloatText(
            value=0.0,
            description="v_sponge:",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px"),
            tooltip="Sponge-layer viscosity. Auto-derived from grid spacing "
            "(spacing / 10) unless you edit it or it was saved into a DomainSpec.",
        )
        self.derive_btn = W.Button(
            description=label_for("buttons.derive", "Derive from grid").label,
            icon="refresh",
            layout=W.Layout(width="160px"),
            tooltip="Build the grid and set any untouched v_sponge/open-boundary "
            "values from it (sponge = spacing/10; boundaries from the land mask).",
        )
        self.derive_status = W.HTML("")
        # --- nesting (optional child grid) ---
        self.nest_enable = W.Checkbox(
            value=False,
            description="This grid is a parent (enter child grid info below).",
            indent=False,
            tooltip=_tip("nesting", "nest_enable"),
            # sized to its label: the default 300px cut the sentence off
            layout=W.Layout(width="auto"),
        )
        self.nest_help = W.HTML(
            "<div class='forge-aside'>"
            "This will enable the extract_data module and output boundary info "
            "for your child grid.</div>"
        )
        self.nest_domain_dd = W.Dropdown(
            options=["<custom>", *domains],
            description="Child from:",
            value="<custom>",
            style={"description_width": "110px"},
            tooltip=_tip("nesting", "nest_domain_dd"),
        )
        self.child_w: dict[str, Any] = {}
        for k in _GRID_INT:
            self.child_w[k] = W.IntText(
                value=int(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k) + " (child/inner grid)",
            )
        for k in _GRID_FLOAT + _SCOORD:
            self.child_w[k] = W.FloatText(
                value=float(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k) + " (child/inner grid)",
            )
        self.child_topo_source = W.Dropdown(
            options=[_NESTED_TOPO_INHERIT, *_TOPO_SOURCES],
            value=_NESTED_TOPO_INHERIT,
            description="child topo:",
            style={"description_width": "90px"},
            layout=W.Layout(width="260px"),
            tooltip=_tip("nesting", "child_topo_source"),
        )
        self.child_topo_path = W.Text(
            value="",
            description="child topo path:",
            style={"description_width": "110px"},
            layout=W.Layout(width="360px"),
            placeholder=_NESTED_TOPO_PATH_PLACEHOLDER,
            tooltip=_tip("nesting", "child_topo_path"),
        )
        self.nest_period = W.FloatText(
            value=3600.0,
            description="extract period (s):",
            style={"description_width": "130px"},
            layout=W.Layout(width="260px"),
            tooltip=_tip("nesting", "nest_period"),
        )
        self.nest_pressure_fluxes = W.Checkbox(
            value=False,
            description="include pressure fluxes",
            indent=False,
            tooltip=_tip("nesting", "nest_pressure_fluxes"),
        )
        # --- nesting plot (parent+child boundary overlay, separate from the Grid
        # section's parent-only plot) ---
        self.nest_plot_btn = W.Button(
            description=label_for("buttons.refresh_plot", "Refresh plot").label,
            icon="refresh",
            tooltip="Build the parent and child grids from current settings and "
            "render both via plot_nesting (parent+child boundary overlay).",
        )
        self.nest_plot_status = W.HTML("Click Refresh preview to draw the grids.")
        self.nest_plot_img = W.Image(
            format="png",
            layout=W.Layout(min_width="400px", max_width="600px", display="none"),
        )

        # --- parent (optional: this grid is a child nested inside a parent) ---
        self.parent_enable = W.Checkbox(
            value=False,
            description="This is a child grid (enter parent grid info below).",
            indent=False,
            tooltip=_tip("nesting", "parent_enable"),
            # sized to its label: the default 300px cut the sentence off
            layout=W.Layout(width="auto"),
        )
        self.parent_help = W.HTML(
            "<div class='forge-aside'>"
            "This will align the mask and topography with the parent grid. It "
            "will also disable boundary tides, switch sponge ub_tune to False, "
            "and skip boundary forcing generation.</div>"
        )
        self.parent_domain_dd = W.Dropdown(
            options=["<custom>", *domains],
            description="Parent from:",
            value="<custom>",
            style={"description_width": "110px"},
            tooltip=_tip("nesting", "parent_domain_dd"),
        )
        self.parent_w: dict[str, Any] = {}
        for k in _GRID_INT:
            self.parent_w[k] = W.IntText(
                value=int(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k) + " (parent/outer grid)",
            )
        for k in _GRID_FLOAT + _SCOORD:
            self.parent_w[k] = W.FloatText(
                value=float(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k) + " (parent/outer grid)",
            )
        self.parent_topo_source = W.Dropdown(
            options=[_NESTED_TOPO_INHERIT, *_TOPO_SOURCES],
            value=_NESTED_TOPO_INHERIT,
            description="parent topo:",
            style={"description_width": "90px"},
            layout=W.Layout(width="260px"),
            tooltip=_tip("nesting", "parent_topo_source"),
        )
        self.parent_topo_path = W.Text(
            value="",
            description="parent topo path:",
            style={"description_width": "110px"},
            layout=W.Layout(width="360px"),
            placeholder=_NESTED_TOPO_PATH_PLACEHOLDER,
            tooltip=_tip("nesting", "parent_topo_path"),
        )
        # --- parent plot (this grid's boundary within its parent) ---
        self.parent_plot_btn = W.Button(
            description=label_for("buttons.refresh_plot", "Refresh plot").label,
            icon="refresh",
            tooltip="Build the parent grid and this grid from current settings and "
            "render both via plot_nesting (parent+this-grid boundary overlay).",
        )
        self.parent_plot_status = W.HTML("Click Refresh preview to draw the grids.")
        self.parent_plot_img = W.Image(
            format="png",
            layout=W.Layout(min_width="400px", max_width="600px", display="none"),
        )

        # --- grid extended options ---
        self.hmin = W.FloatText(
            value=_GRID_KWARGS_LEFT_OUT["hmin"],
            description="hmin (m):",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px"),
            tooltip=_tip("grid", "hmin"),
        )
        self.close_narrow_chk = W.Checkbox(
            value=_GRID_KWARGS_LEFT_OUT["close_narrow_channels"],
            description="close narrow channels",
            indent=False,
            tooltip=_tip("grid", "close_narrow_channels"),
        )
        self.mask_shapefile = W.Text(
            value="",
            description="mask shapefile:",
            style={"description_width": "120px"},
            layout=W.Layout(width="380px"),
            placeholder="path to custom land-mask shapefile (optional)",
            tooltip=_tip("grid", "mask_shapefile"),
        )
        self.topo_source = W.Dropdown(
            options=["ETOPO5", "SRTM15", "EMOD"],
            value="ETOPO5",
            description="topo source:",
            style={"description_width": "120px"},
            tooltip=_tip("grid", "topography_source"),
        )
        self.topo_path = W.Text(
            value="",
            description="topo path:",
            style={"description_width": "120px"},
            layout=W.Layout(width="380px"),
            placeholder=_PATH_PLACEHOLDER,
            tooltip=_tip("grid", "topography_path"),
        )

        # --- user-provided grid file (attach a pre-made grid netCDF instead of
        # generating one from the kwargs above) ---
        # `_grid_file` = the cached {"location","content_hash"} dict emitted via
        # grid_file=; `_grid_file_grid` = the loaded roms_tools.Grid, cached so
        # _rebuild()/_gather() never reload or rehash the file (see
        # _finish_grid_file_attach). Both None when detached.
        self._grid_file: dict[str, Any] | None = None
        self._grid_file_grid: Any | None = None
        # Snapshot of grid_w/scoord_chk taken the moment an attach first
        # overwrites them with the loaded file's own values (see
        # _finish_grid_file_attach) -- restored by _on_grid_file_detach so the
        # Detach button gives back the user's own pre-attach geometry instead
        # of leaving the (unlocked, but now-wrong) file's values sitting there.
        # None means "nothing to restore" (never attached, or already restored).
        self._grid_widgets_snapshot: dict[str, Any] | None = None
        self.grid_file_path = W.Text(
            value="",
            description="Grid file:",
            placeholder="path to a pre-made grid netCDF",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
            tooltip=_tip("grid", "grid_file"),
            # Fire `value` on Enter / focus-out only (not per keystroke) so
            # _on_grid_file_path_submit can auto-attach a finished path.
            continuous_update=False,
        )
        self.grid_file_attach_btn = W.Button(
            description=label_for("buttons.attach", "Attach").label, icon="link"
        )
        self.grid_file_detach_btn = W.Button(
            description=label_for("buttons.detach", "Detach").label, icon="unlink"
        )
        self.grid_file_upload = W.FileUpload(
            accept=".nc", multiple=False, description="…or upload"
        )
        self.grid_file_status = W.HTML("")

        # --- timestep ---
        self.dt = W.FloatText(
            value=0.0,  # the derived dt replaces it on the first _rebuild
            description="dt (s):",
            style={"description_width": "90px"},
            layout=W.Layout(width="220px"),
            tooltip=_tip("run", "dt"),
        )
        self.dt_btn = W.Button(
            description=label_for("buttons.compute_dt", "Use CFL value").label,
            icon="calculator",
            tooltip=_tip("timestep", "dt_btn"),
        )
        self.dt_status = W.HTML("")

        # --- grid plot ---
        self.plot_btn = W.Button(
            description=label_for("buttons.refresh_plot", "Refresh plot").label,
            icon="refresh",
            tooltip="Build the grid from current settings and render it. Updates "
            "automatically when a domain is selected from the catalog.",
        )
        self.plot_status = W.HTML("Click Refresh preview to draw the grid.")
        # width 100% of its column (400-600px): a fixed-size image would hold its
        # column at 600px and squeeze the fields beside it
        self.plot_img = W.Image(
            format="png",
            layout=W.Layout(
                width="100%", min_width="400px", max_width="600px", display="none"
            ),
        )

        # Domain-derived properties (v_sponge, open boundaries): "touched" means a
        # manual edit (or a value restored from a loaded blueprint/DomainSpec) has
        # made this the user's authoritative value -- nothing auto-overwrites it
        # again. v_sponge is cheap (pure arithmetic on grid spacing) and is kept
        # live-derived in _rebuild() when untouched, so it needs no separate
        # "derived" flag. Boundaries need a grid build (mask-based), so
        # _boundaries_derived tracks whether that's happened since the last
        # grid-affecting edit -- see _on_grid_kwarg_change/_on_derive_domain_properties.
        self._v_sponge_touched = False
        # The full grid-derived v_sponge the widget shows rounded (see
        # _show_derived_v_sponge), so a snapshot keeps its precision.
        self._v_sponge_derived: float | None = None
        self._boundaries_touched = False
        self._boundaries_derived = False
        # What the widgets cannot hold of the last loaded blueprint or DomainSpec,
        # written back on save: the grid_kwargs of "grid", "parent" and "child"
        # (see _merge_loaded_kwargs). The Run card keeps the run datetimes'.
        self._loaded_grid_kwargs: dict[str, dict[str, Any]] = {}
        # dt follows v_sponge: derived from the grid (CFL) and mirrored into the
        # widget until a typed value, a loaded blueprint or a DomainSpec carrying
        # dt sets it; "Use CFL value" clears it. See sync_derived_dt.
        self._dt_touched = False
        self.grid_lock_banner = components.banner_widget(
            W,
            "warn",
            "<b>Grid fields are locked</b> because a pre-made grid file is "
            "attached. Detach it to edit the grid here.",
        )
        self.grid_lock_banner.layout.display = "none"

    # ---- facts for the wizard and the other cards -----------------------------
    @property
    def from_catalog(self) -> bool:
        """Whether the Domain dropdown names a catalog DomainSpec (not "<custom>")."""
        return self.domain_dd.value != "<custom>"

    @property
    def parent_enabled(self) -> bool:
        """Whether "This is a child grid" is ticked (an attached grid file aside)."""
        return bool(self.parent_enable.value)

    @property
    def grid_file_attached(self) -> bool:
        """Whether a pre-made grid file is attached (loaded or not)."""
        return self._grid_file is not None

    @property
    def boundaries_pending(self) -> bool:
        """Whether the open boundaries are still the provisional defaults: neither
        set by hand (or loaded) nor derived from the current grid.
        """
        return not self._boundaries_touched and not self._boundaries_derived

    @property
    def grid_center(self) -> tuple[float, float]:
        """The grid's ``(center_lat, center_lon)`` as the widgets show it."""
        return (
            float(self.grid_w["center_lat"].value),
            float(self.grid_w["center_lon"].value),
        )

    @property
    def grid_fingerprint(self) -> tuple[tuple[str, str], ...]:
        """Every grid geometry widget's value, for a cache keyed on the grid."""
        return tuple(sorted((k, repr(w.value)) for k, w in self.grid_w.items()))

    # ---- wiring and layout ---------------------------------------------------
    def wire(self) -> None:
        """Register the card's own observers and click handlers. The wizard calls
        this from its ``_wire`` right after it registers ``_on_domain`` on
        ``domain_dd``; it registers its reactions on ``parent_domain_dd`` and
        ``parent_enable``, and its rebuild on `rebuild_on` last, itself.
        """
        self.dt_btn.on_click(self._on_compute_dt)
        self.plot_btn.on_click(self.on_plot)
        self.nest_plot_btn.on_click(self._on_nest_plot)
        self.grid_file_attach_btn.on_click(self._on_grid_file_attach)
        self.grid_file_detach_btn.on_click(self._on_grid_file_detach)
        self.grid_file_path.observe(self._on_grid_file_path_submit, names="value")
        self.grid_file_upload.observe(self._on_grid_file_upload, names="value")
        self.nest_domain_dd.observe(self._on_nest_domain, names="value")
        self.parent_plot_btn.on_click(self.on_parent_plot)
        self.derive_btn.on_click(self._on_derive_domain_properties)
        # Grid-geometry/mask-affecting widgets: a change invalidates any prior
        # mask-derived boundaries (a stale mask must never be silently reused),
        # in addition to the plain rebuild every `rebuild_on` widget gets.
        for w in (
            *self.grid_w.values(),
            self.hmin,
            self.close_narrow_chk,
            self.mask_shapefile,
        ):
            w.observe(self._on_grid_kwarg_change, names="value")
        # Domain-derived properties: a manual edit "touches" the property so
        # nothing auto-overwrites it again (mirrors _on_editor_edit for the
        # advanced-settings accordion).
        for w in self.bnd.values():
            w.observe(self._on_boundary_edit, names="value")
        self.v_sponge.observe(self._on_v_sponge_edit, names="value")
        self.dt.observe(self._on_dt_edit, names="value")

    @property
    def rebuild_on(self) -> tuple[Any, ...]:
        """The widgets whose every change re-resolves the blueprint: the grid name,
        vertical coordinate, bathymetry source and the nested grids. The wizard
        registers its rebuild on them last in its ``_wire``, after its reactions on
        ``parent_enable``. The grid geometry, boundaries, sponge and time step
        rebuild from the card's own handlers instead (see `wire`).
        """
        return (
            self.grid_name,
            self.scoord_chk,
            self.topo_source,
            self.topo_path,
            self.nest_enable,
            self.nest_period,
            self.nest_pressure_fluxes,
            self.parent_enable,
            *self.child_w.values(),
            *self.parent_w.values(),
            self.child_topo_source,
            self.child_topo_path,
            self.parent_topo_source,
            self.parent_topo_path,
        )

    def rows(self) -> list[Any]:
        """The card body's rows, in order: the Domain pick and grid name, the grid
        file, the lock banner, the geometry with its preview, the vertical
        coordinate, the bathymetry, the derived properties and the nesting pane.
        The wizard adds its error banner above them. Builds ``gridfile_accordion``.
        """
        W = self.W
        # Danger-styled destructive actions.
        self.grid_file_detach_btn.button_style = "danger"
        geometry_rows = [
            components.field_row(
                W, f"grid.{k}", self.grid_w[k], width="110px", label_width="170px"
            )
            for k in (_GRID_INT + _GRID_FLOAT)
        ]
        # The stylesheet sizes the fields and the preview column (`forge-geo-row`
        # in WIZARD_CSS): the fields keep their room, and in a narrow card the
        # preview moves below them.
        grid_plot_col = W.VBox(
            [self.plot_btn, self.plot_status, self.plot_img],
            layout=W.Layout(padding="0 0 0 20px"),
        )
        geometry_grid = components.field_grid(W, geometry_rows)
        geometry_row = W.HBox([geometry_grid, grid_plot_col])
        geometry_row.add_class("forge-geo-row")
        geometry_sub = components.subsection(W, "grid.geometry", geometry_row)
        vertical_sub = components.subsection(
            W,
            "grid.vertical",
            self.scoord_chk,
            components.field_grid(
                W,
                [
                    components.field_row(W, f"grid.{k}", self.grid_w[k], width="110px")
                    for k in _SCOORD
                ],
            ),
        )
        bathymetry_sub = components.subsection(
            W,
            "grid.bathymetry",
            components.field_grid(
                W,
                [
                    components.field_row(W, "topo_source", self.topo_source),
                    components.field_row(W, "topo_path", self.topo_path),
                    components.field_row(W, "hmin", self.hmin),
                    components.field_row(W, "close_narrow_chk", self.close_narrow_chk),
                ],
            ),
            components.field_row(W, "mask_shapefile", self.mask_shapefile),
        )
        self.gridfile_accordion = components.collapsible(
            W,
            W.VBox(
                [
                    W.HBox(
                        [
                            self.grid_file_path,
                            self.grid_file_attach_btn,
                            self.grid_file_detach_btn,
                        ]
                    ),
                    self.grid_file_upload,
                    self.grid_file_status,
                ]
            ),
            section_for("grid.gridfile").title,
        )
        derived_sub = components.subsection(
            W,
            "grid.derived",
            components.banner_widget(
                W,
                "info",
                "Open boundaries, sponge viscosity and the time step are "
                "derived from the grid until you set them. The sponge viscosity "
                "and time step shown follow the grid; derive the boundaries now "
                "to check or edit them.",
            ),
            components.field_row(W, "bnd", W.HBox(list(self.bnd.values()))),
            components.field_row(
                W,
                "v_sponge",
                self.v_sponge,
                extra=(self.derive_btn, self.derive_status),
            ),
            components.field_row(W, "dt", self.dt, extra=(self.dt_btn, self.dt_status)),
        )

        child_grid_rows = [
            components.field_row(W, f"grid.{k}", self.child_w[k], width="110px")
            for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)
        ]
        parent_grid_rows = [
            components.field_row(W, f"grid.{k}", self.parent_w[k], width="110px")
            for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)
        ]
        child_plot_col = W.VBox(
            [W.HBox([self.nest_plot_btn, self.nest_plot_status]), self.nest_plot_img],
            layout=W.Layout(padding="0 0 0 20px"),
        )
        child_section = components.subsection(
            W,
            "grid.nesting.child",
            self.nest_enable,
            self.nest_help,
            W.HBox(
                [
                    W.VBox(
                        [
                            components.field_row(
                                W, "nest_domain_dd", self.nest_domain_dd
                            ),
                            components.field_grid(W, child_grid_rows),
                            components.field_row(
                                W, "child_topo_source", self.child_topo_source
                            ),
                            components.field_row(
                                W, "child_topo_path", self.child_topo_path
                            ),
                            components.field_row(W, "nest_period", self.nest_period),
                            components.field_row(
                                W, "nest_pressure_fluxes", self.nest_pressure_fluxes
                            ),
                        ]
                    ),
                    child_plot_col,
                ]
            ),
            default_title="Child grid",
        )
        parent_plot_col = W.VBox(
            [
                W.HBox([self.parent_plot_btn, self.parent_plot_status]),
                self.parent_plot_img,
            ],
            layout=W.Layout(padding="0 0 0 20px"),
        )
        parent_section = components.subsection(
            W,
            "grid.nesting.parent",
            self.parent_enable,
            self.parent_help,
            W.HBox(
                [
                    W.VBox(
                        [
                            components.field_row(
                                W, "parent_domain_dd", self.parent_domain_dd
                            ),
                            components.field_grid(W, parent_grid_rows),
                            components.field_row(
                                W, "parent_topo_source", self.parent_topo_source
                            ),
                            components.field_row(
                                W, "parent_topo_path", self.parent_topo_path
                            ),
                        ]
                    ),
                    parent_plot_col,
                ]
            ),
            default_title="Parent grid",
        )
        nesting_accordion = components.collapsible(
            W,
            W.VBox([child_section, parent_section]),
            section_for("grid.nesting").title,
        )

        return [
            components.field_grid(
                W,
                [
                    components.field_row(W, "domain_dd", self.domain_dd),
                    components.field_row(W, "grid_name", self.grid_name),
                ],
            ),
            self.gridfile_accordion,
            self.grid_lock_banner,
            geometry_sub,
            vertical_sub,
            bathymetry_sub,
            derived_sub,
            nesting_accordion,
        ]

    def status_chip(self) -> str:
        """The card's chip while it owns no problem: how many derived fields are
        still to check, else complete.
        """
        complete = components.chip("● Complete", "ok")
        if not self._boundaries_derived and not self._boundaries_touched:
            n = 1 + (1 if not self._v_sponge_touched else 0)
            grid_chip = components.chip(f"● {n} fields to check", "warn")
        else:
            grid_chip = complete
        return grid_chip

    # ---- the wizard's share: resolver kwargs, load-back and each rebuild -------
    def gather_grid(self) -> dict[str, Any]:
        """The ``grid_name``, ``grid_kwargs`` and ``open_boundaries`` resolver kwargs."""
        return dict(
            grid_name=self.grid_name.value,
            grid_kwargs=self._base_grid_kwargs(),
            open_boundaries={d: w.value for d, w in self.bnd.items()},
        )

    def gather_derived(self) -> dict[str, Any]:
        """The ``dt`` and ``v_sponge`` resolver kwargs, each ``None`` while it follows
        the grid.
        """
        return dict(
            # Untouched: None, so the resolver derives it from the grid (CFL).
            dt=float(self.dt.value) if self._dt_touched else None,
            # Untouched: pass None so the resolver derives a fresh default from
            # the current grid (see _rebuild, which mirrors it back into the
            # widget for live display); touched: the user's/loaded value wins.
            v_sponge=float(self.v_sponge.value) if self._v_sponge_touched else None,
        )

    def gather_topography(self) -> dict[str, Any]:
        """The ``topography_path`` (when one is typed) and ``topography_source``
        resolver kwargs.
        """
        kw: dict[str, Any] = {}
        if self.topo_path.value.strip():  # blank = derive default topography path
            kw["topography_path"] = self.topo_path.value.strip()
        kw["topography_source"] = self.topo_source.value
        return kw

    def gather_file_and_nesting(self) -> dict[str, Any]:
        """The attached grid file's resolver kwargs (``grid_file``, with its loaded
        ``grid``) and the nested grids' (``grid_kwargs_child`` and its
        ``metadata_child`` and pressure fluxes, ``grid_kwargs_parent``).
        """
        kw: dict[str, Any] = {}
        if self._grid_file is not None:
            kw["grid_file"] = self._grid_file
            # Pass the already-loaded grid through to skip re-reading/re-hashing
            # the file (the resolver accepts a pre-built `grid=` alongside
            # `grid_file=` for exactly this -- see build_forge_blueprint). Omitted
            # (not None) when a reattach failed to reload -- the resolver then
            # reloads from `grid_file["location"]` itself and raises the same
            # "file not found", surfacing loudly via _rebuild()'s Invalid status.
            if self._grid_file_grid is not None:
                kw["grid"] = self._grid_file_grid
        is_parent, is_child = self.nesting_role()
        if is_parent:
            kw["grid_kwargs_child"] = self._nested_grid_kwargs("child")
            kw["metadata_child"] = {"period": float(self.nest_period.value)}
            if self.nest_pressure_fluxes.value:
                kw["nesting_include_pressure_fluxes"] = True
        if is_child:
            kw["grid_kwargs_parent"] = self._nested_grid_kwargs("parent")
        return kw

    def select_custom(self) -> None:
        """Select "<custom>" in the Domain dropdown: a loaded blueprint, not a
        catalog entry, is authoritative (the wizard holds it suspended).
        """
        self.domain_dd.value = "<custom>"

    def populate_grid(self, cfg: ForgeBlueprint) -> None:
        """Set the grid name, geometry, mask keys, open boundaries and v_sponge from a
        loaded blueprint (the wizard holds it suspended).
        """
        self.grid_name.value = cfg.domain.grid_name
        gk = cfg.domain.grid_kwargs
        self._set_loaded_grid_kwargs("grid", gk)
        for k, w in self.grid_w.items():
            if k in gk:
                w.value = gk[k]
        self.hmin.value = float(gk.get("hmin", _GRID_KWARGS_LEFT_OUT["hmin"]))
        self.close_narrow_chk.value = bool(
            gk.get(
                "close_narrow_channels",
                _GRID_KWARGS_LEFT_OUT["close_narrow_channels"],
            )
        )
        self.mask_shapefile.value = str(gk.get("mask_shapefile") or "")
        self.scoord_chk.value = any(k in gk for k in _SCOORD)
        for d, w in self.bnd.items():
            w.value = bool(getattr(cfg.domain.open_boundaries, d))
        # A loaded file's boundaries/v_sponge are a deliberate, already-resolved
        # choice (mirrors _name_touched above), not a default to keep
        # auto-deriving over -- freeze both as touched. v_sponge falls back to
        # the model_settings leaf for a pre-domain.v_sponge file (backward
        # compat: older blueprints only ever wrote it there).
        self._boundaries_touched = True
        self._boundaries_derived = False
        self.derive_status.value = ""
        loaded_v_sponge = cfg.domain.v_sponge
        if loaded_v_sponge is None:
            loaded_v_sponge = (cfg.model_settings.get("v_sponge") or {}).get("v_sponge")
        if loaded_v_sponge is not None:
            self.v_sponge.value = float(loaded_v_sponge)
        self._v_sponge_touched = loaded_v_sponge is not None

    def populate_topography(self, cfg: ForgeBlueprint) -> None:
        """Set the topography and re-attach a loaded blueprint's grid file, or
        detach one it lacks (the wizard holds it suspended).
        """
        self.topo_source.value = getattr(
            cfg.domain.topography_source, "value", cfg.domain.topography_source
        )
        self.topo_path.value = cfg.domain.topography_path or ""
        # Grid file: re-attach from its recorded location (reusing the stored
        # content_hash, never recomputing it) if the file still exists;
        # missing/failed reload leaves the grid_file state + locked widgets in
        # place with the error surfaced -- see _reattach_grid_file.
        if cfg.domain.grid_file is not None:
            self._reattach_grid_file(cfg.domain.grid_file)
        else:
            self._detach_grid_file()

    def populate_dt(self, cfg: ForgeBlueprint) -> None:
        """Set dt from a loaded blueprint (the wizard holds it suspended)."""
        # dt: prefer the first-class domain.dt field; fall back to the
        # model_settings leaf for a pre-domain.dt file (backward compat --
        # older blueprints only ever wrote it there).
        loaded_dt = cfg.domain.dt
        if loaded_dt is None:
            loaded_dt = (cfg.model_settings.get("time_stepping") or {}).get("dt")
        if loaded_dt is not None:
            self.dt.value = float(loaded_dt)
        self._dt_touched = loaded_dt is not None

    def follow_resolved(self, cfg: ForgeBlueprint) -> None:
        """Show the sponge viscosity the resolver derived for ``cfg`` while it is
        untouched.
        """
        # v_sponge is cheap (pure arithmetic on grid spacing, no grid build) --
        # keep it live-derived in the widget whenever untouched, by reading back
        # what the resolver just computed. Under _suspend() so this programmatic
        # write doesn't itself flip _v_sponge_touched (see _on_v_sponge_edit).
        if not self._v_sponge_touched:
            with self._suspend():
                self._show_derived_v_sponge(float(cast("float", cfg.domain.v_sponge)))

    def warn_boundaries_pending(self) -> None:
        """Say in ``derive_status`` that the open boundaries are not derived yet,
        while they are the provisional defaults and nothing else is shown there.
        """
        # Surface (never silently ship) provisional open-boundary defaults: the
        # checkboxes currently reflect whatever's live, but that's only a real
        # mask-derived value once _boundaries_derived is True or the user has
        # touched them -- see ensure_boundaries_derived for the Save/Run
        # guarantee. Only fills a *blank* status -- an actual derive/error
        # message (set by _on_derive_domain_properties) stays visible instead
        # of being clobbered by this generic warning on every rebuild.
        if (
            not self._boundaries_touched
            and not self._boundaries_derived
            and not self.derive_status.value
        ):
            self.derive_status.value = components.status_span(
                "warn", "⚠ not derived yet"
            )

    def spec_ref(self) -> SpecRef:
        """The blueprint's ``composition.domain``: the picked DomainSpec, or the grid
        name as a custom domain. ``modified`` starts ``False``; the wizard sets it.
        """
        return (
            SpecRef(name=self.domain_dd.value, origin="catalog")
            if self.domain_dd.value != "<custom>"
            else SpecRef(name=self.grid_name.value, origin="custom")
        )

    def snapshot(self, layout: dict[str, Any]) -> dict[str, Any]:
        """The domain-defining widget values, with the Run card's processor
        ``layout`` among them: what the wizard's ``_domain_snapshot`` seeds at a
        catalog Domain pick and compares with to detect an edit made after it
        (``composition.domain.modified``).
        """
        return {
            "grid_name": self.grid_name.value,
            "grid_w": {k: w.value for k, w in self.grid_w.items()},
            "scoord_chk": self.scoord_chk.value,
            "bnd": {d: w.value for d, w in self.bnd.items()},
            "v_sponge": self._v_sponge_value(),
            "dt": self.dt.value,
            **layout,
            "topo_source": self.topo_source.value,
            "topo_path": self.topo_path.value,
            "nest_enable": self.nest_enable.value,
            "child_w": {k: w.value for k, w in self.child_w.items()},
            "nest_period": self.nest_period.value,
            "nest_pressure_fluxes": self.nest_pressure_fluxes.value,
            "parent_enable": self.parent_enable.value,
            "parent_w": {k: w.value for k, w in self.parent_w.items()},
            "child_topo": (self.child_topo_source.value, self.child_topo_path.value),
            "parent_topo": (
                self.parent_topo_source.value,
                self.parent_topo_path.value,
            ),
        }

    def spec_data(
        self,
        kw: dict[str, Any],
        *,
        description: str,
        model_name: str,
        dates: dict[str, str],
    ) -> dict[str, Any]:
        """A DomainSpec-shaped dict of the card's widget state, given the resolver
        kwargs ``kw`` it was gathered into and what other cards hold: the spec's
        ``description``, the ``model_name`` and the run ``dates``. See the wizard's
        ``_domain_spec_data``.
        """
        data: dict[str, Any] = {
            "description": description,
            "model_name": model_name,
            "grid_name": self.grid_name.value,
            **dates,
            "grid_kwargs": kw["grid_kwargs"],
            "partitioning": kw["partitioning"],
            "topography_source": self.topo_source.value,
        }
        # v_sponge/open_boundaries: included only when the user has touched
        # them (a manual edit or a value restored from a prior save/load) --
        # otherwise omitted so a reload re-derives fresh from the grid (see
        # _on_domain's symmetric restore-only-if-present, and
        # ensure_boundaries_derived for the pre-save safety net that fills
        # kw["open_boundaries"] with a real mask-derived value first).
        if self._v_sponge_touched:
            data["v_sponge"] = kw["v_sponge"]
        if self._dt_touched:
            data["dt"] = kw["dt"]
        if self._boundaries_touched:
            data["open_boundaries"] = kw["open_boundaries"]
        if self.topo_path.value.strip():
            data["topography_path"] = self.topo_path.value.strip()
        for k in (
            "grid_kwargs_child",
            "grid_kwargs_parent",
            "metadata_child",
            "nesting_include_pressure_fluxes",
        ):
            if k in kw:
                data[k] = kw[k]
        return data

    def reselect(self, name: str | None) -> None:
        """Re-read the catalog's DomainSpecs into ``domain_dd`` after "Save as new
        spec" and select *name*; ``None`` keeps the current selection (the wizard
        holds it suspended, and reseeds ``modified``).
        """
        old_value = self.domain_dd.value
        self.domain_dd.options = _dd_options(
            self.catalog,
            list(self.catalog.domain_names),
            "domain",
            prefix=["<custom>"],
        )
        self.domain_dd.value = name if name is not None else old_value

    def load_picked_domain(self) -> tuple[dict[str, Any], bool] | None:
        """Set the card from the DomainSpec picked in ``domain_dd`` (not "<custom>"):
        detach a grid file, read the spec, and set the grid, its derived properties,
        the topography and the nested grids from it. Return the spec's data, for
        the other cards, and whether a parent grid was dropped (this grid had one,
        the spec has none); ``None`` when the spec's child nesting is refused (the
        summary says why) and nothing else was set.
        """
        name = self.domain_dd.value
        if self._grid_file is not None:
            # A catalog Domain pick replaces the grid wholesale via grid_kwargs,
            # which a user-provided grid file forbids carrying alongside it
            # (Domain._grid_file_excludes_generation_geometry) -- detach first so
            # the catalog's grid_kwargs land on now-unlocked widgets instead of
            # being silently discarded.
            self._detach_grid_file()
        data = self.catalog.domain_data(name)
        gk = data.get("grid_kwargs", {}) or {}
        split = self._split_child_nesting(
            data.get("grid_kwargs_child"),
            data.get("metadata_child"),
            bool(data.get("nesting_include_pressure_fluxes", False)),
        )
        if split is None:
            return None
        child, child_metadata, pressure_fluxes = split
        was_parented = self.parent_enable.value
        parent_dropped = False
        with self._suspend():
            self.grid_name.value = data.get("grid_name", name)
            self._set_loaded_grid_kwargs("grid", gk)
            for k, w in self.grid_w.items():
                if k in gk:
                    w.value = gk[k]
            self.scoord_chk.value = any(k in gk for k in _SCOORD)
            # The grid_kwargs keys with their own widgets (as _populate_from).
            self.hmin.value = float(gk.get("hmin", _GRID_KWARGS_LEFT_OUT["hmin"]))
            self.close_narrow_chk.value = bool(
                gk.get(
                    "close_narrow_channels",
                    _GRID_KWARGS_LEFT_OUT["close_narrow_channels"],
                )
            )
            self.mask_shapefile.value = str(gk.get("mask_shapefile") or "")
            # Domain-derived properties: a picked catalog Domain replaces the
            # current grid entirely, so reset touched/derived state first, then
            # restore only what the saved DomainSpec actually carries --
            # absence means "derive fresh from the grid" (click "Derive from
            # grid", or the Save/Run safety net), mirroring
            # _domain_spec_data's save-only-when-touched symmetry. Leaves the
            # boundary checkboxes untouched (rather than resetting to False)
            # when absent, since there's nothing to derive from without a grid
            # build -- the derive_status warning surfaces that instead.
            self._v_sponge_touched = False
            self._boundaries_touched = False
            self._boundaries_derived = False
            self.derive_status.value = ""
            saved_v_sponge = data.get("v_sponge")
            if saved_v_sponge is not None:
                self.v_sponge.value = float(saved_v_sponge)
                self._v_sponge_touched = True
            saved_bnd = data.get("open_boundaries")
            if saved_bnd is not None:
                for d, w in self.bnd.items():
                    if d in saved_bnd:
                        w.value = bool(saved_bnd[d])
                self._boundaries_touched = True
            saved_dt = data.get("dt")
            if saved_dt is not None:
                self.dt.value = float(saved_dt)
            self._dt_touched = saved_dt is not None
            # model_name in a DomainSpec is provenance (the model in use when the
            # domain was saved), not a preference: picking a domain must not
            # override the user's Model selection, so it is deliberately NOT
            # restored here (unlike the load-a-full-blueprint path, where the
            # blueprint's composition.model is authoritative).
            self.topo_source.value = data.get("topography_source", "ETOPO5")
            self.topo_path.value = data.get("topography_path", "") or ""
            # Nesting: mirrors populate_nesting (loaded-blueprint path) but reads
            # from a DomainSpec (only present if the spec was saved
            # via register_domain_from_dict with nesting active).
            self._set_loaded_grid_kwargs("child", child)
            self.nest_enable.value = child is not None
            if child:
                for k, w in self.child_w.items():
                    if k in child:
                        w.value = child[k]
                self._set_nested_topo_widgets("child", child)
                period = (child_metadata or {}).get("period")
                if period is not None:
                    self.nest_period.value = float(period)
                self.nest_pressure_fluxes.value = pressure_fluxes
            # Parent: mirrors the child block above, reading grid_kwargs_parent
            # (only present if the spec was saved with a parent grid active).
            parent = data.get("grid_kwargs_parent")
            self._set_loaded_grid_kwargs("parent", parent)
            self.parent_enable.value = parent is not None
            if parent:
                for k, w in self.parent_w.items():
                    if k in parent:
                        w.value = parent[k]
                self._set_nested_topo_widgets("parent", parent)
            elif was_parented:
                parent_dropped = True
        return data, parent_dropped

    def load_picked_parent(self) -> bool:
        """Prefill the parent grid from the DomainSpec picked in ``parent_domain_dd``,
        and enable it; ``False`` for "<custom>", which sets nothing.
        """
        name = self.parent_domain_dd.value
        if name == "<custom>":
            return False
        data = self.catalog.domain_data(name)
        gk = data.get("grid_kwargs", {}) or {}
        with self._suspend():
            self.parent_enable.value = True
            self._set_loaded_grid_kwargs("parent", gk)
            for k, w in self.parent_w.items():
                if k in gk:
                    w.value = gk[k]
            # Carry the parent spec's topography too: the parent is rebuilt so
            # align_grids can blend this grid toward the parent's mask/bathymetry,
            # so it must be built from what the PARENT run used -- not this grid's
            # (typically finer, differently-tiled) dataset, which may not even
            # cover the larger parent footprint. Explicit, never the inherit
            # sentinel: the spec said what it was built with.
            self.parent_topo_source.value = data.get("topography_source", "ETOPO5")
            self.parent_topo_path.value = data.get("topography_path", "") or ""
        return True

    # ---- the grid, its file, derived properties, previews and nesting ---------
    def _on_grid_kwarg_change(self, _change):
        # A geometry/mask-affecting edit invalidates any prior mask-derived
        # boundaries -- a stale mask must never be silently reused. Only the
        # "derived" freshness flag resets; an already-touched (manually set or
        # loaded) value is never overwritten by this. Clearing derive_status lets
        # _rebuild's "not derived yet" warning reappear instead of leaving a
        # stale "✓ derived" message visible against the now-invalidated mask.
        if not self._is_suspended():
            self._boundaries_derived = False
            self.derive_status.value = ""
        self._rebuild()

    def _set_grid_widgets_locked(self, locked: bool) -> None:
        """Disable/re-enable every grid-generation widget a user-provided grid
        file makes meaningless: geometry/vertical kwargs, mask/topography, and
        nesting (custom grid + nesting is unsupported -- see
        ``Domain._grid_file_excludes_generation_geometry``). Open boundaries and
        v_sponge stay editable/derivable -- both work fine off a loaded grid.
        """
        for w in (
            *self.grid_w.values(),
            self.scoord_chk,
            self.hmin,
            self.close_narrow_chk,
            self.mask_shapefile,
            self.topo_source,
            self.topo_path,
            self.nest_enable,
            self.parent_enable,
        ):
            w.disabled = locked
        self.grid_lock_banner.layout.display = "" if locked else "none"

    def _populate_grid_widgets_from_grid(self, grid: Any) -> None:
        """Display a loaded grid file's own attributes on the (now-locked) grid
        widgets, skipping any that are ``None`` (a hand-made file may lack
        ``size_x``/``size_y`` -- see ``build_forge_blueprint``'s grid_file
        docstring). Purely cosmetic here -- ``_gather()`` never reads these
        widget values while a grid file is attached (``grid_kwargs`` is ``{}``).
        """
        with self._suspend():
            for key in _GRID_INT + _GRID_FLOAT + _SCOORD:
                val = getattr(grid, key, None)
                if val is not None:
                    self.grid_w[key].value = val
            if all(
                getattr(grid, a, None) is not None for a in ("theta_s", "theta_b", "hc")
            ):
                self.scoord_chk.value = True

    def _finish_grid_file_attach(
        self, file_dict: dict[str, Any], grid: Any, *, snapshot: bool = True
    ) -> None:
        """Common tail of a successful (or reused-hash) grid-file attach: cache
        state, populate + lock widgets, invalidate any prior mask-derived
        boundaries (a new grid means a new mask -- mirrors
        ``_on_grid_kwarg_change``), and show the attach status.

        ``snapshot=False`` skips capturing the pre-attach widget snapshot -- used
        by ``_reattach_grid_file`` (blueprint load-back), where the widgets hold
        leftover values from whatever was on screen before the load, not a
        user-authored geometry worth restoring on Detach.
        """
        if snapshot and self._grid_widgets_snapshot is None:
            # Capture the user's own pre-attach geometry exactly once per
            # detached->attached transition -- a second attach while already
            # attached (e.g. picking a different file without detaching first)
            # must not overwrite the ORIGINAL values with the first file's.
            self._grid_widgets_snapshot = {
                **{k: w.value for k, w in self.grid_w.items()},
                "scoord_chk": self.scoord_chk.value,
            }
        self._grid_file = file_dict
        self._grid_file_grid = grid
        self._populate_grid_widgets_from_grid(grid)
        self._set_grid_widgets_locked(True)
        self._boundaries_derived = False
        self.derive_status.value = ""
        self.grid_file_status.value = _user_file_status_html(file_dict)

    def _attach_grid_file_from_path(self, path_str: str) -> None:
        self.grid_file_status.value = "<i>attaching…</i>"
        try:
            path = Path(path_str).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"grid file not found: {path}")
            import roms_tools as rt

            grid = rt.Grid(filename=str(path))
            content_hash = hash_netcdf_contents(path)
        except Exception as exc:
            self.grid_file_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        self._finish_grid_file_attach(
            {"location": str(path), "content_hash": content_hash}, grid
        )
        self._rebuild()

    def _on_grid_file_attach(self, _btn):
        path_str = self.grid_file_path.value.strip()
        if not path_str:
            self.grid_file_status.value = components.status_span(
                "err", "Enter a path first."
            )
            return
        # Explicit click always re-loads/re-hashes (the file may have changed on
        # disk since the last attach) -- no dedupe here.
        self._attach_grid_file_from_path(path_str)

    def _maybe_attach_grid_file(self) -> None:
        """Attach the path in the grid-file box unless it's blank or already the
        attached file. Fired on Enter/focus-out of the Text (and after an
        upload lands), so a user who types/pastes a path and moves on without
        clicking Attach doesn't silently keep building from the grid_kwargs
        widgets instead of the file they meant to use.
        """
        path_str = self.grid_file_path.value.strip()
        if not path_str or _same_attached_file(self._grid_file, path_str):
            return
        self._attach_grid_file_from_path(path_str)

    def _on_grid_file_path_submit(self, _change) -> None:
        if self._is_suspended():
            return
        self._maybe_attach_grid_file()

    def _on_grid_file_upload(self, change):
        items = change["new"]
        if not items:
            return
        item = (
            items[0] if isinstance(items, (list, tuple)) else next(iter(items.values()))
        )
        dest = _stage_uploaded_netcdf(item["name"], bytes(item["content"]))
        # Setting the Text fires _on_grid_file_path_submit (attach); the explicit
        # call covers a same-name re-upload where the value doesn't change.
        self.grid_file_path.value = str(dest)
        self._maybe_attach_grid_file()

    def _detach_grid_file(self) -> None:
        """Pure state reset (no _rebuild(), no widget-value restore) -- shared
        by the Detach button and _on_domain/_populate_from (a catalog Domain
        pick or a freshly-loaded grid-kwargs blueprint both set grid_w to their
        OWN authoritative values immediately after calling this, which must
        win outright -- restoring the pre-attach snapshot here would clobber
        them with stale values from a since-superseded attach). Only
        _on_grid_file_detach (the explicit user action, with nothing else
        about to overwrite grid_w) restores the snapshot -- see there.
        """
        self._grid_file = None
        self._grid_file_grid = None
        self._grid_widgets_snapshot = None
        self._set_grid_widgets_locked(False)
        self._boundaries_derived = False
        self.derive_status.value = ""
        self.grid_file_path.value = ""
        self.grid_file_upload.value = ()
        self.grid_file_status.value = ""

    def _on_grid_file_detach(self, _btn):
        snapshot = (
            self._grid_widgets_snapshot
        )  # read before _detach_grid_file clears it
        self._detach_grid_file()
        if snapshot is not None:
            with self._suspend():
                for k, v in snapshot.items():
                    if k == "scoord_chk":
                        self.scoord_chk.value = v
                    elif k in self.grid_w:
                        self.grid_w[k].value = v
        self._rebuild()

    def _reattach_grid_file(self, file_obj: Any) -> None:
        """``_populate_from``'s grid_file restore: reload the grid but REUSE the
        blueprint's recorded content_hash (never recompute it on load -- mirrors
        the CDR-file restore). On a reload failure, keep the grid_file dict and
        widgets locked (rather than silently falling back to the generic/default
        grid_kwargs, which would gather a completely different blueprint) and
        surface the error -- the resolver will raise the same "file not found"
        on the next _gather()/_rebuild(), which is the loud failure this must
        produce instead of a silent wrong answer.
        """
        file_dict = {
            "location": file_obj.location,
            "content_hash": file_obj.content_hash,
        }
        self._grid_file = file_dict
        # Whatever sits in the grid widgets right now predates this load and is
        # not a geometry worth restoring on Detach -- drop any stale snapshot.
        self._grid_widgets_snapshot = None
        self.grid_file_path.value = file_dict["location"]
        self._set_grid_widgets_locked(True)
        try:
            path = Path(file_dict["location"]).expanduser()
            if not path.exists():
                raise FileNotFoundError(f"grid file not found: {path}")
            import roms_tools as rt

            grid = rt.Grid(filename=str(path))
        except Exception as exc:
            self._grid_file_grid = None
            self._boundaries_derived = False
            self.derive_status.value = ""
            self.grid_file_status.value = components.status_span(
                "err", f"⚠ could not re-attach grid_file: {type(exc).__name__}: {exc}"
            )
            return
        self._finish_grid_file_attach(file_dict, grid, snapshot=False)

    def _on_boundary_edit(self, _change):
        # A programmatic backfill (see _apply_grid_derived_properties, _on_domain,
        # _populate_from) happens under _suspend() -- only a real user edit should
        # "touch" the property against further auto-derivation.
        if self._is_suspended():
            return
        self._boundaries_touched = True
        self.derive_status.value = ""
        self._rebuild()

    def _show_derived_v_sponge(self, value: float) -> None:
        """Show the grid-derived sponge viscosity ``value`` rounded for display
        (8333.333333333332 reads 8333.3), keeping the full value for
        `_v_sponge_value`. The blueprint never takes it from the widget while it
        is derived: the resolver derives it again from ``v_sponge=None``.
        """
        self._v_sponge_derived = value
        self.v_sponge.value = _display_round(value)

    def _v_sponge_value(self) -> float:
        """The sponge viscosity the widget stands for: the full derived value while
        it is untouched and shows that value rounded, else what it holds.
        """
        derived = self._v_sponge_derived
        if (
            not self._v_sponge_touched
            and derived is not None
            and self.v_sponge.value == _display_round(derived)
        ):
            return derived
        return float(self.v_sponge.value)

    def _on_v_sponge_edit(self, _change):
        if self._is_suspended():
            return
        self._v_sponge_touched = True
        self._rebuild()

    def _on_dt_edit(self, _change):
        if self._is_suspended():
            return
        self._dt_touched = True
        self._rebuild()

    def sync_derived_dt(self) -> None:
        """While dt is untouched, show the CFL dt for the current grid, which is
        what the resolver derives from ``dt=None``. Pure arithmetic, so it works
        while the rest of the blueprint does not resolve.
        """
        if self._dt_touched:
            self.dt_status.value = ""
            return
        try:
            derived = float(cfl_dt(self._base_grid_kwargs(), self._attached_grid()))
        except Exception as exc:
            self.dt_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc} Type a time step."
            )
            return
        with self._suspend():
            self.dt.value = derived
        self.dt_status.value = components.status_span(
            "ok", "derived from the grid (CFL)"
        )

    def _on_nest_domain(self, _change):
        """Prefill the child grid from a selected DomainSpec (and enable nesting)."""
        name = self.nest_domain_dd.value
        if name == "<custom>":
            return
        data = self.catalog.domain_data(name)
        split = self._split_child_nesting(
            data.get("grid_kwargs", {}) or {},
            None,
            bool(self.nest_pressure_fluxes.value),
        )
        if split is None:
            return
        gk, metadata, pressure_fluxes = split
        gk = gk or {}
        with self._suspend():
            self.nest_enable.value = True
            # The child spec's own grid_kwargs (its hmin, its channel closing)
            # describe the child, so they go with it (see _nested_grid_kwargs).
            self._set_loaded_grid_kwargs("child", gk)
            if (period := (metadata or {}).get("period")) is not None:
                self.nest_period.value = float(period)
            self.nest_pressure_fluxes.value = pressure_fluxes
            for k, w in self.child_w.items():
                if k in gk:
                    w.value = gk[k]
            # The child spec's own topography, not this grid's (see
            # _on_parent_domain for why this matters).
            self.child_topo_source.value = data.get("topography_source", "ETOPO5")
            self.child_topo_path.value = data.get("topography_path", "") or ""
        self._rebuild()
        self._on_nest_plot(None)

    def _split_child_nesting(
        self,
        grid_kwargs_child: dict[str, Any] | None,
        metadata_child: dict[str, Any] | None,
        include_pressure_fluxes: bool,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, bool] | None:
        """Fold a spec's child nesting settings out of its ``grid_kwargs_child``.

        See :func:`split_child_nesting_metadata`. A spec it refuses is reported in
        the status banner and gives ``None``, so the caller leaves the widgets be.
        """
        try:
            return split_child_nesting_metadata(
                grid_kwargs_child, metadata_child, include_pressure_fluxes
            )
        except ValueError as exc:
            self._show_summary(
                components.banner(
                    "err", f"<b>Invalid child domain.</b> {html.escape(str(exc))}"
                )
            )
            return None

    def _set_nested_topo_widgets(self, which: str, gk: dict[str, Any]) -> None:
        """Load a nested grid's optional ``topography_source``/``topography_path``
        (from a blueprint or DomainSpec ``grid_kwargs_parent``/``_child`` dict)
        into its widgets; absent keys mean "inherit this grid's" (the sentinel /
        blank). Called inside a ``_suspend()`` block by the populate paths.
        """
        src_w = getattr(self, f"{which}_topo_source")
        path_w = getattr(self, f"{which}_topo_path")
        src = gk.get("topography_source")
        if isinstance(src, dict):  # hand-authored roms-tools {'name','path'}
            path_w.value = str(gk.get("topography_path") or src.get("path") or "")
            src = src.get("name")
        else:
            path_w.value = str(gk.get("topography_path") or "")
        src = getattr(src, "value", src)
        src_w.value = str(src) if src in _TOPO_SOURCES else _NESTED_TOPO_INHERIT

    def _base_grid_kwargs(self) -> dict[str, Any]:
        """The main grid's generation kwargs, read from the grid widgets.

        ``{}`` while a user-provided grid file is attached: it forbids the
        generation-geometry keys alongside it
        (``Domain._grid_file_excludes_generation_geometry``), so ``grid_file=``/
        ``grid=`` carry the grid instead. Otherwise laid over the grid_kwargs last
        loaded into the widgets, so a loaded key with no widget survives a save.
        """
        gk: dict[str, Any] = {}
        if self._grid_file is not None:
            return gk
        for k in _GRID_INT:
            gk[k] = int(self.grid_w[k].value)
        for k in _GRID_FLOAT:
            gk[k] = float(self.grid_w[k].value)
        if self.scoord_chk.value:
            for k in _SCOORD:
                gk[k] = float(self.grid_w[k].value)
        # hmin + close_narrow_channels + mask_shapefile injected into grid_kwargs
        if self.hmin.value != _GRID_KWARGS_LEFT_OUT["hmin"]:
            gk["hmin"] = float(self.hmin.value)
        if self.close_narrow_chk.value:
            gk["close_narrow_channels"] = True
        if self.mask_shapefile.value.strip():
            gk["mask_shapefile"] = self.mask_shapefile.value.strip()
        return _merge_loaded_kwargs(
            self._loaded_grid_kwargs.get("grid", {}),
            gk,
            owned={*self.grid_w, *_GRID_KWARGS_LEFT_OUT},
            left_out=_GRID_KWARGS_LEFT_OUT,
        )

    def _nested_grid_kwargs(self, which: str) -> dict[str, Any]:
        """``which`` in ("parent", "child"): that grid's generation kwargs, read
        from its widgets and laid over the grid_kwargs last loaded into them. A
        parent's ``hmin`` or ``close_narrow_channels`` has no widget here, and the
        child is aligned to a parent rebuilt from exactly these kwargs.
        """
        boxes = getattr(self, f"{which}_w")
        gk: dict[str, Any] = {}
        for k in _GRID_INT:
            gk[k] = int(boxes[k].value)
        for k in _GRID_FLOAT + _SCOORD:
            gk[k] = float(boxes[k].value)
        gk.update(
            _nested_topo_kwargs(
                getattr(self, f"{which}_topo_source").value,
                getattr(self, f"{which}_topo_path").value,
            )
        )
        return _merge_loaded_kwargs(
            self._loaded_grid_kwargs.get(which, {}),
            gk,
            owned={*boxes, *_NESTED_TOPO_KEYS},
        )

    def _set_loaded_grid_kwargs(self, which: str, gk: dict[str, Any] | None) -> None:
        """Record ``gk`` as the grid_kwargs just loaded into ``which``'s widgets
        ("grid", "parent" or "child"); see :func:`_merge_loaded_kwargs`.
        """
        self._loaded_grid_kwargs[which] = copy.deepcopy(dict(gk or {}))

    def _attached_grid(self) -> Any:
        """The attached grid file's loaded ``roms_tools.Grid``, or ``None`` when no
        grid file is attached.
        """
        if self._grid_file_grid is not None:
            return self._grid_file_grid
        if self._grid_file is not None:
            # Attached but not currently loaded (a reattach failed -- e.g. the
            # file went missing since it was recorded -- see _reattach_grid_file).
            # Must not silently fall through to building a grid from whatever
            # stale/default values happen to be sitting in the (locked, but
            # still holding old values) grid_w widgets -- that would plot/derive
            # boundaries and v_sponge from a completely different, wrong grid
            # with no indication anything is amiss.
            raise RuntimeError(
                "grid_file is attached but failed to (re)load -- see "
                "grid_file_status; re-attach a valid file first."
            )
        return None

    def populate_nesting(self, cfg: ForgeBlueprint):
        """Set the nesting widgets from a loaded config (called inside _suspend)."""
        child = cfg.domain.grid_kwargs_child
        self._set_loaded_grid_kwargs("child", child)
        self.nest_enable.value = child is not None
        if child:
            for k, w in self.child_w.items():
                if k in child:
                    w.value = child[k]
            self._set_nested_topo_widgets("child", child)
            # The period comes from metadata_child (generation writes the namelist's
            # extract_period from it), except an older file's Advanced
            # extract_period override, which that file ran with; the save then
            # records it in metadata_child.
            period = (cfg.composition.overrides.get("extract_data") or {}).get(
                "extract_period", cfg.domain.child_extract_period
            )
            if period is not None:
                self.nest_period.value = float(period)
        self.nest_pressure_fluxes.value = bool(
            cfg.domain.nesting_include_pressure_fluxes
        )
        parent = cfg.domain.grid_kwargs_parent
        self._set_loaded_grid_kwargs("parent", parent)
        self.parent_enable.value = parent is not None
        if parent:
            for k, w in self.parent_w.items():
                if k in parent:
                    w.value = parent[k]
            self._set_nested_topo_widgets("parent", parent)

    def nesting_role(self) -> tuple[bool, bool]:
        """``(is_parent, is_child)`` for the grid being edited, as the nesting
        checkboxes request it. Nesting is unsupported alongside a user-provided grid
        file (see ``Domain._grid_file_excludes_generation_geometry``), so an attached
        file ignores both even if the (now-disabled) checkboxes are still checked.
        Read by ``_gather`` (what is resolved) and by the settings editor's key, so
        the Output card follows a nesting toggle even before the blueprint is valid.
        """
        if self._grid_file is not None:
            return False, False
        return bool(self.nest_enable.value), bool(self.parent_enable.value)

    def _on_compute_dt(self, _):
        # Back to the derived dt (_rebuild shows it), which then follows grid
        # edits again.
        self._dt_touched = False
        self._rebuild()

    def build_grid_from_widgets(self) -> Any:
        """Build a ``roms_tools.Grid`` from the current (main-domain) grid kwargs
        -- or return the already-loaded grid file's grid, when one is attached.

        Shared by the plot button, "Derive from grid", and the export-time
        safety net -- the only three places that pay the cost of an actual
        grid build (the live preview / ``_rebuild`` never does, so typing in
        the grid fields stays instant).
        """
        attached = self._attached_grid()
        if attached is not None:
            return attached

        from roms_tools import Grid

        # A loaded key with no widget is kept for the save (_merge_loaded_kwargs),
        # but Forge's own keys (a nesting ``metadata`` block) are not Grid's.
        gk = grid_build_kwargs(self._base_grid_kwargs())
        topo, self._grid_build_topo_note = self._this_grid_plot_topography()
        if topo is not None:
            gk["topography_source"] = topo
        return Grid(**gk)

    def _this_grid_plot_topography(self) -> tuple[dict[str, str] | None, str]:
        """This grid's ``topography_source`` for a wizard-side build, or the
        ETOPO5 fallback + note when the file isn't available here.
        """
        return _plot_topography_source(self.topo_source.value, self.topo_path.value)

    def _nested_plot_topography(self, which: str) -> tuple[dict[str, str] | None, str]:
        """``which`` in ("parent", "child"): that grid's effective topography
        (its own widgets, or this grid's when set to inherit) for a wizard-side
        build, with the same local-availability fallback.
        """
        name, path = _effective_nested_topo(
            getattr(self, f"{which}_topo_source").value,
            getattr(self, f"{which}_topo_path").value,
            self.topo_source.value,
            self.topo_path.value,
        )
        topo, note = _plot_topography_source(name, path)
        return topo, note.replace("(plotted", f"({which} grid plotted", 1)

    def _apply_grid_derived_properties(self, grid: Any) -> None:
        """Set any untouched v_sponge/open-boundary values from a built grid.

        Boundaries come from roms-tools' own ``check_and_set_boundaries`` (the
        same logic ``rt.Grid``/downstream tools use to infer active edges from
        the land mask); v_sponge from the existing grid-spacing formula. Only
        overwrites properties the user hasn't touched -- see the module-level
        touched/derived state-machine notes on ``_v_sponge_touched`` etc.
        """
        from roms_tools.setup.utils import check_and_set_boundaries

        from cstar.applications.forge.util import compute_v_sponge_from_grid

        mask = grid.ds.get("mask_rho")
        if mask is None:
            raise ValueError(
                "grid.ds has no 'mask_rho' -- cannot derive open boundaries from it"
            )
        boundaries = check_and_set_boundaries(None, mask)
        with self._suspend():
            if not self._boundaries_touched:
                for d, w in self.bnd.items():
                    if d in boundaries:
                        w.value = bool(boundaries[d])
            if not self._v_sponge_touched:
                self._show_derived_v_sponge(
                    compute_v_sponge_from_grid(grid.size_x, grid.nx)
                )
        self._boundaries_derived = True

    def _on_derive_domain_properties(self, _):
        """Handle the "Derive from grid" button: build the grid once and
        refresh any untouched v_sponge/open-boundary values from it.
        """
        self.derive_status.value = "<i>building grid…</i>"
        try:
            grid = self.build_grid_from_widgets()
            self._apply_grid_derived_properties(grid)
            self.derive_status.value = components.status_span(
                "ok", "✓ derived from grid"
            )
        except Exception as exc:
            self.derive_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
        self._rebuild()

    def ensure_boundaries_derived(self) -> bool:
        """Export-time safety net for every blueprint save (the wizard's
        ``save_blueprint``: Save, Run Forge here and the Workplan page): if
        boundaries are untouched and have never been derived against the current
        grid, force one grid build now so a save can never ship the provisional
        checkbox defaults (east+north) silently. Saving a DomainSpec does not call
        it: the spec omits untouched boundaries, which re-derive when it loads.

        Returns True if boundaries are known-good (touched, already derived, or
        just successfully derived here); False if an explicit derive was needed
        and failed (see ``derive_status`` for the error) -- callers MUST abort
        rather than proceed and silently persist the provisional defaults.

        The passive browser download link (a plain data-URI anchor, regenerated
        on every ``_rebuild``) has no click-time Python hook to apply this to --
        forcing a grid build on every edit would reintroduce the UI-stall this
        design explicitly avoids. ``ReviewCard.refresh_download_links`` withholds it
        instead until the boundaries are touched or derived.
        """
        if self._boundaries_touched or self._boundaries_derived:
            return True
        self._on_derive_domain_properties(None)
        return self._boundaries_derived

    def on_plot(self, _):
        """Build a roms_tools.Grid from the current grid kwargs and render it."""
        self.plot_status.value = "<i>building grid…</i>"
        try:
            import io

            import matplotlib.pyplot as plt

            plt.ioff()
            try:
                grid = self.build_grid_from_widgets()
                grid.plot()
                fig = plt.gcf()
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=80, bbox_inches="tight")
                plt.close(fig)
            finally:
                plt.ion()

            buf.seek(0)
            self.plot_img.value = buf.read()
            self.plot_img.layout.display = ""
            self.plot_status.value = components.status_span("ok", "✓") + getattr(
                self, "_grid_build_topo_note", ""
            )
        except Exception as exc:
            self.plot_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )

    def _on_nest_plot(self, _):
        """Build the parent+child roms_tools Grids and render ``plot_nesting``
        (parent+child boundary overlay) into the Nesting section's own plot,
        separate from the parent-only plot in the Grid section.
        """
        self.nest_plot_status.value = "<i>building grids…</i>"
        try:
            import io

            import matplotlib.pyplot as plt
            from roms_tools import Grid, plot_nesting

            # This grid as Derive and the grid plot build it: an attached grid
            # file, or the kwargs a save writes with its topography resolved for
            # this machine.
            this_grid = self.build_grid_from_widgets()
            note_self = (
                "" if self._grid_file is not None else self._grid_build_topo_note
            )

            ck = {
                k: v
                for k, v in grid_build_kwargs(self._nested_grid_kwargs("child")).items()
                if k not in _NESTED_TOPO_KEYS
            }

            ck_topo, note_child = self._nested_plot_topography("child")
            if ck_topo is not None:
                ck["topography_source"] = ck_topo

            plt.ioff()
            try:
                parent = this_grid
                child = Grid(**ck)
                # plot_nesting calls plt.show() internally (no way to suppress it,
                # no return value); under Jupyter's inline backend that renders-
                # and-closes the figure immediately, so plt.gcf() right after
                # would return a fresh blank figure instead of the one just
                # drawn. Neutralize show() for the duration so we can grab and
                # save the actual figure ourselves.
                _real_show, plt.show = plt.show, lambda *a, **k: None
                try:
                    plot_nesting(parent, child)
                finally:
                    plt.show = _real_show
                fig = plt.gcf()
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=80, bbox_inches="tight")
                plt.close(fig)
            finally:
                plt.ion()

            buf.seek(0)
            self.nest_plot_img.value = buf.read()
            self.nest_plot_img.layout.display = ""
            self.nest_plot_status.value = (
                components.status_span("ok", "✓") + note_self + note_child
            )
        except Exception as exc:
            self.nest_plot_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )

    def on_parent_plot(self, _):
        """Build the parent+this-grid roms_tools Grids and render ``plot_nesting``
        (boundary overlay of this grid within its parent) into the Parent
        section's own plot. This grid is the fine/inner grid; the parent is the
        coarse/outer grid it's aligned into.
        """
        self.parent_plot_status.value = "<i>building grids…</i>"
        try:
            import io

            import matplotlib.pyplot as plt
            from roms_tools import Grid, plot_nesting

            pk = {
                k: v
                for k, v in grid_build_kwargs(
                    self._nested_grid_kwargs("parent")
                ).items()
                if k not in _NESTED_TOPO_KEYS
            }

            # Each grid is built with ITS topography where the file is available
            # on this machine (mirrors the executor's per-grid resolution), so a
            # parent that falls outside its dataset's coverage fails here, in the
            # preview, instead of only at executor time.
            pk_topo, note_parent = self._nested_plot_topography("parent")
            if pk_topo is not None:
                pk["topography_source"] = pk_topo

            plt.ioff()
            try:
                parent = Grid(**pk)
                # This grid as Derive and the grid plot build it: an attached
                # grid file, or the kwargs a save writes with its topography
                # resolved for this machine.
                this_grid = self.build_grid_from_widgets()
                note_self = (
                    "" if self._grid_file is not None else self._grid_build_topo_note
                )
                # See _on_nest_plot for why plt.show() is neutralized here.
                _real_show, plt.show = plt.show, lambda *a, **k: None
                try:
                    plot_nesting(parent, this_grid)
                finally:
                    plt.show = _real_show
                fig = plt.gcf()
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=80, bbox_inches="tight")
                plt.close(fig)
            finally:
                plt.ion()

            buf.seek(0)
            self.parent_plot_img.value = buf.read()
            self.parent_plot_img.layout.display = ""
            self.parent_plot_status.value = (
                components.status_span("ok", "✓") + note_parent + note_self
            )
        except Exception as exc:
            self.parent_plot_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
