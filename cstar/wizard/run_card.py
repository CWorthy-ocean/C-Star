"""The Forge blueprint wizard's Run card.

`RunCard` owns the card's widgets: the run window, the processor layout (the MPI
tiling and ParallelIO) and the CDR section, with its CdrSpec picker, the per-mode
panels and the plot column. It owns their observers, the card's share of the
resolver kwargs and of loading a blueprint or a DomainSpec back, and its status
chip.

`ForgeBlueprintWizard` places the card's rows, reacts to a change by observing its
widgets, and otherwise reads and sets the card only through its methods and
properties. The card reaches back only through the callables it is given.
"""

from __future__ import annotations

import copy
import html
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cstar.applications.forge.blueprint import CdrSpec, SpecRef
from cstar.applications.forge.namelist_model import cdr_tracer_role
from cstar.applications.forge.resolve import (
    read_cdr_forcing_yaml,
    with_cdr_lite_tracer_set,
)
from cstar.base.digest import hash_netcdf_contents
from cstar.wizard.fields import (
    _CDR_FILE_HINT,
    _dd_options,
    _dd_values,
    _same_attached_file,
    _stage_uploaded_netcdf,
    _tip,
    _user_file_status_html,
)
from cstar.wizard.ui import components
from cstar.wizard.ui.labels import label_for

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager

    from cstar.applications.forge.blueprint import ForgeBlueprint


def _run_time_label(value: datetime) -> str:
    """A run datetime for the status bar: the date, with the time when it is not
    midnight (a loaded nested run starts at 06:00).
    """
    if value.time() == datetime.min.time():
        return value.date().isoformat()
    return value.isoformat(sep=" ", timespec="minutes")


def _run_time_iso(value: datetime) -> str:
    """A run datetime as a DomainSpec writes it: the date alone at midnight, else
    the full ISO-8601 datetime.
    """
    if value.time() == datetime.min.time():
        return value.date().isoformat()
    return value.isoformat()


def _iso_time(value: Any) -> Any:
    """``value`` as an ISO-8601 string when it is a date, a datetime or text
    that reads as one; anything else unchanged.
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).isoformat()
        except ValueError:
            return value
    return value


def _cdr_times_as_iso(cdr_forcing: dict[str, Any]) -> dict[str, Any]:
    """A copy of a ``CDRForcing`` kwargs dict with its times as ISO-8601 text,
    so a dict read from YAML (datetimes) compares equal to a compiled one.
    """
    out = copy.deepcopy(cdr_forcing)
    for key in ("start_time", "end_time"):
        if key in out:
            out[key] = _iso_time(out[key])
    for release in out.get("releases") or []:
        if isinstance(release, dict) and isinstance(release.get("times"), list):
            release["times"] = [_iso_time(t) for t in release["times"]]
    return out


def _cdr_simple_kept_reason(cdr_forcing: dict[str, Any]) -> str:
    """What a CDR forcing has beyond one release at a constant ALK flux, for the
    simple panel's note ("2 releases, a DIC flux").
    """
    releases = [r for r in cdr_forcing.get("releases") or [] if isinstance(r, dict)]
    reasons = []
    if len(releases) != 1:
        reasons.append(f"{len(releases)} releases")
    fluxes = [r.get("tracer_fluxes") or {} for r in releases]
    if others := sorted({t for f in fluxes for t in f} - {"ALK"}):
        reasons.append(f"a flux of {', '.join(others)}")
    if any(
        isinstance(v, list) and any(x != v[0] for x in v)
        for f in fluxes
        for v in f.values()
    ):
        reasons.append("a flux that changes in time")
    return ", ".join(reasons) or "settings these fields do not set"


def _cdr_forcing_from_netcdf(path: str | Path, grid: Any) -> Any:
    """Reconstruct a roms-tools ``CDRForcing`` from a pre-made CDR-forcing
    netCDF, for the CDR plot widget's "netcdf" mode (there is no
    ``CDRForcing.from_netcdf`` in roms-tools -- this is the wizard's own
    best-effort inverse of ``CDRForcingDatasetBuilder.build()``, see
    ``roms_tools.setup.cdr_forcing``).

    Every variable is selected by dimension name (``ncdr``/``ntracers``/
    ``time``), never by positional axis order, since the builder doesn't
    guarantee one. A file with generated tracers (``CDR_*``/``passive_tracer*``,
    ``tracer_set`` ``cdr_lite``/``passive``) names the release feeding each in
    its ``tracer_release`` coordinate (roms-tools >= 5.1): such a release is
    rebuilt from its own columns, keyed by role (``ALK``/``DIC``/
    ``passive_tracer``) with its ``tracer_set``. Any other release is a MARBL
    one: its tracer-perturbation fluxes are the ``ALK`` column alone when ``ALK``
    is on the axis (otherwise every non-generated column), and its volume
    concentrations every non-generated column. Three things this can't recover
    exactly, all acceptable for a plot-only reconstruction:

    * The builder interpolates every release onto the *union* of all releases'
      original times before writing the file, so the file carries one shared
      ``time`` axis -- a reconstructed release's ``times`` is that shared
      axis, not necessarily whatever the original release actually used.
    * A tracer-perturbation file with no ``tracer_name`` coordinate at all
      (unusual -- normal output always carries one) has no way to know which
      tracer a flux series belongs to; it's arbitrarily labeled "ALK".
    * A passive volume release's ``temp``/``salt`` (the file's rows for them
      hold the discharged water's physics) are left to roms-tools' defaults.

    Raises a ``ValueError`` prefixed ``"could not reconstruct releases from
    <path>: ..."`` for any failure, including the ones ``rt.CDRForcing``'s own
    validators would otherwise raise a more confusing pydantic error for (e.g.
    a single-time-point file, which fails its strict ``start_time <
    end_time``).
    """
    import pandas as pd
    import roms_tools as rt
    import xarray as xr

    path = Path(path)
    try:
        with xr.open_dataset(path) as ds:
            if "release_name" not in ds.coords:
                raise ValueError(
                    "no 'release_name' coordinate found -- is this really a "
                    "CDR-forcing file?"
                )
            release_names = [str(n) for n in ds["release_name"].values]
            times = [t.to_pydatetime() for t in pd.to_datetime(ds["time"].values)]
            if len(times) < 2 or times[0] >= times[-1]:
                raise ValueError(
                    "fewer than two distinct times on the shared 'time' axis -- "
                    "a CDRForcing requires start_time < end_time"
                )
            tracer_names = (
                [str(t) for t in ds["tracer_name"].values]
                if "tracer_name" in ds.coords
                else None
            )
            roles = [cdr_tracer_role(t) for t in tracer_names or ()]
            feeding: dict[str, list[tuple[int, str, str]]] = {}
            if any(roles):
                if "tracer_release" not in ds.coords:
                    raise ValueError(
                        "generated CDR tracers but no 'tracer_release' "
                        "coordinate -- the file predates roms-tools 5.1"
                    )
                for j, release in enumerate(ds["tracer_release"].values):
                    if (role := roles[j]) is not None:
                        feeding.setdefault(str(release), []).append((j, *role))
            marbl_columns = [
                (tn, j)
                for j, (tn, role) in enumerate(zip(tracer_names or (), roles))
                if role is None
            ]
            is_tracer_perturbation = "cdr_trcflx" in ds.data_vars
            is_volume = "cdr_volume" in ds.data_vars
            if not (is_tracer_perturbation or is_volume):
                raise ValueError(
                    "neither 'cdr_trcflx' nor 'cdr_volume' found -- "
                    "unrecognized CDR-forcing layout"
                )

            releases: list[Any] = []
            for i, name in enumerate(release_names):
                common = {
                    "name": name,
                    "lat": float(ds["cdr_lat"].isel(ncdr=i).item()),
                    "lon": float(ds["cdr_lon"].isel(ncdr=i).item()),
                    "depth": float(ds["cdr_dep"].isel(ncdr=i).item()),
                    "hsc": float(ds["cdr_hsc"].isel(ncdr=i).item()),
                    "vsc": float(ds["cdr_vsc"].isel(ncdr=i).item()),
                    "times": times,
                    "tracer_set": feeding[name][0][1] if name in feeding else "marbl",
                }
                if is_tracer_perturbation:
                    var = ds["cdr_trcflx"].isel(ncdr=i)
                    if name in feeding:
                        tracer_fluxes = {
                            key: var.isel(ntracers=j).values.tolist()
                            for j, _, key in feeding[name]
                        }
                    elif tracer_names is not None and "ALK" in tracer_names:
                        # ALK-only: this reconstruction only feeds the plot
                        # widget's "Tracer flux (ALK)" option.
                        idx = tracer_names.index("ALK")
                        tracer_fluxes = {"ALK": var.isel(ntracers=idx).values.tolist()}
                    elif tracer_names is not None:
                        tracer_fluxes = {
                            tn: var.isel(ntracers=j).values.tolist()
                            for tn, j in marbl_columns
                        }
                    elif "ntracers" in var.dims:
                        # No tracer_name coordinate at all -- can't identify the
                        # tracer(s); best-effort label the first as "ALK".
                        tracer_fluxes = {"ALK": var.isel(ntracers=0).values.tolist()}
                    else:
                        tracer_fluxes = {"ALK": var.values.tolist()}
                    releases.append(
                        rt.TracerPerturbation(
                            **common,
                            tracer_fluxes=tracer_fluxes,
                            release_type="tracer_perturbation",
                        )
                    )
                else:
                    volume_fluxes = ds["cdr_volume"].isel(ncdr=i).values.tolist()
                    tracer_var = ds.get("cdr_tracer")
                    if tracer_var is not None and tracer_names:
                        columns = (
                            [(key, j) for j, _, key in feeding[name]]
                            if name in feeding
                            else marbl_columns
                        )
                        tracer_concentrations = {
                            tn: tracer_var.isel(ncdr=i, ntracers=j).values.tolist()
                            for tn, j in columns
                        }
                    else:
                        tracer_concentrations = {}
                    releases.append(
                        rt.VolumeRelease(
                            **common,
                            volume_fluxes=volume_fluxes,
                            tracer_concentrations=tracer_concentrations,
                            release_type="volume",
                        )
                    )
        return rt.CDRForcing(
            grid=grid, start_time=times[0], end_time=times[-1], releases=releases
        )
    except Exception as exc:
        raise ValueError(f"could not reconstruct releases from {path}: {exc}") from exc


class RunCard:
    """The wizard's Run card: when the run starts and ends, how it is split over
    MPI ranks and whether it builds with ParallelIO, and the CDR forcing it
    releases.

    The callables are the wizard's: ``rebuild`` re-resolves the blueprint,
    ``suspend`` returns the wizard's suspend context manager, ``is_suspended``
    says whether one is open, ``cdr_lite`` says whether the Model card's BGC mode
    is "cdr_lite", ``grid_center`` gives the Grid card's centre ``(lat, lon)``,
    ``grid_fingerprint`` a comparable snapshot of its grid fields, and
    ``build_grid`` builds the grid they describe (for the CDR plot). Construction
    builds the widgets and sets them to their defaults but registers no observers
    and applies no CDR mode: the wizard calls `wire` and then `apply_cdr_mode`
    once it is built.
    """

    def __init__(
        self,
        W: Any,
        catalog: Any,
        *,
        rebuild: Callable[[], None],
        suspend: Callable[[], AbstractContextManager[Any]],
        is_suspended: Callable[[], bool],
        cdr_lite: Callable[[], bool],
        grid_center: Callable[[], tuple[float, float]],
        grid_fingerprint: Callable[[], tuple[Any, ...]],
        build_grid: Callable[[], Any],
    ) -> None:
        self.W = W
        self.catalog = catalog
        self._rebuild = rebuild
        self._suspend = suspend
        self._is_suspended = is_suspended
        self._cdr_lite = cdr_lite
        self._grid_center = grid_center
        self._grid_fingerprint = grid_fingerprint
        self._build_grid = build_grid

        # --- processor layout ---
        self.npx = W.IntText(
            value=1,
            description="n_procs_x:",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px"),
            tooltip=_tip("domain", "npx"),
        )
        self.npy = W.IntText(
            value=1,
            description="n_procs_y:",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px"),
            tooltip=_tip("domain", "npy"),
        )
        self.use_pio_chk = W.Checkbox(
            value=False,
            description="use ParallelIO (PIO)",
            indent=False,
            tooltip="Build ROMS against the ParallelIO library: inputs are written as "
            "classic-format (CDF-5) netCDF and ROMS reads/writes joined files.",
        )
        self.auto_tiling_chk = W.Checkbox(
            value=False,
            description="auto tiling",
            indent=False,
            tooltip="Pick the MPI tiling at runtime from the land mask "
            "(ucla-roms >= 0.5.0 MPI_MASKING). Requires PIO -- forces it on and "
            "locks it. Experimental.",
        )
        self.n_cores = W.IntText(
            value=1,
            description="n_cores:",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px", display="none"),
            tooltip="Total MPI ranks; the tiling itself is chosen at runtime "
            "from the land mask when auto tiling is on.",
        )

        # --- run window ---
        self.start = W.DatePicker(
            value=date(2012, 1, 1),
            description="Start:",
            style={"description_width": "110px"},
            tooltip=_tip("run", "start"),
        )
        self.end = W.DatePicker(
            value=date(2012, 1, 2),
            description="End:",
            style={"description_width": "110px"},
            tooltip=_tip("run", "end"),
        )
        self.model_ref_date = W.DatePicker(
            value=date(2000, 1, 1),
            description="Model ref date:",
            style={"description_width": "130px"},
            tooltip=_tip("run", "model_ref_date"),
        )
        # The run datetimes last loaded into the date pickers (a blueprint's or a
        # DomainSpec's), whose time of day the pickers drop (see _run_datetime);
        # written back on save while a picker still shows the loaded date.
        self._loaded_run_times: dict[str, datetime] = {}

        # --- CDR (Carbon Dioxide Removal) forcing -----------------------------
        # A top-level, independently composable spec (CdrSpec/Composition.cdr) --
        # NOT part of Forcing. ``cdr_dd`` picks a named CdrSpec from the catalog
        # (mirrors forcing_dd/output_dd/domain_dd); ``cdr_mode_dd`` picks which of
        # the five CdrSpec.mode variants is active and shows/hides the matching
        # sub-panel below (see ``apply_cdr_mode``). Picking a catalog CdrSpec
        # loads its mode + fields; hand-editing anything afterward is reported via
        # composition.cdr.modified (a snapshot diff against ``_cdr_seed``, mirroring
        # domain/forcing -- the dropdown itself never auto-reverts to "<custom>").
        _cdr_names = list(self.catalog.cdr_names)
        self.cdr_dd = W.Dropdown(
            options=_dd_options(self.catalog, _cdr_names, "cdr", prefix=["<custom>"]),
            value="<custom>",
            description="CDR spec:",
            style={"description_width": "110px"},
            tooltip="Select a named CdrSpec from the catalog to seed the CDR mode "
            "and fields below, or leave as <custom> to author one by hand.",
        )
        # snapshot of _cdr_snapshot() at the last catalog pick; None means no
        # catalog CdrSpec is currently selected (mirrors _domain_seed/_forcing_seed).
        self._cdr_seed: dict[str, Any] | None = None

        self.cdr_mode_dd = W.Dropdown(
            options=[
                ("None", "none"),
                ("Simple perturbation", "simple"),
                ("Import ROMS-tools YAML", "yaml"),
                ("Import netCDF", "netcdf"),
                ("Upscaled CDR forcing", "upscaled"),
            ],
            value="none",
            description="Mode:",
            style={"description_width": "110px"},
            tooltip=_tip("cdr", "mode"),
        )
        # Shown only while the Model card's BGC mode is "cdr_lite"
        # (sync_cdr_lite_hint):
        # in that mode a CDR forcing is required and its releases must target the
        # CDR-lite tracers.
        self.cdr_lite_hint = W.HTML(
            components.status_span(
                "note",
                "BGC mode <code>cdr_lite</code>: releases "
                "drive the CDR-lite tracers, and a CDR forcing is required. Simple "
                "mode builds one ALK (OAE) release; a YAML or netCDF forcing must use "
                "<code>tracer_set: cdr_lite</code>.",
            )
        )
        # The mode apply_cdr_mode() last actually applied -- lets it detect a
        # real transition (for leaving-mode cleanup) whether invoked via the
        # dropdown's own .observe or a direct call (e.g. from populate_cdr,
        # which must call it explicitly since suspended observers are no-ops).
        self._cdr_mode_active: str | None = None

        # --- CDR mode "simple": a single hand-authored tracer-perturbation
        # release, compiled into a roms-tools CDRForcing kwargs dict at gather
        # time (see _simple_cdr_forcing_dict). Laid out as field rows (labels and
        # units from the glossary) in cdr_simple_box below. ---
        self.cdr_simple_name = W.Text(value="my_cdr", layout=W.Layout(width="220px"))
        self.cdr_simple_lat = W.FloatText(value=0.0)
        self.cdr_simple_lon = W.FloatText(value=0.0)
        self.cdr_simple_depth = W.FloatText(value=1.0)
        self.cdr_simple_hsc = W.FloatText(value=10.0)
        self.cdr_simple_vsc = W.FloatText(value=10.0)
        self.cdr_simple_flux = W.FloatText(value=2 * 10**6)
        self.cdr_simple_start = W.DatePicker()
        self.cdr_simple_end = W.DatePicker()
        self.cdr_simple_fields = (
            self.cdr_simple_name,
            self.cdr_simple_lat,
            self.cdr_simple_lon,
            self.cdr_simple_depth,
            self.cdr_simple_hsc,
            self.cdr_simple_vsc,
            self.cdr_simple_flux,
            self.cdr_simple_start,
            self.cdr_simple_end,
        )
        # A loaded "simple" CDR forcing these fields cannot show (several
        # releases, a flux ramp, another tracer, a time of day): written back
        # unchanged, with the fields locked, until the user replaces it (see
        # _populate_cdr_simple).
        self._cdr_simple_loaded: dict[str, Any] | None = None
        # The values entering "simple" mode seeded into the fields (see
        # apply_cdr_mode); a field still showing its seed follows the grid
        # center and the run window (see _reseed_cdr_simple).
        self._cdr_simple_seeded: dict[str, Any] = {}
        self.cdr_simple_note = W.HTML("")
        self.cdr_simple_replace_btn = W.Button(
            description="Edit as one release",
            icon="pencil",
            layout=W.Layout(width="auto", display="none"),
            tooltip=(
                "Replace the loaded CDR forcing with one release at a constant "
                "ALK flux, taken from its first release, and unlock the fields."
            ),
        )
        self.cdr_simple_box = W.VBox(
            [
                self.cdr_simple_note,
                self.cdr_simple_replace_btn,
                components.field_row(W, "cdr_simple_name", self.cdr_simple_name),
                *(
                    components.field_row(W, key, widget, width="140px")
                    for key, widget in (
                        ("cdr_simple_lat", self.cdr_simple_lat),
                        ("cdr_simple_lon", self.cdr_simple_lon),
                        ("cdr_simple_depth", self.cdr_simple_depth),
                        ("cdr_simple_hsc", self.cdr_simple_hsc),
                        ("cdr_simple_vsc", self.cdr_simple_vsc),
                        ("cdr_simple_flux", self.cdr_simple_flux),
                    )
                ),
                components.field_row(W, "cdr_simple_start", self.cdr_simple_start),
                components.field_row(W, "cdr_simple_end", self.cdr_simple_end),
            ]
        )

        # --- CDR mode "yaml": an uploaded roms-tools CDRForcing.to_yaml() dump.
        # Parsed dict lives on the instance (not a widget) since FileUpload can't
        # be repopulated with the original file on load; cdr()/populate_cdr
        # read and write this directly. ---
        self._cdr_forcing: dict[str, Any] | None = None
        self.cdr_upload = W.FileUpload(
            accept=".yml,.yaml",
            multiple=False,
            description="Upload CDR YAML",
            tooltip=(
                "A roms-tools CDRForcing.to_yaml(...) dump. Validated immediately on "
                "upload; toggles the CDR compile/run-time settings on when accepted."
            ),
        )
        self.cdr_clear_btn = W.Button(description="Clear CDR", icon="times")
        self.cdr_status = W.HTML("")
        _cdr_yaml_help = W.HTML(
            components.status_span(
                "note",
                "Follow the <a href="
                "'https://roms-tools.readthedocs.io/en/latest/cdr_forcing.html' "
                "target='_blank'>roms-tools CDR forcing documentation</a> to build a "
                "<code>CDRForcing</code> and export it with <code>to_yaml()</code>, "
                "then upload that file here.",
            )
        )
        self.cdr_yaml_box = W.VBox(
            [
                _cdr_yaml_help,
                W.HBox([self.cdr_upload, self.cdr_clear_btn]),
                self.cdr_status,
            ]
        )

        # --- CDR mode "netcdf": a user-supplied pre-made CDR-forcing netCDF. ---
        self._cdr_forcing_file: dict[str, Any] | None = None
        self.cdr_file_path = W.Text(
            value="",
            description="CDR file:",
            placeholder="path to a pre-made CDR-forcing netCDF",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
            tooltip=_tip("cdr", "cdr_file"),
            continuous_update=False,  # see the wizard's grid_file_path
        )
        self.cdr_file_attach_btn = W.Button(
            description=label_for("buttons.attach", "Attach").label, icon="link"
        )
        # "Clear" removes the attached file exactly like "Detach" elsewhere (grid,
        # river custom-file) -- same verb pair everywhere, not a distinct action.
        self.cdr_file_clear_btn = W.Button(
            description=label_for("buttons.detach", "Detach").label, icon="times"
        )
        self.cdr_file_upload = W.FileUpload(
            accept=".nc", multiple=False, description="…or upload"
        )
        self.cdr_file_status = W.HTML("")
        _cdr_netcdf_help = W.HTML(
            components.status_span(
                "note",
                "Follow the <a href="
                "'https://roms-tools.readthedocs.io/en/latest/cdr_forcing.html' "
                "target='_blank'>roms-tools CDR forcing documentation</a> to build and "
                "<code>save()</code> the forcing as netCDF (or otherwise produce a "
                "CDR-forcing netCDF), then attach or upload it here.",
            )
        )
        self.cdr_netcdf_box = W.VBox(
            [
                _cdr_netcdf_help,
                W.HBox(
                    [
                        self.cdr_file_path,
                        self.cdr_file_attach_btn,
                        self.cdr_file_clear_btn,
                    ]
                ),
                self.cdr_file_upload,
                self.cdr_file_status,
            ]
        )

        # --- CDR mode "upscaled": nothing to configure -- ROMS reads CDR forcing
        # supplied at runtime from a child domain's upscaled signal; the blueprint
        # carries a placeholder path C-Star's orchestrator replaces. No dataset
        # exists at authoring time, so no plot either. ---
        self.cdr_upscaled_box = W.VBox(
            [
                W.HTML(
                    components.status_span(
                        "note",
                        "ROMS will read CDR forcing supplied "
                        "at runtime from a child domain's upscaled signal. The "
                        "blueprint carries a placeholder path that C-Star's "
                        "orchestrator replaces at run time -- nothing to configure "
                        "here.",
                    )
                )
            ]
        )

        # --- CDR plotting (WP6): visible for simple/yaml/netcdf only. The built
        # roms-tools CDRForcing (plus the grid it was built against) and rendered
        # PNGs are cached on the instance -- see refresh_cdr's cache-invalidation
        # comment -- so switching plot type/release re-renders without rebuilding.
        self._cdr_plot_object: Any = None  # last-built rt.CDRForcing, or None
        self._cdr_plot_grid: Any = None  # the rt.Grid it was built against
        self._cdr_plot_cache: dict[tuple[str, str | None], bytes] = {}
        # snapshot of _cdr_plot_inputs_fingerprint() at the last cache clear --
        # lets _invalidate_cdr_plot_cache skip clearing on rebuilds triggered by
        # plot-unrelated edits (e.g. the description field).
        self._cdr_plot_fingerprint: tuple | None = None
        self.cdr_plot_btn = W.Button(
            description=label_for("buttons.refresh_plot", "Refresh plot").label,
            icon="area-chart",
        )
        self.cdr_plot_status = W.HTML("")
        self.cdr_plot_type_dd = W.Dropdown(
            options=[
                ("Release locations", "locations"),
                ("Distribution", "distribution"),
                ("Tracer flux (ALK)", "tracer_flux"),
            ],
            value="locations",
            description="Plot:",
            style={"description_width": "110px"},
        )
        self.cdr_plot_release_dd = W.Dropdown(
            options=[], description="Release:", style={"description_width": "110px"}
        )
        self.cdr_plot_release_dd.layout.display = "none"  # shown only if >1 release
        # sized to its column like the grid preview (the wizard's plot_img)
        self.cdr_plot_img = W.Image(
            format="png",
            layout=W.Layout(
                width="100%", min_width="400px", max_width="600px", display="none"
            ),
        )
        self.cdr_plot_box = W.VBox(
            [
                W.HTML("<b>Generate plots</b>"),
                W.HBox([self.cdr_plot_btn, self.cdr_plot_status]),
                W.HBox([self.cdr_plot_type_dd, self.cdr_plot_release_dd]),
                self.cdr_plot_img,
            ],
            # Sits to the RIGHT of the per-mode panels (same side-by-side HBox
            # arrangement as the grid plot) -- the padding separates the columns.
            layout=W.Layout(padding="0 0 0 20px"),
        )
        self.cdr_plot_box.layout.display = "none"  # only for simple/yaml/netcdf

        self._sync_auto_tiling()

    # ---- wiring and layout ---------------------------------------------------
    def wire(self) -> None:
        """Register the card's own observers. The wizard calls this from its
        ``_wire``, before it registers its rebuild on the card's widgets, so the
        auto-tiling sync runs before the rebuild.
        """
        self.cdr_dd.observe(self._on_cdr_spec, names="value")
        self.cdr_mode_dd.observe(self._on_cdr_mode_change, names="value")
        self.cdr_upload.observe(self._on_cdr_upload, names="value")
        self.cdr_clear_btn.on_click(self._on_cdr_clear)
        self.cdr_file_attach_btn.on_click(self._on_cdr_file_attach)
        self.cdr_file_clear_btn.on_click(self._on_cdr_file_clear)
        self.cdr_file_path.observe(self._on_cdr_file_path_submit, names="value")
        self.cdr_file_upload.observe(self._on_cdr_file_upload, names="value")
        self.cdr_plot_btn.on_click(self._on_cdr_plot_generate)
        self.cdr_plot_type_dd.observe(self._on_cdr_plot_option_change, names="value")
        self.cdr_plot_release_dd.observe(self._on_cdr_plot_option_change, names="value")
        self.cdr_simple_replace_btn.on_click(self._on_cdr_simple_replace)
        self.auto_tiling_chk.observe(self._sync_auto_tiling, names="value")

    def rows(self) -> list[Any]:
        """The card body's rows, in order: the run window, the processor layout and
        the CDR section. The wizard adds its error banner above them.
        """
        W = self.W
        # Danger-styled destructive actions.
        self.cdr_clear_btn.button_style = "danger"
        self.cdr_file_clear_btn.button_style = "danger"
        cdr_row = W.HBox(
            [
                W.VBox(
                    [
                        self.cdr_simple_box,
                        self.cdr_yaml_box,
                        self.cdr_netcdf_box,
                        self.cdr_upscaled_box,
                    ]
                ),
                self.cdr_plot_box,
            ]
        )
        cdr_row.add_class("forge-cdr-row")
        return [
            components.subsection(
                W,
                "run.window",
                components.field_grid(
                    W,
                    [
                        components.field_row(W, "start", self.start),
                        components.field_row(W, "end", self.end),
                        components.field_row(W, "model_ref_date", self.model_ref_date),
                    ],
                ),
            ),
            components.subsection(
                W,
                "run.partitioning",
                components.field_grid(
                    W,
                    [
                        components.field_row(W, "npx", self.npx),
                        components.field_row(W, "npy", self.npy),
                        components.field_row(W, "n_cores", self.n_cores),
                        components.field_row(
                            W, "auto_tiling_chk", self.auto_tiling_chk
                        ),
                    ],
                ),
                components.field_row(W, "use_pio_chk", self.use_pio_chk),
            ),
            components.subsection(
                W,
                "run.cdr",
                components.field_grid(
                    W,
                    [
                        components.field_row(W, "cdr_dd", self.cdr_dd),
                        components.field_row(W, "cdr_mode_dd", self.cdr_mode_dd),
                    ],
                ),
                self.cdr_lite_hint,
                # Per-mode panels on the left, plot column on the right --
                # the same side-by-side arrangement as the Grid card, and the
                # plot moves below them in a narrow card (see WIZARD_CSS).
                cdr_row,
            ),
        ]

    def status_chip(self) -> str:
        """The card's chip while it owns no problem: it is always complete, since
        every field has a default.
        """
        return components.chip("● Complete", "ok")

    # ---- what the card says --------------------------------------------------
    @property
    def use_pio(self) -> bool:
        """Whether the run builds against ParallelIO."""
        return self.use_pio_chk.value

    @property
    def has_run_dates(self) -> bool:
        """Whether both run dates are set (a date picker can be cleared)."""
        return self.start.value is not None and self.end.value is not None

    def start_datetime(self) -> datetime:
        """The run's start (see `_run_datetime`)."""
        return self._run_datetime("start", self.start.value)

    def end_datetime(self) -> datetime:
        """The run's end (see `_run_datetime`)."""
        return self._run_datetime("end", self.end.value)

    def model_reference_date(self) -> datetime | None:
        """The model reference date for the resolver, or ``None`` while it is unset
        or 2000-01-01, the resolver's own default.
        """
        if not self.model_ref_date.value:
            return None
        ref = self._run_datetime("model_reference_date", self.model_ref_date.value)
        return ref if ref != datetime(2000, 1, 1) else None

    def partitioning(self) -> dict[str, Any]:
        """The resolver's ``partitioning``: the MPI rank count under auto tiling,
        otherwise the tiling itself.
        """
        return (
            {"auto_tiling": True, "n_cores": int(self.n_cores.value)}
            if self.auto_tiling_chk.value
            else {
                "n_procs_x": int(self.npx.value),
                "n_procs_y": int(self.npy.value),
            }
        )

    def layout_snapshot(self) -> dict[str, Any]:
        """The processor-layout widgets' values, for the wizard's DomainSpec
        snapshot (``_domain_snapshot``).
        """
        return {
            "npx": self.npx.value,
            "npy": self.npy.value,
            "auto_tiling": self.auto_tiling_chk.value,
            "n_cores": self.n_cores.value,
        }

    def domain_dates(self) -> dict[str, str]:
        """The run window as a DomainSpec writes it: ``start_time`` and
        ``end_time``.
        """
        return {
            "start_time": _run_time_iso(self.start_datetime()),
            "end_time": _run_time_iso(self.end_datetime()),
        }

    @property
    def cdr_mode(self) -> str:
        """The CDR mode: "none", "simple", "yaml", "netcdf" or "upscaled"."""
        return self.cdr_mode_dd.value

    def cdr(self) -> dict[str, Any]:
        """The resolver's ``cdr`` (see `_current_cdr_dict`)."""
        return self._current_cdr_dict()

    def cdr_spec_ref(self) -> SpecRef:
        """``composition.cdr`` before its ``modified`` flag: the picked catalog
        CdrSpec, or an empty reference while the CDR is authored here.
        """
        # CDR is independently composable (CdrSpec) and, unlike domain, has no
        # meaningful "custom name" of its own when unpicked -- <custom> just
        # means "hand-authored, not saved as a spec" (name=None).
        return (
            SpecRef(name=self.cdr_dd.value, origin="catalog")
            if self.cdr_dd.value != "<custom>"
            else SpecRef()
        )

    def cdr_deviates_from_spec(self) -> bool:
        """Whether the CDR was edited since its catalog CdrSpec was picked
        (``composition.cdr.modified``); ``False`` while none is picked.
        """
        return (
            self.cdr_dd.value != "<custom>"
            and self._cdr_seed is not None
            and self._cdr_snapshot() != self._cdr_seed
        )

    def window_seconds(self) -> float | None:
        """The run window's length in seconds, or ``None`` while it is unset or
        empty.
        """
        if self.start.value is None or self.end.value is None:
            return None
        seconds = (
            self._run_datetime("end", self.end.value)
            - self._run_datetime("start", self.start.value)
        ).total_seconds()
        return seconds if seconds > 0 else None

    # ---- what the wizard sets ------------------------------------------------
    def set_use_pio(self, value: bool) -> None:
        """Tick or clear ParallelIO (to the Model card's spec default, at
        construction and on a model switch).
        """
        self.use_pio_chk.value = value

    def sync_cdr_lite_hint(self) -> None:
        """Show the CDR panel's hint only while the BGC mode is "cdr_lite", like
        the Model card's carbonate sensitivity group.
        """
        self.cdr_lite_hint.layout.display = "" if self._cdr_lite() else "none"

    def refresh_cdr(self) -> None:
        """Move the simple release's untouched fields to the grid centre and run
        window, then drop the CDR plot if an input it was drawn from changed. The
        wizard calls this first in every rebuild.
        """
        self._reseed_cdr_simple()
        # CDR plot cache: the wizard's _rebuild() is the one hook that reliably
        # fires for every CDR-panel edit, every mode switch, and every
        # grid-geometry edit (see _invalidate_cdr_plot_cache's own docstring for
        # why this spot was chosen over a bespoke .observe on each CDR/grid widget).
        self._invalidate_cdr_plot_cache()

    def load_legacy_cdr(self, cdr_forcing: dict[str, Any]) -> None:
        """Route a legacy ForcingSpec's embedded ``cdr_forcing`` into the CDR box as
        if it had been uploaded as a YAML (see the wizard's ``_on_forcing_spec``).
        """
        self.cdr_mode_dd.value = "yaml"
        self.apply_cdr_mode()
        self._cdr_forcing = cdr_forcing
        self.cdr_status.value = components.status_span(
            "ok",
            f"✓ CDR loaded from legacy ForcingSpec "
            f"embed: {len(cdr_forcing.get('releases', []))} release(s)",
        )

    def populate_window(self, cfg: ForgeBlueprint) -> None:
        """Set the run window from a loaded blueprint, keeping its times of day
        (the wizard holds it suspended).
        """
        self.start.value = cfg.run.start_date.date()
        self.end.value = cfg.run.end_date.date()
        self.model_ref_date.value = cfg.run.model_reference_date.date()
        self._loaded_run_times = {
            "start": cfg.run.start_date,
            "end": cfg.run.end_date,
            "model_reference_date": cfg.run.model_reference_date,
        }

    def populate_layout(self, cfg: ForgeBlueprint) -> None:
        """Set the processor layout and ParallelIO from a loaded blueprint (the
        wizard holds it suspended).
        """
        if cfg.domain.partitioning.n_procs_x is not None:
            self.npx.value = cfg.domain.partitioning.n_procs_x
        if cfg.domain.partitioning.n_procs_y is not None:
            self.npy.value = cfg.domain.partitioning.n_procs_y
        self.use_pio_chk.value = bool(
            (cfg.model_settings.get("cppdefs") or {}).get("use_pio", False)
        )
        self.auto_tiling_chk.value = cfg.domain.partitioning.auto_tiling
        loaded_n_cores = cfg.domain.partitioning.n_cores
        if loaded_n_cores is not None:
            self.n_cores.value = int(loaded_n_cores)
        # auto_tiling_chk.observe fires synchronously above regardless of the
        # wizard's _suspend() (which only sets a flag other handlers check -- see
        # its _Suspender), so the disabled/visible state is already correct;
        # call again explicitly for robustness in case that ever changes.
        self._sync_auto_tiling()

    def populate_cdr(self, cfg: ForgeBlueprint) -> None:
        """Set the CDR mode and its panel from a loaded blueprint, after the grid
        and the BGC mode are set (the wizard holds it suspended).
        """
        # CDR: a top-level, independently composable section (CdrSpec) -- see
        # ``self.cdr_mode_dd``/``apply_cdr_mode``. FileUpload can't be
        # repopulated with the original file, but the parsed dict persists on
        # the instance and re-emits via cdr(), so load stays non-lossy.
        self.cdr_mode_dd.value = cfg.cdr.mode
        self.apply_cdr_mode()
        if cfg.cdr.mode == "simple":
            self._populate_cdr_simple(cfg.cdr.cdr_forcing or {})
        elif cfg.cdr.mode == "yaml":
            self._cdr_forcing = cfg.cdr.cdr_forcing or None
            self._clear_cdr_upload_value()
            self.cdr_status.value = (
                components.status_span(
                    "ok",
                    f"✓ CDR loaded: "
                    f"{len(self._cdr_forcing.get('releases', []))} release(s)",
                )
                if self._cdr_forcing
                else ""
            )
        elif cfg.cdr.mode == "netcdf":
            # Trust the stored hash (never recompute); a missing file is a
            # warning, not a blocker -- Save must still round-trip losslessly.
            cff = cfg.cdr.cdr_forcing_file
            if cff is not None:
                self._cdr_forcing_file = {
                    "location": cff.location,
                    "content_hash": cff.content_hash,
                }
                self.cdr_file_path.value = cff.location
                missing = not Path(cff.location).expanduser().exists()
                self.cdr_file_status.value = _user_file_status_html(
                    self._cdr_forcing_file
                ) + (
                    "<br>"
                    + components.status_span("err", "⚠ file not found at this path")
                    if missing
                    else ""
                )
            else:
                self._cdr_forcing_file = None
                self.cdr_file_path.value = ""
                self.cdr_file_status.value = _CDR_FILE_HINT

    def select_cdr_spec(self, cfg: ForgeBlueprint) -> None:
        """Select a loaded blueprint's CdrSpec, or "<custom>" when the catalog
        lacks it, and seed ``modified`` from the catalog spec (the wizard holds it
        suspended, after `populate_cdr`).
        """
        # CDR spec selection: unlike forcing/output, "<custom>" (no catalog
        # entry) is a normal, common state (mode="none" is the default) --
        # fall back to "<custom>"/no seed rather than the first catalog
        # entry. The mode/fields above already reflect the loaded file's
        # actual CDR state; only the seed is (re)computed here, against the
        # catalog spec (not the just-loaded values) so a file that matches
        # its recorded CdrSpec loads as unmodified and a hand-edited one
        # loads as modified -- mirrors the wizard's forcing_seed block.
        cname = cfg.composition.cdr.name
        _cdr_values = _dd_values(self.cdr_dd)
        if cname is not None and cname in _cdr_values:
            self.cdr_dd.value = cname
            try:
                seed = self.catalog.cdr_data(cname)
                self._cdr_seed = {k: v for k, v in seed.items() if k != "description"}
            except Exception:
                self._cdr_seed = None
        else:
            self.cdr_dd.value = "<custom>"
            self._cdr_seed = None

    def populate_from_domain(self, data: dict[str, Any]) -> None:
        """Set the processor layout and the run dates a picked DomainSpec carries;
        what it lacks keeps its value (the wizard holds it suspended).
        """
        part = data.get("partitioning", {}) or {}
        saved_npx = part.get("n_procs_x")
        if saved_npx is not None:
            self.npx.value = int(saved_npx)
        saved_npy = part.get("n_procs_y")
        if saved_npy is not None:
            self.npy.value = int(saved_npy)
        self.auto_tiling_chk.value = bool(part.get("auto_tiling", False))
        saved_n_cores = part.get("n_cores")
        if saved_n_cores is not None:
            self.n_cores.value = int(saved_n_cores)
        self._sync_auto_tiling()
        for key, run_key, picker in (
            ("start_time", "start", self.start),
            ("end_time", "end", self.end),
        ):
            if data.get(key):
                when = datetime.fromisoformat(str(data[key]))
                picker.value = when.date()
                self._loaded_run_times[run_key] = when

    def reselect_cdr(self, name: str | None) -> None:
        """Re-read the catalog's CdrSpecs into ``cdr_dd`` after "Save as new spec"
        and select *name*, seeding ``modified`` from now; ``None`` keeps the
        current selection (the wizard holds it suspended).
        """
        old_value = self.cdr_dd.value
        self.cdr_dd.options = _dd_options(
            self.catalog, list(self.catalog.cdr_names), "cdr", prefix=["<custom>"]
        )
        self.cdr_dd.value = name if name is not None else old_value
        if name is not None:
            self._cdr_seed = self._cdr_snapshot()

    # ---- the run window and processor layout ---------------------------------
    def _run_datetime(self, key: str, day: date) -> datetime:
        """The datetime for run date picker ``key`` ("start", "end",
        "model_reference_date") showing ``day``: the time of day last loaded into
        it while it still shows the loaded date (the pickers hold whole days, and
        a nested run starts at 06:00), otherwise midnight.
        """
        loaded = self._loaded_run_times.get(key)
        if loaded is not None and loaded.date() == day:
            return loaded
        return datetime.combine(day, datetime.min.time())

    def _sync_auto_tiling(self, _change: Any = None) -> None:
        # Auto tiling picks n_procs_x/y at runtime from the land mask, so those
        # boxes become meaningless (disabled) and n_cores takes over; it also
        # requires PIO, so use_pio is forced on and locked while it's active.
        on = self.auto_tiling_chk.value
        if on and _change is not None and _change.get("old") is False:
            # Real off->on toggle (user click, or a load path flipping the
            # checkbox): seed n_cores from the explicit grid already entered,
            # so the user doesn't multiply by hand. Load paths restore a saved
            # n_cores AFTER setting the checkbox, so this seed never clobbers
            # it; direct _sync_auto_tiling() calls (_change=None) never seed.
            self.n_cores.value = int(self.npx.value) * int(self.npy.value)
        self.npx.disabled = on
        self.npy.disabled = on
        self.n_cores.layout.display = "" if on else "none"
        if on:
            self.use_pio_chk.value = True
        self.use_pio_chk.disabled = on

    # ---- CDR ---------------------------------------------------------------
    def _on_cdr_spec(self, _change: Any) -> None:
        """Selecting a CdrSpec loads its mode + fields into the box."""
        if self._is_suspended():
            return
        name = self.cdr_dd.value
        if name == "<custom>":
            self._cdr_seed = None
            self._rebuild()
            return
        data = self.catalog.cdr_data(name)
        # Set the mode under _suspend() (its observer would run apply_cdr_mode
        # itself) so the single explicit call below is the only apply -- and its
        # "simple" seeding runs before _populate_cdr_simple overwrites it.
        with self._suspend():
            self.cdr_mode_dd.value = data.get("mode", "none")
        self.apply_cdr_mode()
        mode = data.get("mode", "none")
        if mode == "simple":
            self._populate_cdr_simple(data.get("cdr_forcing") or {})
        elif mode == "yaml":
            cdr = data.get("cdr_forcing")
            self._cdr_forcing = cdr
            self.cdr_status.value = (
                components.status_span(
                    "ok",
                    f"✓ CDR loaded from spec '{name}': "
                    f"{len(cdr.get('releases', []))} release(s)",
                )
                if cdr
                else ""
            )
        elif mode == "netcdf":
            cff = data.get("cdr_forcing_file")
            self._cdr_forcing_file = cff
            if cff:
                self.cdr_file_path.value = cff.get("location", "")
                self.cdr_file_status.value = _user_file_status_html(cff)
        self._cdr_seed = self._cdr_snapshot()
        self._rebuild()

    def _cdr_snapshot(self) -> dict[str, Any]:
        """The CDR-defining state at the moment of a catalog CdrSpec pick.
        Compared against the current state in `_rebuild()` to detect an edit made
        after selection (`composition.cdr.modified`) -- mirrors
        the wizard's `_domain_snapshot`/`_forcing_seed`.
        """
        return self._current_cdr_dict()

    def _on_cdr_mode_change(self, _change: Any) -> None:
        if self._is_suspended():
            return
        self.apply_cdr_mode()

    def apply_cdr_mode(self) -> None:
        """Show/hide the per-mode sub-panel for the current ``cdr_mode_dd`` value,
        seed defaults on first activation of "simple", clear stale state left
        over from whichever mode is being left, invalidate the plot cache (a
        mode switch always changes what would be plotted), and rebuild.

        Called both from ``cdr_mode_dd``'s own ``.observe`` (via
        ``_on_cdr_mode_change``) and directly wherever ``cdr_mode_dd.value`` is
        set programmatically under the wizard's suspend (e.g. `populate_cdr`,
        `_on_cdr_spec`) -- suspended observers are no-ops, so those callers must
        invoke this themselves. The wizard calls it once at the end of its
        construction.
        """
        old_mode = self._cdr_mode_active
        mode = self.cdr_mode_dd.value
        if old_mode == "yaml" and mode != "yaml":
            self._cdr_forcing = None
            self._clear_cdr_upload_value()
            self.cdr_status.value = ""
        if old_mode == "netcdf" and mode != "netcdf":
            self._cdr_forcing_file = None
            self.cdr_file_path.value = ""
            self.cdr_file_upload.value = ()
            self.cdr_file_status.value = ""
        if old_mode == "simple" and mode != "simple":
            self._keep_cdr_simple(None)
            self._cdr_simple_seeded = {}
        self._cdr_mode_active = mode

        self.cdr_simple_box.layout.display = "" if mode == "simple" else "none"
        self.cdr_yaml_box.layout.display = "" if mode == "yaml" else "none"
        self.cdr_netcdf_box.layout.display = "" if mode == "netcdf" else "none"
        self.cdr_upscaled_box.layout.display = "" if mode == "upscaled" else "none"
        self.cdr_plot_box.layout.display = (
            "" if mode in ("simple", "yaml", "netcdf") else "none"
        )
        if (
            mode == "netcdf"
            and self._cdr_forcing_file is None
            and not self.cdr_file_status.value
        ):
            # CdrSpec rejects mode="netcdf" without cdr_forcing_file; say what to
            # do here rather than leave only the pydantic dump in the preview.
            self.cdr_file_status.value = _CDR_FILE_HINT

        if mode == "simple" and old_mode != "simple":
            # Seed from the grid center / run window; a field the user leaves
            # alone keeps following them (see _reseed_cdr_simple).
            lat, lon = self._grid_center()
            with self._suspend():
                self.cdr_simple_lat.value = lat
                self.cdr_simple_lon.value = lon
                if self.start.value is not None:
                    self.cdr_simple_start.value = self.start.value
                if self.end.value is not None:
                    self.cdr_simple_end.value = self.end.value
            self._cdr_simple_seeded = {
                key: widget.value
                for key, widget in self._cdr_simple_seed_targets().items()
            }

        self._invalidate_cdr_plot_cache()
        self._rebuild()

    def _resolved_cdr_dict(self, current: dict[str, Any]) -> dict[str, Any]:
        """``current`` (a :meth:`_current_cdr_dict`) with ``cdr_forcing`` as the
        resolver will hand it to roms-tools: under ``bgc_mode: cdr_lite``, with
        the tracer set it defaults (``with_cdr_lite_tracer_set`` decides which
        modes that is). The plot builds from this, so what it shows is what
        will be generated.
        """
        if not self._cdr_lite():
            return current
        spec = with_cdr_lite_tracer_set(CdrSpec(**current))
        return {**current, "cdr_forcing": spec.cdr_forcing}

    def _cdr_plot_inputs_fingerprint(self) -> tuple[str, tuple]:
        """A cheap, comparable snapshot of everything the CDR plot depends on:
        the CDR selection it builds from (mode + compiled/attached config, as
        resolved for the BGC mode) and the grid geometry the ``rt.CDRForcing``
        would be built against.
        """
        selection = self._current_cdr_dict()
        try:
            selection = self._resolved_cdr_dict(selection)
        except ValueError:
            pass  # incomplete or invalid selection: building the plot reports it
        return (
            repr(selection),
            self._grid_fingerprint(),
        )

    def _invalidate_cdr_plot_cache(self) -> None:
        """Drop the built ``rt.CDRForcing``/grid and every rendered plot PNG --
        but only when a plot-affecting input actually changed.

        Hooked into the wizard's ``_rebuild()`` itself (through `refresh_cdr`)
        rather than onto each individual CDR/grid widget -- the least-invasive
        idiom for "any of several unrelated inputs invalidates this": ``_rebuild()`` already
        fires on every CDR-panel edit, every mode switch (via ``apply_cdr_mode``),
        and every grid-geometry edit (``watched``/``_on_grid_kwarg_change`` both
        end in a rebuild). Because ``_rebuild()`` ALSO fires for edits that can't
        affect the plot (e.g. the description field), the fingerprint check below
        skips the clear when nothing plot-affecting changed -- an already-rendered
        plot survives unrelated edits instead of silently blanking.
        """
        fingerprint = self._cdr_plot_inputs_fingerprint()
        if fingerprint == getattr(self, "_cdr_plot_fingerprint", None):
            return
        self._cdr_plot_fingerprint = fingerprint
        self._cdr_plot_object = None
        self._cdr_plot_grid = None
        self._cdr_plot_cache = {}
        self.cdr_plot_img.value = b""
        self.cdr_plot_img.layout.display = "none"
        self.cdr_plot_status.value = ""
        self.cdr_plot_release_dd.options = []
        self.cdr_plot_release_dd.layout.display = "none"

    def _current_cdr_dict(self) -> dict[str, Any]:
        """The current CDR selection as a ``cdr=`` kwarg dict for
        ``build_forge_blueprint`` (shape matches :class:`CdrSpec`, and --
        ``cdr_forcing``/``cdr_forcing_file`` always both present, unused ones
        ``None`` -- matches ``catalog.cdr_data()`` too, so ``_cdr_snapshot()``
        can compare directly against a freshly-picked catalog entry). Used by
        `cdr` (the wizard's ``_gather()``), ``_cdr_snapshot()``, and the plot widget (simple/yaml
        share this dict's ``cdr_forcing`` as the ``rt.CDRForcing`` kwargs).
        """
        mode = self.cdr_mode_dd.value
        cdr_forcing = None
        cdr_forcing_file = None
        if mode == "simple":
            cdr_forcing = self._simple_cdr_forcing_dict()
        elif mode == "yaml":
            cdr_forcing = self._cdr_forcing
        elif mode == "netcdf":
            cdr_forcing_file = self._cdr_forcing_file
        return {
            "mode": mode,
            "cdr_forcing": cdr_forcing,
            "cdr_forcing_file": cdr_forcing_file,
        }

    def _simple_cdr_forcing_dict(self) -> dict[str, Any]:
        """The "simple" mode's ``cdr_forcing``: a loaded one the panel cannot
        show, unchanged (see ``_populate_cdr_simple``), else the panel compiled
        (see ``_compile_simple_cdr``).
        """
        if self._cdr_simple_loaded is not None:
            return copy.deepcopy(self._cdr_simple_loaded)
        return self._compile_simple_cdr()

    def _compile_simple_cdr(self) -> dict[str, Any]:
        """Compile the "simple" panel into a roms-tools ``CDRForcing`` kwargs
        dict: a single tracer-perturbation release, a flat two-point ALK pulse
        (the same static flux at both the run's start and end) -- see the CDR
        Overhaul plan's "simple mode compile" note. Values are ISO-8601 strings
        (not ``datetime`` objects) so the dict stays YAML/JSON-serializable on
        the blueprint; both ``rt.CDRForcing`` (pydantic) and ``CdrSpec`` parse
        an ISO string back into a datetime on their own. A date the run shares
        takes the run's time of day (see ``_run_datetime``).
        """
        start = self.cdr_simple_start.value or self.start.value
        end = self.cdr_simple_end.value or self.end.value
        start_iso = (
            self._run_datetime("start", start).isoformat()
            if start is not None
            else None
        )
        end_iso = (
            self._run_datetime("end", end).isoformat() if end is not None else None
        )
        flux = float(self.cdr_simple_flux.value)
        return {
            "start_time": start_iso,
            "end_time": end_iso,
            "releases": [
                {
                    "name": self.cdr_simple_name.value,
                    "lat": float(self.cdr_simple_lat.value),
                    "lon": float(self.cdr_simple_lon.value),
                    "depth": float(self.cdr_simple_depth.value),
                    "hsc": float(self.cdr_simple_hsc.value),
                    "vsc": float(self.cdr_simple_vsc.value),
                    "times": [start_iso, end_iso],
                    "tracer_fluxes": {"ALK": [flux, flux]},
                    "release_type": "tracer_perturbation",
                }
            ],
        }

    def _populate_cdr_simple(self, cdr_forcing: dict[str, Any]) -> None:
        """Reverse-derive the "simple" panel's widget values from a compiled CDR
        forcing dict (the shape ``_simple_cdr_forcing_dict`` produces) -- the
        load-back half of "simple" mode's round trip. A dict the fields would
        not compile back to unchanged is kept as loaded instead (see
        ``_keep_cdr_simple``), with the fields showing its first release.
        """
        releases = cdr_forcing.get("releases") or []
        release = releases[0] if releases else {}
        with self._suspend():
            self.cdr_simple_name.value = release.get("name", "my_cdr")
            self.cdr_simple_lat.value = float(release.get("lat", 0.0))
            self.cdr_simple_lon.value = float(release.get("lon", 0.0))
            self.cdr_simple_depth.value = float(release.get("depth", 1.0))
            self.cdr_simple_hsc.value = float(release.get("hsc", 10.0))
            self.cdr_simple_vsc.value = float(release.get("vsc", 10.0))
            fluxes = (release.get("tracer_fluxes") or {}).get("ALK")
            flux = fluxes[0] if isinstance(fluxes, list) else fluxes
            self.cdr_simple_flux.value = float(flux) if flux is not None else 2 * 10**6
            times = release.get("times") or []
            if times:
                self.cdr_simple_start.value = datetime.fromisoformat(
                    str(times[0])
                ).date()
                self.cdr_simple_end.value = datetime.fromisoformat(
                    str(times[-1])
                ).date()
        # A loaded release is the user's, not a seed to move with the grid.
        self._cdr_simple_seeded = {}

        # The fields hold one release at a constant ALK flux; anything else
        # (a second release, a ramp, DIC, a time of day) is kept as loaded. Both
        # sides are compared as resolved: under cdr_lite a saved release carries
        # the tracer_set the resolver gave it.
        def resolved(forcing: dict[str, Any]) -> dict[str, Any]:
            selection = {"mode": "simple", "cdr_forcing": forcing}
            try:
                forcing = self._resolved_cdr_dict(
                    {**selection, "cdr_forcing_file": None}
                )["cdr_forcing"]
            except ValueError:  # e.g. a volume release under cdr_lite
                pass
            return _cdr_times_as_iso(forcing)

        shown = resolved(self._compile_simple_cdr())
        self._keep_cdr_simple(None if resolved(cdr_forcing) == shown else cdr_forcing)

    def _keep_cdr_simple(self, cdr_forcing: dict[str, Any] | None) -> None:
        """Keep ``cdr_forcing`` as the "simple" mode's forcing, unchanged, and
        lock the panel's fields with a note saying why; ``None`` unlocks them.
        """
        self._cdr_simple_loaded = copy.deepcopy(cdr_forcing)
        kept = cdr_forcing is not None
        for widget in self.cdr_simple_fields:
            widget.disabled = kept
        self.cdr_simple_replace_btn.layout.display = "" if kept else "none"
        self.cdr_simple_note.value = (
            components.banner(
                "warn",
                "<b>Kept as loaded.</b> This CDR forcing has "
                f"{html.escape(_cdr_simple_kept_reason(cdr_forcing))}, which the "
                "fields below cannot show; it is saved unchanged. "
                "<b>Edit as one release</b> replaces it with its first release at "
                "a constant ALK flux.",
            )
            if cdr_forcing is not None
            else ""
        )

    def _on_cdr_simple_replace(self, _btn: Any) -> None:
        """Handle "Edit as one release": drop the kept CDR forcing for what the
        fields show.
        """
        self._keep_cdr_simple(None)
        self._rebuild()

    def _cdr_simple_seed_targets(self) -> dict[str, Any]:
        """The simple CDR fields that entering "simple" mode seeds, by key."""
        return {
            "lat": self.cdr_simple_lat,
            "lon": self.cdr_simple_lon,
            "start": self.cdr_simple_start,
            "end": self.cdr_simple_end,
        }

    def _reseed_cdr_simple(self) -> None:
        """Move each simple CDR field that still shows its seed to the current
        grid center or run window, so moving either does not leave the release
        behind. A field the user edited, a kept or loaded release, and another
        CDR mode are left alone.
        """
        if self.cdr_mode_dd.value != "simple" or self._cdr_simple_loaded is not None:
            return
        lat, lon = self._grid_center()
        now = {
            "lat": lat,
            "lon": lon,
            "start": self.start.value,
            "end": self.end.value,
        }
        with self._suspend():
            for key, widget in self._cdr_simple_seed_targets().items():
                if (
                    key in self._cdr_simple_seeded
                    and widget.value == self._cdr_simple_seeded[key]
                    and now[key] is not None
                    and now[key] != widget.value
                ):
                    widget.value = now[key]
                    self._cdr_simple_seeded[key] = now[key]

    # ---- CDR forcing upload ----------------------------------------------------
    def _on_cdr_upload(self, change: Any) -> None:
        items = change["new"]
        if not items:
            return
        item = (
            items[0] if isinstance(items, (list, tuple)) else next(iter(items.values()))
        )
        content = bytes(item["content"])
        try:
            parsed = read_cdr_forcing_yaml(content.decode("utf-8"))
            # Eager-validate against the real roms-tools class -- the resolver never
            # constructs a CDRForcing, so a structurally-valid-but-semantically-broken
            # upload (empty releases, bad time ordering, unknown tracer, an
            # incompatible roms-tools version) would otherwise pass silently through
            # _rebuild() and only fail much later, during blueprint processing.
            import roms_tools as rt

            cdr = rt.CDRForcing(**parsed)
        except Exception as exc:
            self._cdr_forcing = None
            self.cdr_status.value = components.status_span(
                "err", f"CDR invalid: {type(exc).__name__}: {exc}"
            )
            self._rebuild()
            return
        self._cdr_forcing = parsed
        cleared_note = ""
        if self._cdr_forcing_file is not None:
            # Mutually exclusive with an attached CDR file (Forcing's own
            # validator forbids both) -- the upload wins, matching the
            # attach-clears-upload direction in _attach_cdr_file_from_path.
            self._cdr_forcing_file = None
            self.cdr_file_path.value = ""
            self.cdr_file_upload.value = ()
            self.cdr_file_status.value = ""
            cleared_note = "<br>" + components.status_span(
                "note", "(cleared the attached CDR file -- mutually exclusive)"
            )
        self.cdr_status.value = (
            components.status_span("ok", f"✓ CDR: {len(cdr.releases)} release(s)")
            + f"{cleared_note}"
        )
        self._rebuild()

    def _clear_cdr_upload_value(self) -> None:
        """Reset ``cdr_upload.value`` back to its empty state.

        ``FileUpload.value`` is a read-only trait (traitlets raises ``TraitError`` on
        a plain ``widget.value = ...`` assignment) -- ``set_trait`` is the sanctioned
        way to write a read-only trait from outside the widget's own JS-driven update
        path. The empty container's type also differs by ipywidgets major version:
        a dict in 7.x, a tuple of dicts in 8.x (mirroring the tuple/dict duality
        `_on_cdr_upload` already handles on read) -- match whatever the widget's
        current value already is, so this works under either version.
        """
        empty: tuple[Any, ...] | dict[str, Any] = (
            () if isinstance(self.cdr_upload.value, (list, tuple)) else {}
        )
        self.cdr_upload.set_trait("value", empty)

    def _on_cdr_clear(self, _: Any) -> None:
        self._cdr_forcing = None
        self._clear_cdr_upload_value()
        self.cdr_status.value = ""
        self._rebuild()

    # ---- CDR forcing: user-provided pre-made netCDF ---------------------------
    def _attach_cdr_file_from_path(self, path_str: str) -> None:
        self.cdr_file_status.value = "<i>attaching…</i>"
        try:
            path = Path(path_str).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"CDR-forcing file not found: {path}")
            content_hash = hash_netcdf_contents(path)
        except Exception as exc:
            self.cdr_file_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        # Light validation only (never blocks the attach -- the executor is the
        # hard gate): warn if the file doesn't even look like CDR forcing.
        warning = ""
        try:
            import xarray as xr

            with xr.open_dataset(path) as ds:
                if "ncdr" not in ds.dims:
                    warning = "<br>" + components.status_span(
                        "warn",
                        "⚠ no 'ncdr' dimension "
                        "found -- is this really a CDR-forcing file?",
                    )
                if (
                    self._cdr_lite()
                    and "tracer_name" in ds.coords
                    and not any(
                        role is not None and role[0] == "cdr_lite"
                        for role in map(
                            cdr_tracer_role, map(str, ds["tracer_name"].values)
                        )
                    )
                ):
                    warning += "<br>" + components.status_span(
                        "warn",
                        "⚠ BGC mode is cdr_lite "
                        "but this file's tracer_name axis has no CDR_* tracers -- "
                        "its releases need tracer_set cdr_lite.",
                    )
        except Exception:
            pass
        self._cdr_forcing_file = {"location": str(path), "content_hash": content_hash}
        cleared_note = ""
        if self._cdr_forcing is not None:
            # Mutually exclusive with an uploaded CDR YAML (Forcing's own
            # validator forbids both) -- the file attach wins here.
            self._cdr_forcing = None
            self._clear_cdr_upload_value()
            self.cdr_status.value = ""
            cleared_note = "<br>" + components.status_span(
                "note", "(cleared the uploaded CDR YAML -- mutually exclusive)"
            )
        self.cdr_file_status.value = (
            _user_file_status_html(self._cdr_forcing_file) + warning + cleared_note
        )
        self._rebuild()

    def _on_cdr_file_attach(self, _btn: Any) -> None:
        path_str = self.cdr_file_path.value.strip()
        if not path_str:
            self.cdr_file_status.value = components.status_span(
                "err", "Enter a path first."
            )
            return
        # Explicit click always re-hashes -- no dedupe here.
        self._attach_cdr_file_from_path(path_str)

    def _maybe_attach_cdr_file(self) -> None:
        """CDR-file twin of the wizard's `_maybe_attach_grid_file`: attach the typed path on
        Enter/focus-out unless blank or already attached. CDR mode "netcdf"
        requires cdr_forcing_file (CdrSpec), so a typed-but-unattached path
        used to leave the blueprint invalid with no hint as to why.
        """
        path_str = self.cdr_file_path.value.strip()
        if not path_str or _same_attached_file(self._cdr_forcing_file, path_str):
            return
        self._attach_cdr_file_from_path(path_str)

    def _on_cdr_file_path_submit(self, _change: Any) -> None:
        if self._is_suspended():
            return
        self._maybe_attach_cdr_file()

    def _on_cdr_file_upload(self, change: Any) -> None:
        items = change["new"]
        if not items:
            return
        item = (
            items[0] if isinstance(items, (list, tuple)) else next(iter(items.values()))
        )
        dest = _stage_uploaded_netcdf(item["name"], bytes(item["content"]))
        self.cdr_file_path.value = str(dest)  # fires _on_cdr_file_path_submit
        self._maybe_attach_cdr_file()  # same-name re-upload: value unchanged

    def _on_cdr_file_clear(self, _btn: Any) -> None:
        self._cdr_forcing_file = None
        self.cdr_file_path.value = ""
        self.cdr_file_upload.value = ()
        # Still in "netcdf" mode with nothing attached -> invalid until re-attached.
        self.cdr_file_status.value = _CDR_FILE_HINT
        self._rebuild()

    # ---- CDR plotting (WP6) ----------------------------------------------------
    # The built rt.CDRForcing/grid (self._cdr_plot_object/_cdr_plot_grid) and every
    # rendered PNG (self._cdr_plot_cache, keyed by (plot_type, release_name)) are
    # cached on the instance -- invalidated in one place, refresh_cdr() (see its own
    # comment and _invalidate_cdr_plot_cache's docstring). Switching plot type or
    # release therefore re-renders (or reuses a cached PNG) WITHOUT rebuilding --
    # _on_cdr_plot_option_change/_render_cdr_plot below never call _rebuild().
    def _build_cdr_forcing_for_plot(self) -> Any:
        """Build (or return the cached) roms-tools ``CDRForcing`` for the
        current CDR mode. Raises if the current mode has nothing plottable yet
        (upscaled/none never reach here -- the plot panel is hidden for them)
        or the current selection is incomplete (e.g. yaml mode with nothing
        uploaded).
        """
        if self._cdr_plot_object is not None:
            return self._cdr_plot_object
        import roms_tools as rt

        mode = self.cdr_mode_dd.value
        current = self._current_cdr_dict()
        grid = self._build_grid()
        if mode in ("simple", "yaml"):
            if not current["cdr_forcing"]:
                raise ValueError("No CDR forcing uploaded yet.")
            forcing = self._resolved_cdr_dict(current)["cdr_forcing"]
            cdr = rt.CDRForcing(grid=grid, **forcing)
        elif mode == "netcdf":
            cff = current["cdr_forcing_file"]
            if not cff:
                raise ValueError("No CDR-forcing netCDF attached yet.")
            cdr = _cdr_forcing_from_netcdf(cff["location"], grid)
        else:
            raise ValueError(f"CDR plotting is not available in mode {mode!r}.")
        self._cdr_plot_object = cdr
        self._cdr_plot_grid = grid
        return cdr

    def _sync_cdr_plot_release_options(self) -> None:
        """After a (re)build, refresh the release dropdown (shown only when the
        forcing has more than one release) and hide the tracer flux option for a
        volume-type forcing (``plot_tracer_flux`` raises for those -- ALK-only
        per the CDR Overhaul design brief). A forcing with generated tracers
        (cdr_lite/passive) has no ``ALK`` tracer, so its option plots the
        selected release's own tracer instead.
        """
        cdr = self._cdr_plot_object
        names = [r.name for r in cdr.releases]
        is_volume = cdr.release_type == "volume"
        flux_label = "Tracer flux" if cdr.release_tracers else "Tracer flux (ALK)"
        options = [
            ("Release locations", "locations"),
            ("Distribution", "distribution"),
            *([] if is_volume else [(flux_label, "tracer_flux")]),
        ]
        with self._suspend():
            self.cdr_plot_release_dd.options = names
            if names:
                self.cdr_plot_release_dd.value = names[0]
            self.cdr_plot_release_dd.layout.display = "" if len(names) > 1 else "none"
            old_type = self.cdr_plot_type_dd.value
            self.cdr_plot_type_dd.options = options
            self.cdr_plot_type_dd.value = (
                old_type if old_type in dict(options).values() else options[0][1]
            )
        if is_volume:
            self.cdr_plot_status.value = components.status_span(
                "warn",
                "⚠ volume-type CDR forcing -- "
                "tracer flux plotting (ALK-only) isn't available.",
            )

    def _render_cdr_plot(self) -> None:
        """Render the current plot-type/release selection, from the cached
        ``rt.CDRForcing`` and a cached PNG when one already exists for this
        exact (plot_type, release) pair.
        """
        cdr = self._cdr_plot_object
        if cdr is None:
            return
        plot_type = self.cdr_plot_type_dd.value
        release_name = (
            self.cdr_plot_release_dd.value if self.cdr_plot_release_dd.options else None
        )
        key = (
            plot_type,
            release_name if plot_type in ("distribution", "tracer_flux") else None,
        )
        cached = self._cdr_plot_cache.get(key)
        if cached is None:
            import io

            import matplotlib.pyplot as plt

            plt.ioff()
            # None of plot_locations/plot_distribution/plot_tracer_flux call
            # plt.show() as of this roms-tools version, but neutralize it
            # anyway (cheap insurance against a future version that does --
            # see the wizard's _on_nest_plot for why this matters under the inline
            # backend).
            _real_show, plt.show = plt.show, lambda *a, **k: None
            try:
                if plot_type == "locations":
                    cdr.plot_locations()
                elif plot_type == "distribution":
                    if release_name is None:
                        raise ValueError("No release to plot a distribution for.")
                    cdr.plot_distribution(release_name)
                elif plot_type == "tracer_flux":
                    # A generated-tracer release plots its own tracer; a MARBL
                    # one (alone or beside generated ones) plots ALK.
                    tracer = (cdr.release_tracers.get(release_name) or ("ALK",))[0]
                    only = (
                        {}
                        if release_name is None
                        else {"release_names": [release_name]}
                    )
                    cdr.plot_tracer_flux(tracer, **only)
                else:
                    raise ValueError(f"Unknown CDR plot type {plot_type!r}.")
                fig = plt.gcf()
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=80, bbox_inches="tight")
                plt.close(fig)
            finally:
                plt.show = _real_show
                plt.ion()
            cached = buf.getvalue()
            self._cdr_plot_cache[key] = cached
        self.cdr_plot_img.value = cached
        self.cdr_plot_img.layout.display = ""

    def _on_cdr_plot_generate(self, _btn: Any) -> None:
        self.cdr_plot_status.value = "<i>building…</i>"
        try:
            self._build_cdr_forcing_for_plot()
            # Clear the building marker BEFORE the sync step: _sync may set a
            # volume-type ⚠ that must survive, and the ✓ below only fills an
            # empty status (leaving the marker in place would stick forever).
            self.cdr_plot_status.value = ""
            self._sync_cdr_plot_release_options()
            self._render_cdr_plot()
            if not self.cdr_plot_status.value:
                self.cdr_plot_status.value = components.status_span("ok", "✓")
        except Exception as exc:
            self._cdr_plot_object = None
            self._cdr_plot_grid = None
            self.cdr_plot_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )

    def _on_cdr_plot_option_change(self, _change: Any) -> None:
        if self._is_suspended():
            return
        if self._cdr_plot_object is None:
            return
        self._render_cdr_plot()
