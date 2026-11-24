"""The Forge blueprint wizard's Model card.

`ModelCard` owns the card's widgets: the ModelSpec picker with its "Show archived"
filter, the BGC mode, the ucla-roms and MARBL refs, and the carbonate sensitivities
group that BGC mode "cdr_lite" shows. It owns their observers, the card's share of
the resolver kwargs (`ModelCard.gather`) and of loading a blueprint back
(`ModelCard.select_model`, `ModelCard.populate`), and its status chip.

`ForgeBlueprintWizard` places the card's rows, reacts to a change by observing its
widgets, and otherwise reads and sets the card only through its methods and
properties. The card reaches back only through the callables it is given.
"""

from __future__ import annotations

import html
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, get_args

from cstar.applications.forge.blueprint import CarbonateSensitivitySpec
from cstar.applications.forge.namelist_model import BgcMode, bgc_mode_from_cppdefs
from cstar.applications.forge.resolve import (
    find_carbonate_sensitivity_files,
    load_model_spec_data,
)
from cstar.base.digest import hash_netcdf_contents
from cstar.base.utils import NetCDFFormat, netcdf_format
from cstar.wizard.fields import (
    _CARBONATE_DIR_HINT,
    _carbonate_status_html,
    _dd_options,
    _dd_values,
)
from cstar.wizard.ui import components
from cstar.wizard.ui.labels import label_for

if TYPE_CHECKING:
    from collections.abc import Callable, Collection
    from contextlib import AbstractContextManager

    from cstar.applications.forge.blueprint import ForgeBlueprint

# Preselected in the Model dropdown when present in the catalog (falls back to
# the first catalog model otherwise): it's the newest tagged-release ModelSpec
# (ucla-roms 0.16.0). 'pio-dev' (ucla-roms branch `main`) remains available in the
# catalog but is no longer the default.
_DEFAULT_MODEL = "roms-marbl-0.16-default"


def _pinned_roms_ref(cfg: ForgeBlueprint) -> str:
    """The ucla-roms ref a blueprint pins (``code.roms``'s commit, else its branch),
    or "" when it pins neither.
    """
    return cfg.code.roms.commit or cfg.code.roms.branch or ""


class ModelCard:
    """The wizard's Model card: which ModelSpec, BGC mode and code refs a blueprint
    builds with and, for BGC mode "cdr_lite", where its carbonate sensitivities
    come from.

    The callables are the wizard's: ``rebuild`` re-resolves the blueprint,
    ``suspend`` returns the wizard's suspend context manager, ``is_suspended``
    says whether one is open, and ``use_pio`` reads the Run card's ParallelIO
    checkbox. Construction builds the widgets and sets them from the selected
    ModelSpec but registers no observers: the wizard calls `wire` once it is
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
        use_pio: Callable[[], bool],
    ) -> None:
        self.W = W
        self.catalog = catalog
        self._rebuild = rebuild
        self._suspend = suspend
        self._is_suspended = is_suspended
        self._use_pio = use_pio

        # --- spec selectors ---
        self.show_archived_models = W.Checkbox(
            value=False,
            description="Show archived",
            indent=False,
            tooltip=(
                "Also list model specs marked archived (older ucla-roms releases). "
                "They stay loadable; this only controls the dropdown."
            ),
            layout=W.Layout(width="auto"),
        )
        self.model_dd = W.Dropdown(
            options=self._model_dd_options(),
            description="Model:",
            style={"description_width": "110px"},
        )
        models = _dd_values(self.model_dd)
        self.model_dd.value = (
            _DEFAULT_MODEL
            if _DEFAULT_MODEL in models
            else (models[0] if models else None)
        )
        self.bgc_dd = W.Dropdown(
            options=list(get_args(BgcMode)),
            value="marbl",
            description="BGC:",
            style={"description_width": "110px"},
            tooltip=(
                "Biogeochemistry mode. 'marbl' builds ROMS-MARBL and includes the "
                "MARBL codebase; 'none' builds physics-only (no MARBL, no BGC forcing). "
                "'cdr_lite' builds ucla-roms' CDR-lite tracers without MARBL: it needs "
                "a CDR forcing with cdr_lite releases and carbonate sensitivity files, "
                "by default supplied at run time by the carbonate-sensitivity-from "
                "directive."
            ),
        )
        self.roms_ref = W.Text(
            value="",  # populated from the selected Model's pinned default below
            description="ucla-roms ref:",
            style={"description_width": "120px"},
            layout=W.Layout(width="260px"),
            placeholder="commit / tag / branch",
            tooltip="ucla-roms checkout target (commit hash, tag, or branch). "
            "Prefilled from the selected Model's pinned default; edit to override.",
        )
        self.marbl_ref = W.Text(
            value="",  # populated from the selected Model's pinned default below
            description="MARBL ref:",
            style={"description_width": "120px"},
            layout=W.Layout(width="260px"),
            placeholder="commit / tag / branch",
            tooltip="MARBL checkout target (commit hash, tag, or branch). "
            "Prefilled from the selected Model's pinned default; edit to override. "
            'Only used when BGC mode is "marbl".',
        )

        # --- carbonate sensitivities (bgc_mode "cdr_lite" only) -------------------
        # A cdr_lite build reads the carbonate sensitivities (ddic_dco2/ddic_dalk)
        # as surface forcing from ROMS ``_cdrgas`` files. The default leaves them
        # off the blueprint: the carbonate-sensitivity-from workplan directive
        # supplies them at run time ("directive"). "path" attaches a directory of
        # them now. What is stored is the resolved ``{"files": [{location,
        # content_hash}]}`` (never the directory): gather() and the wizard's
        # rebuild then neither rescan nor rehash on every edit, and a loaded
        # blueprint round-trips exactly -- the trust-the-stored-hash idiom of the
        # CDR netCDF attach flow.
        self._carbonate_sensitivity: dict[str, Any] | None = None
        self.cs_source_dd = W.Dropdown(
            options=[
                ("Supplied by a workplan directive at run time", "directive"),
                ("Directory of ROMS _cdrgas files", "path"),
            ],
            value="directive",
            description="Source:",
            style={"description_width": "110px"},
            layout=W.Layout(width="360px"),
            tooltip=(
                "Where the carbonate sensitivity files come from. By default the "
                "carbonate-sensitivity-from workplan directive supplies them from an "
                "earlier UCLA-ROMS step at run time; attach a directory to put them "
                "on the blueprint now."
            ),
        )
        self.cs_path = W.Text(
            value="",
            description="Directory:",
            placeholder="directory of ROMS _cdrgas files",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
            continuous_update=False,  # see the wizard's grid_file_path
        )
        self.cs_attach_btn = W.Button(
            description=label_for("buttons.attach", "Attach").label, icon="link"
        )
        self.cs_clear_btn = W.Button(
            description=label_for("buttons.detach", "Detach").label, icon="times"
        )
        self.cs_status = W.HTML("")
        self.cs_path_box = W.VBox(
            [
                components.field_row(
                    W,
                    "cs_path",
                    self.cs_path,
                    extra=(self.cs_attach_btn, self.cs_clear_btn),
                ),
                self.cs_status,
            ]
        )
        # The time-interpolation switch: the settings editor's own widget (its
        # model_box), mounted on each editor rebuild (see `show_settings`), so it
        # shows only where the ucla-roms release has the switch and the ModelSpec
        # declares it -- on an invalid blueprint too (see the wizard's
        # _show_unresolved_editor).
        self.cs_interp_box = W.VBox([])
        self.cs_interp_note = W.HTML(
            "<div class='forge-hint' style='margin-bottom:10px'>Leave this off if "
            "your sensitivity files hold "
            "time-averaged values; turn it on if they hold instantaneous values. "
            "UCLA-ROMS writes the sensitivities time-averaged by default "
            "(<code>wrt_cdr_gas_avg</code>), so the wizard starts with it off.</div>"
        )
        self.cs_group = components.subsection(
            W,
            "model.carbonate",
            components.field_row(W, "cs_source_dd", self.cs_source_dd),
            self.cs_path_box,
            self.cs_interp_box,
        )

        self.apply_spec_defaults()
        self._sync_marbl_ref_visibility()
        self._sync_carbonate_sensitivity_visibility()

    # ---- wiring and layout ---------------------------------------------------
    def wire(self) -> None:
        """Register the card's own observers. The wizard calls this from its
        ``_wire``, before it registers its own observers on the card's widgets, so
        the card's visibility syncs run first.
        """
        self.show_archived_models.observe(self._on_show_archived_change, names="value")
        self.bgc_dd.observe(self._sync_marbl_ref_visibility, names="value")
        self.bgc_dd.observe(self._sync_carbonate_sensitivity_visibility, names="value")
        self.cs_source_dd.observe(
            self._sync_carbonate_sensitivity_visibility, names="value"
        )
        self.cs_attach_btn.on_click(self._on_cs_attach)
        self.cs_clear_btn.on_click(self._on_cs_clear)
        self.cs_path.observe(self._on_cs_path_submit, names="value")

    def rows(self) -> list[Any]:
        """The card body's rows, in order. The wizard adds its error banner above
        them and the ForcingSpec row below.
        """
        W = self.W
        # Danger-styled destructive action.
        self.cs_clear_btn.button_style = "danger"
        return [
            components.field_row(
                W, "model_dd", self.model_dd, extra=(self.show_archived_models,)
            ),
            # bgc_dd is placed before roms_ref/marbl_ref's grid so marbl_ref
            # (only relevant once bgc mode is "marbl") reads after bgc_dd.
            components.field_row(W, "bgc_dd", self.bgc_dd),
            components.field_grid(
                W,
                [
                    components.field_row(W, "roms_ref", self.roms_ref),
                    components.field_row(W, "marbl_ref", self.marbl_ref),
                ],
            ),
            self.cs_group,
        ]

    def status_chip(self) -> str:
        """The card's chip while it owns no problem: it is always complete, since
        every field has a ModelSpec default.
        """
        return components.chip("● Complete", "ok")

    def show_settings(self, model_box: Any | None) -> None:
        """Mount the settings editor's Model card widgets (its ``model_box``: the
        carbonate sensitivity time-interpolation switch) in the carbonate group,
        with their note; ``None`` when the editor has none.
        """
        self.cs_interp_box.children = (
            [model_box, self.cs_interp_note] if model_box is not None else []
        )

    # ---- what the card says --------------------------------------------------
    @property
    def name(self) -> str:
        """The selected ModelSpec's catalog name."""
        return self.model_dd.value

    @property
    def bgc_mode(self) -> str:
        """The selected BGC mode ("marbl", "none" or "cdr_lite")."""
        return self.bgc_dd.value

    @property
    def typed_roms_ref(self) -> str:
        """The ucla-roms ref typed in the card, stripped; "" when blank."""
        return self.roms_ref.value.strip()

    @property
    def typed_marbl_ref(self) -> str:
        """The MARBL ref typed in the card, stripped; "" when blank."""
        return self.marbl_ref.value.strip()

    def model_path(self) -> Path:
        """The selected ModelSpec's file in the catalog."""
        return self.catalog.model_path(self.model_dd.value)

    def declared(self) -> dict[str, Any]:
        """The selected ModelSpec's declared ``roms_ref``/``marbl_ref``/``bgc_mode``/
        ``use_pio``.

        Single re-parse of the ModelSpec file backing the ``default_*`` methods
        below and the wizard's spec-deviation check in ``_rebuild`` -- both need the
        same catalog-declared values to compare live widget state against.
        """
        try:
            data = load_model_spec_data(self.model_path())
            model = data["model"]
            roms = model.get("code", {}).get("roms", {}) or {}
            marbl = model.get("code", {}).get("marbl", {}) or {}
            return {
                "roms_ref": roms.get("commit") or roms.get("branch") or "",
                "marbl_ref": marbl.get("commit") or marbl.get("branch") or "",
                "bgc_mode": model.get("bgc_mode", "marbl"),
                "use_pio": bool(model.get("use_pio", False)),
            }
        except Exception:
            return {
                "roms_ref": "",
                "marbl_ref": "",
                "bgc_mode": "marbl",
                "use_pio": False,
            }

    def default_roms_ref(self) -> str:
        """The selected model's pinned ucla-roms checkout target (commit or branch)."""
        return self.declared()["roms_ref"]

    def default_marbl_ref(self) -> str:
        """The selected model's pinned MARBL checkout target (commit or branch)."""
        return self.declared()["marbl_ref"]

    def default_bgc_mode(self) -> str:
        """The selected model's ModelSpec-declared bgc_mode (prepopulates bgc_dd)."""
        return self.declared()["bgc_mode"]

    def default_use_pio(self) -> bool:
        """The selected model's ModelSpec-declared use_pio (prepopulates the Run
        card's use_pio_chk).
        """
        return self.declared()["use_pio"]

    def effective_roms_ref(self) -> str | None:
        """The ucla-roms ref that determines the run-time-settings schema.

        Mirrors the resolver's own precedence for ``roms_ref`` (see `gather`): the
        live override in ``roms_ref`` when non-blank, else the selected ModelSpec's
        pinned ``code.roms`` ref. ``None`` when neither is set, so the wizard's
        ``_wizard_settings_cls_for_ref``/``run_time_settings_for_ref`` fall back to
        the legacy schema instead of warning on an empty ref.
        """
        return self.typed_roms_ref or self.default_roms_ref() or None

    def deviates_from_spec(self, declared: dict[str, Any]) -> bool:
        """Whether the BGC mode, or a typed ucla-roms or MARBL ref, differs from what
        the ModelSpec declares (*declared*, from `declared`). A blank ref takes the
        spec's pin, so it never deviates.
        """
        return (
            self.bgc_dd.value != declared["bgc_mode"]
            or bool((ref := self.typed_roms_ref) and ref != declared["roms_ref"])
            or bool((mref := self.typed_marbl_ref) and mref != declared["marbl_ref"])
        )

    def gather(self) -> dict[str, Any]:
        """The card's resolver kwargs after ``model_path`` (see `model_path`):
        ``bgc_mode``, the carbonate sensitivities when attached, and the refs
        typed over the ModelSpec's pins.

        Raises ``ValueError`` when the directory source is picked but nothing is
        attached.
        """
        kw: dict[str, Any] = {"bgc_mode": self.bgc_dd.value}
        # The default ("directive") leaves carbonate_sensitivity off: the workplan
        # directive supplies the files at run time. The resolver rejects them
        # outside bgc_mode "cdr_lite", so attached state is only passed there.
        if self.bgc_dd.value == "cdr_lite" and self.cs_source_dd.value == "path":
            if self._carbonate_sensitivity is None:
                raise ValueError(
                    "carbonate sensitivities: attach a directory of ROMS _cdrgas "
                    "files, or switch the source to the workplan directive"
                )
            kw["carbonate_sensitivity"] = self._carbonate_sensitivity
        if self.typed_roms_ref:
            kw["roms_ref"] = self.typed_roms_ref
        if self.typed_marbl_ref:
            kw["marbl_ref"] = self.typed_marbl_ref
        return kw

    # ---- what the wizard sets ------------------------------------------------
    def apply_spec_defaults(self) -> None:
        """Set the refs and the BGC mode to the selected ModelSpec's (on a model
        switch, or construction).
        """
        self.roms_ref.value = self.default_roms_ref()
        self.marbl_ref.value = self.default_marbl_ref()
        self.bgc_dd.value = self.default_bgc_mode()

    def select_model(self, cfg: ForgeBlueprint, unknown: list[str]) -> None:
        """Select a loaded blueprint's ModelSpec, showing archived specs when it is
        one; a name the catalog lacks is appended to *unknown* and the selection
        kept. The wizard calls this suspended, before `populate`.
        """
        if cfg.composition.model.name in self.catalog.archived_model_names:
            self.show_archived_models.value = True
        if cfg.composition.model.name in _dd_values(self.model_dd):
            self.model_dd.value = cfg.composition.model.name
        elif cfg.composition.model.name:
            unknown.append(f"model '{cfg.composition.model.name}'")

    def populate(self, cfg: ForgeBlueprint) -> None:
        """Set the BGC mode, the carbonate sensitivities and the refs from a loaded
        blueprint, after `select_model` (the wizard holds it suspended).
        """
        self.bgc_dd.value = bgc_mode_from_cppdefs(
            cfg.model_settings.get("cppdefs") or {}
        )
        self._populate_carbonate_sensitivity(cfg)
        # Show the file's actual pinned ref, falling back to the (now-selected)
        # model's default when the file matches it exactly.
        stored_ref = _pinned_roms_ref(cfg)
        default_ref = self.default_roms_ref()
        self.roms_ref.value = default_ref if stored_ref == default_ref else stored_ref
        # Same for MARBL -- code.marbl is absent when the file was saved with
        # bgc_mode="none", in which case fall back to the model's default so
        # re-enabling BGC picks the pinned ref back up.
        stored_marbl = ""
        if cfg.code.marbl is not None:
            stored_marbl = cfg.code.marbl.commit or cfg.code.marbl.branch or ""
        default_marbl = self.default_marbl_ref()
        self.marbl_ref.value = (
            stored_marbl
            if stored_marbl and stored_marbl != default_marbl
            else default_marbl
        )

    def reselect(self, name: str) -> None:
        """Re-read the catalog's ModelSpecs into ``model_dd`` and select *name*
        (after "Save as new spec"; the wizard holds it suspended).
        """
        old_value = self.model_dd.value
        self.model_dd.options = self._model_dd_options(old_value)
        self.model_dd.value = name

    # ---- the ModelSpec dropdown ----------------------------------------------
    def _visible_model_names(
        self, archived: Collection[str], selected: str | None = None
    ) -> list[str]:
        """Catalog models ``model_dd`` lists: all of them when "Show archived"
        is checked, otherwise the non-*archived* ones plus *selected* (so
        unchecking never drops the current choice).
        """
        names = list(self.catalog.model_names)
        if self.show_archived_models.value:
            return names
        return [n for n in names if n not in archived or n == selected]

    def _model_dd_options(self, selected: str | None = None) -> list:
        archived = self.catalog.archived_model_names
        return _dd_options(
            self.catalog,
            self._visible_model_names(archived, selected),
            "model",
            archived=archived,
        )

    def _on_show_archived_change(self, _change):
        """Re-option ``model_dd`` for the new checkbox state, keeping its value."""
        with self._suspend():
            selected = self.model_dd.value
            self.model_dd.options = self._model_dd_options(selected)
            self.model_dd.value = selected

    # ---- visibility ----------------------------------------------------------
    def _sync_marbl_ref_visibility(self, _change=None):
        # MARBL ref is inert without MARBL, so hide it (value is kept, not cleared)
        self.marbl_ref.layout.display = "" if self.bgc_dd.value == "marbl" else "none"

    def _sync_carbonate_sensitivity_visibility(self, _change=None):
        # The carbonate sensitivity group only matters in "cdr_lite" mode (the
        # wizard shows the CDR panel's hint then too, see its _sync_cdr_lite_hint);
        # the attach row only for the "path" source. Attached state is kept (not
        # cleared) while hidden, like marbl_ref's value.
        cdr_lite = self.bgc_dd.value == "cdr_lite"
        path = self.cs_source_dd.value == "path"
        self.cs_group.layout.display = "" if cdr_lite else "none"
        self.cs_path_box.layout.display = "" if path else "none"
        if path and self._carbonate_sensitivity is None and not self.cs_status.value:
            self.cs_status.value = _CARBONATE_DIR_HINT

    # ---- carbonate sensitivities: a directory of ROMS _cdrgas files ------------
    def _attach_carbonate_sensitivity_dir(self, path_str: str) -> None:
        """Scan ``path_str`` for joined ``_cdrgas`` files, check each is netCDF,
        hash them, and store the resolved list as ``_carbonate_sensitivity``.

        A failure clears any earlier attach (the status says why): leaving it
        would quietly keep building with files the path box no longer names.
        """
        self.cs_status.value = "<i>attaching…</i>"
        try:
            joined = find_carbonate_sensitivity_files(
                Path(path_str).expanduser().resolve()
            )
            formats = {f.path: netcdf_format(f.path) for f in joined}
            not_netcdf = [
                p.name for p, fmt in formats.items() if fmt is NetCDFFormat.UNRECOGNIZED
            ]
            if not_netcdf:
                raise ValueError(f"not netCDF files: {', '.join(not_netcdf)}")
            files = [
                {"location": str(f.path), "content_hash": hash_netcdf_contents(f.path)}
                for f in joined
            ]
            # The blueprint's own rules (e.g. distinct basenames: the scan is
            # recursive, so a parent of several runs' output can repeat them).
            CarbonateSensitivitySpec.model_validate({"files": files})
        except Exception as exc:
            self._carbonate_sensitivity = None
            message = html.escape(" ".join(str(exc).split()))
            self.cs_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {message}"
            )
            self._rebuild()
            return
        self._carbonate_sensitivity = {"files": files}
        first = min(f.timestamp for f in joined)
        last = max(f.timestamp for f in joined)
        status = _carbonate_status_html(
            len(files), f"{first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M}"
        )
        if self._use_pio() and not all(fmt.is_classic for fmt in formats.values()):
            status += "<br>" + components.status_span(
                "note",
                "ParallelIO reads classic netCDF "
                "only: Forge converts the staged copies to CDF-5 (the originals "
                "are untouched).",
            )
        self.cs_status.value = status
        self._rebuild()

    def _populate_carbonate_sensitivity(self, cfg: ForgeBlueprint) -> None:
        """Load-back half of the attach flow: a blueprint listing carbonate
        sensitivity files selects the "path" source with the stored hashes
        trusted (never recomputed; a missing file is a warning, not a blocker --
        Save must still round-trip losslessly); otherwise the "directive" source.
        """
        spec = cfg.carbonate_sensitivity
        if spec is None:
            self._carbonate_sensitivity = None
            self.cs_source_dd.value = "directive"
            self.cs_path.value = ""
            self.cs_status.value = ""
            return
        self._carbonate_sensitivity = {
            "files": [
                {"location": f.location, "content_hash": f.content_hash}
                for f in spec.files
            ]
        }
        parents = [str(Path(f.location).expanduser().parent) for f in spec.files]
        try:
            directory = os.path.commonpath(parents)
        except ValueError:  # relative and absolute locations mixed
            directory = parents[0]
        self.cs_source_dd.value = "path"
        self.cs_path.value = directory
        missing = [f for f in spec.files if not Path(f.location).expanduser().exists()]
        self.cs_status.value = _carbonate_status_html(len(spec.files)) + (
            "<br>"
            + components.status_span(
                "err", f"⚠ {len(missing)} file(s) not found at these paths"
            )
            if missing
            else ""
        )

    def _on_cs_attach(self, _btn):
        path_str = self.cs_path.value.strip()
        if not path_str:
            self.cs_status.value = components.status_span("err", "Enter a path first.")
            return
        # Explicit click always re-scans and re-hashes -- no dedupe here.
        self._attach_carbonate_sensitivity_dir(path_str)

    def _on_cs_path_submit(self, _change) -> None:
        """Attach the typed directory on Enter/focus-out (twin of the wizard's
        `_on_cdr_file_path_submit`): a typed-but-unattached path would otherwise
        leave the blueprint invalid with no hint as to why.
        """
        if self._is_suspended():
            return
        path_str = self.cs_path.value.strip()
        if path_str:
            self._attach_carbonate_sensitivity_dir(path_str)

    def _on_cs_clear(self, _btn):
        self._carbonate_sensitivity = None
        self.cs_path.value = ""  # fires _on_cs_path_submit, a no-op when blank
        self.cs_status.value = _CARBONATE_DIR_HINT
        self._rebuild()
