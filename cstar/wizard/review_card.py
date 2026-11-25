"""The Forge blueprint wizard's Save and Next steps cards.

`ReviewCard` owns both cards' widgets. Card 7 (Save) holds the validation and
summary banners, the YAML preview, the blueprint's name and description, the
download links and the Save path with its button. Card 8 (Next steps) holds what to
do with the saved blueprint: the workplan row and the Run Forge here pane. The two
share one record, the blueprint last saved, which card 7's Save writes and card 8
reads on every rebuild. The card owns their observers, the blueprint's ``name`` and
``description`` resolver kwargs and their share of loading a blueprint back, and
card 7's status chip.

`ForgeBlueprintWizard` places the cards' rows, adds the "Save specs" pane to card 8,
calls the card at each step of a rebuild, and otherwise reads and sets the card
only through its methods and properties. The card's Save and Run save the blueprint
through the wizard's ``save_blueprint``, which derives the open boundaries first
and answers with the file written or a `SaveRefused`. Every write of the blueprint
goes through the wizard's ``_save_config``, which tells the card with
`ReviewCard.record_save`. The card reaches back only through the callables it is
given.
"""

from __future__ import annotations

import base64
import html
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, NamedTuple

from cstar.applications.forge.blueprint import (
    DEFAULT_APPLICATION,
    PLACEHOLDER_DESCRIPTION,
)
from cstar.applications.forge.resolve import default_blueprint_description
from cstar.wizard.fields import _tip
from cstar.wizard.streaming import (
    _STREAM_READ_SIZE,
    _drain_stream_buffer,
    _schedule_coroutine,
)
from cstar.wizard.ui import components
from cstar.wizard.ui.catalog_bar import schedule_confirm_reset
from cstar.wizard.ui.labels import section_for

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from contextlib import AbstractContextManager

    from cstar.applications.forge.blueprint import ForgeBlueprint

WIZARD_TOOL: Final[str] = "wizard"
"""The ``provenance.generated_by.tool`` of a blueprint the wizard writes."""


class SaveRefused(Enum):
    """Why the wizard's ``save_blueprint`` wrote nothing. Each caller says so in
    its own words.
    """

    BOUNDARIES = "boundaries"
    """The open boundaries are still the provisional defaults, and deriving them
    from the grid failed (the Grid card's derive status says why)."""

    INVALID = "invalid"
    """The blueprint's inputs are invalid, so there is no config to save."""


# What the Download links say while they wait (see ReviewCard.refresh_download_links).
_DOWNLOAD_WAITS_FOR_BOUNDARIES: Final[str] = (
    "The download is ready once the open boundaries are derived from the grid: "
    "press Derive from grid in the Grid section, set the boundaries by hand, or "
    "save the blueprint."
)

_SAVE_FIRST = "Save the blueprint above first."
"""Card 8's message while the blueprint is not saved as it stands."""


class _SavedBlueprint(NamedTuple):
    """The last blueprint file the wizard wrote, and what it wrote."""

    requested: Path  #: the Save path the save was made with, as given
    path: Path  #: the file written, resolved
    text: str  #: ``to_yaml_str()`` of the config written


class WorkplanHooks(NamedTuple):
    """What the Blueprint page needs from a Workplan page, when it sits beside one."""

    open_steps: Callable[[Path, str], None]
    """Add the forge-then-run steps for (saved blueprint path, blueprint name) to the
    Workplan page and show that page. May raise ``ValueError``."""

    saved: Callable[[], None]
    """A workplan file was written into the catalog: refresh the Workplan page's
    Load list."""


class ReviewCard:
    """The wizard's Save and Next steps cards: the blueprint's name, description and
    Save path, its validation, preview and downloads, and what to do with it once
    saved (a workplan, or a Forge run in this session).

    The callables are the wizard's: ``rebuild`` re-resolves the blueprint,
    ``suspend`` returns the wizard's suspend context manager, ``is_suspended`` says
    whether one is open, ``config`` reads the resolved blueprint (``None`` while the
    inputs are invalid), ``save_blueprint`` saves it to a path, deriving the open
    boundaries first, and returns the file written or why nothing was
    (`SaveRefused`), and ``boundaries_pending`` says whether the boundaries are
    still the provisional defaults. Construction builds the widgets but registers
    no observers: the wizard calls `wire` once it is built.
    """

    def __init__(
        self,
        W: Any,
        catalog: Any,
        *,
        rebuild: Callable[[], None],
        suspend: Callable[[], AbstractContextManager[Any]],
        is_suspended: Callable[[], bool],
        config: Callable[[], ForgeBlueprint | None],
        save_blueprint: Callable[[Path], Path | SaveRefused],
        boundaries_pending: Callable[[], bool],
    ) -> None:
        self.W = W
        self.catalog = catalog
        self._rebuild = rebuild
        self._suspend = suspend
        self._is_suspended = is_suspended
        self._get_config = config
        self._save_blueprint = save_blueprint
        self._boundaries_pending = boundaries_pending

        self.derived = W.HTML("")
        self.validation = W.HTML("")
        self.preview = W.Output(
            layout=W.Layout(
                padding="6px",
                max_height="380px",
                overflow="auto",
            )
        )
        self.preview.add_class("forge-log")
        # Browser download (works in Voilà / JupyterLab without server file access)
        self.download_link = W.HTML("")
        # Save to the server/working-dir filesystem (handy for local or HPC use)
        self.save_path = W.Text(
            value="forge_blueprint.yaml",
            description="Save to:",
            style={"description_width": "110px"},
            layout=W.Layout(width="520px"),
        )
        self.save_btn = W.Button(description="Save to disk", icon="save")
        self.save_status = W.HTML("")

        # --- canonical blueprint name (also in the Export section) ---
        # Defaults to the resolver's derived name and keeps tracking it until the user
        # edits the field (self._name_touched flips True on their first real edit;
        # programmatic backfills in _rebuild() happen under _suspend() so they don't
        # themselves flip it). save_path tracks the derived name the same way.
        self._name_touched = False
        self.name = W.Text(
            value="",
            description="Name:",
            placeholder="(derived from model/grid/procs)",
            style={"description_width": "110px"},
            layout=W.Layout(width="320px"),
            tooltip=_tip("export", "name"),
        )
        # The blueprint description follows the resolver's derived one-line summary
        # the same way, until the user types in it (_description_touched).
        self._description_touched = False
        self.description = W.Text(
            value="",
            description="Description:",
            placeholder="(derived from the grid, run window, model and forcing)",
            style={"description_width": "110px"},
            layout=W.Layout(width="100%", max_width="760px"),
            tooltip=_tip("export", "description"),
        )
        self._save_path_touched = False

        # --- run (invokes the C-Star CLI on the just-saved blueprint) ---
        from cstar.system.manager import system_label

        self.run_warning = W.HTML(
            f"<b class='{components.status_class('warn')}'>⚠ Processing a blueprint can use substantial "
            "memory and CPU depending on grid size.</b> Run this from a compute node "
            "(or another host) with resources appropriate for your domain — this is "
            f"not checked automatically. System: <code>{html.escape(system_label())}</code>."
        )
        self.run_later_note = W.HTML(
            components.status_span(
                "note",
                "ℹ To run this later, or on a different "
                "machine, save the blueprint above and then (from the "
                "<code>cstar-env</code> environment) call: "
                "<code>cstar blueprint run &lt;path/to/forge_blueprint.yaml&gt;</code>. ",
            )
        )
        self.run_btn = W.Button(description="Run", icon="play")
        self.run_status = W.HTML("")

        # --- next steps: what to do with the saved blueprint ---
        self.next_status = W.HTML("")
        self.save_workplan_btn = W.Button(
            description="Save workplan", icon="save", button_style="primary"
        )
        self.open_workplan_btn = W.Button(
            description="Open in the Workplan builder",
            icon="arrow-right",
            layout=W.Layout(display="none"),
        )
        self.workplan_status = W.HTML("")
        self.workplan_download = W.HTML("")
        self._run_in_progress = False
        self._workplan_hooks: WorkplanHooks | None = None
        self._workplan_confirm: Path | None = None
        self._workplan_confirm_timer: Any = None
        self._saved_blueprint: _SavedBlueprint | None = None

        # Keep the run log pinned to the LATEST lines instead of snapping to the
        # top on every append. Two cooperating pieces (verified live in voila):
        # 1. display=flex + column-reverse on this node anchors its scroll
        #    position at the flex start, which column-reverse puts at the bottom.
        # 2. The stylesheet below stops the inner .jp-OutputArea from being its
        #    own scroll container: JupyterLab CSS bounds its height and gives it
        #    overflow auto, so IT would do the scrolling (resetting to the top on
        #    each append) while this node never overflows. flex: 0 0 auto lets it
        #    grow to its content so the scrolling happens on this node instead.
        #    (Layout can only style this node itself, hence a real stylesheet.)
        # Safe because _run_async only ever calls append_stdout, and the frontend
        # merges consecutive same-stream outputs into a single block -- a second
        # output type (stderr, display_data) would render above it, reversed.
        self.run_output = W.Output(
            layout=W.Layout(
                padding="6px",
                max_height="380px",
                overflow="auto",
                display="flex",
                flex_flow="column-reverse",
            )
        )
        self.run_output.add_class("forge-run-log")
        self.run_output.add_class("forge-log")
        self._run_log_style = W.HTML(
            "<style>.forge-run-log > .jp-OutputArea "
            "{ flex: 0 0 auto; height: auto; max-height: none; }</style>"
        )

        # A second, plain HTML mirror of `download_link` for the Review card --
        # `download_link` itself lives once, in the wizard's sticky bar (see its
        # `widget`).
        self.download_link_review = W.HTML("")

    @property
    def config(self) -> ForgeBlueprint | None:
        """The wizard's resolved blueprint, ``None`` while its inputs are invalid."""
        return self._get_config()

    @property
    def save_target(self) -> Path:
        """The file the Save path names, as typed."""
        return Path(self.save_path.value)

    # ---- wiring and layout ---------------------------------------------------
    def wire(self) -> None:
        """Register the cards' own observers and click handlers. The wizard calls
        this from its ``_wire``.
        """
        self.name.observe(self._on_name_change, names="value")
        self.description.observe(self._on_description_change, names="value")
        self.save_path.observe(self._on_save_path_change, names="value")
        self.save_btn.on_click(self._on_save)
        self.run_btn.on_click(self._on_run)
        self.save_workplan_btn.on_click(self._on_save_workplan)
        self.open_workplan_btn.on_click(self._on_open_workplan)

    @property
    def rebuild_on(self) -> tuple[Any, ...]:
        """None: ``name`` and ``description`` rebuild from their own handlers (see
        `wire`), which first mark the field as typed so the rebuild does not fill it
        back in; listing them here would rebuild twice. ``save_path`` changes no
        blueprint.
        """
        return ()

    def rows(self) -> list[Any]:
        """Card 7's body rows, in order."""
        W = self.W
        return [
            self.validation,
            self.derived,
            self.preview,
            components.field_row(W, "name", self.name),
            components.field_row(W, "description", self.description),
            self.download_link_review,
            components.field_row(
                W, "save_path", self.save_path, extra=(self.save_btn,)
            ),
            self.save_status,
        ]

    def next_rows(self) -> list[Any]:
        """Card 8's body rows, in order. The wizard adds the "Save specs" pane below
        them. Builds ``run_forge_pane``.
        """
        W = self.W
        run_forge = section_for("next.run_forge")
        self.run_forge_pane = components.collapsible(
            W,
            W.VBox(
                [
                    W.HTML(
                        "<p class='forge-hint'>Runs Forge in this session to build "
                        "the input files and the UCLA-ROMS blueprint. It does not "
                        "run UCLA-ROMS; to do both, use a workplan above.</p>"
                    ),
                    self.run_warning,
                    self.run_later_note,
                    W.HBox([self.run_btn, self.run_status]),
                    self._run_log_style,
                    self.run_output,
                ]
            ),
            components.accordion_title(
                run_forge.title, components.Pill(run_forge.desc, "off")
            ),
        )
        workplan_sub = components.subsection(
            W,
            "next.workplan",
            W.HTML(
                "<p class='forge-hint'>Most runs need both steps: Forge builds the "
                "inputs and the UCLA-ROMS blueprint, then UCLA-ROMS runs it. Save a "
                "workplan that does both.</p>"
            ),
            W.HBox([self.save_workplan_btn, self.open_workplan_btn]),
            self.workplan_status,
            self.workplan_download,
        )
        return [self.next_status, workplan_sub, self.run_forge_pane]

    def status_chip(self, valid: bool) -> str:
        """Card 7's chip: whether the blueprint resolves (``valid``)."""
        return (
            components.chip("● Ready", "ok")
            if valid
            else components.chip("● Invalid", "warn")
        )

    # ---- the wizard's share: resolver kwargs, load-back and each rebuild -------
    def gather(self) -> dict[str, Any]:
        """The blueprint's ``description`` and ``name`` resolver kwargs."""
        return dict(
            # Untouched (or cleared): None, so the resolver derives a fresh one-line
            # description from the inputs, as it does the name below.
            description=(
                (self.description.value.strip() or None)
                if self._description_touched
                else None
            ),
            # Untouched: always pass None so the resolver recomputes a fresh default
            # (self.name.value may still hold the *previous* rebuild's backfilled
            # default -- reusing it here would freeze it instead of tracking inputs).
            name=(self.name.value.strip() or None) if self._name_touched else None,
        )

    def populate(self, cfg: ForgeBlueprint) -> None:
        """Set the name, description and Save path from a loaded blueprint. The
        wizard calls this inside its suspend block.
        """
        # A file's own description is kept, unless it is no more than a default:
        # none, the old placeholder, or the text the resolver derives for it
        # (saved untouched). Those follow the inputs again, like a new blueprint.
        self._description_touched = cfg.description.strip() not in {
            "",
            PLACEHOLDER_DESCRIPTION,
            default_blueprint_description(cfg),
        }
        self.description.value = cfg.description if self._description_touched else ""
        self.name.value = cfg.name
        self._name_touched = True  # a loaded name is a deliberate choice, not a default
        self.save_path.value = self._default_blueprint_path(cfg.name)
        self._save_path_touched = True

    def clear_results(self) -> None:
        """Clear the preview and the validation banner at the start of a rebuild.

        The validation banner reflects the *last successful* resolve; every early
        return of a rebuild (missing dates, a build/validation exception) would leave
        a stale ✓/⚠ message sitting under the summary's fresh error otherwise -- clear
        it once up front so only `show_resolved` sets it again.
        """
        self.preview.clear_output(wait=True)
        self.validation.value = ""

    def show_summary(self, summary: str) -> None:
        """Set the summary line under the validation banner (an error, or what the
        blueprint resolved to).
        """
        self.derived.value = summary

    def show_unresolved(self, summary: str, error: str | None = None) -> None:
        """Show that the blueprint does not resolve: withdraw both download links,
        print ``error`` (the full message) in the preview and show ``summary``.
        """
        self.download_link.value = ""
        self.download_link_review.value = ""
        if error is not None:
            with self.preview:
                print(error)
        self.show_summary(summary)

    def follow_derived(self, cfg: ForgeBlueprint, prev_name: str | None) -> None:
        """Keep the name, description and Save path following the resolved
        blueprint ``cfg`` until the user edits them; ``prev_name`` is the name the
        blueprint resolved to before this rebuild.
        """
        # Backfill the Export name/description/save-path fields with the current
        # derived defaults until the user edits one -- their edit "locks in" that
        # field (see _on_name_change/_on_description_change/_on_save_path_change).
        # Under _suspend() so these programmatic writes don't themselves flip the
        # touched flags or recurse.
        if (
            not self._name_touched
            or not self._description_touched
            or not self._save_path_touched
        ):
            with self._suspend():
                if not self._name_touched:
                    self.name.value = cfg.name
                if not self._description_touched:
                    self.description.value = cfg.description
                if not self._save_path_touched:
                    self.save_path.value = self._default_blueprint_path(cfg.name)
        # Independent of the backfill above (and of _name_touched): the derived
        # name can also change from a grid/procs/model edit with Name left on
        # auto-derive but Save-to customized to a specific directory -- that
        # combination skips the backfill (save_path IS touched) entirely, so
        # without this the filename would never re-sync. Re-sync only on a
        # genuine rename, and only when the current filename still exactly
        # tracks the *previous* derived name (i.e. the user hasn't customized
        # the filename itself) -- preserves the user's directory and any
        # deliberately-custom filename. Under _suspend() so this doesn't
        # itself flip _save_path_touched or recurse.
        if (
            self._save_path_touched
            and prev_name is not None
            and cfg.name != prev_name
            and Path(self.save_path.value).name == f"{prev_name}.yaml"
        ):
            with self._suspend():
                self.save_path.value = str(
                    Path(self.save_path.value).parent / f"{cfg.name}.yaml"
                )

    def show_resolved(
        self, cfg: ForgeBlueprint, problems: Sequence[str], notes: Sequence[str]
    ) -> None:
        """Show a resolved blueprint: the settings validation (``problems``), what it
        would run with that nothing refuses but its author likely did not mean
        (``notes``), the summary line, and its YAML in the preview.
        """
        if problems:
            self.validation.value = components.banner(
                "err",
                "<b>Settings validation:</b><br>"
                + "<br>".join(f"&nbsp;&nbsp;{p}" for p in problems[:10]),
            )
        else:
            self.validation.value = components.banner(
                "ok", "<b>Blueprint is valid.</b> All settings pass validation."
            )
        if notes:
            self.validation.value += components.banner(
                "warn",
                "<br>".join(f"<b>Warning:</b> {html.escape(note)}" for note in notes),
            )
        comp = cfg.composition
        self.show_summary(
            f"<b>name</b>: <code>{cfg.name}</code> &nbsp; "
            f"<b>casename</b>: <code>{cfg.casename}</code><br>"
            f"<b>composition</b>: model=<code>{comp.model.name}</code> ({comp.model.origin}), "
            f"domain=<code>{comp.domain.name or '—'}</code> ({comp.domain.origin}), "
            f"forcing ({comp.forcing.origin})"
        )
        with self.preview:
            print(cfg.to_yaml_str())

    def record_save(self, requested: Path, written: Path, cfg: ForgeBlueprint) -> None:
        """Note that the wizard wrote ``cfg`` (stamped) to ``written``, asked for as
        the Save path ``requested``: point the download links at it, remember it as
        the saved blueprint, and make card 8 follow.
        """
        self.refresh_download_links(cfg)
        self._saved_blueprint = _SavedBlueprint(
            requested, Path(written).expanduser().resolve(), cfg.to_yaml_str()
        )
        self.sync_next_steps()

    # ---- card 7: name, description, Save path, Save and downloads -----------
    def _on_name_change(self, _change):
        # A programmatic backfill (see _rebuild) happens under _suspend() -- only a
        # real user edit should "lock in" the field against further auto-updates.
        if self._is_suspended():
            return
        self._name_touched = True
        self._rebuild()

    def _on_description_change(self, _change):
        # Mirrors _on_name_change: the backfill in _rebuild runs under _suspend().
        # Not in `rebuild_on`, so the flag is set before the rebuild
        # that would otherwise backfill over the user's first keystroke.
        if self._is_suspended():
            return
        self._description_touched = True
        self._rebuild()

    def _on_save_path_change(self, _change):
        if self._is_suspended():
            return
        self._save_path_touched = True
        self.sync_next_steps()

    def _default_blueprint_path(self, name: str) -> str:
        """Default "Save to:" path for a blueprint named *name*.

        Prefers the active catalog's ``blueprints/forge/`` directory so a save lands
        where the wizard's other catalog-aware specs look; falls back to a
        bare filename (CWD-relative) when the catalog isn't a local filesystem
        (e.g. loaded from a GitHub/http URL) and so isn't writable.
        """
        fname = f"{name}.yaml"
        cat = getattr(self, "catalog", None)
        try:
            # A read-only catalog (e.g. the bundled one loaded as a single
            # store) must not be offered as a save destination.
            if (
                cat is not None
                and getattr(cat, "_is_local", False)
                and not getattr(cat, "read_only", False)
            ):
                return str(cat.blueprint_dir(DEFAULT_APPLICATION) / fname)
        except Exception:
            pass
        return fname

    def spec_description(self) -> str:
        """The description a catalog spec saved from this session gets.

        The text the user typed for the blueprint, or empty while the field holds
        the derived one: that summarizes the whole run (grid, dates, forcing, CDR),
        so it would describe no single spec. Empty leaves a ModelSpec with the
        description of the spec it was saved from (``register_model_from_settings``).
        """
        return self.description.value.strip() if self._description_touched else ""

    def refresh_download_links(self, cfg: ForgeBlueprint) -> None:
        """Point both download links at ``cfg``, stamped once so they agree.

        A download is a copy for the user, not a save: a stamp minted here is not
        carried (that is the wizard's ``_save_config``'s job). A ``cfg`` that is
        already stamped, as the config just saved is, comes back from
        ``stamp_provenance`` as it is, so its links hand out exactly the file that
        was written.

        While the open boundaries are the provisional checkbox defaults (neither
        touched nor derived from the grid), the links are withheld: Save and Run
        derive them first (the wizard's ``save_blueprint``), but a link has no click
        hook to do that, and building a grid on every rebuild would stall typing.
        """
        if self._boundaries_pending():
            self.download_link.value = (
                f"<span class='forge-dl-off {components.status_class('warn')}' "
                f'title="{html.escape(_DOWNLOAD_WAITS_FOR_BOUNDARIES)}">'
                "⬇ Derive boundaries to download</span>"
            )
            self.download_link_review.value = components.status_span(
                "warn",
                f"⚠ {html.escape(_DOWNLOAD_WAITS_FOR_BOUNDARIES)}",
            )
            return
        download_cfg = cfg.stamp_provenance(WIZARD_TOOL)
        self.download_link.value = self._download_html(
            download_cfg, caption="Download blueprint"
        )
        self.download_link_review.value = self._download_html(download_cfg)

    @staticmethod
    def _download_html(cfg: ForgeBlueprint, caption: str | None = None) -> str:
        """A data-URI download link for the resolved YAML — works in the browser
        (Voilà / JupyterLab) with no server-side file access.

        ``caption`` overrides the link text with a fixed, short caption (the
        filename moves into ``title=`` instead) -- used by the sticky bar,
        which has no room for a full filename. The default keeps the Review
        card's "Download <code>fname</code>" copy.
        """
        payload = cfg.to_yaml_str().encode("utf-8")
        b64 = base64.b64encode(payload).decode("ascii")
        fname = f"{cfg.name}.forge_blueprint.yaml"
        if caption is None:
            text, title_attr = f"Download <code>{fname}</code>", ""
        else:
            text, title_attr = caption, f' title="{fname}"'
        return (
            f'⬇ <a class="forge-dl-btn" download="{fname}"{title_attr} '
            f'href="data:text/yaml;base64,{b64}">'
            f"{text}</a>"
        )

    def _on_save(self, _):
        try:
            saved = self._save_blueprint(self.save_target)
        except Exception as exc:
            self.save_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        if saved is SaveRefused.BOUNDARIES:
            self.save_status.value = components.status_span(
                "err",
                "Save aborted — open boundaries could "
                "not be derived from the grid (see the Domain-derived properties "
                "status above). A save must never ship provisional defaults; fix "
                "the grid or set boundaries manually, then retry.",
            )
        elif saved is SaveRefused.INVALID:
            self.save_status.value = components.status_span(
                "err", "Nothing to save — config is invalid."
            )
        else:
            self.save_status.value = components.status_span("ok", f"Saved {saved}")

    # ---- card 8: next steps ----------------------------------------------------
    def _saved_path(self) -> Path | None:
        """The file the current config was last saved to, or ``None`` when there is
        no such file: nothing saved yet, the config edited since, or the Save path
        changed. Compared in memory (this runs on every rebuild), so a file deleted
        or edited on disk is not noticed here.
        """
        saved = self._saved_blueprint
        if (
            self.config is None
            or saved is None
            or Path(self.save_path.value) != saved.requested
            or self.config.to_yaml_str() != saved.text
        ):
            return None
        return saved.path

    def set_workplan_hooks(self, hooks: WorkplanHooks | None) -> None:
        """Connect this page to a Workplan page (or disconnect it with ``None``).

        Without hooks only "Save workplan" shows: there is no page to open.
        """
        self._workplan_hooks = hooks
        self.open_workplan_btn.layout.display = "" if hooks else "none"
        self.sync_next_steps()

    def sync_next_steps(self) -> None:
        """Make card 8 follow whether the blueprint above is saved and current."""
        path = self._saved_path()
        ready = path is not None
        saved = self._saved_blueprint
        if path is not None:
            note = f"Saved to <code>{html.escape(str(path))}</code>."
        elif saved is not None and self.config is not None:
            note = (
                f"{_SAVE_FIRST} It has changed since you saved it to "
                f"<code>{html.escape(str(saved.path))}</code>."
            )
        else:
            note = _SAVE_FIRST
        self.next_status.value = f"<span class='forge-hint'>{note}</span>"
        self.save_workplan_btn.disabled = self.open_workplan_btn.disabled = not ready
        self.run_btn.disabled = self._run_in_progress or not ready
        if not ready:
            self.workplan_download.value = ""
            self._reset_workplan_confirm()

    def _workplan_msg(self, kind: str, message: str) -> None:
        self.workplan_status.value = f"<span class='forge-msg-{kind}'>{message}</span>"

    def _reset_workplan_confirm(self) -> None:
        """Disarm the overwrite confirm: cancel its timer, restore the button."""
        if self._workplan_confirm_timer is not None:
            self._workplan_confirm_timer.cancel()
        self._workplan_confirm = self._workplan_confirm_timer = None
        self.save_workplan_btn.description = "Save workplan"
        self.save_workplan_btn.button_style = "primary"

    def _on_save_workplan(self, _btn: Any) -> None:
        """Write the forge-then-run workplan for the saved blueprint to the catalog's
        ``workplans/`` (beside the blueprint when there is no writable catalog), and
        offer it for download. Replacing a different file takes a second click.
        """
        from cstar.wizard.workplan_builder import (
            WorkplanBuilderPage,
            default_workplan_path,
            forge_then_run_workplan,
            yaml_text,
        )

        path = self._saved_path()
        if path is None or self.config is None:
            self._workplan_msg("err", _SAVE_FIRST)
            return
        if not path.exists():
            self._workplan_msg(
                "err",
                f"The saved blueprint <code>{html.escape(str(path))}</code> is "
                "gone; save it again above.",
            )
            return
        try:
            workplan = forge_then_run_workplan(path, self.config.name)
        except ValueError as exc:
            self._workplan_msg("err", html.escape(str(exc)))
            return
        text = yaml_text(workplan)
        target = default_workplan_path(self.catalog, workplan.name, path.parent)
        shown = html.escape(str(target))
        if target.exists() and self._workplan_confirm != target:
            try:
                differs = target.read_text() != text
            except OSError:
                differs = True
            if differs:
                self._reset_workplan_confirm()
                self._workplan_confirm = target
                self.save_workplan_btn.description = "Confirm overwrite"
                self.save_workplan_btn.button_style = "danger"
                self._workplan_confirm_timer = schedule_confirm_reset(
                    self._reset_workplan_confirm
                )
                self._workplan_msg(
                    "warn",
                    f"A different workplan is already saved at <code>{shown}</code>. "
                    "Click Confirm overwrite to replace it.",
                )
                return
        self._reset_workplan_confirm()
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text)
        except OSError as exc:
            self._workplan_msg("err", html.escape(f"{type(exc).__name__}: {exc}"))
            return
        self._workplan_msg(
            "ok",
            f"Saved <code>{shown}</code>. Submit it with <code>cstar workplan submit "
            f"{shown}</code>, or open it in the Workplan builder to add compute "
            "settings.",
        )
        self.workplan_download.value = WorkplanBuilderPage._download_html(
            workplan, text
        )
        if self._workplan_hooks is not None:
            self._workplan_hooks.saved()

    def _on_open_workplan(self, _btn: Any) -> None:
        """Add the forge and run steps for the saved blueprint to the Workplan page."""
        hooks = self._workplan_hooks
        path = self._saved_path()
        if hooks is None or path is None or self.config is None:
            self._workplan_msg("err", _SAVE_FIRST)
            return
        try:
            hooks.open_steps(path, self.config.name)
        except ValueError as exc:
            self._workplan_msg("err", html.escape(str(exc)))
            return
        self._workplan_msg(
            "ok", "Opened the Workplan page with the forge and run steps."
        )

    def _build_run_command(self, blueprint_path: str) -> list[str]:
        """Command the Run button invokes: ``cstar blueprint run <path>``.

        The same command the docs give for both pipeline steps, and the same one
        the emitted ``ucla_roms`` blueprint is run with -- the button exposes no
        per-run flags, so the full-option ``cstar forge run`` passthrough buys it
        nothing. Resolving ``application: forge`` this way goes through C-Star's
        registry, which finds the app as the in-tree
        ``cstar.applications.forge`` module -- no separate install required.

        Uses the ``cstar`` console script installed alongside the running
        interpreter, so the subprocess stays in this environment rather than
        taking whatever is first on PATH. Where that script is absent (the
        ``cstar`` entry point not installed), falls back to
        ``python -m cstar.cli.cli forge run``, the same unified CLI's ``forge``
        subcommand invoked as a module.
        """
        import sys

        cstar_exe = Path(sys.executable).with_name("cstar")
        if cstar_exe.exists():
            return [str(cstar_exe), "blueprint", "run", blueprint_path]
        return [sys.executable, "-m", "cstar.cli.cli", "forge", "run", blueprint_path]

    def _on_run(self, _):
        _schedule_coroutine(self._run_async())

    async def _run_async(self):
        """Save the current blueprint through the wizard's ``save_blueprint``, then
        launch the C-Star CLI as a subprocess and stream its combined stdout/stderr
        into ``run_output`` line by line as it arrives (not all at once at the end).

        A save that is refused or fails is reported in ``run_status`` and starts
        nothing, so the previous run's log stays.
        """
        import asyncio

        self.run_status.value = "<i>saving blueprint…</i>"
        try:
            saved = self._save_blueprint(self.save_target)
        except Exception as exc:
            self.run_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        if saved is SaveRefused.BOUNDARIES:
            self.run_status.value = components.status_span(
                "err",
                "Run aborted — open boundaries could "
                "not be derived from the grid (see the Domain-derived properties "
                "status above). Fix the grid or set boundaries manually, then "
                "retry.",
            )
            return
        if saved is SaveRefused.INVALID:
            self.run_status.value = components.status_span(
                "err", "Nothing to run — config is invalid."
            )
            return

        self._run_in_progress = True
        self.run_btn.disabled = True
        self.run_output.clear_output(wait=True)
        cmd: list[str] | None = None
        try:
            cmd = self._build_run_command(str(saved))
            self.run_status.value = f"<i>running: {' '.join(cmd)}</i>"
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            # Read fixed-size chunks and reassemble lines ourselves rather than
            # `async for line in proc.stdout` (StreamReader.readline), which raises
            # ValueError, "Separator is not found, and chunk exceed the limit", when a
            # child emits 64 KiB without a newline -- e.g. a \r-redrawn progress bar.
            # append_stdout (not `with self.run_output: print(...)`) appends directly
            # to the widget's output list, so lines land in the log whether or not a
            # live Jupyter kernel is routing stdout via iopub messaging.
            buf = b""
            while True:
                chunk = await proc.stdout.read(_STREAM_READ_SIZE)
                if not chunk:
                    break
                lines, buf = _drain_stream_buffer(buf + chunk)
                for line in lines:
                    self.run_output.append_stdout(line)
            lines, buf = _drain_stream_buffer(buf, at_eof=True)
            for line in lines:
                self.run_output.append_stdout(line)
            code = await proc.wait()
            # On success the log above names the emitted ucla_roms blueprint (the
            # runner logs where it published it). Point at it rather than restating
            # a path this widget would have to derive for itself.
            self.run_status.value = (
                components.status_span("ok", "✓ finished")
                + " — run the emitted "
                + "UCLA-ROMS blueprint named in the log with <code>cstar blueprint "
                "run &lt;path&gt;</code>"
                if code == 0
                else components.status_span("err", f"exited with code {code}")
            )
        except Exception as exc:
            # Name the command in the status so a failure isn't a context-free
            # error string (cmd is None only if building the command threw).
            where = f" while running: {' '.join(cmd)}" if cmd else ""
            self.run_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}{where}"
            )
        finally:
            self._run_in_progress = False
            self.sync_next_steps()
