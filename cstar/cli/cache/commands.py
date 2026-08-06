"""Commands for inspecting and managing the artifact cache."""

import shutil
import typing as t

import typer
from rich.console import Console
from rich.table import Column, Table

from cstar.caching.models import CacheEntry, CacheTier
from cstar.caching.store import CacheManager, CacheStore
from cstar.orchestration.serialization import model_to_yaml

app = typer.Typer()
console = Console()

_TIER_CHOICES: t.Final[str] = ", ".join(tier.value for tier in CacheTier)


def _parse_tier(value: str | None) -> CacheTier | None:
    """Convert a user-supplied tier name to a `CacheTier`, or `None`."""
    if not value:
        return None
    try:
        return CacheTier(value.lower())
    except ValueError as ex:
        msg = f"Invalid tier {value!r}; expected one of: {_TIER_CHOICES}"
        raise typer.BadParameter(msg) from ex


def _human_size(num_bytes: int) -> str:
    """Format a byte count for display."""
    size = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{int(size)} B"


def _short_function(function: str, parts: int = 2) -> str:
    """Return the trailing segments of a module-qualified function name."""
    return ".".join(function.split(".")[-parts:])


def _entry_row(entry: CacheEntry) -> tuple[str, ...]:
    """Build a display row for an entry."""
    manifest = entry.manifest
    return (
        manifest.key[:12],
        manifest.label,
        _short_function(manifest.function),
        entry.tier.value,
        manifest.provenance.created_at.strftime("%Y-%m-%d %H:%M"),
        str(len(manifest.files)),
        _human_size(entry.total_size_bytes),
    )


@app.command(name="list")
def list_entries(
    function: t.Annotated[
        str,
        typer.Option(
            "--function",
            help="Only show entries whose function name contains this substring.",
        ),
    ] = "",
    tier: t.Annotated[
        str,
        typer.Option(
            "--tier",
            help=f"Only show entries from one tier ({_TIER_CHOICES}).",
        ),
    ] = "",
) -> None:
    """List the entries in the artifact cache."""
    tier_filter = _parse_tier(tier)
    manager = CacheManager.from_env()

    table = Table(
        Column("key", no_wrap=True, min_width=12),
        "label",
        "function",
        "tier",
        "created",
        "files",
        "size",
        title="Artifact cache entries",
    )

    count = 0
    for entry in manager.iter_all():
        if tier_filter is not None and entry.tier != tier_filter:
            continue
        if function and function not in entry.manifest.function:
            continue
        table.add_row(*_entry_row(entry))
        count += 1

    if count:
        console.print(table)
    else:
        console.print("No cache entries found.")

    _print_roots(manager)


def _print_roots(manager: CacheManager) -> None:
    """Show the resolved cache roots beneath a listing."""
    console.print(f"personal cache: {manager.personal.root}")
    if manager.group is not None:
        console.print(f"group cache:    {manager.group.root}")
    else:
        console.print("group cache:    <not configured>")


@app.command(name="show")
def show_entry(
    reference: t.Annotated[
        str,
        typer.Argument(help="A cache key prefix (>= 6 characters) or entry label."),
    ],
) -> None:
    """Show the full manifest of a cache entry."""
    manager = CacheManager.from_env()
    entry = _resolve_or_exit(manager, reference)

    console.print(f"[bold]tier:[/bold] {entry.tier.value}")
    console.print(f"[bold]location:[/bold] {entry.entry_dir}")
    console.print(model_to_yaml(entry.manifest))


@app.command(name="promote")
def promote_entry(
    reference: t.Annotated[
        str,
        typer.Argument(help="A cache key prefix (>= 6 characters) or entry label."),
    ],
    delete_source: t.Annotated[
        bool,
        typer.Option(
            "--delete-source",
            help="Remove the personal-tier copy after promotion. Note that this "
            "breaks symlinks placed in output directories by prior runs.",
        ),
    ] = False,
    yes: t.Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip the confirmation prompt."),
    ] = False,
) -> None:
    """Promote a personal cache entry into the shared group cache."""
    manager = CacheManager.from_env()
    entry = _resolve_or_exit(manager, reference)

    if manager.group is None:
        console.print(
            "[red]No group cache is configured; set CSTAR_CACHE_GROUP_ROOT "
            "to enable the group tier.[/red]",
        )
        raise typer.Exit(code=1)

    if not yes:
        prompt = (
            f"Promote {entry.manifest.key[:12]} ({entry.manifest.function}, "
            f"{_human_size(entry.total_size_bytes)}) to the group cache?"
        )
        typer.confirm(prompt, abort=True)

    promoted = manager.promote(entry, delete_source=delete_source)
    console.print(
        f"Promoted {promoted.manifest.key[:12]} into the group cache at "
        f"{promoted.entry_dir}",
    )


@app.command(name="clear")
def clear_entries(
    reference: t.Annotated[
        str,
        typer.Argument(
            help="A cache key prefix (>= 6 characters) or entry label. "
            "Omit with --all to clear an entire tier.",
        ),
    ] = "",
    tier: t.Annotated[
        str,
        typer.Option(
            "--tier",
            help=f"The tier to clear from ({_TIER_CHOICES}). "
            "Defaults to personal; clearing group entries requires --tier group.",
        ),
    ] = CacheTier.personal.value,
    clear_all: t.Annotated[
        bool,
        typer.Option("--all", help="Clear every entry in the selected tier."),
    ] = False,
    yes: t.Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip the confirmation prompt."),
    ] = False,
    dry_run: t.Annotated[
        bool,
        typer.Option("--dry-run", help="Show what would be removed without removing."),
    ] = False,
) -> None:
    """Remove entries from the artifact cache."""
    tier_choice = _parse_tier(tier)
    if tier_choice is None:
        msg = f"A tier is required; expected one of: {_TIER_CHOICES}"
        raise typer.BadParameter(msg)

    if bool(reference) == clear_all:
        msg = "Provide either an entry reference or --all (not both)."
        raise typer.BadParameter(msg)

    manager = CacheManager.from_env()
    store = manager.personal if tier_choice == CacheTier.personal else manager.group
    if store is None:
        console.print("[red]No group cache is configured.[/red]")
        raise typer.Exit(code=1)

    targets = (
        list(store.iter_entries())
        if clear_all
        else [_resolve_or_exit(manager, reference, tier=tier_choice)]
    )
    staging_leftovers = (
        sorted(store.staging_dir.glob("*"))
        if clear_all and store.staging_dir.is_dir()
        else []
    )

    if not targets and not staging_leftovers:
        console.print("Nothing to remove.")
        return

    for entry in targets:
        if dry_run:
            console.print(f"would remove: {entry.entry_dir}")
            continue
        if not yes:
            typer.confirm(f"Remove {entry.entry_dir}?", abort=True)
        _remove_entry(store, entry)
        console.print(f"removed: {entry.entry_dir}")

    # crashed runs (e.g. SIGKILL) can strand staging directories that no
    # cache lookup will ever surface; --all is the reaping opportunity
    for leftover in staging_leftovers:
        if dry_run:
            console.print(f"would remove staging leftover: {leftover}")
            continue
        shutil.rmtree(leftover, ignore_errors=True)
        console.print(f"removed staging leftover: {leftover}")


def _remove_entry(store: CacheStore, entry: CacheEntry) -> None:
    """Remove one entry via its owning store."""
    store.remove(entry)


def _resolve_or_exit(
    manager: CacheManager,
    reference: str,
    tier: CacheTier | None = None,
) -> CacheEntry:
    """Resolve a reference or exit with a readable error."""
    from cstar.caching.store import CacheError

    try:
        return manager.resolve(reference, tier=tier)
    except CacheError as ex:
        console.print(f"[red]{ex}[/red]")
        raise typer.Exit(code=1) from ex
