from pathlib import Path

from typer.testing import CliRunner

from cstar.cli.admin.migrate_outputs import MigrationReport, app, migrate_output_layout


def test_migrate_output_layout_nested_subtask_dir(tmp_path: Path) -> None:
    """Verify that a `joined_output` directory nested under `tasks/<slug>/`
    is found and migrated the same as a top-level one.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to build the fake run/step tree in.
    """
    step_dir = tmp_path / "tasks" / "my-step"
    jo_dir = step_dir / "joined_output"
    jo_dir.mkdir(parents=True)
    src = jo_dir / "out_a.nc"
    src.write_text("data")

    report = migrate_output_layout(tmp_path)

    out_dir = step_dir / "output"
    dst = out_dir / "out_a.nc"
    assert (src, dst) in report.moved
    assert dst.is_file()
    assert dst.read_text() == "data"
    assert not jo_dir.exists()
    assert jo_dir in report.removed_dirs


def test_migrate_output_layout_pio_style_run(tmp_path: Path) -> None:
    """Verify a PIO-style run (empty `output`, populated `joined_output`) is
    migrated by moving every file into `output` and removing `joined_output`.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to build the fake run/step tree in.
    """
    (tmp_path / "output").mkdir()
    jo_dir = tmp_path / "joined_output"
    jo_dir.mkdir()
    (jo_dir / "out_final.nc").write_text("final data")

    report = migrate_output_layout(tmp_path)

    out_final = tmp_path / "output" / "out_final.nc"
    assert out_final.is_file()
    assert out_final.read_text() == "final data"
    assert not jo_dir.exists()
    assert jo_dir in report.removed_dirs
    assert report.skipped_collisions == []


def test_migrate_output_layout_non_pio_style_run(tmp_path: Path) -> None:
    """Verify a non-PIO-style run (partitioned pieces already in `output`,
    a joined file in `joined_output`) ends with the joined file in `output`
    and the partition pieces relocated to `temp_output`.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to build the fake run/step tree in.
    """
    out_dir = tmp_path / "output"
    out_dir.mkdir()
    piece_a = out_dir / "output_rst.20120201000000.000.nc"
    piece_a.write_text("piece-a")
    piece_b = out_dir / "output_rst.20120201000000.001.nc"
    piece_b.write_text("piece-b")

    jo_dir = tmp_path / "joined_output"
    jo_dir.mkdir()
    joined_src = jo_dir / "final_output.nc"
    joined_src.write_text("joined data")

    report = migrate_output_layout(tmp_path)

    temp_dir = tmp_path / "temp_output"
    assert (out_dir / "final_output.nc").is_file()
    assert (out_dir / "final_output.nc").read_text() == "joined data"
    assert not jo_dir.exists()

    assert not piece_a.exists()
    assert not piece_b.exists()
    assert (temp_dir / "output_rst.20120201000000.000.nc").read_text() == "piece-a"
    assert (temp_dir / "output_rst.20120201000000.001.nc").read_text() == "piece-b"

    assert (piece_a, temp_dir / piece_a.name) in report.moved
    assert (piece_b, temp_dir / piece_b.name) in report.moved
    assert (joined_src, out_dir / "final_output.nc") in report.moved


def test_migrate_output_layout_collision_left_in_place(tmp_path: Path) -> None:
    """Verify a filename collision between `joined_output` and a pre-existing
    file in `output` leaves both files untouched and is reported.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to build the fake run/step tree in.
    """
    out_dir = tmp_path / "output"
    out_dir.mkdir()
    existing = out_dir / "dup.nc"
    existing.write_text("existing data")

    jo_dir = tmp_path / "joined_output"
    jo_dir.mkdir()
    colliding = jo_dir / "dup.nc"
    colliding.write_text("colliding data")

    report = migrate_output_layout(tmp_path)

    # Neither file was touched.
    assert existing.read_text() == "existing data"
    assert colliding.read_text() == "colliding data"
    assert colliding in report.skipped_collisions

    # `joined_output` is not removed since it still holds the un-migrated file.
    assert jo_dir.exists()
    assert jo_dir not in report.removed_dirs
    assert report.moved == []


def test_migrate_output_layout_removes_symlink_only_gather_dir(
    tmp_path: Path,
) -> None:
    """Verify a `joined_output` directory holding only symlinks (an old
    run-level `cstar workplan gather` result) is removed outright rather
    than migrated.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to build the fake run/step tree in.
    """
    jo_dir = tmp_path / "joined_output"
    jo_dir.mkdir()
    (jo_dir / "linked.nc").symlink_to("no-longer-exists.nc")

    report = migrate_output_layout(tmp_path)

    assert not jo_dir.exists()
    assert jo_dir in report.gather_dirs_removed
    assert report.moved == []
    assert report.removed_dirs == []


def _build_mixed_tree(root: Path) -> None:
    """Build a tree exercising every migration case under one root.

    Parameters
    ----------
    root : Path
        The directory to build the tree in.
    """
    # Nested subtask, plain move.
    nested_jo = root / "tasks" / "my-step" / "joined_output"
    nested_jo.mkdir(parents=True)
    (nested_jo / "out_a.nc").write_text("data")

    # Non-PIO step: partition pieces already in `output`, joined file in
    # `joined_output`.
    step_dir = root / "tasks" / "roms-step"
    (step_dir / "output").mkdir(parents=True)
    (step_dir / "output" / "output_rst.20120201000000.000.nc").write_text("piece")
    jo_dir = step_dir / "joined_output"
    jo_dir.mkdir()
    (jo_dir / "final_output.nc").write_text("joined")

    # Collision.
    collide_dir = root / "tasks" / "collide-step"
    (collide_dir / "output").mkdir(parents=True)
    (collide_dir / "output" / "dup.nc").write_text("existing")
    collide_jo = collide_dir / "joined_output"
    collide_jo.mkdir()
    (collide_jo / "dup.nc").write_text("colliding")

    # Old run-level gather result (symlink-only).
    gather_dir = root / "joined_output"
    gather_dir.mkdir()
    (gather_dir / "linked.nc").symlink_to("no-longer-exists.nc")


def _normalize_report(report: MigrationReport, root: Path) -> dict[str, object]:
    """Render a `MigrationReport` as root-relative strings for comparison.

    `migrate_output_layout` resolves its `root` argument, so two reports
    produced from equivalent trees under two different roots only compare
    equal once every path in them is expressed relative to its own root.

    Parameters
    ----------
    report : MigrationReport
        The report to normalize.
    root : Path
        The (possibly unresolved) root the report was produced from; it is
        resolved the same way `migrate_output_layout` resolves its own
        `root` argument, so `Path.relative_to` succeeds against the
        report's already-resolved paths.

    Returns
    -------
    dict[str, object]
        A JSON-comparable, order-independent view of the report.
    """
    root = root.expanduser().resolve()

    def rel(p: Path) -> str:
        return str(p.relative_to(root))

    return {
        "moved": sorted((rel(src), rel(dst)) for src, dst in report.moved),
        "skipped_collisions": sorted(rel(p) for p in report.skipped_collisions),
        "removed_dirs": sorted(rel(p) for p in report.removed_dirs),
        "gather_dirs_removed": sorted(rel(p) for p in report.gather_dirs_removed),
    }


def test_migrate_output_layout_dry_run_matches_real_run(tmp_path: Path) -> None:
    """Verify `dry_run=True` leaves the tree untouched and reports exactly
    the same plan (moves, skips, and removals, path-for-path) that a real
    run performs on an identical copy of the tree.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to build two identical fake run trees in.
    """
    dry_root = tmp_path / "dry"
    real_root = tmp_path / "real"
    dry_root.mkdir()
    real_root.mkdir()
    _build_mixed_tree(dry_root)
    _build_mixed_tree(real_root)

    before = sorted(str(p.relative_to(dry_root)) for p in dry_root.rglob("*"))

    dry_report = migrate_output_layout(dry_root, dry_run=True)

    after = sorted(str(p.relative_to(dry_root)) for p in dry_root.rglob("*"))
    assert before == after

    # The plan still reflects the intended changes.
    assert dry_report.moved
    assert dry_report.skipped_collisions
    assert dry_report.removed_dirs
    assert dry_report.gather_dirs_removed

    real_report = migrate_output_layout(real_root, dry_run=False)

    assert _normalize_report(dry_report, dry_root) == _normalize_report(
        real_report, real_root
    )


def test_cli_admin_migrate_outputs_invocation(tmp_path: Path) -> None:
    """Verify the `migrate-outputs` typer command runs end-to-end, mirroring
    the invocation style used by the `clean` command's tests.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to build the fake run tree in.
    """
    jo_dir = tmp_path / "joined_output"
    jo_dir.mkdir()
    (jo_dir / "out_final.nc").write_text("final data")

    runner = CliRunner()
    result = runner.invoke(app, [str(tmp_path)], color=False, catch_exceptions=False)

    assert result.exit_code == 0
    assert (tmp_path / "output" / "out_final.nc").read_text() == "final data"
    assert not jo_dir.exists()
    assert "Summary" in result.stdout


def test_cli_admin_migrate_outputs_dry_run_flag(tmp_path: Path) -> None:
    """Verify `--dry-run` prefixes output and performs no changes."""
    jo_dir = tmp_path / "joined_output"
    jo_dir.mkdir()
    (jo_dir / "out_final.nc").write_text("final data")

    runner = CliRunner()
    result = runner.invoke(
        app,
        [str(tmp_path), "--dry-run"],
        color=False,
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "[dry-run]" in result.stdout
    assert jo_dir.exists()
    assert (jo_dir / "out_final.nc").exists()
    assert not (tmp_path / "output").exists()
