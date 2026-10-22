import csv
import datetime
import io
import json
import logging
import shutil
import typing as t
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from click.testing import Result
from rich.console import Console
from rich.table import Table
from rich.text import Text
from typer.testing import CliRunner

from cstar.cli.workplan.ls import (
    FIELD_NAMES,
    FORMATS,
    INCLUSIONS,
    WHERE_INCLUSIONS,
    ItemView,
    adapt_runs_to_views,
    csv_formatter,
    filter_size,
    filter_time,
    format_runid_filter,
    formatters,
    json_formatter,
    ls_runs,
    sorters,
    table_formatter,
)
from cstar.cli.workplan.ls import app as app_ls
from cstar.orchestration.index import RunIndex
from cstar.orchestration.models import LEGACY_TRANSFORMED_NAME_SUFFIX
from cstar.orchestration.tracking import (
    KEY_RUN_NAME,
    KEY_RUN_UUID,
    TrackingRepository,
    WorkplanRun,
)
from cstar.tests.unit_tests.orchestration.manifest_runs import BuiltRun

LS_LOGGER = "cstar.cli.workplan.ls"

UTC = datetime.UTC
T0 = datetime.datetime(2026, 9, 12, 3, 4, 0, tzinfo=UTC)
"""An arbitrary tz-aware reference time used to build test data."""


def make_run(
    run_id: str,
    size: int | None = None,
    name: str | None = None,
    start: datetime.datetime | None = None,
) -> WorkplanRun:
    """Build a `WorkplanRun` with dummy paths for filter tests.

    Parameters
    ----------
    run_id : str
        The unique run identifier.
    size : int | None
        When supplied, stored as a single step's measured size; `None`
        leaves `step_sizes` empty (i.e. unmeasured).
    name : str | None
        When supplied, stored in metadata under the run-name key.
    start : datetime.datetime | None
        When supplied, used as the run start time.

    Returns
    -------
    WorkplanRun
    """
    metadata: dict[str, str] = {}

    if name is not None:
        metadata[KEY_RUN_NAME] = name

    step_sizes: dict[str, int] = {} if size is None else {"step": size}

    kwargs: dict[str, t.Any] = {} if start is None else {"start_at": start}

    return WorkplanRun(
        workplan_path=Path(f"/tmp/{run_id}/workplan.yaml"),
        trx_workplan_path=Path(f"/tmp/{run_id}/trx.yaml"),
        output_path=Path(f"/tmp/{run_id}/out"),
        run_id=run_id,
        metadata=metadata,
        step_sizes=step_sizes,
        **kwargs,
    )


def make_view(
    run_id: str = "run-1",
    name: str = "plan-a",
    raw_size: int = 5,
    raw_start: datetime.datetime = T0,
    format: FORMATS = "table",
) -> ItemView:
    """Build an `ItemView` for sorter and formatter tests.

    Parameters
    ----------
    run_id : str
        The unique run identifier.
    name : str
        The workplan name.
    raw_size : int
        The disk usage in MB (-1 means not yet computed).
    raw_start : datetime.datetime
        The run start time.
    format : FORMATS
        The output format the view will be rendered with.

    Returns
    -------
    ItemView
    """
    return ItemView(
        run_id=run_id,
        name=name,
        raw_size=raw_size,
        raw_start=raw_start,
        format=format,
    )


# ---------------------------------------------------------------------------
# ItemView display fields
# ---------------------------------------------------------------------------


def test_itemview_size_shows_hint_for_unsized_table_rows() -> None:
    """Verify an uncomputed size renders the refresh hint in table format."""
    view = make_view(run_id="run-9", raw_size=-1, format="table")

    assert "cstar run status run-9" in view.size
    assert "--size" in view.size


@pytest.mark.parametrize("format", ["csv", "json"])
def test_itemview_size_shows_raw_value_for_machine_formats(format: FORMATS) -> None:
    """Verify an uncomputed size is not replaced by the hint in csv/json.

    Parameters
    ----------
    format : FORMATS
        The machine-readable output format under test.
    """
    view = make_view(raw_size=-1, format=format)

    assert view.size == "-1MB"


@pytest.mark.parametrize("format", ["table", "csv", "json"])
def test_itemview_size_includes_units(format: FORMATS) -> None:
    """Verify a computed size renders with MB units in every format.

    Parameters
    ----------
    format : FORMATS
        The output format under test.
    """
    view = make_view(raw_size=42, format=format)

    assert view.size == "42MB"


def test_itemview_start_renders_local_time_for_tables() -> None:
    """Verify the table start time is local wall-clock at minute precision."""
    raw = datetime.datetime(2026, 9, 12, 3, 4, 59, tzinfo=UTC)
    view = make_view(raw_start=raw, format="table")

    assert view.start == raw.astimezone().strftime("%Y-%m-%d %H:%M")


@pytest.mark.parametrize("format", ["csv", "json"])
def test_itemview_start_renders_isoformat_for_machine_formats(
    format: FORMATS,
) -> None:
    """Verify csv/json start times carry the full offset-aware timestamp.

    Parameters
    ----------
    format : FORMATS
        The machine-readable output format under test.
    """
    view = make_view(raw_start=T0, format=format)

    assert view.start == "2026-09-12T03:04:00+00:00"


# ---------------------------------------------------------------------------
# filter_size
# ---------------------------------------------------------------------------


def test_filter_size_without_bounds_returns_copy_of_all_runs() -> None:
    """Verify no bounds means no filtering and the input is not aliased."""
    runs = [make_run("a", size=1), make_run("b", size=2)]

    actual = filter_size(runs)

    assert actual == runs
    assert actual is not runs


@pytest.mark.parametrize(
    ("lt_filter", "gt_filter", "expected"),
    [
        pytest.param(5, None, ["a", "b"], id="max only"),
        pytest.param(None, 5, ["b", "c"], id="min only"),
        pytest.param(9, 1, ["b"], id="window"),
        pytest.param(10, None, ["a", "b", "c"], id="max::boundary is inclusive"),
        pytest.param(None, 10, ["c"], id="min::boundary is inclusive"),
        pytest.param(0, None, ["a"], id="max of zero is honored"),
        pytest.param(None, 0, ["a", "b", "c"], id="min of zero is honored"),
        pytest.param(4, 6, [], id="empty window"),
    ],
)
def test_filter_size_bounds(
    lt_filter: int | None,
    gt_filter: int | None,
    expected: list[str],
) -> None:
    """Verify min/max size bounds, including zero-valued bounds.

    Parameters
    ----------
    lt_filter : int | None
        The maximum size to include.
    gt_filter : int | None
        The minimum size to include.
    expected : list[str]
        The run-ids expected to remain after filtering.
    """
    runs = [make_run("a", size=0), make_run("b", size=5), make_run("c", size=10)]

    actual = filter_size(runs, lt_filter, gt_filter)

    assert [r.run_id for r in actual] == expected


def test_filter_size_never_drops_unsized_runs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify unmeasured runs pass every filter and warn once."""
    runs = [make_run("sized", size=100), make_run("unsized")]

    with caplog.at_level(logging.WARNING, logger=LS_LOGGER):
        actual = filter_size(runs, lt_filter=10)

    assert [r.run_id for r in actual] == ["unsized"]
    assert any("unsized" in r.message for r in caplog.records)


def test_filter_size_does_not_warn_when_all_runs_are_sized(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify no warning is emitted when every run has a computed size."""
    runs = [make_run("a", size=1)]

    with caplog.at_level(logging.WARNING, logger=LS_LOGGER):
        filter_size(runs, lt_filter=10)

    assert not caplog.records


# ---------------------------------------------------------------------------
# filter_time
# ---------------------------------------------------------------------------


def test_filter_time_without_bounds_returns_copy_of_all_runs() -> None:
    """Verify no bounds means no filtering and the input is not aliased."""
    runs = [make_run("a"), make_run("b")]

    actual = filter_time(runs)

    assert actual == runs
    assert actual is not runs


@pytest.mark.parametrize(
    ("lt_offset", "gt_offset", "expected"),
    [
        pytest.param(1, None, ["a", "b"], id="max only"),
        pytest.param(None, 1, ["b", "c"], id="min only"),
        pytest.param(1, 1, ["b"], id="window"),
        pytest.param(2, None, ["a", "b", "c"], id="max::boundary is inclusive"),
        pytest.param(None, 2, ["c"], id="min::boundary is inclusive"),
        pytest.param(None, 3, [], id="empty results"),
    ],
)
def test_filter_time_bounds(
    lt_offset: int | None,
    gt_offset: int | None,
    expected: list[str],
) -> None:
    """Verify min/max time bounds against runs started at hourly offsets.

    Runs a, b, c start at T0, T0+1h, and T0+2h; bounds are given as hour
    offsets from T0.

    Parameters
    ----------
    lt_offset : int | None
        The maximum start time to include, as hours after T0.
    gt_offset : int | None
        The minimum start time to include, as hours after T0.
    expected : list[str]
        The run-ids expected to remain after filtering.
    """

    def at_hour(offset: int) -> datetime.datetime:
        return T0 + datetime.timedelta(hours=offset)

    runs = [make_run(rid, start=at_hour(i)) for i, rid in enumerate("abc")]
    lt_filter = None if lt_offset is None else at_hour(lt_offset)
    gt_filter = None if gt_offset is None else at_hour(gt_offset)

    actual = filter_time(runs, lt_filter, gt_filter)

    assert [r.run_id for r in actual] == expected


def test_filter_time_normalizes_mixed_offsets() -> None:
    """Verify equal instants expressed in different zones compare as equal."""
    plus_two = datetime.timezone(datetime.timedelta(hours=2))
    run = make_run("a", start=T0.astimezone(plus_two))

    actual = filter_time([run], lt_filter=T0, gt_filter=T0)

    assert [r.run_id for r in actual] == ["a"]


# ---------------------------------------------------------------------------
# sorters
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        pytest.param("name", ["z", "x", "y"], id="name"),
        pytest.param("run-id", ["x", "y", "z"], id="run-id"),
        pytest.param("size", ["y", "z", "x"], id="size"),
        pytest.param("time", ["y", "x", "z"], id="time"),
    ],
)
@pytest.mark.parametrize("reverse", [False, True], ids=["ascending", "descending"])
def test_sorters_order_views_by_field(
    field: FIELD_NAMES,
    expected: list[str],
    reverse: bool,
) -> None:
    """Verify each sorter orders by its field and honors the reverse flag.

    Parameters
    ----------
    field : FIELD_NAMES
        The sorter under test.
    expected : list[str]
        The run-ids in expected ascending order for the field.
    reverse : bool
        Whether to reverse the sort order.
    """
    views = [
        make_view(run_id="x", name="bbb", raw_size=10, raw_start=T0),
        make_view(
            run_id="y",
            name="ccc",
            raw_size=-1,
            raw_start=T0 - datetime.timedelta(hours=1),
        ),
        make_view(
            run_id="z",
            name="aaa",
            raw_size=2,
            raw_start=T0 + datetime.timedelta(hours=1),
        ),
    ]

    actual = sorters[field](views, reverse)

    ordered = list(reversed(expected)) if reverse else expected
    assert [v.run_id for v in actual] == ordered


def test_size_sorter_is_numeric_not_lexicographic() -> None:
    """Verify sizes sort as integers (2 < 10, not "10" < "2")."""
    views = [make_view(run_id=str(size), raw_size=size) for size in (10, 2, 100)]

    actual = sorters["size"](views, False)

    assert [v.raw_size for v in actual] == [2, 10, 100]


def test_time_sorter_uses_full_timestamp_precision() -> None:
    """Verify runs in the same display minute sort by their true start time."""
    later = make_view(run_id="later", raw_start=T0.replace(second=59))
    earlier = make_view(run_id="earlier", raw_start=T0.replace(second=1))
    assert later.start == earlier.start  # identical at display precision

    actual = sorters["time"]([later, earlier], False)

    assert [v.run_id for v in actual] == ["earlier", "later"]


def test_sorters_cover_every_sortable_field() -> None:
    """Verify each FIELD_NAMES option has a registered sorter."""
    assert set(t.get_args(FIELD_NAMES)) == set(sorters)


# ---------------------------------------------------------------------------
# formatters
# ---------------------------------------------------------------------------


def render(renderable: object) -> str:
    """Render a rich renderable to plain text.

    Parameters
    ----------
    renderable : object
        Any object printable by a rich console.

    Returns
    -------
    str
        The rendered console output.
    """
    buffer = io.StringIO()
    console = Console(file=buffer, width=200)
    console.print(renderable)
    return buffer.getvalue()


def test_csv_formatter_writes_header_and_rows_in_order() -> None:
    """Verify CSV output has a deterministic header and preserves row order."""
    views = [
        make_view(run_id="r2", name="beta", raw_size=7, format="csv"),
        make_view(run_id="r1", name="alpha", raw_size=-1, format="csv"),
    ]

    document = csv_formatter(views)

    assert isinstance(document, Text)
    rows = list(csv.reader(io.StringIO(document.plain)))
    assert rows[0] == sorted(INCLUSIONS)
    by_header = [dict(zip(rows[0], row)) for row in rows[1:]]
    assert [r["run_id"] for r in by_header] == ["r2", "r1"]
    assert by_header[0]["name"] == "beta"
    assert by_header[0]["size"] == "7MB"
    assert by_header[1]["size"] == "-1MB"
    assert by_header[0]["start"] == "2026-09-12T03:04:00+00:00"


def test_csv_formatter_with_no_rows_emits_header_only() -> None:
    """Verify an empty run list still yields a parseable header row."""
    document = csv_formatter([])

    assert isinstance(document, Text)
    rows = list(csv.reader(io.StringIO(document.plain)))
    assert rows == [sorted(INCLUSIONS)]


def test_json_formatter_emits_included_fields_only() -> None:
    """Verify JSON output parses and carries exactly the included fields."""
    views = [make_view(run_id="r1", name="alpha", raw_size=7, format="json")]

    document = json_formatter(views)

    assert isinstance(document, Text)
    parsed = json.loads(document.plain)
    assert set(parsed) == {"data"}
    assert len(parsed["data"]) == 1
    row = parsed["data"][0]
    assert set(row) == INCLUSIONS
    assert row == {
        "run_id": "r1",
        "name": "alpha",
        "size": "7MB",
        "start": "2026-09-12T03:04:00+00:00",
    }


def test_json_formatter_with_no_rows_emits_empty_data() -> None:
    """Verify an empty run list yields an empty data container."""
    document = json_formatter([])

    assert isinstance(document, Text)
    assert json.loads(document.plain) == {"data": []}


def test_table_formatter_renders_one_row_per_view() -> None:
    """Verify the table has the expected columns and cell contents."""
    views = [
        make_view(run_id="r1", name="alpha", raw_size=7),
        make_view(run_id="r2", name="beta", raw_size=-1),
    ]

    table = table_formatter(views)

    assert isinstance(table, Table)
    assert table.row_count == 2
    assert [c.header for c in table.columns] == [
        "run-id",
        "workplan",
        "disk space",
        "start time",
    ]
    rendered = render(table)
    assert "r1" in rendered
    assert "alpha" in rendered
    assert "7MB" in rendered
    assert "cstar run status r2" in rendered
    assert T0.astimezone().strftime("%Y-%m-%d %H:%M") in rendered


def test_table_formatter_with_no_rows_renders_empty_table() -> None:
    """Verify an empty run list produces a table with headers and no rows."""
    table = table_formatter([])

    assert isinstance(table, Table)
    assert table.row_count == 0


def test_formatters_cover_every_format_option() -> None:
    """Verify each FORMATS option has a registered formatter."""
    assert set(t.get_args(FORMATS)) == set(formatters)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("MyRun", "myrun", id="mixed case"),
        pytest.param("already-lower", "already-lower", id="no-op"),
        pytest.param("", "", id="empty"),
    ],
)
def test_format_runid_filter_casefolds(value: str, expected: str) -> None:
    """Verify the --run-filter callback normalizes case for comparisons.

    Parameters
    ----------
    value : str
        The raw option value supplied by the user.
    expected : str
        The normalized value expected after the callback.
    """
    assert format_runid_filter(value) == expected


# ---------------------------------------------------------------------------
# adapt_runs_to_views
# ---------------------------------------------------------------------------


def stub_load_workplans(
    names: Mapping[Path, str | None],
    calls: list[list[Path]] | None = None,
) -> t.Any:
    """Build a `load_workplans` replacement serving canned workplan stubs.

    Parameters
    ----------
    names : Mapping[Path, str | None]
        Workplan name to store in the cache per path; a missing or falsy
        entry simulates a failed load (the path is left out of the cache).
    calls : list[list[Path]] | None
        When supplied, receives the path list of each invocation.

    Returns
    -------
    t.Any
        An async callable matching the `load_workplans` signature.
    """

    async def _stub(paths: Sequence[Path], cache: dict[Path, t.Any]) -> None:
        if calls is not None:
            calls.append(list(paths))
        for p in paths:
            if names.get(p):
                cache[p] = SimpleNamespace(name=names[p])

    return _stub


async def test_adapt_runs_builds_views_from_cached_workplans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify names, sizes, start times, and format are threaded into views."""
    run = make_run("r1", size=7, start=T0)
    names = {run.trx_workplan_path: "plan-a"}
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.load_workplans", stub_load_workplans(names)
    )

    views = await adapt_runs_to_views([run], {}, "json")

    assert len(views) == 1
    view = views[0]
    assert (view.run_id, view.name, view.raw_size) == ("r1", "plan-a", 7)
    assert view.raw_start == T0
    assert view.format == "json"


@pytest.mark.parametrize(
    "name",
    ["Workplan 0", "Workplan 1", "Workplan 2"],
)
async def test_adapt_runs_builds_views_from_run_metadata(name: str) -> None:
    """Verify that the name in the run metadata is used when the workplan can't be
    loaded from disk.
    """
    run = make_run("r1", size=7, start=T0, name=name)

    views = await adapt_runs_to_views([run], {}, "json")

    assert len(views) == 1
    view = views[0]
    assert view.run_id == "r1"
    assert view.raw_size == 7
    assert view.raw_start == T0
    assert view.format == "json"
    assert view.name == name


async def test_adapt_runs_strips_the_legacy_suffix_from_a_stored_name() -> None:
    """Verify a run recorded by an earlier release, whose stored name carries
    the `(transformed)` suffix, is listed under the plain workplan name.
    """
    run = make_run("r1", name=f"plan-a{LEGACY_TRANSFORMED_NAME_SUFFIX}")

    views = await adapt_runs_to_views([run], {}, "json")

    assert views[0].name == "plan-a"


async def test_adapt_runs_defaults_size_when_metadata_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a run without measured step sizes renders the -1 sentinel."""
    run = make_run("r1")
    names = {run.trx_workplan_path: "plan-a"}
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.load_workplans", stub_load_workplans(names)
    )

    views = await adapt_runs_to_views([run], {}, "table")

    assert views[0].raw_size == -1


async def test_adapt_runs_falls_back_to_unknown_on_failed_deserialization(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a workplan that fails to load yields an 'unknown' name row."""
    run = make_run("r1", size=1)
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.load_workplans",
        stub_load_workplans({run.trx_workplan_path: None}),
    )

    with caplog.at_level(logging.DEBUG, logger=LS_LOGGER):
        views = await adapt_runs_to_views([run], {}, "table")

    assert [v.name for v in views] == ["unknown"]
    assert any("workplan not readable" in r.message for r in caplog.records)
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


async def test_adapt_runs_deserializes_each_path_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify shared and pre-cached workplan paths are not loaded again."""
    first, second = make_run("r1", size=1), make_run("r2", size=2)
    object.__setattr__(second, "trx_workplan_path", first.trx_workplan_path)
    cached = make_run("r3", size=3)
    calls: list[list[Path]] = []
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.load_workplans",
        stub_load_workplans({first.trx_workplan_path: "shared"}, calls),
    )
    plan_cache = {cached.trx_workplan_path: SimpleNamespace(name="cached")}

    views = await adapt_runs_to_views(
        [first, second, cached],
        t.cast("dict[Path, t.Any]", plan_cache),
        "table",
    )

    assert calls == [[first.trx_workplan_path]]
    assert [v.name for v in views] == ["shared", "shared", "cached"]


async def test_adapt_runs_only_loads_workplans_for_runs_missing_a_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a cached run-name skips the workplan load entirely.

    A run whose metadata already carries a name must not trigger
    `load_workplans`; a run without one must.
    """
    named = make_run("r1", name="already-named")
    unnamed = make_run("r2")
    calls: list[list[Path]] = []
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.load_workplans",
        stub_load_workplans({unnamed.trx_workplan_path: "plan-b"}, calls),
    )

    views = await adapt_runs_to_views([named, unnamed], {}, "table")

    assert calls == [[unnamed.trx_workplan_path]]
    assert [v.name for v in views] == ["already-named", "plan-b"]


# ---------------------------------------------------------------------------
# ls_runs command body
# ---------------------------------------------------------------------------


def patch_ls_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    runs: Sequence[WorkplanRun],
) -> tuple[io.StringIO, mock.AsyncMock]:
    """Patch the IO boundaries of `ls_runs` and capture console output.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        The active monkeypatch fixture.
    runs : Sequence[WorkplanRun]
        The run records the stubbed repository will return.

    Returns
    -------
    tuple[io.StringIO, mock.AsyncMock]
        The buffer receiving the command's console output, and the mock
        standing in for `refresh_disk_usage`.
    """

    class StubRepo:
        @classmethod
        def bound(cls, limit: int) -> t.Any:
            @asynccontextmanager
            async def _manager() -> AsyncGenerator[t.Any]:
                yield cls()

            return _manager()

        async def list_latest_runs(self, run_id_filter: str = "") -> t.Any:
            return list(runs)

    names = {r.trx_workplan_path: f"plan-{r.run_id}" for r in runs}
    refresh_mock = mock.AsyncMock()
    buffer = io.StringIO()
    monkeypatch.setattr("cstar.cli.workplan.ls.TrackingRepository", StubRepo)
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.load_workplans", stub_load_workplans(names)
    )
    monkeypatch.setattr("cstar.cli.workplan.ls.refresh_disk_usage", refresh_mock)
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.console", Console(file=buffer, width=200)
    )
    return buffer, refresh_mock


def test_ls_runs_renders_sorted_json_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the end-to-end command emits parseable, sorted JSON."""
    runs = [
        make_run("r2", size=5, start=T0 + datetime.timedelta(hours=1)),
        make_run("r1", size=9, start=T0),
    ]
    buffer, _ = patch_ls_pipeline(monkeypatch, runs)

    ls_runs(mock.Mock(), format="json")

    parsed = json.loads(buffer.getvalue())
    assert [row["run_id"] for row in parsed["data"]] == ["r1", "r2"]
    assert parsed["data"][0]["name"] == "plan-r1"


def test_ls_runs_applies_filters_and_reverse_sort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify size/time filters and reverse ordering apply before rendering."""
    runs = [
        make_run("small", size=1, start=T0),
        make_run("large", size=50, start=T0 + datetime.timedelta(hours=1)),
        make_run("old", size=5, start=T0 - datetime.timedelta(days=1)),
        make_run("big-old", size=50, start=T0 - datetime.timedelta(days=1)),
    ]
    buffer, _ = patch_ls_pipeline(monkeypatch, runs)

    ls_runs(
        mock.Mock(),
        format="json",
        sort="size",
        reverse=True,
        size_gt_filter=2,
        time_gt_filter=T0 - datetime.timedelta(hours=1),
    )

    parsed = json.loads(buffer.getvalue())
    assert [row["run_id"] for row in parsed["data"]] == ["large"]


def test_ls_runs_refresh_usage_awaits_refresh_disk_usage_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `--size` triggers exactly one refresh call with all filtered runs."""
    runs = [make_run("r1", size=1), make_run("r2", size=2)]
    buffer, refresh_mock = patch_ls_pipeline(monkeypatch, runs)

    ls_runs(mock.Mock(), format="json", refresh_usage=True)

    refresh_mock.assert_awaited_once()
    assert refresh_mock.await_args is not None
    awaited_runs = refresh_mock.await_args.args[0]
    assert {r.run_id for r in awaited_runs} == {"r1", "r2"}
    assert buffer.getvalue()


def test_ls_runs_without_refresh_usage_never_awaits_refresh_disk_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify omitting `--size` never triggers a refresh."""
    runs = [make_run("r1", size=1)]
    _buffer, refresh_mock = patch_ls_pipeline(monkeypatch, runs)

    ls_runs(mock.Mock(), format="json", refresh_usage=False)

    refresh_mock.assert_not_awaited()


# ---------------------------------------------------------------------------
# --shared and --all
# ---------------------------------------------------------------------------

EXIT_BAD_ARGS: t.Final[int] = 2
LATER: t.Final[datetime.datetime] = T0 + datetime.timedelta(days=2)
"""When the runs the tests below put under tracking started."""


class Listing:
    """Runs `cstar run ls` against the tracking and the run directories of the test."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.buffer = io.StringIO()
        monkeypatch.setattr(
            "cstar.cli.workplan.ls.console", Console(file=self.buffer, width=400)
        )

    def run(self, *args: str) -> Result:
        """Invoke the command; its console output is `buffer`."""
        self.buffer.seek(0)
        self.buffer.truncate()
        return CliRunner().invoke(app_ls, ["ls", *args], color=False)

    def rows(self, *args: str) -> dict[str, dict[str, str]]:
        """List as JSON and key the rows by run-id."""
        result = self.run("--format", "json", *args)
        assert result.exit_code == 0, result.output
        data = json.loads(self.buffer.getvalue())["data"]
        assert len(data) == len({row["run_id"] for row in data}), data
        return {row["run_id"]: row for row in data}

    def where(self, *args: str) -> dict[str, str]:
        """List as JSON and map each run-id to where it is kept."""
        return {run_id: row["where"] for run_id, row in self.rows(*args).items()}


@pytest.fixture
def listing(monkeypatch: pytest.MonkeyPatch) -> Listing:
    return Listing(monkeypatch)


def track(
    built: BuiltRun, *, size: int | None = None, start: datetime.datetime = LATER
) -> WorkplanRun:
    """Put a built run under tracking, as a run started on this machine."""
    run = WorkplanRun(
        workplan_path=built.root / "workplan.yaml",
        trx_workplan_path=built.root / f"{built.run_id}_transformed.yaml",
        output_path=built.root,
        run_id=built.run_id,
        start_at=start,
        metadata={KEY_RUN_NAME: built.run_id, KEY_RUN_UUID: built.uuid},
        step_sizes={} if size is None else {"step": size},
    )
    TrackingRepository().put_workplan_run_sync(run)
    return run


@pytest.fixture
def populated(
    build_run: t.Callable[..., BuiltRun],
    local_run: t.Callable[..., BuiltRun],
    project_run: t.Callable[..., BuiltRun],
    project_runs: Path,
    tmp_path: Path,
) -> dict[str, BuiltRun]:
    """Runs of every kind, keyed by run-id.

    - `mine` and `copied`: tracked, with directories in the data home
    - `mine` is also linked from the project area
    - `copied` also has a materialized copy in the project area
    - `orphan`: in the data home, untracked
    - `kept`: materialized in the project area
    - `linked`: a link in the project area to a run kept elsewhere
    """
    hour = datetime.timedelta(hours=1)
    runs = {
        "mine": local_run("mine", start_at=T0),
        "copied": local_run("copied", start_at=T0),
        "orphan": local_run("orphan", start_at=T0 + hour),
        "kept": project_run("kept", start_at=T0 + 2 * hour),
        "linked": build_run(
            tmp_path / "elsewhere" / "linked", "linked", start_at=T0 + 3 * hour
        ),
    }
    track(runs["mine"])
    track(runs["copied"], size=5)

    shutil.copytree(runs["copied"].root, project_runs / "copied-copy", symlinks=True)
    (project_runs / "mine-link").symlink_to(runs["mine"].root)
    (project_runs / "linked").symlink_to(runs["linked"].root)
    return runs


def test_the_default_listing_never_scans(
    populated: dict[str, BuiltRun],
    listing: Listing,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the listing of the runs started here reads no run directory and has
    no `where` column.
    """
    scan = mock.Mock(side_effect=AssertionError("the default listing scanned"))
    monkeypatch.setattr(RunIndex, "locations", scan)

    rows = listing.rows()

    assert set(rows) == {"mine", "copied"}
    assert all("where" not in row for row in rows.values())
    scan.assert_not_called()


def test_shared_lists_the_project_area_alone(
    populated: dict[str, BuiltRun], listing: Listing
) -> None:
    """Verify `--shared` lists what the project's runs area holds, says which are
    links, and lists no run kept only in the data home.
    """
    rows = listing.rows("--shared")

    assert {run_id: row["where"] for run_id, row in rows.items()} == {
        "kept": "project",
        "linked": "project, linked",
        "copied": "project",
        "mine": "project, linked",
    }
    assert rows["kept"]["start"] == (T0 + datetime.timedelta(hours=2)).isoformat()
    assert rows["kept"]["name"] == "kept"
    assert rows["kept"]["size"] == "-1MB"


def test_all_lists_what_is_tracked_and_what_only_manifests_name(
    populated: dict[str, BuiltRun], listing: Listing
) -> None:
    """Verify `--all` lists the runs tracked here, then the others the data home
    and the project's runs area hold, each once: a run's own directory, and a
    copy of it that holds its uuid, are the tracked run.
    """
    rows = listing.rows("--all")

    assert {run_id: row["where"] for run_id, row in rows.items()} == {
        "mine": "local",
        "copied": "local",
        "orphan": "local, untracked",
        "kept": "project",
        "linked": "project, linked",
    }
    assert rows["copied"]["size"] == "5MB"
    assert rows["orphan"]["start"] == (T0 + datetime.timedelta(hours=1)).isoformat()


def test_both_flags_list_what_all_lists(
    populated: dict[str, BuiltRun], listing: Listing
) -> None:
    """Verify `--shared` beside `--all` changes nothing."""
    assert listing.rows("--shared", "--all") == listing.rows("--all")


def test_shared_without_a_project_home_is_a_bad_argument(
    local_run: t.Callable[..., BuiltRun],
    listing: Listing,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify there is nothing to list, and the way to set it, when no project is configured."""
    local_run("orphan")

    result = listing.run("--shared")

    assert result.exit_code == EXIT_BAD_ARGS
    text = flatten_cli_output(result.stderr)
    assert "CSTAR_PROJECT_HOME" in text
    assert "cstar/runs" in text
    assert listing.buffer.getvalue() == ""


def test_all_without_a_project_home_lists_the_local_runs(
    local_run: t.Callable[..., BuiltRun], listing: Listing
) -> None:
    """Verify `--all` is the runs of the data home when there is no project."""
    mine = local_run("mine")
    local_run("orphan")
    track(mine)

    assert listing.where("--all") == {"mine": "local", "orphan": "local, untracked"}


def local_spelling(moment: datetime.datetime) -> str:
    """Spell a moment as `--min-time` and `--max-time` read it: local, without an offset."""
    return moment.astimezone().replace(tzinfo=None).isoformat(timespec="seconds")


def test_the_filters_apply_to_the_runs_only_a_manifest_names(
    populated: dict[str, BuiltRun], listing: Listing
) -> None:
    """Verify the run-id and time filters drop index rows as they drop tracked ones."""
    assert set(listing.where("--all", "--run-filter", "or")) == {"orphan"}

    between = local_spelling(T0 + datetime.timedelta(minutes=90))
    assert set(listing.where("--all", "--min-time", between)) == {
        "mine",
        "copied",
        "kept",
        "linked",
    }
    assert set(listing.where("--shared", "--max-time", between)) == {
        "copied",
        "mine",
    }


def test_a_tracked_run_a_filter_drops_does_not_return_as_its_copy(
    populated: dict[str, BuiltRun], listing: Listing
) -> None:
    """Verify a tracked run that a filter drops is not listed again as the link
    or the copy of it in the project area, whose manifests start earlier and so
    pass the same filter.
    """
    before_tracking = local_spelling(T0 + datetime.timedelta(days=1))

    rows = listing.where("--all", "--max-time", before_tracking)

    assert set(rows) == {"orphan", "kept", "linked"}


def test_a_tracked_run_whose_directory_is_gone_does_not_hide_its_copy(
    populated: dict[str, BuiltRun], listing: Listing
) -> None:
    """Verify the copy in the project area of a tracked run whose directory was
    purged is listed beside it: the copy is the one still readable.
    """
    shutil.rmtree(populated["copied"].root)

    result = listing.run("--all", "--format", "json")

    assert result.exit_code == 0, result.output
    data = json.loads(listing.buffer.getvalue())["data"]
    assert sorted(row["where"] for row in data if row["run_id"] == "copied") == [
        "local",
        "project",
    ]


def test_the_size_filters_never_drop_a_run_whose_size_is_not_known(
    populated: dict[str, BuiltRun],
    listing: Listing,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify the runs tracking does not hold are kept by a size filter, and named."""
    with caplog.at_level(logging.WARNING, logger=LS_LOGGER):
        rows = listing.where("--all", "--max-size", "1")

    assert set(rows) == {"mine", "orphan", "kept", "linked"}
    named = " ".join(r.getMessage() for r in caplog.records)
    assert all(run_id in named for run_id in ("orphan", "kept", "linked"))


def test_the_table_says_where_and_does_not_hint_at_a_size_it_cannot_measure(
    populated: dict[str, BuiltRun], listing: Listing
) -> None:
    """Verify the table adds a column, shows `-` for the size of a run that
    is not tracked here, and keeps the hint for one that is.
    """
    result = listing.run("--all")

    assert result.exit_code == 0, result.output
    lines = listing.buffer.getvalue().splitlines()
    assert "where" in lines[0] or "where" in lines[1]
    cells = {
        line.split("│")[1].strip(): [c.strip() for c in line.split("│")[1:-1]]
        for line in lines
        if line.startswith("│")
    }
    assert cells["orphan"][2] == "-"
    assert cells["orphan"][4] == "local, untracked"
    assert cells["linked"][4] == "project, linked"
    assert "cstar run status mine --size" in " ".join(cells["mine"])
    assert cells["copied"][2] == "5MB"
    assert listing.run().exit_code == 0
    assert "where" not in listing.buffer.getvalue()


def test_csv_says_where(populated: dict[str, BuiltRun], listing: Listing) -> None:
    """Verify the CSV has a `where` column when a flag is given, and not otherwise."""
    assert listing.run("--format", "csv", "--all").exit_code == 0
    rows = [row for row in csv.reader(io.StringIO(listing.buffer.getvalue())) if row]
    assert rows[0] == sorted(WHERE_INCLUSIONS)
    where = {
        dict(zip(rows[0], row))["run_id"]: dict(zip(rows[0], row))["where"]
        for row in rows[1:]
    }
    assert where["orphan"] == "local, untracked"

    assert listing.run("--format", "csv").exit_code == 0
    assert next(csv.reader(io.StringIO(listing.buffer.getvalue()))) == sorted(
        INCLUSIONS
    )


def test_the_formatters_add_where_on_request() -> None:
    """Verify the `where` of a view is rendered by each formatter only when asked."""
    views = [
        ItemView(
            run_id="r1",
            name="alpha",
            raw_size=-1,
            raw_start=T0,
            format="json",
            where="project, linked",
            tracked=False,
        )
    ]

    plain = json_formatter(views)
    wide = json_formatter(views, with_where=True)
    assert isinstance(plain, Text)
    assert isinstance(wide, Text)
    assert set(json.loads(plain.plain)["data"][0]) == INCLUSIONS
    row = json.loads(wide.plain)["data"][0]
    assert row["where"] == "project, linked"
    assert set(row) == WHERE_INCLUSIONS
    table = table_formatter(views, with_where=True)
    assert isinstance(table, Table)
    assert [c.header for c in table.columns][-1] == "where"
