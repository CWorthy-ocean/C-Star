import csv
import datetime
import io
import json
import logging
import typing as t
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from rich.console import Console
from rich.table import Table
from rich.text import Text

from cstar.cli.workplan.ls import (
    FIELD_NAMES,
    FORMATS,
    INCLUSIONS,
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
from cstar.orchestration.tracking import KEY_RUN_SIZE, WorkplanRun

LS_LOGGER = "cstar.cli.workplan.ls"

UTC = datetime.UTC
T0 = datetime.datetime(2026, 9, 12, 3, 4, 0, tzinfo=UTC)
"""An arbitrary tz-aware reference time used to build test data."""


def make_run(
    run_id: str,
    size: int | None = None,
    start: datetime.datetime | None = None,
) -> WorkplanRun:
    """Build a `WorkplanRun` with dummy paths for filter tests.

    Parameters
    ----------
    run_id : str
        The unique run identifier.
    size : int | None
        When supplied, stored in metadata under the disk-usage key.
    start : datetime.datetime | None
        When supplied, used as the run start time.

    Returns
    -------
    WorkplanRun
    """
    metadata = {} if size is None else {KEY_RUN_SIZE: str(size)}
    kwargs: dict[str, t.Any] = {} if start is None else {"start_at": start}

    return WorkplanRun(
        workplan_path=Path(f"/tmp/{run_id}/workplan.yaml"),
        trx_workplan_path=Path(f"/tmp/{run_id}/trx.yaml"),
        output_path=Path(f"/tmp/{run_id}/out"),
        run_id=run_id,
        metadata=metadata,
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

    assert "cstar workplan status run-9" in view.size
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
    """Verify runs with the -1 sentinel pass every filter and warn once."""
    runs = [make_run("sized", size=100), make_run("unsized", size=-1)]

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
    assert "cstar workplan status r2" in rendered
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


def stub_deserialize_all(
    names: Mapping[Path, str | None],
    calls: list[list[Path]] | None = None,
) -> t.Any:
    """Build a `deserialize_all` replacement serving canned workplan stubs.

    Parameters
    ----------
    names : Mapping[Path, str | None]
        Workplan name to serve per path; None simulates a failed load.
    calls : list[list[Path]] | None
        When supplied, receives the path list of each invocation.

    Returns
    -------
    t.Any
        An async callable matching the `deserialize_all` signature.
    """

    async def _stub(paths: list[Path], *args: t.Any, **kwargs: t.Any) -> list[t.Any]:
        if calls is not None:
            calls.append(list(paths))
        return [SimpleNamespace(name=names[p]) if names.get(p) else None for p in paths]

    return _stub


async def test_adapt_runs_builds_views_from_cached_workplans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify names, sizes, start times, and format are threaded into views."""
    run = make_run("r1", size=7, start=T0)
    names = {run.trx_workplan_path: "plan-a"}
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.deserialize_all", stub_deserialize_all(names)
    )

    views = await adapt_runs_to_views([run], {}, "json")

    assert len(views) == 1
    view = views[0]
    assert (view.run_id, view.name, view.raw_size) == ("r1", "plan-a", 7)
    assert view.raw_start == T0
    assert view.format == "json"


async def test_adapt_runs_defaults_size_when_metadata_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a run without size metadata renders the -1 sentinel."""
    run = make_run("r1")
    names = {run.trx_workplan_path: "plan-a"}
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.deserialize_all", stub_deserialize_all(names)
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
        "cstar.cli.workplan.ls.deserialize_all",
        stub_deserialize_all({run.trx_workplan_path: None}),
    )

    with caplog.at_level(logging.WARNING, logger=LS_LOGGER):
        views = await adapt_runs_to_views([run], {}, "table")

    assert [v.name for v in views] == ["unknown"]
    assert any("not loaded into cache" in r.message for r in caplog.records)


async def test_adapt_runs_deserializes_each_path_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify shared and pre-cached workplan paths are not loaded again."""
    first, second = make_run("r1", size=1), make_run("r2", size=2)
    object.__setattr__(second, "trx_workplan_path", first.trx_workplan_path)
    cached = make_run("r3", size=3)
    calls: list[list[Path]] = []
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.deserialize_all",
        stub_deserialize_all({first.trx_workplan_path: "shared"}, calls),
    )
    plan_cache = {cached.trx_workplan_path: SimpleNamespace(name="cached")}

    views = await adapt_runs_to_views(
        [first, second, cached],
        t.cast("dict[Path, t.Any]", plan_cache),
        "table",
    )

    assert calls == [[first.trx_workplan_path]]
    assert [v.name for v in views] == ["shared", "shared", "cached"]


# ---------------------------------------------------------------------------
# ls_runs command body
# ---------------------------------------------------------------------------


def patch_ls_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    runs: Sequence[WorkplanRun | None],
) -> io.StringIO:
    """Patch the IO boundaries of `ls_runs` and capture console output.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        The active monkeypatch fixture.
    runs : Sequence[WorkplanRun | None]
        The run records the stubbed repository will return.

    Returns
    -------
    io.StringIO
        The buffer receiving the command's console output.
    """

    class StubRepo:
        @classmethod
        def bound(cls, limit: int) -> t.Any:
            @asynccontextmanager
            async def _manager() -> t.Any:
                yield cls()

            return _manager()

        async def list_latest_runs(self, run_id_filter: str = "") -> t.Any:
            return list(runs)

    async def stub_attach(
        attached: Sequence[WorkplanRun], refresh: bool = False
    ) -> None:
        for run in attached:
            run.metadata.setdefault(KEY_RUN_SIZE, "-1")

    names = {r.trx_workplan_path: f"plan-{r.run_id}" for r in runs if r}
    buffer = io.StringIO()
    monkeypatch.setattr("cstar.cli.workplan.ls.TrackingRepository", StubRepo)
    monkeypatch.setattr("cstar.cli.workplan.ls.attach_disk_usage", stub_attach)
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.deserialize_all", stub_deserialize_all(names)
    )
    monkeypatch.setattr(
        "cstar.cli.workplan.ls.console", Console(file=buffer, width=200)
    )
    return buffer


def test_ls_runs_renders_sorted_json_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the end-to-end command emits parseable, sorted JSON."""
    runs = [
        make_run("r2", size=5, start=T0 + datetime.timedelta(hours=1)),
        make_run("r1", size=9, start=T0),
    ]
    buffer = patch_ls_pipeline(monkeypatch, runs)

    ls_runs(mock.Mock(), format="json")

    parsed = json.loads(buffer.getvalue())
    assert [row["run_id"] for row in parsed["data"]] == ["r1", "r2"]
    assert parsed["data"][0]["name"] == "plan-r1"


def test_ls_runs_warns_about_unreadable_records(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify unreadable run records are dropped with a single warning."""
    buffer = patch_ls_pipeline(monkeypatch, [make_run("r1", size=5), None])

    with caplog.at_level(logging.WARNING, logger=LS_LOGGER):
        ls_runs(mock.Mock(), format="json")

    assert any("could not be read" in r.message for r in caplog.records)
    parsed = json.loads(buffer.getvalue())
    assert [row["run_id"] for row in parsed["data"]] == ["r1"]


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
    buffer = patch_ls_pipeline(monkeypatch, runs)

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
