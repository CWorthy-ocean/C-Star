import os
import typing as t
from pathlib import Path

import pytest

from cstar.base.env import ENV_CSTAR_CACHE_DISABLE
from cstar.caching import CacheManager, CacheTier, cached_artifact
from cstar.caching.store import function_slug

if t.TYPE_CHECKING:
    from cstar.tests.unit_tests.caching.conftest import CountingArtifact


def test_miss_executes_and_populates_cache(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    outdir = tmp_path / "run" / "output"
    handle = counting_artifact("demo", 2, True, output_dir=outdir)

    assert counting_artifact.calls == 1
    assert handle.hit is False
    assert handle.tier == CacheTier.personal
    assert len(handle.paths) == 2

    for link, payload in zip(handle.paths, handle.payload_paths, strict=True):
        assert link.is_symlink(), "output files must be symlinks into the cache"
        assert link.resolve() == payload.resolve()
        assert link.read_text().startswith("demo")

    entry = manager.personal.find(function_slug(handle.function), handle.key)
    assert entry is not None
    assert entry.manifest.label == "counting"
    assert entry.manifest.key_material["args"] == {"name": "demo", "count": 2}


def test_second_call_hits_without_executing(
    counting_artifact: "CountingArtifact",
    tmp_path: Path,
) -> None:
    first = counting_artifact("demo", 2, True, output_dir=tmp_path / "run1")
    second = counting_artifact("demo", 2, False, output_dir=tmp_path / "run2")

    assert counting_artifact.calls == 1, "excluded arg change must not re-execute"
    assert second.hit is True
    assert second.tier == CacheTier.personal
    assert second.result == [
        tmp_path / "run2" / "nested" / "demo_0.dat",
        tmp_path / "run2" / "nested" / "demo_1.dat",
    ]
    assert [p.read_text() for p in second.paths] == [p.read_text() for p in first.paths]


def test_argument_change_misses(
    counting_artifact: "CountingArtifact",
    tmp_path: Path,
) -> None:
    counting_artifact("demo", 2, True, output_dir=tmp_path / "a")
    counting_artifact("demo", 3, True, output_dir=tmp_path / "b")
    assert counting_artifact.calls == 2


def test_group_entry_preferred_over_personal(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    handle = counting_artifact("demo", 1, True, output_dir=tmp_path / "seed")
    entry = manager.personal.find(function_slug(handle.function), handle.key)
    assert entry is not None
    manager.promote(entry)

    served = counting_artifact("demo", 1, True, output_dir=tmp_path / "later")
    assert served.hit is True
    assert served.tier == CacheTier.group
    assert counting_artifact.calls == 1


def test_no_cache_bypasses_and_records_nothing(
    counting_artifact: "CountingArtifact",
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    os.environ[ENV_CSTAR_CACHE_DISABLE] = "1"
    outdir = tmp_path / "direct"
    handle = counting_artifact("demo", 2, True, output_dir=outdir)

    assert counting_artifact.calls == 1
    assert handle.hit is False
    assert handle.tier is None
    assert len(handle.paths) == 2
    assert all(not p.is_symlink() for p in handle.paths)
    assert handle.result[0].read_text().startswith("demo")

    assert list(manager.iter_all()) == [], "bypass must not record entries"


def test_no_cache_does_not_write_through_symlinks(
    counting_artifact: "CountingArtifact",
    tmp_path: Path,
) -> None:
    """A bypass run into a dir holding cache symlinks must not touch the cache."""
    outdir = tmp_path / "shared-output"
    cached = counting_artifact("demo", 1, True, output_dir=outdir)
    payload = cached.payload_paths[0]
    original_content = payload.read_text()
    assert "call=1" in original_content

    os.environ[ENV_CSTAR_CACHE_DISABLE] = "1"
    bypass = counting_artifact("demo", 1, True, output_dir=outdir)

    assert counting_artifact.calls == 2
    assert payload.read_text() == original_content, "cache payload was poisoned"
    produced = bypass.paths[0]
    assert not produced.is_symlink(), "bypass output must be a real file"
    assert "call=2" in produced.read_text()


def test_missing_output_param_rejected_at_decoration_time() -> None:
    with pytest.raises(TypeError, match="output_dir"):

        @cached_artifact()
        def no_output(name: str) -> None: ...


def test_custom_output_param(tmp_path: Path) -> None:
    @cached_artifact(output_param="target_dir")
    def produce(name: str, target_dir: Path) -> Path:
        path = target_dir / f"{name}.txt"
        path.write_text(name)
        return path

    first = produce("demo", target_dir=tmp_path / "a")
    second = produce("demo", target_dir=tmp_path / "b")
    assert not first.hit
    assert second.hit
    assert second.result == tmp_path / "b" / "demo.txt"


def test_call_without_output_value_raises(tmp_path: Path) -> None:
    @cached_artifact()
    def produce(name: str, output_dir: Path | None = None) -> None: ...

    with pytest.raises(TypeError, match="output"):
        produce("demo")


def test_exception_cleans_staging_and_propagates(
    manager: CacheManager,
    tmp_path: Path,
) -> None:
    attempts = {"n": 0}

    @cached_artifact()
    def flaky(name: str, output_dir: Path) -> Path:
        attempts["n"] += 1
        (output_dir / "partial.dat").write_text("partial")
        if attempts["n"] == 1:
            msg = "boom"
            raise RuntimeError(msg)
        path = output_dir / "final.dat"
        path.write_text("ok")
        return path

    with pytest.raises(RuntimeError, match="boom"):
        flaky("demo", output_dir=tmp_path / "a")

    staging_leftovers = (
        list(manager.personal.staging_dir.glob("*"))
        if manager.personal.staging_dir.exists()
        else []
    )
    assert staging_leftovers == [], "failed staging dirs must be removed"
    assert list(manager.iter_all()) == [], "failed calls must not be cached"

    retry = flaky("demo", output_dir=tmp_path / "b")
    assert attempts["n"] == 2
    assert retry.result.read_text() == "ok"


def test_return_restore_single_path(tmp_path: Path) -> None:
    @cached_artifact()
    def produce(name: str, output_dir: Path) -> Path:
        path = output_dir / f"{name}.txt"
        path.write_text(name)
        return path

    produce("demo", output_dir=tmp_path / "a")
    hit = produce("demo", output_dir=tmp_path / "b")
    assert hit.result == tmp_path / "b" / "demo.txt"
    assert hit.result.read_text() == "demo"


def test_return_restore_path_map(tmp_path: Path) -> None:
    @cached_artifact()
    def produce(name: str, output_dir: Path) -> dict[str, Path]:
        grid = output_dir / "grid.nc"
        forcing = output_dir / "forcing.nc"
        grid.write_text("g")
        forcing.write_text("f")
        return {"grid": grid, "forcing": forcing}

    produce("demo", output_dir=tmp_path / "a")
    hit = produce("demo", output_dir=tmp_path / "b")
    assert hit.result == {
        "grid": tmp_path / "b" / "grid.nc",
        "forcing": tmp_path / "b" / "forcing.nc",
    }


def test_return_restore_json_value(tmp_path: Path) -> None:
    @cached_artifact()
    def produce(name: str, output_dir: Path) -> dict:
        (output_dir / "stats.json").write_text("{}")
        return {"mean": 0.5, "count": 42}

    fresh = produce("demo", output_dir=tmp_path / "a")
    hit = produce("demo", output_dir=tmp_path / "b")
    assert fresh.result == {"mean": 0.5, "count": 42}
    assert hit.result == {"mean": 0.5, "count": 42}


def test_return_tuple_normalizes_to_list_on_miss_and_hit(tmp_path: Path) -> None:
    """Tuples must survive the manifest round-trip and agree between miss/hit.

    Regression test: a tuple stored verbatim serializes as a python-tagged
    YAML node that `yaml.safe_load` cannot read, permanently invalidating
    the entry after the function already ran.
    """

    @cached_artifact()
    def produce(name: str, output_dir: Path) -> tuple[int, str]:
        (output_dir / "out.dat").write_text("x")
        return (42, name)

    fresh = produce("demo", output_dir=tmp_path / "a")
    hit = produce("demo", output_dir=tmp_path / "b")

    assert fresh.result == [42, "demo"], "miss result must match what hits return"
    assert hit.hit is True, "entry must remain valid and servable"
    assert hit.result == [42, "demo"]


def test_return_string_staging_path_not_persisted(tmp_path: Path) -> None:
    """String paths into the staging dir must not be served on hits.

    Regression test: `str(path)` returns classify as JSON and would embed a
    dead path into the renamed-away staging directory.
    """

    @cached_artifact()
    def produce(name: str, output_dir: Path) -> str:
        path = output_dir / "out.dat"
        path.write_text("x")
        return str(path)

    produce("demo", output_dir=tmp_path / "a")
    hit = produce("demo", output_dir=tmp_path / "b")

    assert hit.hit is True
    assert hit.result is None, "a dead staging path must not be restored"
    assert hit.paths[0].read_text() == "x", "files still restore"


def test_no_cache_works_with_unkeyable_arguments(tmp_path: Path) -> None:
    """--no-cache must bypass key computation failures, not raise them."""

    class Opaque:
        pass

    @cached_artifact()
    def produce(config: object, output_dir: Path) -> Path:
        path = output_dir / "out.dat"
        path.write_text("x")
        return path

    os.environ[ENV_CSTAR_CACHE_DISABLE] = "1"
    handle = produce(Opaque(), output_dir=tmp_path / "a")
    assert handle.tier is None
    assert handle.result.read_text() == "x"


def test_return_unrestorable_yields_none_on_hit(tmp_path: Path) -> None:
    class Heavy:
        pass

    @cached_artifact()
    def produce(name: str, output_dir: Path) -> Heavy:
        (output_dir / "big.nc").write_text("data")
        return Heavy()

    fresh = produce("demo", output_dir=tmp_path / "a")
    assert isinstance(fresh.result, Heavy), "miss must return the raw value"

    hit = produce("demo", output_dir=tmp_path / "b")
    assert hit.hit
    assert hit.result is None
    assert hit.paths[0].read_text() == "data", "files restore even when value cannot"


def test_return_path_outside_payload_not_restorable(tmp_path: Path) -> None:
    external = tmp_path / "external.txt"
    external.write_text("outside")

    @cached_artifact()
    def produce(name: str, output_dir: Path) -> Path:
        (output_dir / "inside.txt").write_text("in")
        return external

    fresh = produce("demo", output_dir=tmp_path / "a")
    assert fresh.result == external

    hit = produce("demo", output_dir=tmp_path / "b")
    assert hit.result is None


async def test_async_function_supported(tmp_path: Path) -> None:
    calls = {"n": 0}

    @cached_artifact()
    async def produce(name: str, output_dir: Path) -> Path:
        calls["n"] += 1
        path = output_dir / f"{name}.txt"
        path.write_text(name)
        return path

    fresh = await produce("demo", output_dir=tmp_path / "a")
    hit = await produce("demo", output_dir=tmp_path / "b")

    assert calls["n"] == 1
    assert not fresh.hit
    assert hit.hit
    assert hit.result == tmp_path / "b" / "demo.txt"


def test_handle_provenance_populated(
    counting_artifact: "CountingArtifact",
    tmp_path: Path,
) -> None:
    handle = counting_artifact("demo", 1, True, output_dir=tmp_path / "a")
    assert handle.provenance is not None
    assert handle.provenance.created_by
    assert handle.provenance.hostname
    assert handle.created_at == handle.provenance.created_at
