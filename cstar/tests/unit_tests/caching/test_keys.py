import enum
import typing as t
from collections.abc import Mapping
from pathlib import Path

import pytest
from pydantic import BaseModel

from cstar.caching.keys import (
    CacheKeyError,
    compute_key,
    file_fingerprint,
    tokenize,
)


def sample_fn(
    name: str, count: int = 3, verbose: bool = False, output_dir: Path | None = None
) -> None:
    """A function signature used for key computation tests."""


def test_key_is_deterministic() -> None:
    key1, _ = compute_key(sample_fn, ("demo", 2), {}, version="1")
    key2, _ = compute_key(sample_fn, ("demo", 2), {}, version="1")
    assert key1 == key2
    assert len(key1) == 64


def test_key_invariant_to_call_style() -> None:
    positional, _ = compute_key(sample_fn, ("demo", 3), {}, version="1")
    keyword, _ = compute_key(sample_fn, (), {"name": "demo", "count": 3}, version="1")
    defaulted, _ = compute_key(sample_fn, ("demo",), {}, version="1")
    reordered, _ = compute_key(sample_fn, (), {"count": 3, "name": "demo"}, version="1")

    assert positional == keyword == defaulted == reordered


def test_key_changes_with_arguments() -> None:
    key1, _ = compute_key(sample_fn, ("demo", 2), {}, version="1")
    key2, _ = compute_key(sample_fn, ("demo", 3), {}, version="1")
    key3, _ = compute_key(sample_fn, ("other", 2), {}, version="1")
    assert len({key1, key2, key3}) == 3


def test_key_changes_with_version() -> None:
    key1, _ = compute_key(sample_fn, ("demo",), {}, version="1")
    key2, _ = compute_key(sample_fn, ("demo",), {}, version="2")
    assert key1 != key2


def test_key_exclude_removes_influence() -> None:
    key1, _ = compute_key(
        sample_fn, ("demo",), {"verbose": True}, version="1", key_exclude=("verbose",)
    )
    key2, _ = compute_key(
        sample_fn, ("demo",), {"verbose": False}, version="1", key_exclude=("verbose",)
    )
    assert key1 == key2


def test_output_param_is_excluded() -> None:
    key1, _ = compute_key(
        sample_fn,
        ("demo",),
        {"output_dir": Path("/a")},
        version="1",
        output_param="output_dir",
    )
    key2, _ = compute_key(
        sample_fn,
        ("demo",),
        {"output_dir": Path("/b")},
        version="1",
        output_param="output_dir",
    )
    assert key1 == key2


def test_self_is_excluded() -> None:
    class Producer:
        def make(self, name: str) -> None: ...

    key1, _ = compute_key(Producer.make, (Producer(), "demo"), {}, version="1")
    key2, _ = compute_key(Producer.make, (Producer(), "demo"), {}, version="1")
    assert key1 == key2


def test_key_extra_static_mapping() -> None:
    key1, _ = compute_key(sample_fn, ("demo",), {}, version="1")
    key2, _ = compute_key(
        sample_fn, ("demo",), {}, version="1", key_extra={"tool_version": "3.1"}
    )
    key3, _ = compute_key(
        sample_fn, ("demo",), {}, version="1", key_extra={"tool_version": "3.2"}
    )
    assert len({key1, key2, key3}) == 3


def test_key_extra_callable_receives_bound_arguments() -> None:
    seen: dict = {}

    def extra(bound: Mapping[str, t.Any]) -> Mapping[str, t.Any]:
        seen.update(bound)
        return {"derived": bound["name"].upper()}

    key1, material = compute_key(sample_fn, ("demo",), {}, version="1", key_extra=extra)
    assert seen["name"] == "demo"
    assert seen["count"] == 3, "defaults must be applied before key_extra"
    assert material["extra"] == {"derived": "DEMO"}

    key2, _ = compute_key(sample_fn, ("other",), {}, version="1", key_extra=extra)
    assert key1 != key2


def test_key_by_transform_applies() -> None:
    key_plain, _ = compute_key(sample_fn, ("demo",), {}, version="1")
    key_by, _ = compute_key(
        sample_fn, ("demo",), {}, version="1", key_by={"name": str.upper}
    )
    assert key_plain != key_by


def test_key_material_records_inputs() -> None:
    _, material = compute_key(sample_fn, ("demo", 2), {}, version="7")
    assert material["function"].endswith("sample_fn")
    assert material["version"] == "7"
    assert material["args"] == {
        "name": "demo",
        "count": 2,
        "verbose": False,
        "output_dir": None,
    }


class Flavor(enum.StrEnum):
    vanilla = enum.auto()
    chocolate = enum.auto()


class Settings(BaseModel):
    depth: int = 5
    flavor: Flavor = Flavor.vanilla


def test_tokenize_supported_types(tmp_path: Path) -> None:
    assert tokenize(None) is None
    assert tokenize(True) is True
    assert tokenize(7) == 7
    assert tokenize("x") == "x"
    assert tokenize(1.5) == {"__float__": "1.5"}

    enum_token = tokenize(Flavor.vanilla)
    assert enum_token["__enum__"].endswith("Flavor")
    assert enum_token["value"] == "vanilla"

    model_token = tokenize(Settings())
    assert model_token["value"]["depth"] == 5

    assert tokenize([1, "a"]) == [1, "a"]
    assert tokenize((1, "a")) == [1, "a"]
    assert tokenize({"k": 1}) == {"k": 1}


def test_tokenize_bool_and_int_are_distinct() -> None:
    import json

    assert json.dumps(tokenize(True)) != json.dumps(tokenize(1))


def test_tokenize_sets_are_order_stable() -> None:
    assert tokenize({3, 1, 2}) == tokenize({2, 3, 1})


def test_tokenize_set_and_list_are_distinct() -> None:
    import json

    assert json.dumps(tokenize({1, 2})) != json.dumps(tokenize([1, 2]))


def test_tokenize_datetime_and_string_are_distinct() -> None:
    import datetime as dt
    import json

    moment = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    assert json.dumps(tokenize(moment)) != json.dumps(tokenize(moment.isoformat()))


def test_model_with_set_key_is_stable_across_hash_seeds(tmp_path: Path) -> None:
    """Set fields inside pydantic models must not leak hash-randomized order.

    Runs the same key computation in subprocesses with different
    PYTHONHASHSEED values; in-process comparisons cannot catch this.
    """
    import os
    import subprocess
    import sys

    script = tmp_path / "compute.py"
    script.write_text(
        "from pydantic import BaseModel\n"
        "from cstar.caching.keys import compute_key\n"
        "class Config(BaseModel):\n"
        "    variables: set[str]\n"
        "def fn(config: Config) -> None: ...\n"
        "cfg = Config(variables={'temp', 'salt', 'u', 'v', 'zeta'})\n"
        "print(compute_key(fn, (cfg,), {}, version='1')[0])\n"
    )

    import cstar

    # pin the import to THIS checkout: the subprocess gets the script's dir
    # as sys.path[0], and an editable install may point at another clone
    repo_root = Path(cstar.__file__).resolve().parent.parent

    keys = set()
    for seed in ("1", "2"):
        env = {
            **os.environ,
            "PYTHONHASHSEED": seed,
            "PYTHONPATH": str(repo_root),
        }
        output = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        keys.add(output.stdout.strip())

    assert len(keys) == 1, "key must not depend on hash randomization"


def test_tokenize_paths_resolve(tmp_path: Path) -> None:
    nested = tmp_path / "sub" / ".." / "file.nc"
    direct = tmp_path / "file.nc"
    assert tokenize(nested) == tokenize(direct)


def test_tokenize_unsupported_type_names_argument() -> None:
    class Opaque:
        pass

    def fn(config: object) -> None: ...

    with pytest.raises(CacheKeyError, match="config"):
        compute_key(fn, (Opaque(),), {}, version="1")


def test_tokenize_non_string_mapping_keys_rejected() -> None:
    with pytest.raises(CacheKeyError):
        tokenize({1: "a"}, "mapping_arg")


def test_file_fingerprint_tracks_content(tmp_path: Path) -> None:
    target = tmp_path / "input.dat"
    target.write_text("original content")
    first = file_fingerprint(target)
    second = file_fingerprint(target)
    assert first == second

    target.write_text("modified content")
    assert file_fingerprint(target) != first


def test_file_fingerprint_is_path_independent(tmp_path: Path) -> None:
    """Identical content at different paths fingerprints identically.

    This is what lets group-cache keys match across users whose copies of
    an input live at different absolute paths.
    """
    first = tmp_path / "user_a" / "input.dat"
    second = tmp_path / "user_b" / "input.dat"
    for path in (first, second):
        path.parent.mkdir()
        path.write_text("same content")

    assert file_fingerprint(first) == file_fingerprint(second)


def test_file_fingerprint_as_key_by(tmp_path: Path) -> None:
    target = tmp_path / "input.dat"
    target.write_text("v1")

    def fn(grid_file: Path) -> None: ...

    key1, _ = compute_key(
        fn, (target,), {}, version="1", key_by={"grid_file": file_fingerprint}
    )
    target.write_text("v2 - different")
    key2, _ = compute_key(
        fn, (target,), {}, version="1", key_by={"grid_file": file_fingerprint}
    )
    assert key1 != key2
