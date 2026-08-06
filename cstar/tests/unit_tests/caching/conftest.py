import os
import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import (
    ENV_CSTAR_CACHE_DISABLE,
    ENV_CSTAR_CACHE_GROUP_ROOT,
    ENV_CSTAR_CACHE_PERSONAL_ROOT,
)
from cstar.caching import CacheManager, cached_artifact
from cstar.caching.models import CacheHandle


@pytest.fixture(autouse=True)
def mock_cache_roots(tmp_path: Path) -> t.Generator[dict[str, Path]]:
    """Point both cache tiers at temporary directories and enable caching."""
    roots = {
        ENV_CSTAR_CACHE_PERSONAL_ROOT: tmp_path.resolve() / "personal-cache",
        ENV_CSTAR_CACHE_GROUP_ROOT: tmp_path.resolve() / "group-cache",
    }
    variables = {name: str(path) for name, path in roots.items()}

    with mock.patch.dict(os.environ, variables):
        os.environ.pop(ENV_CSTAR_CACHE_DISABLE, None)
        yield roots


@pytest.fixture
def manager() -> CacheManager:
    """A cache manager resolved from the mocked environment."""
    return CacheManager.from_env()


class CountingArtifact:
    """A cached test function that counts executions and versions its content."""

    def __init__(self) -> None:
        self.calls = 0

        @cached_artifact(version="1", label="counting", key_exclude=("verbose",))
        def produce(
            name: str,
            count: int,
            verbose: bool,
            output_dir: Path,
        ) -> list[Path]:
            self.calls += 1
            paths = []
            for index in range(count):
                path = output_dir / "nested" / f"{name}_{index}.dat"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(f"{name} {index} call={self.calls}")
                paths.append(path)
            return paths

        self.produce = produce

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> CacheHandle:
        return self.produce(*args, **kwargs)


@pytest.fixture
def counting_artifact() -> CountingArtifact:
    """A cached function whose executions are observable."""
    return CountingArtifact()
