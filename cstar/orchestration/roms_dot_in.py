import tempfile
import typing as t
from collections import defaultdict
from pathlib import Path

from cstar.base.additional_code import AdditionalCode

if t.TYPE_CHECKING:
    from cstar.orchestration.models import CodeRepository


def find_runtime_settings_file(repo: "CodeRepository") -> Path:
    # if possible, retrieve only a *.in file
    files = (
        list(filter(lambda s: s.endswith(".in"), repo.filter.files))
        if repo.filter
        else []
    )
    code = AdditionalCode(
        location=str(repo.location),
        subdir=(str(repo.filter.directory) if repo.filter else ""),
        checkout_target=repo.checkout_target,
        files=files,
    )
    if not code.exists_locally:
        with tempfile.TemporaryDirectory(delete=False) as tmp_dir:
            code.get(tmp_dir)

    if code.working_copy is None:
        raise RuntimeError(f"Unable to retrieve repository: {repo.location}")

    return next(x for x in code.working_copy.paths if x.name.endswith(".in"))


KHVContainer = dict[str, tuple[str, str | list[str]]]


def load_raw_runtime_settings(rts_path: Path) -> KHVContainer:
    content = rts_path.read_text().splitlines()
    content = [x.strip() for x in content if x.strip()]
    kvs: dict[str, list[str]] = defaultdict(list)  # key and value storage
    khs: dict[str, str] = {}  # key and header storage
    key = ""

    while content:
        line = content.pop(0)

        if ":" in line:
            key, header = line.split(":", maxsplit=1)
            khs[key] = header.strip()
            kvs[key] = []
            continue

        kvs[key].append(line.strip())

    results: KHVContainer = {}
    for key, header in khs.items():
        v_: str | list[str] = kvs[key]

        if len(v_) == 1:
            v_ = v_[0]

        results[key] = (header, v_)

    return results


def get_runtime_setting_value(
    repo: "CodeRepository", setting_name: str
) -> str | list[str]:
    rts_path = find_runtime_settings_file(repo)
    settings = load_raw_runtime_settings(rts_path)

    if setting_name not in settings:
        raise ValueError(f"Unable to locate `{setting_name}`")

    _header, value = settings[setting_name]
    return value
