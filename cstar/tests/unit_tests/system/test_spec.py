"""SystemSpec: the system file schema, its refusals, and what each part builds."""

import copy
import json
import logging
import typing as t
from pathlib import Path
from unittest import mock

import pytest
import yaml
from pydantic import ValidationError

from cstar.system.environment import EnvSettingsBase, SlurmSettingsBase
from cstar.system.manager import context_from_file
from cstar.system.migration import CstarSchemaTooNewError
from cstar.system.scheduler import (
    PBSQueue,
    PBSScheduler,
    SlurmPartition,
    SlurmQOS,
    _cpus_per_node_via_sinfo,
    query_max_walltime_via_sacctmgr,
    query_max_walltime_via_sinfo,
)
from cstar.system.spec import (
    BUNDLED_LAYER,
    BUNDLED_SYSTEMS_DIR,
    ScratchSpec,
    SlurmSchedulerSpec,
    SystemFileError,
    SystemSpec,
    read_bundled_system_files,
    read_system_file,
)
from cstar.system.spec import path_is_dir as real_path_is_dir  # before the stand-in

SCHEMA_ROOT = Path(__file__).parents[4] / "docs" / "schemas" / "system"

MINIMAL: dict[str, t.Any] = {
    "name": "mycluster",
    "toolchains": {"cpu": {"compiler": "gnu", "mpi_prefix": "srun"}},
}
"""The smallest valid system file."""


def _slurm(**overrides: t.Any) -> dict[str, t.Any]:
    """A system file with a SLURM scheduler, its scheduler entry overridden."""
    scheduler = {
        "family": "slurm",
        "primary_queue": "batch",
        "queues": [{"name": "batch", "kind": "partition"}],
        **overrides,
    }
    return {**MINIMAL, "scheduler": scheduler}


def _pbs(**overrides: t.Any) -> dict[str, t.Any]:
    """A system file with a PBS scheduler, its scheduler entry overridden."""
    scheduler = {
        "family": "pbs",
        "primary_queue": "main",
        "queues": [{"name": "main", "max_walltime": "12:00:00"}],
        **overrides,
    }
    return {**MINIMAL, "scheduler": scheduler}


def _write(tmp_path: Path, data: dict[str, t.Any], stem: str = "") -> Path:
    """Write a system file named after its system unless `stem` says otherwise."""
    path = tmp_path / f"{stem or data['name']}.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


# --- the bundled files --------------------------------------------------------


def test_bundled_files_validate_with_unique_names() -> None:
    """Every bundled file validates at the current schema and names itself uniquely."""
    files = read_bundled_system_files()
    current = SystemSpec.model_fields["schema_version"].default

    assert len(files) == len(list(BUNDLED_SYSTEMS_DIR.glob("*.yaml")))
    assert len({f.spec.name for f in files}) == len(files)
    for f in files:
        assert f.spec.schema_version == current
        assert f.layer == BUNDLED_LAYER
        assert f.path.stem == f.spec.name


def test_published_schema_matches_the_model() -> None:
    """The published schema of the current version is the model's schema."""
    version = SystemSpec.model_fields["schema_version"].default
    path = SCHEMA_ROOT / f"system_schema.{version}.json"

    assert path.exists(), f"no published schema at {path}"
    assert json.loads(path.read_text()) == json.loads(
        json.dumps(SystemSpec.model_json_schema())
    ), f"{path.name} differs from the model's schema; bump schema_version"


def test_record_describes_the_file(tmp_path: Path) -> None:
    """A system file's record names its path, layer, digest and schema version."""
    path = _write(tmp_path, MINIMAL)

    record = read_system_file(path, "mine").record()

    assert record["path"] == path.as_posix()
    assert record["layer"] == "mine"
    assert len(record["sha256"]) == 64
    assert record["schema_version"] == "1.0.0"


# --- refusals -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("data", "match"),
    [
        pytest.param(
            {**MINIMAL, "name": "My Cluster"}, "should match pattern", id="bad-name"
        ),
        pytest.param(
            {**MINIMAL, "toolchains": {"gpu": MINIMAL["toolchains"]["cpu"]}},
            "cpu toolchain is required",
            id="no-cpu",
        ),
        pytest.param(
            {**MINIMAL, "toolchains": {"fpga": MINIMAL["toolchains"]["cpu"]}},
            "toolchains.fpga",
            id="unknown-toolchain",
        ),
        pytest.param({**MINIMAL, "modules": ["x"]}, "modules", id="unknown-key"),
        pytest.param(
            _slurm(queues=[{"name": "batch", "kind": "partition", "nodes": 4}]),
            "nodes",
            id="unknown-nested-key",
        ),
        pytest.param(
            _slurm(primary_queue="day"),
            "primary_queue 'day' is not in queues",
            id="primary-not-listed",
        ),
        pytest.param(
            _slurm(gpu_nodes={"gpus_per_node": 4, "queue": "gpu"}),
            "gpu_nodes.queue 'gpu' is not in queues",
            id="gpu-queue-not-listed",
        ),
        pytest.param(
            _slurm(
                queues=[
                    {"name": "batch", "kind": "partition"},
                    {"name": "batch", "kind": "qos"},
                ]
            ),
            "listed more than once: batch",
            id="duplicate-queue",
        ),
        pytest.param(
            _slurm(queues=[{"name": "batch"}]), "kind", id="slurm-queue-needs-kind"
        ),
        pytest.param(
            _slurm(
                queues=[{"name": "batch", "kind": "partition", "max_walltime": "1h"}]
            ),
            "max_walltime must be one of sinfo, sacctmgr",
            id="slurm-walltime-unknown",
        ),
        pytest.param(
            _pbs(
                queues=[
                    {"name": "main", "kind": "partition", "max_walltime": "1:00:00"}
                ]
            ),
            "kind",
            id="pbs-queue-has-no-kind",
        ),
        pytest.param(
            _pbs(queues=[{"name": "main"}]), "max_walltime", id="pbs-walltime-required"
        ),
        pytest.param(
            _pbs(requires_task_distribution=False),
            "requires_task_distribution",
            id="pbs-always-distributes",
        ),
        pytest.param(_slurm(require=["project"]), "require", id="unknown-requirement"),
        pytest.param(
            {**MINIMAL, "scheduler": {"family": "lsf", "primary_queue": "q"}},
            "family",
            id="unknown-family",
        ),
        pytest.param(
            {**MINIMAL, "detect": [{"env": "HOST"}]}, "detect", id="detect-env-no-value"
        ),
        pytest.param(
            {**MINIMAL, "detect": [{"command": "hostname"}]},
            "detect",
            id="detect-command",
        ),
        pytest.param(
            {
                **MINIMAL,
                "toolchains": {
                    "cpu": {**MINIMAL["toolchains"]["cpu"], "env": {"1BAD": "x"}}
                },
            },
            "should match pattern",
            id="bad-variable-name",
        ),
        pytest.param(
            {
                **MINIMAL,
                "toolchains": {
                    "cpu": {**MINIMAL["toolchains"]["cpu"], "env": {"FLAG": True}}
                },
            },
            "env.FLAG",
            id="boolean-env-value",
        ),
    ],
)
def test_spec_refuses(data: dict[str, t.Any], match: str) -> None:
    """Verify each invalid system file is refused, naming the problem."""
    with pytest.raises(ValidationError, match=match):
        SystemSpec.model_validate(data)


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(_pbs(queues=[{"name": "main", "max_walltime": 43200}]), id="pbs"),
        pytest.param(
            _slurm(
                queues=[{"name": "batch", "kind": "partition", "max_walltime": 43200}]
            ),
            id="slurm",
        ),
    ],
)
def test_unquoted_walltime_is_refused_with_advice(data: dict[str, t.Any]) -> None:
    """YAML reads an unquoted 12:00:00 as 43200; the refusal says to quote it."""
    text = yaml.safe_dump(data).replace("43200", "12:00:00")
    assert yaml.safe_load(text)["scheduler"]["queues"][0]["max_walltime"] == 43200

    with pytest.raises(ValidationError, match="a walltime must be quoted"):
        SystemSpec.model_validate(yaml.safe_load(text))


def test_unquoted_number_in_env_is_a_string() -> None:
    """An unquoted number in `env` is read as the string it spells."""
    data = copy.deepcopy(MINIMAL)
    data["toolchains"]["cpu"]["env"] = {"MPICH_GPU_SUPPORT_ENABLED": 1, "X": 1.5}

    spec = SystemSpec.model_validate(data)

    assert spec.toolchains["cpu"].env == {"MPICH_GPU_SUPPORT_ENABLED": "1", "X": "1.5"}


def test_env_keeps_its_order() -> None:
    """`env` entries keep the order the file gives them (later ones refer to earlier)."""
    data = copy.deepcopy(MINIMAL)
    data["toolchains"]["cpu"]["env"] = {"Z": "1", "A": "${Z}", "M": "${A}"}

    assert list(SystemSpec.model_validate(data).toolchains["cpu"].env) == [
        "Z",
        "A",
        "M",
    ]


def test_newer_major_is_refused(tmp_path: Path) -> None:
    """A file from a newer schema major is refused as too new."""
    path = _write(tmp_path, {**MINIMAL, "schema_version": "2.0.0"})

    with pytest.raises(SystemFileError) as ex:
        read_system_file(path, "mine")
    assert isinstance(ex.value.__cause__, CstarSchemaTooNewError)


def test_newer_minor_with_unknown_key_is_refused(tmp_path: Path) -> None:
    """A newer minor's key this build does not know refuses the file: a system
    file is never read tolerantly, because a dropped key changes the job.
    """
    path = _write(tmp_path, {**_pbs(resources={"x": "y"}), "schema_version": "1.1.0"})

    with pytest.raises(SystemFileError, match="resources"):
        read_system_file(path, "mine")


def test_name_must_match_the_file(tmp_path: Path) -> None:
    """A file named other than the system it describes is refused."""
    path = _write(tmp_path, MINIMAL, stem="othername")

    with pytest.raises(SystemFileError, match="must be named after it"):
        read_system_file(path, "mine")


def test_bad_yaml_is_refused(tmp_path: Path) -> None:
    """A file that is not YAML is refused with its path."""
    path = tmp_path / "broken.yaml"
    path.write_text("name: [unclosed\n")

    with pytest.raises(SystemFileError, match=str(path)):
        read_system_file(path, "mine")


# --- what the parts build -----------------------------------------------------


@pytest.mark.parametrize(
    ("queue", "exp_klass", "exp_query", "exp_walltime"),
    [
        pytest.param(
            {"name": "p", "kind": "partition"},
            SlurmPartition,
            "p",
            query_max_walltime_via_sinfo,
            id="partition-default",
        ),
        pytest.param(
            {"name": "q", "kind": "qos", "query_name": "q_1"},
            SlurmQOS,
            "q_1",
            query_max_walltime_via_sacctmgr,
            id="qos-default",
        ),
        pytest.param(
            {"name": "p", "kind": "partition", "max_walltime": "sacctmgr"},
            SlurmPartition,
            "p",
            query_max_walltime_via_sacctmgr,
            id="partition-sacctmgr",
        ),
    ],
)
def test_slurm_queue_builds(
    queue: dict[str, str],
    exp_klass: type,
    exp_query: str,
    exp_walltime: t.Callable[[str], str | None],
) -> None:
    """A SLURM queue entry becomes the queue class and walltime query it names."""
    spec = SystemSpec.model_validate(
        _slurm(queues=[queue], primary_queue=queue["name"])
    )
    assert isinstance(spec.scheduler, SlurmSchedulerSpec)

    built = spec.scheduler.queues[0].build()

    assert type(built) is exp_klass
    assert built.query_name == exp_query
    method = built._max_walltime_method or built._default_max_walltime_method
    assert method is exp_walltime


def test_fixed_slurm_walltime_needs_no_query() -> None:
    """A fixed SLURM walltime is answered without running sinfo or sacctmgr."""
    queue = {"name": "p", "kind": "partition", "max_walltime": "2-00:00:00"}
    spec = SystemSpec.model_validate(_slurm(queues=[queue], primary_queue="p"))
    assert spec.scheduler is not None

    with mock.patch("cstar.system.scheduler._run_cmd", side_effect=AssertionError):
        assert spec.scheduler.queues[0].build().max_walltime == "2-00:00:00"


def test_partition_capped_by_qos_asks_sinfo_by_partition_name() -> None:
    """A partition capped by its QOS (`max_walltime: sacctmgr`, `query_name` the
    QOS) asks sacctmgr about the QOS for its walltime and sinfo about the
    partition for its CPUs per node.
    """
    queue = {
        "name": "icx",
        "kind": "partition",
        "max_walltime": "sacctmgr",
        "query_name": "qicx",
    }
    spec = SystemSpec.model_validate(_slurm(queues=[queue], primary_queue="icx"))
    assert spec.scheduler is not None
    built = spec.scheduler.queues[0].build()

    commands: list[str] = []

    def answer(cmd: str, **_: object) -> str:
        commands.append(cmd)
        return "qicx|2-00:00:00" if cmd.startswith("sacctmgr") else "80 1"

    _cpus_per_node_via_sinfo.cache_clear()
    try:
        with mock.patch("cstar.system.scheduler._run_cmd", side_effect=answer):
            assert built.max_walltime == "48:00:00"
            assert built.max_cpus_per_node == 80
    finally:
        _cpus_per_node_via_sinfo.cache_clear()

    sacctmgr, sinfo = commands
    assert "show qos qicx " in sacctmgr
    assert "--partition=icx" in sinfo
    assert "qicx" not in sinfo


def test_pbs_scheduler_builds() -> None:
    """A PBS entry becomes a PBSScheduler that always distributes tasks."""
    spec = SystemSpec.model_validate(_pbs(max_cpus_per_node=128))
    assert spec.scheduler is not None

    scheduler = spec.scheduler.build("https://docs")

    assert type(scheduler) is PBSScheduler
    assert scheduler.requires_task_distribution is True
    assert scheduler.documentation == "https://docs"
    assert isinstance(scheduler.queues[0], PBSQueue)
    assert scheduler.queues[0].max_walltime == "12:00:00"


def test_pbs_honours_max_cpus_per_node() -> None:
    """A fixed `max_cpus_per_node` is used on PBS without querying pbsnodes."""
    scheduler = PBSScheduler(
        queues=[PBSQueue(name="main", max_walltime="12:00:00")],
        primary_queue_name="main",
        max_cpus_per_node=64,
    )

    with mock.patch("cstar.system.scheduler._run_cmd", side_effect=AssertionError):
        assert scheduler.global_max_cpus_per_node == 64


def test_pbs_queries_max_cpus_per_node_once() -> None:
    """Without a fixed value, PBS asks pbsnodes once and keeps the answer."""
    scheduler = PBSScheduler(
        queues=[PBSQueue(name="main", max_walltime="12:00:00")],
        primary_queue_name="main",
    )

    with mock.patch("cstar.system.scheduler._run_cmd", return_value="128") as run:
        assert scheduler.global_max_cpus_per_node == 128
        assert scheduler.global_max_cpus_per_node == 128
    run.assert_called_once()


@pytest.mark.parametrize(
    ("data", "exp_required"),
    [
        pytest.param(MINIMAL, None, id="no-scheduler"),
        pytest.param(_slurm(), set(), id="nothing-required"),
        pytest.param(
            _slurm(require=["account"]), {"CSTAR_SLURM_ACCOUNT"}, id="account"
        ),
        pytest.param(_slurm(require=["queue"]), {"CSTAR_SLURM_QUEUE"}, id="queue"),
        pytest.param(
            _slurm(require=["account", "queue"]),
            {"CSTAR_SLURM_ACCOUNT", "CSTAR_SLURM_QUEUE"},
            id="both",
        ),
        pytest.param(_pbs(require=["account"]), {"CSTAR_SLURM_ACCOUNT"}, id="pbs"),
    ],
)
def test_settings_class_requires_what_require_lists(
    data: dict[str, t.Any], exp_required: set[str] | None
) -> None:
    """`require` decides which settings a job must name; no scheduler needs none."""
    klass = SystemSpec.model_validate(data).settings_klass()

    if exp_required is None:
        assert klass is EnvSettingsBase
        return
    assert issubclass(klass, SlurmSettingsBase)
    assert klass.required_settings() == exp_required


def test_context_builds_its_settings_class_once(tmp_path: Path) -> None:
    """A context returns the same settings class on every call."""
    ctx = context_from_file(read_system_file(_write(tmp_path, _slurm()), "mine"))

    assert ctx.settings_klass() is ctx.settings_klass()


def test_context_without_gpu_toolchain_says_so(tmp_path: Path) -> None:
    """Asking for a toolchain the file does not list names the system."""
    ctx = context_from_file(read_system_file(_write(tmp_path, MINIMAL), "mine"))

    with pytest.raises(Exception, match="mycluster has no GPU toolchain"):
        ctx.toolchain("gpu")


def test_toolchain_profile_is_derived(tmp_path: Path) -> None:
    """The cpu toolchain has no profile name; any other is named by its key."""
    data = copy.deepcopy(MINIMAL)
    data["toolchains"]["gpu"] = {"compiler": "nvfortran", "mpi_prefix": "srun"}
    ctx = context_from_file(read_system_file(_write(tmp_path, data), "mine"))

    assert ctx.toolchain("cpu").profile == ""
    assert ctx.toolchain("gpu").profile == "gpu"


# --- detection and scratch ----------------------------------------------------


def test_detect_any_rule_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Any one `detect` rule selects the system: an env value, or a directory."""
    data = {
        **MINIMAL,
        "detect": [{"env": "SITE", "equals": "mine"}, {"path": "/site/mount"}],
    }
    ctx = context_from_file(read_system_file(_write(tmp_path, data), "mine"))
    monkeypatch.delenv("SITE", raising=False)

    assert not ctx.is_match()
    monkeypatch.setenv("SITE", "mine")
    assert ctx.is_match()
    monkeypatch.setenv("SITE", "other")
    monkeypatch.setattr("cstar.system.spec.path_is_dir", lambda p: p == "/site/mount")
    assert ctx.is_match()


def test_path_rule_never_raises(tmp_path: Path) -> None:
    """A directory that cannot be checked is no match, not an error."""
    assert real_path_is_dir(tmp_path.as_posix()) is True
    with mock.patch.object(Path, "is_dir", side_effect=PermissionError):
        assert real_path_is_dir("/oscar/home") is False


class TestScratchRule:
    """`scratch`: the first matching directory in sorted order, plus `subdir`."""

    @pytest.fixture
    def home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        home = tmp_path / "home"
        home.mkdir()
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("USER", "testuser")
        return home

    def test_first_sorted_directory_and_user(self, home: Path) -> None:
        for name in ("scratch_pi_b", "scratch_pi_a"):
            (home / name).mkdir()
        (home / "scratch_pi_0").touch()  # a file is never chosen

        rule = ScratchSpec(match="~/scratch_pi_*", subdir="{user}")

        assert rule.resolve() == home / "scratch_pi_a" / "testuser"

    def test_user_in_match(self, home: Path) -> None:
        (home / "testuser-scratch").mkdir()

        assert ScratchSpec(match="~/{user}-scratch").resolve() == (
            home / "testuser-scratch"
        )

    def test_no_match_is_none(self, home: Path) -> None:
        assert ScratchSpec(match="~/scratch").resolve() is None

    def test_failure_warns_and_is_none(
        self, home: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        with (
            caplog.at_level(logging.WARNING),
            mock.patch("cstar.system.spec.glob.glob", side_effect=OSError("stale")),
        ):
            assert ScratchSpec(match="~/scratch").resolve() is None
        assert "Failed to look for the scratch directory ~/scratch" in caplog.text
