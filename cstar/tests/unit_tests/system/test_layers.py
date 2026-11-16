import hashlib
import logging
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from cstar.base.env import (
    BUNDLED_CATALOG_ROOT,
    ENV_CSTAR_CATALOG,
    ENV_CSTAR_SYSTEM,
    CatalogRoots,
    catalog_roots,
)
from cstar.base.exceptions import CstarError
from cstar.base.provenance import system_spec_digest
from cstar.system import manager
from cstar.system.layers import (
    REASON_DIR_WORLD_WRITABLE,
    REASON_WORLD_WRITABLE,
    read_layer,
)
from cstar.system.manager import (
    SystemSelection,
    SystemSource,
    detection_would_select,
    file_in_use,
    get_registered_sys_contexts,
    registered_context,
    reload_system_files,
    select_system,
    system_stack,
)
from cstar.system.spec import (
    BUNDLED_LAYER,
    BUNDLED_SYSTEMS_DIR,
    DRAFT_LAYER,
    SHARED_LAYER,
    SYSTEMS_DIR_NAME,
    USER_LAYER,
    SystemFile,
    SystemFileError,
    read_system_file,
)

BUNDLED_TEMPLATE = BUNDLED_SYSTEMS_DIR / "linux_x86_64.yaml"

MakeFile = Callable[..., Path]


@pytest.fixture(autouse=True)
def _read_layers(system_file_layers: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Read `CSTAR_CATALOG` for every test here, and start from a clean registry."""
    monkeypatch.delenv(ENV_CSTAR_SYSTEM, raising=False)
    monkeypatch.setattr(manager, "_WARNED_SYSTEM", set())
    reload_system_files()


@pytest.fixture(autouse=True)
def _forget_layers_after() -> "object":
    """Drop the registry built from a test's directories when the test ends."""
    yield
    reload_system_files()


@pytest.fixture
def make_file() -> MakeFile:
    """Return a function writing a valid system file into a ``systems/`` directory."""

    def make(root: Path, name: str, *, text: str | None = None) -> Path:
        directory = root / SYSTEMS_DIR_NAME
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{name}.yaml"
        body = text if text is not None else BUNDLED_TEMPLATE.read_text()
        if text is None:
            body = body.replace("name: linux_x86_64", f"name: {name}", 1)
        path.write_text(body)
        path.chmod(0o644)
        return path

    return make


@pytest.fixture
def user_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Make a user catalog root the whole of `CSTAR_CATALOG`."""
    root = tmp_path / "user"
    root.mkdir()
    monkeypatch.setenv(ENV_CSTAR_CATALOG, str(root))
    return root.resolve()


def _file(name: str) -> SystemFile:
    """Return the system file in effect for a name."""
    file = registered_context(name).system_file
    assert file is not None
    return file


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


class TestCatalogRoots:
    """The local roots of `CSTAR_CATALOG`."""

    def test_unset_is_the_default_user_root(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """An unset variable gives the default user catalog and no shared one."""
        monkeypatch.delenv(ENV_CSTAR_CATALOG, raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))

        roots = catalog_roots()

        assert roots.user is not None
        assert roots.user.parts[-2:] == ("cstar", "catalog")
        assert roots.shared == ()

    def test_empty_is_the_default_user_root(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """An empty variable means the same as an unset one."""
        monkeypatch.setenv(ENV_CSTAR_CATALOG, "")
        monkeypatch.setenv("HOME", str(tmp_path))

        roots = catalog_roots()

        assert roots.user is not None
        assert roots.user.parts[-2:] == ("cstar", "catalog")
        assert roots.shared == ()

    def test_first_entry_is_user_and_the_rest_shared(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The first entry is the user layer and the later ones are shared."""
        a, b = tmp_path / "a", tmp_path / "b"
        monkeypatch.setenv(ENV_CSTAR_CATALOG, f"{a}{os.pathsep}{b}")

        roots = catalog_roots()

        assert roots == CatalogRoots(a.resolve(), (b.resolve(),))
        assert roots.local == (a.resolve(), b.resolve())

    def test_remote_entries_are_skipped(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A GitHub or http entry is not a local root."""
        b = tmp_path / "b"
        monkeypatch.setattr(
            "cstar.base.env.catalog_entries",
            lambda: ["https://github.com/org/repo", "http://x", str(b)],
        )

        roots = catalog_roots()

        assert roots.user is None
        assert roots.shared == (b.resolve(),)

    def test_local_literal_and_bundled_root_are_skipped(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Neither the literal ``local`` nor the bundled root is a layer."""
        a = tmp_path / "a"
        monkeypatch.setenv(
            ENV_CSTAR_CATALOG,
            os.pathsep.join([str(a), "local", str(BUNDLED_CATALOG_ROOT)]),
        )

        assert catalog_roots() == CatalogRoots(a.resolve(), ())

    def test_duplicates_are_dropped(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A directory listed twice, even spelled differently, counts once."""
        a = tmp_path / "a"
        a.mkdir()
        monkeypatch.setenv(
            ENV_CSTAR_CATALOG, os.pathsep.join([str(a), str(a / "."), str(a)])
        )

        assert catalog_roots() == CatalogRoots(a.resolve(), ())

    def test_tilde_is_expanded(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """``~`` in an entry is the home directory."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv(ENV_CSTAR_CATALOG, "~/mine")

        assert catalog_roots().user == (tmp_path / "mine").resolve()


class TestPrecedence:
    """Which layer's file is in effect."""

    def test_user_over_shared_over_bundled(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_file: MakeFile,
    ) -> None:
        """The top layer's file wins for a name found in several layers."""
        user, shared = tmp_path / "user", tmp_path / "shared"
        mine = make_file(user, "alpha")
        theirs = make_file(shared, "alpha")
        only_shared = make_file(shared, "beta")
        shadow_bundled = make_file(shared, "anvil")
        monkeypatch.setenv(ENV_CSTAR_CATALOG, f"{user}{os.pathsep}{shared}")

        assert _file("alpha").path == mine.resolve()
        assert _file("beta").path == only_shared.resolve()
        assert _file("anvil").path == shadow_bundled.resolve()
        assert _file("alpha").layer == USER_LAYER
        assert _file("beta").layer == SHARED_LAYER
        below = system_stack().layers.shadowed["alpha"]
        assert [f.path for f in below] == [theirs.resolve()]

    def test_detection_order_lists_user_systems_first(
        self, user_root: Path, make_file: MakeFile
    ) -> None:
        """Detection tries the user layer's systems before the bundled ones."""
        make_file(user_root, "alpha")

        names = [c.name for c in get_registered_sys_contexts()]

        assert names[0] == "alpha"
        assert "linux_x86_64" in names[1:]


class TestShadowing:
    """A file hiding another of the same name."""

    def test_warns_once_naming_both_paths(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_file: MakeFile,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """One warning names the shadowing file and the one it hides."""
        user, shared = tmp_path / "user", tmp_path / "shared"
        mine = make_file(user, "alpha")
        theirs = make_file(shared, "alpha")
        monkeypatch.setenv(ENV_CSTAR_CATALOG, f"{user}{os.pathsep}{shared}")

        with caplog.at_level(logging.WARNING):
            system_stack()
            reload_system_files()
            system_stack()

        shadows = [m for m in _warnings(caplog) if "shadows" in m]
        assert len(shadows) == 1
        assert str(mine.resolve()) in shadows[0]
        assert str(theirs.resolve()) in shadows[0]

    def test_shadowing_a_bundled_system_is_described(
        self, user_root: Path, make_file: MakeFile
    ) -> None:
        """A user ``perlmutter.yaml`` is used, and the label says it shadows bundled."""
        path = make_file(
            user_root,
            "perlmutter",
            text=(BUNDLED_SYSTEMS_DIR / "perlmutter.yaml").read_text(),
        )

        context = registered_context("perlmutter")
        note = file_in_use(context)
        described = SystemSelection(context, SystemSource.DETECTED).describe()

        assert _file(context.name).path == path.resolve()
        assert note.startswith("from ")
        assert note.endswith("perlmutter.yaml, shadows bundled")
        assert described == f"perlmutter ({note})"

    def test_bundled_file_has_no_note(self) -> None:
        """A bundled system needs no note."""
        assert file_in_use(registered_context("linux_x86_64")) == ""

    def test_shadowing_a_lower_layer_names_its_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, make_file: MakeFile
    ) -> None:
        """A file over a shared one says ``shadows <path>``."""
        user, shared = tmp_path / "user", tmp_path / "shared"
        make_file(user, "alpha")
        theirs = make_file(shared, "alpha")
        monkeypatch.setenv(ENV_CSTAR_CATALOG, f"{user}{os.pathsep}{shared}")

        note = file_in_use(registered_context("alpha"))

        assert note.endswith(f"alpha.yaml, shadows {theirs.resolve()}")


class TestRefusals:
    """Files that cannot be used."""

    def _refused(self, name: str) -> str:
        reasons = [r.reason for r in system_stack().layers.refusals(name)]
        assert len(reasons) == 1
        return reasons[0]

    def test_bad_yaml(
        self,
        user_root: Path,
        make_file: MakeFile,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A file that is not YAML warns with its path and is skipped."""
        path = make_file(user_root, "broken", text="name: [unclosed\n")

        with caplog.at_level(logging.WARNING):
            stack = system_stack()

        assert "broken" not in stack.contexts
        assert [r.path for r in stack.layers.refused] == [path.resolve()]
        assert any(str(path.resolve()) in m for m in _warnings(caplog))

    def test_schema_too_new(self, user_root: Path, make_file: MakeFile) -> None:
        """A file from a newer major schema is refused."""
        text = BUNDLED_TEMPLATE.read_text().replace(
            "schema_version: 1.0.0", "schema_version: 2.0.0", 1
        )
        make_file(user_root, "linux_x86_64", text=text)

        assert self._refused("linux_x86_64")
        assert _file("linux_x86_64").layer == BUNDLED_LAYER

    def test_newer_minor_with_unknown_key(
        self, user_root: Path, make_file: MakeFile
    ) -> None:
        """A newer minor version carrying a key this release lacks is refused."""
        text = BUNDLED_TEMPLATE.read_text().replace(
            "schema_version: 1.0.0",
            "schema_version: 1.99.0\nfuture_key: 1",
            1,
        )
        make_file(user_root, "linux_x86_64", text=text)

        assert self._refused("linux_x86_64")

    def test_unknown_key(self, user_root: Path, make_file: MakeFile) -> None:
        """A key the schema does not know is refused."""
        text = BUNDLED_TEMPLATE.read_text() + "\nnot_a_key: 1\n"
        make_file(user_root, "linux_x86_64", text=text)

        assert "not_a_key" in self._refused("linux_x86_64")

    def test_name_not_matching_file_name(
        self, user_root: Path, make_file: MakeFile
    ) -> None:
        """A file describing another system than its name says is refused."""
        text = BUNDLED_TEMPLATE.read_text()
        make_file(user_root, "other", text=text)

        assert "must be named after it" in self._refused("other")

    def test_world_writable_file(self, user_root: Path, make_file: MakeFile) -> None:
        """A file every user may write is refused."""
        path = make_file(user_root, "alpha")
        path.chmod(0o666)

        assert self._refused("alpha") == REASON_WORLD_WRITABLE
        assert "alpha" not in system_stack().contexts

    def test_world_writable_directory_refuses_every_file(
        self, user_root: Path, make_file: MakeFile
    ) -> None:
        """Every file in a directory every user may write is refused."""
        a = make_file(user_root, "alpha")
        make_file(user_root, "beta")
        directory = a.parent
        directory.chmod(0o777)
        try:
            stack = system_stack()
            reasons = {r.name: r.reason for r in stack.layers.refused}

            assert set(reasons) == {"alpha", "beta"}
            expected = REASON_DIR_WORLD_WRITABLE.format(directory=directory.resolve())
            assert set(reasons.values()) == {expected}
            assert not {"alpha", "beta"} & set(stack.contexts)
        finally:
            directory.chmod(0o755)

    def test_refused_over_bundled_uses_the_lower_file_and_says_so(
        self,
        user_root: Path,
        make_file: MakeFile,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A refused user file leaves the bundled one in effect, and the warning says so."""
        path = make_file(user_root, "anvil", text="name: anvil\nbogus: 1\n")

        with caplog.at_level(logging.WARNING):
            context = registered_context("anvil")

        assert _file(context.name).layer == BUNDLED_LAYER
        message = next(m for m in _warnings(caplog) if "Skipping" in m)
        assert str(path.resolve()) in message
        assert "instead" in message

    def test_refused_over_shared_uses_the_shared_file(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_file: MakeFile,
    ) -> None:
        """A refused user file leaves the shared one in effect."""
        user, shared = tmp_path / "user", tmp_path / "shared"
        make_file(user, "alpha", text="name: alpha\nbogus: 1\n")
        theirs = make_file(shared, "alpha")
        monkeypatch.setenv(ENV_CSTAR_CATALOG, f"{user}{os.pathsep}{shared}")

        assert _file("alpha").path == theirs.resolve()

    def test_missing_directory_is_not_an_error(self, tmp_path: Path) -> None:
        """A layer with no ``systems/`` directory finds and refuses nothing."""
        assert read_layer(tmp_path / "nope", USER_LAYER) == ([], [])


class TestSelectingARefusedOrMissingSystem:
    """CSTAR_SYSTEM naming a system with no usable file."""

    def test_refused_file_error_carries_reason_and_directories(
        self,
        user_root: Path,
        make_file: MakeFile,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The error names the refused file, why, and the directories searched."""
        path = make_file(user_root, "alpha", text="name: alpha\nbogus: 1\n")
        monkeypatch.setenv(ENV_CSTAR_SYSTEM, "alpha")

        with pytest.raises(CstarError) as info:
            select_system()

        message = str(info.value)
        assert str(path.resolve()) in message
        assert "bogus" in message
        assert str(user_root / SYSTEMS_DIR_NAME) in message
        assert str(BUNDLED_SYSTEMS_DIR) in message

    def test_missing_name_error_lists_directories(
        self,
        user_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An unknown name lists where C-Star looked."""
        monkeypatch.setenv(ENV_CSTAR_SYSTEM, "nowhere")

        with pytest.raises(CstarError) as info:
            select_system()

        message = str(info.value)
        assert "not a registered system" in message
        assert str(user_root / SYSTEMS_DIR_NAME) in message
        assert str(BUNDLED_SYSTEMS_DIR) in message

    def test_registered_context_names_the_refused_file(
        self, user_root: Path, make_file: MakeFile
    ) -> None:
        """`registered_context` gives the refusal's reason, not just 'not registered'."""
        make_file(user_root, "alpha", text="name: alpha\nbogus: 1\n")

        with pytest.raises(CstarError, match="cannot be used"):
            registered_context("alpha")


class TestRegistryRebuild:
    """The registry follows the search path."""

    def test_changing_catalog_rebuilds(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_file: MakeFile,
    ) -> None:
        """A new `CSTAR_CATALOG` shows its systems on the next lookup."""
        first, second = tmp_path / "first", tmp_path / "second"
        make_file(first, "alpha")
        make_file(second, "beta")
        monkeypatch.setenv(ENV_CSTAR_CATALOG, str(first))
        assert registered_context("alpha")
        with pytest.raises(CstarError):
            registered_context("beta")

        monkeypatch.setenv(ENV_CSTAR_CATALOG, str(second))

        assert registered_context("beta")
        with pytest.raises(CstarError):
            registered_context("alpha")

    def test_reload_picks_up_a_new_file(
        self, user_root: Path, make_file: MakeFile
    ) -> None:
        """A file added to a layer already read appears after `reload_system_files`."""
        with pytest.raises(CstarError):
            registered_context("alpha")
        make_file(user_root, "alpha")

        with pytest.raises(CstarError):
            registered_context("alpha")
        reload_system_files()

        assert registered_context("alpha").name == "alpha"


class TestReadSystemFileName:
    """`check_name` on `read_system_file`."""

    def test_draft_may_be_named_differently(self, tmp_path: Path) -> None:
        """With ``check_name=False`` a draft need not be named after its system."""
        draft = tmp_path / "draft.yaml"
        draft.write_text(BUNDLED_TEMPLATE.read_text())

        read = read_system_file(draft, DRAFT_LAYER, check_name=False)

        assert read.spec.name == "linux_x86_64"
        assert read.layer == DRAFT_LAYER

    def test_default_refuses_a_different_name(self, tmp_path: Path) -> None:
        """By default the file name must match."""
        draft = tmp_path / "draft.yaml"
        draft.write_text(BUNDLED_TEMPLATE.read_text())

        with pytest.raises(SystemFileError, match="named after it"):
            read_system_file(draft, DRAFT_LAYER)


class TestDetectionWouldSelect:
    """Whether detection on this machine picks a system file's system."""

    @pytest.fixture(autouse=True)
    def _bare_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for name in [n for n in os.environ if n.startswith("LMOD_")]:
            monkeypatch.delenv(name)
        monkeypatch.delenv("MY_SITE", raising=False)

    def _spec(self, tmp_path: Path, name: str, detect: str = "") -> "object":
        text = BUNDLED_TEMPLATE.read_text().replace(
            "name: linux_x86_64", f"name: {name}\n{detect}", 1
        )
        path = tmp_path / f"{name}.yaml"
        path.write_text(text)
        return read_system_file(path, DRAFT_LAYER).spec

    def test_lmod_name_equal_to_the_spec(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Lmod reporting the spec's name selects it."""
        monkeypatch.setenv("LMOD_SYSHOST", "alpha")

        assert detection_would_select(self._spec(tmp_path, "alpha")) is True  # type: ignore[arg-type]

    def test_detect_rule_without_lmod(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A matching ``detect`` rule selects the system when Lmod says nothing."""
        monkeypatch.setenv("MY_SITE", "yes")
        spec = self._spec(
            tmp_path, "alpha", "detect:\n  - {env: MY_SITE, equals: 'yes'}"
        )

        assert detection_would_select(spec) is True  # type: ignore[arg-type]

    def test_rule_not_matching(self, tmp_path: Path) -> None:
        """A rule that does not match, and no Lmod name, selects nothing."""
        spec = self._spec(
            tmp_path, "alpha", "detect:\n  - {env: MY_SITE, equals: 'yes'}"
        )

        assert detection_would_select(spec) is False  # type: ignore[arg-type]

    def test_lmod_reports_another_registered_system(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Lmod naming a different registered system wins over a matching rule."""
        monkeypatch.setenv("LMOD_SYSHOST", "perlmutter")
        monkeypatch.setenv("MY_SITE", "yes")
        spec = self._spec(
            tmp_path, "alpha", "detect:\n  - {env: MY_SITE, equals: 'yes'}"
        )

        assert detection_would_select(spec) is False  # type: ignore[arg-type]


class TestSystemSpecDigest:
    """The digest of the system file in use."""

    def test_digest_of_the_bundled_file(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The digest is sha256 of the file's bytes."""
        monkeypatch.setenv(ENV_CSTAR_SYSTEM, "linux_x86_64")
        expected = hashlib.sha256(BUNDLED_TEMPLATE.read_bytes()).hexdigest()

        assert system_spec_digest() == f"sha256:{expected}"

    def test_empty_when_selection_raises_cstar_error(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A selection failure leaves the digest empty without a warning."""

        def boom() -> None:
            raise CstarError("no system")

        monkeypatch.setattr(manager, "selected_system_file", boom)

        with caplog.at_level(logging.WARNING):
            assert system_spec_digest() == ""
        assert _warnings(caplog) == []

    def test_empty_and_warns_on_an_unexpected_error(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Any other failure leaves the digest empty and logs a warning."""

        def boom() -> None:
            raise RuntimeError("surprise")

        monkeypatch.setattr(manager, "selected_system_file", boom)

        with caplog.at_level(logging.WARNING):
            assert system_spec_digest() == ""
        assert any("surprise" in m for m in _warnings(caplog))


def test_importing_the_manager_does_not_import_the_domain_catalog() -> None:
    """The system manager stays independent of the catalog package's domain code."""
    code = (
        "import sys, cstar.system.manager; "
        "sys.exit(1 if 'cstar.catalog.domain_catalog' in sys.modules else 0)"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr
