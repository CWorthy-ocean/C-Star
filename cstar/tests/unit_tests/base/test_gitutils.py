import os
import shutil
import subprocess
import warnings
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.gitutils import (
    _check_local_repo_changed_from_remote,
    _clone_and_checkout,
    _get_hash_from_checkout_target,
    _get_repo_head_hash,
    _get_repo_remote,
    _has_local_modifications,
    _local_repo_mismatch,
    git_location_to_raw,
)


def _make_repo(path: Path) -> str:
    """Initialize a real git repository at `path` with one tracked commit.

    Uses an explicit initial branch name (`main`) so tests do not depend on the
    ambient `init.defaultBranch` config, and returns the HEAD commit hash.
    """
    subprocess.run(
        ["git", "init", "-b", "main"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    )
    (path / "file.txt").write_text("hello\n")
    subprocess.run(
        ["git", "add", "file.txt"], cwd=path, check=True, capture_output=True, text=True
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.email=test@example.com",
            "-c",
            "user.name=Test",
            "commit",
            "-m",
            "init",
        ],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    )
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_get_repo_remote():
    """Test `_get_repo_remote` to confirm it returns the correct remote URL when `git
    remote get-url origin` succeeds.

    Asserts
    -------
    - Ensures the returned remote URL matches the expected URL.
    """
    local_path = "/dummy/path"
    expected_url = "https://example.com/repo.git"

    # Patch subprocess.run to simulate successful git command
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.Mock(returncode=0, stdout=expected_url + "\n")

        # Call the function
        result = _get_repo_remote(local_path)

        # Check the function output and subprocess call arguments
        assert result == expected_url
        mock_run.assert_called_once_with(
            f"git -C {local_path} remote get-url origin",
            shell=True,
            capture_output=True,
            text=True,
        )


def test_get_repo_head_hash():
    """Test `_get_repo_head_hash` to confirm it returns the correct commit hash when
    `git rev-parse HEAD` succeeds.

    Asserts
    -------
    - Ensures the returned commit hash matches the expected hash.
    """
    local_path = "/dummy/path"
    expected_hash = "abcdef1234567890abcdef1234567890abcdef12"

    # Patch subprocess.run to simulate successful git command
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.Mock(returncode=0, stdout=expected_hash + "\n")

        # Call the function
        result = _get_repo_head_hash(local_path)

        # Check the function output and subprocess call arguments
        assert result == expected_hash
        mock_run.assert_called_once_with(
            f"git -C {local_path} rev-parse HEAD",
            shell=True,
            capture_output=True,
            text=True,
        )


@pytest.mark.parametrize(
    "repo_url, checkout_target, filename, subdir, expected",
    [
        (
            "https://github.com/user/repo.git",
            "main",
            "file.txt",
            "src",
            "https://raw.githubusercontent.com/user/repo/main/src/file.txt",
        ),
        (
            "https://github.com/user/repo.git",
            "v1.0.0",
            "README.md",
            "",
            "https://raw.githubusercontent.com/user/repo/v1.0.0//README.md",
        ),
        (
            "https://gitlab.com/user/repo.git",
            "dev",
            "config.yml",
            "configs",
            "https://gitlab.com/user/repo/-/raw/dev/configs/config.yml",
        ),
        (
            "https://bitbucket.org/team/repo.git",
            "feature-branch",
            "app.py",
            "",
            "https://bitbucket.org/team/repo/raw/feature-branch//app.py",
        ),
    ],
)
def test_git_location_to_raw(repo_url, checkout_target, filename, subdir, expected):
    """Tests that `git_location_to_raw` successfully converts parameters into raw file URL.

    Parameters
    ----------
    repo_url:
        The URL of the repository housing the file
    checkout_target:
        The point in the commit history to fetch the file from
    filename:
        The name of the file being addressed
    subdir:
        The subdirectory path within the repository to find the file
    expected:
        The expected raw file URL based on the other parameters
    """
    result = git_location_to_raw(repo_url, checkout_target, filename, subdir)
    assert result == expected


@pytest.mark.parametrize(
    "repo_url, checkout_target, filename, expected_msg",
    [
        (
            "git@github.com:user/repo.git",
            "main",
            "file.txt",
            "Please provide a HTTP",
        ),
        (
            "https://example.com/user/repo.git",
            "main",
            "file.txt",
            "unsupported",
        ),
    ],
)
def test_git_location_to_raw_errors(repo_url, checkout_target, filename, expected_msg):
    """Tests that `git_location_to_raw` raises if provided an incompatible URL or git service.

    Parameters
    ----------
    repo_url:
        The URL of the repository housing the file
    checkout_target:
        The point in the commit history to fetch the file from
    filename:
        The name of the file being addressed
    subdir:
        The subdirectory path within the repository to find the file
    expected_msg:
        The error message raised by the above parameter combination.
    """
    with pytest.raises(ValueError, match=expected_msg):
        git_location_to_raw(repo_url, checkout_target, filename)


class TestHasLocalModifications:
    """Tests for `_has_local_modifications` against real git repositories in `tmp_path`."""

    def test_clean_repo_is_not_modified(self, tmp_path):
        """A freshly committed repo with no further changes is not modified."""
        _make_repo(tmp_path)
        assert _has_local_modifications(tmp_path) is False

    def test_copied_checkout_is_not_reported_modified(self, tmp_path):
        """Regression for the Anvil bug: a copied checkout must not appear dirty.

        `shutil.copytree` preserves mtimes but assigns new inodes and ctimes,
        leaving git's cached stat data in `.git/index` stale. Plumbing `git
        diff-index HEAD` trusts that cache and reports every tracked file as
        modified purely because of the copy; `_has_local_modifications` uses
        `git status`, which re-hashes stale entries instead of trusting them.
        The final `diff-index` call confirms the check never rewrote the index
        (i.e. it stayed read-only) despite git internally refreshing it.
        """
        src = tmp_path / "src"
        src.mkdir()
        _make_repo(src)

        copy = tmp_path / "copy"
        shutil.copytree(src, copy, symlinks=True, copy_function=shutil.copy2)

        diff_index_before = subprocess.run(
            ["git", "diff-index", "HEAD"],
            cwd=copy,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        assert diff_index_before.strip(), (
            "expected the stale-index trap (copytree vs. diff-index) to "
            "reproduce on this platform"
        )

        assert _has_local_modifications(copy) is False

        diff_index_after = subprocess.run(
            ["git", "diff-index", "HEAD"],
            cwd=copy,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        assert diff_index_after.strip()

    def test_edited_tracked_file_is_modified(self, tmp_path):
        """An actual content change to a tracked file is reported as modified."""
        _make_repo(tmp_path)
        (tmp_path / "file.txt").write_text("changed\n")
        assert _has_local_modifications(tmp_path) is True

    def test_untracked_file_only_is_not_modified(self, tmp_path):
        """A new untracked file alone does not count as a local modification."""
        _make_repo(tmp_path)
        (tmp_path / "new_file.txt").write_text("new\n")
        assert _has_local_modifications(tmp_path) is False


class TestLocalRepoMismatch:
    """Tests for `_local_repo_mismatch` against real git repositories in `tmp_path`."""

    def test_target_is_head_hash(self, tmp_path):
        """The HEAD hash itself is always a clean checkout of itself."""
        head = _make_repo(tmp_path)
        assert _local_repo_mismatch(tmp_path, head) == ""

    def test_target_is_branch_name(self, tmp_path):
        """The branch checked out at HEAD resolves cleanly."""
        _make_repo(tmp_path)
        assert _local_repo_mismatch(tmp_path, "main") == ""

    def test_target_is_lightweight_tag(self, tmp_path):
        """A lightweight tag at HEAD resolves cleanly."""
        _make_repo(tmp_path)
        subprocess.run(
            ["git", "tag", "v1"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
        )
        assert _local_repo_mismatch(tmp_path, "v1") == ""

    def test_target_is_annotated_tag(self, tmp_path):
        """An annotated tag at HEAD resolves cleanly via its peeled commit."""
        _make_repo(tmp_path)
        subprocess.run(
            [
                "git",
                "-c",
                "user.email=test@example.com",
                "-c",
                "user.name=Test",
                "tag",
                "-a",
                "v2",
                "-m",
                "msg",
            ],
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
        )
        assert _local_repo_mismatch(tmp_path, "v2") == ""

    def test_branch_wins_over_same_named_tag(self, tmp_path):
        """A name that is both a branch and a tag resolves to the branch, as
        `git checkout` and `_get_hash_from_checkout_target` do.
        """
        _make_repo(tmp_path)
        subprocess.run(
            ["git", "tag", "dupname"], cwd=tmp_path, check=True, capture_output=True
        )
        (tmp_path / "file.txt").write_text("second\n")
        subprocess.run(
            [
                "git",
                "-c",
                "user.email=test@example.com",
                "-c",
                "user.name=Test",
                "commit",
                "-qam",
                "second",
            ],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "branch", "dupname"], cwd=tmp_path, check=True, capture_output=True
        )
        subprocess.run(
            ["git", "checkout", "-q", "dupname"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )

        assert _local_repo_mismatch(tmp_path, "dupname") == ""

    def test_unreadable_repo_reports_inspection_failure(self, tmp_path):
        """A directory git cannot read as a repository reports git's own
        failure instead of claiming the target is missing.
        """
        not_a_repo = tmp_path / "not_a_repo"
        not_a_repo.mkdir()

        with mock.patch.dict(os.environ, {"GIT_CEILING_DIRECTORIES": str(tmp_path)}):
            result = _local_repo_mismatch(not_a_repo, "main")

        assert result.startswith("git inspection failed")
        assert "not present" not in result

    def test_unknown_target_is_not_present(self, tmp_path):
        """An unresolvable target names itself as not present in the clone."""
        _make_repo(tmp_path)
        assert "not present in the clone" in _local_repo_mismatch(tmp_path, "nope")

    def test_head_moved_off_target(self, tmp_path):
        """HEAD advancing past the target reports both the actual and expected hash."""
        first = _make_repo(tmp_path)
        (tmp_path / "file.txt").write_text("second\n")
        subprocess.run(
            ["git", "add", "file.txt"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [
                "git",
                "-c",
                "user.email=test@example.com",
                "-c",
                "user.name=Test",
                "commit",
                "-m",
                "second",
            ],
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
        )
        assert "HEAD is at" in _local_repo_mismatch(tmp_path, first)

    def test_edited_tracked_file_with_target_at_head(self, tmp_path):
        """A dirty checkout that is otherwise at the target commit is reported as such."""
        head = _make_repo(tmp_path)
        (tmp_path / "file.txt").write_text("changed\n")
        assert (
            _local_repo_mismatch(tmp_path, head)
            == "tracked files have local modifications"
        )

    def test_copied_checkout_matches_target(self, tmp_path):
        """End-to-end bug scenario: a copied (stale-index) checkout at the target
        commit is reported clean.
        """
        src = tmp_path / "src"
        src.mkdir()
        head = _make_repo(src)

        copy = tmp_path / "copy"
        shutil.copytree(src, copy, symlinks=True, copy_function=shutil.copy2)

        assert _local_repo_mismatch(copy, head) == ""

    def test_shell_metacharacters_in_target_are_quoted(self, tmp_path):
        """A target containing shell metacharacters is treated as a literal ref
        name, not interpreted by the shell.
        """
        _make_repo(tmp_path)
        target = "main; touch pwned"

        result = _local_repo_mismatch(tmp_path, target)

        assert "not present" in result
        assert not list(tmp_path.glob("pwned*"))


class TestCheckLocalRepoChangedFromRemoteDirtyCheck:
    """Tests for `_check_local_repo_changed_from_remote`'s post-hash-match dirty
    check, using a real `tmp_path` repo and a patched hash lookup.
    """

    def test_clean_repo_at_expected_hash_is_unchanged(self, tmp_path):
        """A clean repo whose HEAD matches the (patched) expected hash is unchanged."""
        head = _make_repo(tmp_path)
        with mock.patch(
            "cstar.base.gitutils._get_hash_from_checkout_target", return_value=head
        ):
            assert (
                _check_local_repo_changed_from_remote(
                    "https://example.com/repo.git", tmp_path, "main"
                )
                is False
            )

    def test_edited_repo_at_expected_hash_is_changed(self, tmp_path):
        """A repo edited after its HEAD hash was recorded is reported as changed."""
        head = _make_repo(tmp_path)
        (tmp_path / "file.txt").write_text("changed\n")
        with mock.patch(
            "cstar.base.gitutils._get_hash_from_checkout_target", return_value=head
        ):
            assert (
                _check_local_repo_changed_from_remote(
                    "https://example.com/repo.git", tmp_path, "main"
                )
                is True
            )


class TestCloneAndCheckout:
    """Tests for `utils._clone_and_checkout` function, verifying it handles both success
    and failure cases for git clone and checkout operations.

    Mocks
    -----
    subprocess.run : Mock
        Used to simulate success or failure of `git clone` and `git checkout` commands.
    """

    def setup_method(self):
        """Sets up common parameters and patches subprocess for all tests."""
        self.source_repo = "https://example.com/repo.git"
        self.local_path = "/dummy/path"
        self.checkout_target = "main"

        # Patch subprocess.run for all tests
        self.patch_subprocess_run = mock.patch("subprocess.run")
        self.mock_subprocess_run = self.patch_subprocess_run.start()

    def teardown_method(self):
        """Stops patching subprocess after each test."""
        self.patch_subprocess_run.stop()

    def test_clone_and_checkout_success(self):
        """Test that `_clone_and_checkout` runs successfully when both clone and
        checkout commands succeed.

        Asserts
        -------
        - Ensures `subprocess.run` is called twice with the correct arguments.
        """
        # Set the mock to simulate successful clone and checkout commands
        self.mock_subprocess_run.return_value = mock.Mock(returncode=0, stderr="")

        # Call the function
        _clone_and_checkout(self.source_repo, self.local_path, self.checkout_target)

        # Validate subprocess.run is called twice (clone and checkout)
        clone_call = self.mock_subprocess_run.call_args_list[0]
        checkout_call = self.mock_subprocess_run.call_args_list[1]

        # Check the clone command arguments
        assert clone_call[0][0] == f"git clone {self.source_repo} {self.local_path}"
        # Check the checkout command with correct directory and target
        assert (
            checkout_call[0][0]
            == f"git -C {self.local_path} checkout {self.checkout_target}"
        )

    def test_clone_and_checkout_clone_failure(self):
        """Test `_clone_and_checkout` raises RuntimeError if `git clone` fails.

        Asserts
        -------
        - Verifies RuntimeError is raised with an appropriate error message on clone failure.
        """
        # Simulate failure in the clone command
        self.mock_subprocess_run.side_effect = [
            mock.Mock(returncode=1, stderr="Error: clone failed."),
            mock.Mock(returncode=0),  # Checkout won't be reached
        ]

        # Check that the function raises a RuntimeError on clone failure
        with pytest.raises(RuntimeError, match="Error when cloning"):
            _clone_and_checkout(self.source_repo, self.local_path, self.checkout_target)

    def test_clone_and_checkout_checkout_failure(self):
        """Test `_clone_and_checkout` raises RuntimeError if `git checkout` fails.

        Asserts
        -------
        - Verifies RuntimeError is raised with an appropriate error message on checkout failure.
        """
        # Simulate successful clone and failed checkout
        self.mock_subprocess_run.side_effect = [
            mock.Mock(returncode=0, stderr=""),
            mock.Mock(returncode=1, stderr="Error: checkout failed."),
        ]

        # Check that the function raises a RuntimeError on checkout failure
        with pytest.raises(RuntimeError, match="Error when checking out"):
            _clone_and_checkout(self.source_repo, self.local_path, self.checkout_target)


class TestGetHashFromCheckoutTarget:
    """Test class for `_get_hash_from_checkout_target`."""

    def setup_method(self):
        """Setup method to define common variables and mock data."""
        self.repo_url = "https://example.com/repo.git"

        # Mock the output of `git ls-remote` with a variety of refs
        self.ls_remote_output = (
            "abcdef1234567890abcdef1234567890abcdef12\trefs/heads/main\n"  # Branch
            "deadbeef1234567890deadbeef1234567890deadbeef\trefs/heads/feature\n"  # Branch
            "c0ffee1234567890c0ffee1234567890c0ffee1234\trefs/tags/v1.0.0\n"  # Tag
            "feedface1234567890feedface1234567890feedface\trefs/pull/123/head\n"  # Pull request
            "1234567890abcdef1234567890abcdef12345678\trefs/heads/develop\n"  # Branch
            "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1\trefs/tags/v2.0.0\n"  # Annotated tag object
            "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2\trefs/tags/v2.0.0^{}\n"  # ...peeled commit
            "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3\trefs/tags/main\n"  # Tag named like a branch
        )

        # Patch subprocess.run to simulate the `git ls-remote` command
        self.mock_run = mock.patch("subprocess.run").start()
        self.mock_run.return_value = mock.Mock(
            returncode=0, stdout=self.ls_remote_output
        )

    def teardown_method(self):
        """Teardown method to stop all patches."""
        mock.patch.stopall()

    @pytest.mark.parametrize(
        "checkout_target, expected_hash",
        [
            pytest.param(target, hash, id=target)
            for target, hash in [
                # Branches
                ("main", "abcdef1234567890abcdef1234567890abcdef12"),
                ("develop", "1234567890abcdef1234567890abcdef12345678"),
                # Tags
                ("v1.0.0", "c0ffee1234567890c0ffee1234567890c0ffee1234"),
                # Annotated tag: resolves to its peeled commit, not the tag object.
                # (The "main" case above already covers a branch shadowing a
                # same-named tag, now that refs/tags/main is also in the output.)
                ("v2.0.0", "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"),
                # Commit hashes
                (
                    "1234567890abcdef1234567890abcdef12345678",
                    "1234567890abcdef1234567890abcdef12345678",
                ),
            ]
        ],
    )
    def test_valid_targets(self, checkout_target, expected_hash):
        """Test `_get_hash_from_checkout_target` with valid checkout targets.

        Parameters
        ----------
        checkout_target : str
            The checkout target to test (branch, tag, pull request, or commit hash).
        expected_hash : str
            The expected commit hash for the given checkout target.
        """
        # Call the function and assert the result
        result = _get_hash_from_checkout_target(self.repo_url, checkout_target)
        assert result == expected_hash

        # Verify the subprocess call
        self.mock_run.assert_called_with(
            f"git ls-remote {self.repo_url}",
            capture_output=True,
            shell=True,
            text=True,
        )

    def test_invalid_target(self):
        """Test `_get_hash_from_checkout_target` with an invalid checkout target.

        Asserts
        -------
        - A ValueError is raised.
        - The error message includes a list of available branches and tags.
        """
        checkout_target = "invalid-branch"

        # Call the function and expect a ValueError
        with pytest.raises(ValueError) as exception_info:
            _get_hash_from_checkout_target(self.repo_url, checkout_target)

        # Assert the error message includes the expected content
        error_message = str(exception_info.value)
        assert checkout_target in error_message
        assert self.repo_url in error_message
        assert "Available branches:" in error_message
        assert "Available tags:" in error_message
        assert "main" in error_message
        assert "feature" in error_message
        assert "v1.0.0" in error_message

    @pytest.mark.parametrize(
        "checkout_target, should_warn, should_raise",
        [
            # 7-character hex string (valid short hash)
            ("246c11f", True, False),
            # 40-character hex string (valid full hash)
            ("246c11fa537145ba5868f2256dfb4964aeb09a25", True, False),
            # 8-character hex string (invalid length)
            ("246c11fa", False, True),
            # Non-hex string
            ("not-a-hash", False, True),
        ],
    )
    def test_warning_and_error_for_potential_hash(
        self, checkout_target, should_warn, should_raise
    ):
        """Test `_get_hash_from_checkout_target` to ensure a warning or error is raised
        appropriately when the checkout target appears to be a commit hash but is not in
        the dictionary of references returned by git ls-remote.

        Parameters
        ----------
        checkout_target : str
            The checkout target to test.
        should_warn : bool
            Whether a warning should be raised for this target.
        should_raise : bool
            Whether a ValueError should be raised for this target.
        """
        # Use pytest's `warnings.catch_warnings` to capture the warning
        with warnings.catch_warnings(record=True) as warning_list:
            if should_raise:
                # Call the function and expect a ValueError
                with pytest.raises(ValueError):
                    _get_hash_from_checkout_target(self.repo_url, checkout_target)

            else:
                # Call the function and assert the result
                result = _get_hash_from_checkout_target(self.repo_url, checkout_target)
                assert result == checkout_target

            # Check if a warning was raised
            if should_warn:
                assert len(warning_list) == 1
                warning = warning_list[0]
                assert issubclass(warning.category, UserWarning)
                assert (
                    f"C-STAR: The checkout target {checkout_target} appears to be a commit hash, "
                    f"but it is not possible to verify that this hash is a valid checkout target of {self.repo_url}"
                ) in str(warning.message)
            else:
                assert len(warning_list) == 0
