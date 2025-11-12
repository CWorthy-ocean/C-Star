import warnings
from unittest import mock

import pytest

from cstar.base.gitutils import (
    _clone_and_checkout,
    _get_hash_from_checkout_target,
    _get_repo_head_hash,
    _get_repo_remote,
    git_location_to_raw,
)


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
