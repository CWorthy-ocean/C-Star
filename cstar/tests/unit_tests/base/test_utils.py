import hashlib
import warnings
from unittest import mock

import pytest

from cstar.base.gitutils import (
    _clone_and_checkout,
    _get_hash_from_checkout_target,
    _get_repo_head_hash,
    _get_repo_remote,
)
from cstar.base.utils import (
    _dict_to_tree,
    _get_sha256_hash,
    _list_to_concise_str,
    _replace_text_in_file,
)


def test_get_sha256_hash(tmp_path):
    """Test the get_sha256_hash method using the known hash of a temporary file.

    Fixtures
    ----------
    tmp_path : Path
        Pytest fixture providing a temporary directory for isolated filesystem operations.

    Asserts
    -------
    - The calculated hash matches the expected hash
    - A FileNotFoundError is raised if the file does not exist
    """
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("Test data for hash")

    # Compute the expected hash manually
    expected_hash = hashlib.sha256(b"Test data for hash").hexdigest()

    # Call _get_sha256_hash and verify
    calculated_hash = _get_sha256_hash(file_path)
    assert calculated_hash == expected_hash, "Hash mismatch"

    # Test FileNotFoundError
    non_existent_path = tmp_path / "non_existent_file.txt"
    with pytest.raises(FileNotFoundError):
        _get_sha256_hash(non_existent_path)


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


class TestReplaceTextInFile:
    """Tests for `_replace_text_in_file`, verifying correct behavior for text
    replacement within a file.
    """

    def setup_method(self):
        """Common setup for each test, initializing base content that can be modified
        per test.
        """
        self.base_content = "This is a test file with some old_text to replace."

    def test_replace_text_success(self, tmp_path):
        """Test that `_replace_text_in_file` successfully replaces the specified text
        when `old_text` is found in the file.

        Asserts
        -------
        - Ensures `old_text` is correctly replaced by `new_text`.
        - Verifies the function returns True when a replacement occurs.
        """
        # Create a temporary file and write the initial content
        test_file = tmp_path / "test_file.txt"
        test_file.write_text(self.base_content)

        # Run the function
        result = _replace_text_in_file(test_file, "old_text", "new_text")

        # Read the content and check the replacement
        updated_content = test_file.read_text()
        assert updated_content == "This is a test file with some new_text to replace."
        assert result is True, "Expected True when replacement occurs."

    def test_replace_text_not_found(self, tmp_path):
        """Test that `_replace_text_in_file` does not alter the file when `old_text` is
        not found, and returns False.

        Asserts
        -------
        - Ensures the file content remains unchanged when `old_text` is not found.
        - Verifies the function returns False when no replacement occurs.
        """
        # Create a temporary file and write initial content without `old_text`
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("This is a test file without the target text.")

        # Run the function
        result = _replace_text_in_file(test_file, "non_existent_text", "new_text")

        # Read the content and check that no change occurred
        unchanged_content = test_file.read_text()
        assert unchanged_content == "This is a test file without the target text."
        assert result is False, "Expected False when no replacement occurs."

    def test_replace_text_multiple_occurrences(self, tmp_path):
        """Test that `_replace_text_in_file` replaces all occurrences of `old_text` when
        it appears multiple times in the file.

        Asserts
        -------
        - Ensures all instances of `old_text` are replaced by `new_text`.
        - Verifies the function returns True when replacement occurs.
        """
        # Create a temporary file with multiple instances of `old_text`
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("old_text here, old_text there, and old_text everywhere.")

        # Run the function
        result = _replace_text_in_file(test_file, "old_text", "new_text")

        # Read the content and check that all occurrences were replaced
        updated_content = test_file.read_text()
        assert (
            updated_content == "new_text here, new_text there, and new_text everywhere."
        )
        assert result is True, "Expected True when multiple replacements occur."


class TestListToConciseStr:
    """Tests for `_list_to_concise_str`, verifying correct behavior under different list
    lengths and parameter configurations.
    """

    def test_basic_case_no_truncation(self):
        """Test `_list_to_concise_str` with a short list that does not exceed
        `item_threshold`.

        Asserts
        -------
        - Ensures the output is a full list representation with no truncation.
        """
        input_list = ["item1", "item2", "item3"]
        result = _list_to_concise_str(input_list, item_threshold=4, pad=0)
        assert result == "['item1',\n'item2',\n'item3']"

    def test_truncation_case(self):
        """Test `_list_to_concise_str` when the list length exceeds `item_threshold`,
        requiring truncation.

        Asserts
        -------
        - Ensures the output is truncated and includes the item count.
        """
        input_list = ["item1", "item2", "item3", "item4", "item5", "item6"]
        result = _list_to_concise_str(input_list, item_threshold=4, pad=0)
        assert result == "['item1',\n'item2',\n   ...\n'item6'] <6 items>"

    def test_padding_and_item_count_display(self):
        """Test `_list_to_concise_str` with custom padding and item count display
        enabled.

        Asserts
        -------
        - Ensures correct indentation and the presence of item count.
        """
        input_list = ["item1", "item2", "item3", "item4", "item5"]
        result = _list_to_concise_str(
            input_list, item_threshold=3, pad=10, show_item_count=True
        )
        expected_output = "['item1',\n          'item2',\n             ...\n          'item5'] <5 items>"
        assert result == expected_output

    def test_no_item_count_display(self):
        """Test `_list_to_concise_str` with `show_item_count=False` to verify that the
        item count is omitted in the truncated representation.

        Asserts
        -------
        - Ensures the item count is not included in the output.
        """
        input_list = ["item1", "item2", "item3", "item4", "item5"]
        result = _list_to_concise_str(
            input_list, item_threshold=3, pad=0, show_item_count=False
        )
        expected_output = "['item1',\n'item2',\n   ...\n'item5'] "
        assert result == expected_output


class TestDictToTree:
    """Tests for `_dict_to_tree`, verifying the correct tree-like string representation
    of various dictionary structures.
    """

    def test_simple_flat_dict(self):
        """Test `_dict_to_tree` with a single-level dictionary.

        Asserts
        -------
        - Ensures the output matches the expected flat tree representation.
        """
        input_dict = {"branch1": ["leaf1", "leaf2"], "branch2": ["leaf3"]}
        result = _dict_to_tree(input_dict)
        expected_output = (
            "├── branch1\n"
            "│   ├── leaf1\n"
            "│   └── leaf2\n"
            "└── branch2\n"
            "    └── leaf3\n"
        )  # fmt: skip
        assert result == expected_output

    def test_nested_dict(self):
        """Test `_dict_to_tree` with a multi-level nested dictionary.

        Asserts
        -------
        - Ensures the output matches the expected tree representation for nested dictionaries.
        """
        input_dict = {
            "branch1": {"twig1": ["leaf1", "leaf2"], "twig2": ["leaf3"]},
            "branch2": ["leaf4"],
        }
        result = _dict_to_tree(input_dict)
        expected_output = (
            "├── branch1\n"
            "│   ├── twig1\n"
            "│   │   ├── leaf1\n"
            "│   │   └── leaf2\n"
            "│   └── twig2\n"
            "│       └── leaf3\n"
            "└── branch2\n"
            "    └── leaf4\n"
        )
        assert result == expected_output

    def test_empty_dict(self):
        """Test `_dict_to_tree` with an empty dictionary.

        Asserts
        -------
        - Ensures that an empty dictionary returns an empty string.
        """
        input_dict = {}
        result = _dict_to_tree(input_dict)
        assert result == "", "Expected empty string for an empty dictionary."

    def test_complex_nested_structure(self):
        """Test `_dict_to_tree` with a complex nested dictionary containing various
        levels and mixed lists and dictionaries.

        Asserts
        -------
        - Ensures the output correctly represents the entire tree structure.
        """
        input_dict = {
            "branch1": {
                "twig1": {"twiglet1": ["leaf1"], "twiglet2": ["leaf2", "leaf3"]},
                "twig2": ["leaf4"],
            },
            "branch2": {"twig3": ["leaf5", "leaf6"]},
        }
        result = _dict_to_tree(input_dict)
        expected_output = (
            "├── branch1\n"
            "│   ├── twig1\n"
            "│   │   ├── twiglet1\n"
            "│   │   │   └── leaf1\n"
            "│   │   └── twiglet2\n"
            "│   │       ├── leaf2\n"
            "│   │       └── leaf3\n"
            "│   └── twig2\n"
            "│       └── leaf4\n"
            "└── branch2\n"
            "    └── twig3\n"
            "        ├── leaf5\n"
            "        └── leaf6\n"
        )
        assert result == expected_output
