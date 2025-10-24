import hashlib

import pytest

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
