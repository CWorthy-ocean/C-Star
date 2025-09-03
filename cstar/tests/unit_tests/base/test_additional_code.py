from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from cstar.base import AdditionalCode
from cstar.base.datasource import DataSource


class TestInit:
    """Test class for the initialization of the AdditionalCode class.

    The `__init__` method of the AdditionalCode class sets up attributes like
    location, subdirectory, checkout target, and associated files. This class tests
    that instances are correctly initialized with the provided parameters and default values.

    Tests
    -----
    test_init
        Verifies that an AdditionalCode object is correctly initialized with provided attributes.
    test_defaults
        Verifies that an AdditionalCode object is correctly initialized with default values
        when optional attributes are not provided.
    """

    def test_init(self):
        """Test that an AdditionalCode object is initialized with the correct
        attributes.
        """
        additional_code = AdditionalCode(
            location="https://github.com/test/repo.git",
            checkout_target="test123",
            subdir="test/subdir",
            files=["test_file_1.F", "test_file_2.py", "test_file_3.opt"],
        )
        assert additional_code.source.location == "https://github.com/test/repo.git"
        assert additional_code.checkout_target == "test123"
        assert additional_code.subdir == "test/subdir"
        assert additional_code.files == [
            "test_file_1.F",
            "test_file_2.py",
            "test_file_3.opt",
        ]

    def test_defaults(self):
        """Test that a minimal AdditionalCode object is initialized with correct default
        values.
        """
        additional_code = AdditionalCode(location="test/location")

        assert additional_code.source.location == "test/location"
        assert additional_code.subdir == ""
        assert additional_code.checkout_target is None
        assert len(additional_code.files) == 0


class TestStrAndRepr:
    """Test class for the `__str__` and `__repr__` methods of the AdditionalCode class.

    The `__str__` and `__repr__` methods provide string representations of AdditionalCode
    instances, which include key attributes such as location, subdirectory, and associated files.

    Tests
    -----
    test_repr_remote
        Verifies that the `__repr__` method returns the correct string for a remote AdditionalCode instance.
    test_repr_local
        Verifies that the `__repr__` method returns the correct string for a local AdditionalCode instance.
    test_repr_with_working_path
        Verifies that the `__repr__` method includes additional state information when `working_path` is set.
    test_str_remote
        Verifies that the `__str__` method returns the correct string for a remote AdditionalCode instance.
    test_str_local
        Verifies that the `__str__` method returns the correct string for a local AdditionalCode instance.

    Mocks
    -----
    exists_locally
        Patches the `exists_locally` property to simulate the existence or non-existence of files.
    """

    def test_repr_remote(self, fake_additionalcode_remote):
        """Test that the __repr__ method returns the correct string for the example
        remote AdditionalCode instance defined in the above fixture.
        """
        expected_repr = dedent("""\
        AdditionalCode(
        location = 'https://github.com/test/repo.git',
        subdir = 'test/subdir'
        checkout_target = 'test123',
        files = ['test_file_1.F',
                 'test_file_2.py',
                 'test_file_3.opt']
        )""")
        assert repr(fake_additionalcode_remote) == expected_repr, (
            f"expected \n{repr(fake_additionalcode_remote)}\n, got \n{expected_repr}"
        )

    def test_repr_local(self, fake_additionalcode_local):
        """Test that the __repr__ method returns the correct string for the example
        local AdditionalCode instance defined in the above fixture.
        """
        expected_repr = dedent("""\
        AdditionalCode(
        location = '/some/local/directory',
        subdir = 'some/subdirectory'
        checkout_target = None,
        files = ['test_file_1.F',
                 'test_file_2.py',
                 'test_file_3.opt']
        )""")
        assert repr(fake_additionalcode_local) == expected_repr

    @mock.patch(
        "cstar.base.additional_code.AdditionalCode.exists_locally",
        new_callable=mock.PropertyMock,
        return_value=True,
    )
    def test_repr_with_working_path(
        self, mock_exists_locally, fake_additionalcode_local
    ):
        """Test that the __repr__ method contains the correct substring when
        working_path attr is defined.

        Fixtures:
        ------
        - mock_exists: Patches Path.exists() to ensure exists_locally property used in __repr__ returns True.
        - fake_additionalcode_local: An example AdditionalCode instance representing local code
        """
        fake_additionalcode_local.working_path = Path("/mock/local/dir")
        assert "State: <working_path = /mock/local/dir,exists_locally = True>" in repr(
            fake_additionalcode_local
        )

    def test_str_remote(self, fake_additionalcode_remote):
        """Test that the __str__ method returns the correct string for the example
        remote AdditionalCode instance defined in the above fixture.
        """
        expected_str = dedent("""\
        AdditionalCode
        --------------
        Location: https://github.com/test/repo.git
        Subdirectory: test/subdir
        Checkout target: test123
        Working path: None
        Exists locally: False (get with AdditionalCode.get())
        Files:
            test_file_1.F
            test_file_2.py
            test_file_3.opt""")

        assert str(fake_additionalcode_remote) == expected_str, (
            f"expected \n{str(fake_additionalcode_remote)}\n, got \n{expected_str}"
        )

    def test_str_local(self, fake_additionalcode_local):
        """Test that the __str__ method returns the correct string for the example local
        AdditionalCode instance defined in the above fixture.
        """
        expected_str = dedent("""\
        AdditionalCode
        --------------
        Location: /some/local/directory
        Subdirectory: some/subdirectory
        Working path: None
        Exists locally: False (get with AdditionalCode.get())
        Files:
            test_file_1.F
            test_file_2.py
            test_file_3.opt""")

        assert str(fake_additionalcode_local) == expected_str, (
            f"expected \n{str(fake_additionalcode_local)}\n, got \n{expected_str}"
        )


class TestExistsLocally:
    """Test class for the `exists_locally` property of the AdditionalCode class.

    The `exists_locally` property verifies whether the required additional code
    files exist at the specified local working path and have matching hash values.

    Tests
    -----
    test_all_files_exist_and_hashes_match
        Verifies that `exists_locally` returns True when all files exist and their hashes match the cache.
    test_some_files_missing
        Verifies that `exists_locally` returns False when some files are missing.
    test_hash_mismatch
        Verifies that `exists_locally` returns False when a file's hash does not match the cached value.
    test_no_working_path
        Verifies that `exists_locally` returns False when the `working_path` attribute is None.
    test_no_cached_hashes
        Verifies that `exists_locally` returns False when the hash cache is None.

    Mocks
    -----
    _get_sha256_hash
        Patches the `_get_sha256_hash` function to simulate file hash calculation without requiring real files.
    Path.exists
        Patches the `Path.exists` property to handle a variety of situations regarding file existings
    """

    def setup_method(self):
        """Set up common mocks before each test."""
        self.patch_get_sha256_hash = mock.patch(
            "cstar.base.additional_code._get_sha256_hash"
        )
        self.mock_get_sha256_hash = self.patch_get_sha256_hash.start()

        self.patch_exists = mock.patch("pathlib.Path.exists")
        self.mock_exists = self.patch_exists.start()

    def teardown_method(self):
        """Stop all mocks after each test."""
        mock.patch.stopall()

    def test_all_files_exist_and_hashes_match(self):
        """Verifies that `exists_locally` returns True when all files exist and their
        hashes match the cache.

        This test ensures that the `exists_locally` property correctly identifies when all files
        associated with an `AdditionalCode` instance:
        - Exist in the specified `working_path`.
        - Have hash values that match the cached values in `_local_file_hash_cache`.

        Mocks
        -----
        mock_exists : unittest.mock.MagicMock
            Mock for the `Path.exists` method, simulating file existence checks for all files.

        Assertions
        ----------
        - `exists_locally` is True when all files exist and their hashes match the cache.
        - The number of calls to `Path.exists` matches the number of files.
        - The number of calls to `_get_sha256_hash` matches the number of files.
        """
        self.mock_exists.side_effect = [True, True, True]

        additional_code = AdditionalCode(
            location="/mock/local",
            subdir="subdir",
            files=["file1.F", "file2.py", "file3.opt"],
        )
        additional_code.working_path = Path("/mock/local/dir")

        # Simulate correct hash cache
        additional_code._local_file_hash_cache = {
            Path(f"/mock/local/dir/{file}"): f"mock_hash_{file}"
            for file in additional_code.files
        }

        # Mock file hashes to match cached values
        self.mock_get_sha256_hash.side_effect = lambda path: f"mock_hash_{path.name}"

        assert additional_code.exists_locally is True
        assert self.mock_exists.call_count == len(additional_code.files)
        assert self.mock_get_sha256_hash.call_count == len(additional_code.files)

    def test_some_files_missing(self):
        """Verifies that `exists_locally` returns False when some files are missing.

        This test ensures that the `exists_locally` property correctly identifies when
        one or more files associated with an `AdditionalCode` instance:
        - Do not exist in the specified `working_path`.

        Mocks
        -----
        mock_exists : unittest.mock.MagicMock
            Mock for the `Path.exists` method, simulating file existence checks
            where one or more files are missing.

        Assertions
        ----------
        - `exists_locally` is False when some files are missing.
        - `Path.exists` is called for all files before returning False.
        - `_get_sha256_hash` is only called for existing files.
        """
        self.mock_exists.side_effect = [True, True, False]

        additional_code = AdditionalCode(
            location="/mock/local",
            subdir="subdir",
            files=["file1.F", "file2.py", "file3.opt"],
        )
        additional_code.working_path = Path("/mock/local/dir")

        additional_code._local_file_hash_cache = {
            Path(f"/mock/local/dir/{file}"): f"mock_hash_{file}"
            for file in additional_code.files
        }

        self.mock_get_sha256_hash.side_effect = lambda path: f"mock_hash_{path.name}"

        assert additional_code.exists_locally is False
        assert self.mock_exists.call_count == 3
        assert (
            self.mock_get_sha256_hash.call_count == 2
        )  # Stops checking when a file is missing.

    def test_hash_mismatch(self):
        """Verifies that `exists_locally` returns False when a file's hash does not
        match the cached value.

        This test ensures that the `exists_locally` property correctly identifies when:
        - All files exist in the specified `working_path`.
        - At least one file's hash value does not match the cached value in `_local_file_hash_cache`.

        Mocks
        -----
        mock_exists : unittest.mock.MagicMock
            Mock for the `Path.exists` method, simulating successful file existence checks for all files.

        Assertions
        ----------
        - `exists_locally` is False when a hash mismatch occurs for any file.
        - `Path.exists` and `_get_sha256_hash` are only called up to the first mismatch.
        """
        self.mock_exists.return_value = True

        additional_code = AdditionalCode(
            location="/mock/local",
            subdir="subdir",
            files=["file1.F", "file2.py", "file3.opt"],
        )
        additional_code.working_path = Path("/mock/local/dir")

        # Simulate incorrect hash cache
        additional_code._local_file_hash_cache = {
            Path(f"/mock/local/dir/{file}"): f"wrong_hash_{file}"
            for file in additional_code.files
        }

        # Mock file hashes to different values than the cache
        self.mock_get_sha256_hash.side_effect = [
            "mock_hash_file1.F",  # Mismatch
            "mock_hash_file2.py",
            "mock_hash_file3.opt",
        ]

        # Assert that exists_locally is False
        assert additional_code.exists_locally is False

        # Assert that Path.exists and _get_sha256_hash are only called for the first file
        assert self.mock_exists.call_count == 1
        assert self.mock_get_sha256_hash.call_count == 1

    def test_no_working_path(self):
        """Verifies that `exists_locally` returns False when the `working_path`
        attribute is None.

        This test ensures that the `exists_locally` property correctly identifies that
        no local file checks can be performed when `working_path` is not set.

        Assertions
        ----------
        - `exists_locally` is False when `working_path` is None.
        - `_get_sha256_hash` is not called, as no files are checked.
        """
        additional_code = AdditionalCode(
            location="/mock/local",
            subdir="subdir",
            files=["file1.F", "file2.py", "file3.opt"],
        )
        additional_code.working_path = None

        assert additional_code.exists_locally is False
        self.mock_get_sha256_hash.assert_not_called()

    def test_no_cached_hashes(self):
        """Verifies that `exists_locally` returns False when the hash cache
        (`_local_file_hash_cache`) is None.

        This test ensures that the `exists_locally` property correctly identifies that
        file existence and hash validation cannot be performed when the hash cache is unset.

        Assertions
        ----------
        - `exists_locally` is False when `_local_file_hash_cache` is None.
        - `_get_sha256_hash` is not called, as no hashes are available for comparison.
        """
        additional_code = AdditionalCode(
            location="/mock/local",
            subdir="subdir",
            files=["file1.F", "file2.py", "file3.opt"],
        )
        additional_code.working_path = Path("/mock/local/dir")
        additional_code._local_file_hash_cache = None

        assert additional_code.exists_locally is False
        self.mock_get_sha256_hash.assert_not_called()


class TestAdditionalCodeGet:
    """Test class for the `AdditionalCode.get()` method, which handles fetching and
    copying code from both local directories and remote repositories.

    Tests:
    ------
    - test_get_from_local_directory
    - test_get_from_remote_repository
    - test_get_raises_if_checkout_target_none
    - test_get_raises_if_source_incompatible
    - test_get_raises_if_missing_files
    - test_get_with_empty_file_list
    - test_cleanup_temp_directory
    """

    def setup_method(self):
        """Set up common mocks and start patching before each test case.

        Mocks initialized:
        - pathlib.Path.mkdir: Mocked to simulate directory creation.
        - pathlib.Path.exists: Mocked to simulate checking file existence.
        - shutil.copy: Mocked to simulate copying files.
        - tempfile.mkdtemp: Mocked to simulate creating temporary directories.
        - shutil.rmtree: Mocked to simulate cleaning up temporary directories.
        - cstar.base.utils._clone_and_checkout: Mocked to simulate cloning a remote repository.
        - DataSource.location_type and DataSource.source_type: Mocked to simulate
        different source types for the AdditionalCode object.
        """
        # Common mocks
        self.patch_mkdir = mock.patch("pathlib.Path.mkdir")
        self.mock_mkdir = self.patch_mkdir.start()

        self.patch_exists = mock.patch("pathlib.Path.exists", return_value=True)
        self.mock_exists = self.patch_exists.start()

        self.patch_copy = mock.patch("shutil.copy")
        self.mock_copy = self.patch_copy.start()

        self.patch_mkdtemp = mock.patch(
            "tempfile.mkdtemp", return_value="/mock/tmp/dir"
        )
        self.mock_mkdtemp = self.patch_mkdtemp.start()

        self.patch_clone = mock.patch("cstar.base.additional_code._clone_and_checkout")
        self.mock_clone = self.patch_clone.start()

        self.patch_rmtree = mock.patch("shutil.rmtree")
        self.mock_rmtree = self.patch_rmtree.start()

        self.patch_location_type = mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        )
        self.mock_location_type = self.patch_location_type.start()

        self.patch_source_type = mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        )
        self.mock_source_type = self.patch_source_type.start()

        self.patch_hash = mock.patch("cstar.base.additional_code._get_sha256_hash")
        self.mock_hash = self.patch_hash.start()

    def teardown_method(self):
        """Stop all the patches after each test to clean up the mock environment."""
        mock.patch.stopall()

    def test_get_from_local_directory(
        self, mock_path_resolve, fake_additionalcode_local
    ):
        """Test the `get` method when fetching additional code from elsewhere on the
        current filesystem.

        Fixtures:
        ---------
        - fake_additionalcode_local: An example AdditionalCode instance representing local code.
        - mock_location_type: Mocks AdditionalCode.source.location_type as "path"
        - mock_source_type: Mocks AdditionalCode.source.source_type as "directory"
        - mock_path_resolve: Mocks resolving the target directory, returning a mocked resolved path.
        - mock_mkdir: Mocks the creation of the target directory if it doesn’t exist.
        - mock_copy: Mocks the copying of files from the source to the target directory.

        Asserts:
        --------
        - The target directory is created (mock_mkdir is called once with correct parameters).
        - The correct number of files are copied (mock_copy is called for each file).
        - Each file is copied from the correct source path to the correct target path.
        - The `_local_file_hash_cache` is updated with the correct hashes for all copied files.
        - The `working_path` is set to the target directory path after the operation.
        """
        # Set specific mock return values for this test
        self.mock_location_type.return_value = "path"
        self.mock_source_type.return_value = "directory"
        self.mock_hash.return_value = "mock_hash_value"

        # Call the get() method to simulate fetching additional code from a local directory
        fake_additionalcode_local.get("/mock/local/dir")

        # Ensure the directory is created
        self.mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # Ensure that all files are copied
        assert self.mock_copy.call_count == len(fake_additionalcode_local.files)
        for f in fake_additionalcode_local.files:
            src_file_path = (
                Path(f"/some/local/directory/{fake_additionalcode_local.subdir}") / f
            )
            tgt_file_path = Path("/mock/local/dir") / Path(f).name
            self.mock_copy.assert_any_call(src_file_path, tgt_file_path)

        # Ensure that `_local_file_hash_cache` is updated correctly
        for f in fake_additionalcode_local.files:
            tgt_file_path = Path("/mock/local/dir") / Path(f).name
            assert (
                fake_additionalcode_local._local_file_hash_cache[tgt_file_path]
                == "mock_hash_value"
            )

        # Ensure that the working_path is set correctly
        assert fake_additionalcode_local.working_path == Path("/mock/local/dir")

        assert mock_path_resolve.called

    def test_get_from_remote_repository(
        self, mock_path_resolve, fake_additionalcode_remote
    ):
        """Test the `get` method when fetching additional code from a remote Git
        repository.

        Fixtures:
        ---------
        - fake_additionalcode_remote: An example AdditionalCode instance representing code in a remote repo
        - mock_location_type: Mocks AdditionalCode.source.location_type as "url"
        - mock_source_type: Mocks AdditionalCode.source.source_type  as "repository"
        - mock_path_resolve: Mocks resolving the target directory.
        - mock_clone: Mocks cloning the repository and checking out the correct branch or commit.
        - mock_mkdir: Mocks the creation of the target directory (if needed).
        - mock_mkdtemp: Mocks the creation of a temporary directory for cloning the repo into.
        - mock_copy: Mocks the copying of files from the temporary clone to the target directory.
        - mock_rmtree: Mocks the cleanup (removal) of the temporary directory.

        Asserts:
        --------
        - The repository is cloned and checked out at the correct target.
        - The temporary directory is created (mock_mkdtemp is called once).
        - The target directory is created (mock_mkdir is called once with the correct parameters).
        - The correct number of files are copied (mock_copy is called for each file).
        - Each file is copied from the cloned repository to the correct target path.
        - The `_local_file_hash_cache` is updated with the correct hashes for all copied files.
        - The temporary directory is cleaned up (mock_rmtree is called once).
        - The `working_path` is set to the target directory path after the operation.
        """
        # Set specific return values for this test
        self.mock_location_type.return_value = "url"
        self.mock_source_type.return_value = "repository"
        self.mock_hash.return_value = "mock_hash_value"
        # Call get method
        fake_additionalcode_remote.get("/mock/local/dir")

        # Ensure the repository is cloned and checked out
        self.mock_clone.assert_called_once_with(
            source_repo=fake_additionalcode_remote.source.location,
            local_path="/mock/tmp/dir",
            checkout_target=fake_additionalcode_remote.checkout_target,
        )

        # Ensure the temporary directory is created
        self.mock_mkdtemp.assert_called_once()

        # Ensure that all files are copied
        assert self.mock_copy.call_count == len(fake_additionalcode_remote.files)
        for f in fake_additionalcode_remote.files:
            src_file_path = (
                Path(f"/mock/tmp/dir/{fake_additionalcode_remote.subdir}") / f
            )
            tgt_file_path = Path("/mock/local/dir") / Path(f).name
            self.mock_copy.assert_any_call(src_file_path, tgt_file_path)

        # Ensure that `_local_file_hash_cache` is updated correctly
        for f in fake_additionalcode_remote.files:
            tgt_file_path = Path("/mock/local/dir") / Path(f).name
            assert (
                fake_additionalcode_remote._local_file_hash_cache[tgt_file_path]
                == "mock_hash_value"
            )

        # Ensure the temporary directory is cleaned up after use
        self.mock_rmtree.assert_called_once_with("/mock/tmp/dir")

        # Ensure that the working_path is set correctly
        assert fake_additionalcode_remote.working_path == Path("/mock/local/dir")

    # Test failures:

    def test_get_raises_if_checkout_target_none(self, fake_additionalcode_remote):
        """Test that `get` raises a `ValueError` when no checkout_target provided for a
        remote repo.

        Fixtures:
        ---------
        - fake_additionalcode_remote: An example AdditionalCode instance representing code in a remote repo
        - mock_location_type: Mocks AdditionalCode.source.location_type as "url"
        - mock_source_type: Mocks AdditionalCode.source.source_type as "repository"

        Asserts:
        --------
        - A `ValueError` is raised with the correct message when `checkout_target` is `None`.
        - The repository is not cloned (mock_clone is not called).
        - No files are copied (mock_copy is not called).
        """
        # Simulate a remote repository source but without checkout_target
        fake_additionalcode_remote._checkout_target = (
            None  # This should raise a ValueError
        )
        self.mock_location_type.return_value = "url"
        self.mock_source_type.return_value = "repository"

        # Test that calling get raises ValueError
        with pytest.raises(ValueError, match="checkout_target is None"):
            fake_additionalcode_remote.get("/mock/local/dir")

        # Ensure no cloning or copying happens
        self.mock_clone.assert_not_called()
        self.mock_copy.assert_not_called()

    def test_get_raises_if_source_incompatible(self, fake_additionalcode_remote):
        """Test that `get` raises a `ValueError` when the source location and type are
        incompatible.

        Fixtures:
        ---------
        - fake_additionalcode_remote: An example AdditionalCode instance representing code in a remote repo
        - mock_location_type: Mocks AdditionalCode.source.location_type as "url"
        - mock_source_type: Mocks AdditionalCode.source.source_type as "directory"

        Asserts:
        --------
        - A `ValueError` is raised with the correct message when AdditionalCode.source.source_type is "directory" and AdditionalCode.source.location_type is "url"
        - No files are copied (mock_copy is not called).
        """
        self.mock_location_type.return_value = "url"
        self.mock_source_type.return_value = "directory"

        with pytest.raises(ValueError) as exception_info:
            fake_additionalcode_remote.get("/mock/local/dir")

        expected_message = (
            "Invalid source for AdditionalCode. "
            + "AdditionalCode.source.location_type and "
            + "AdditionalCode.source.source_type should be "
            + "'url' and 'repository', or 'path' and 'repository', or"
            + "'path' and 'directory', not"
            + "'url' and 'directory'"
        )

        assert str(exception_info.value) == expected_message

    def test_get_raises_if_missing_files(
        self, mock_path_resolve, fake_additionalcode_local
    ):
        """Test that `get` raises a `FileNotFoundError` when a file is missing in a
        local directory.

        Fixtures:
        ---------
        - fake_additionalcode_local: An example AdditionalCode instance representing local code.
        - mock_location_type: Mocks AdditionalCode.source.location_type as "path".
        - mock_source_type: Mocks AdditionalCode.source.source_type as "directory".
        - mock_path_resolve: Mocks resolving the target directory.
        - mock_exists: Mocks file existence checks, simulating the third file as missing.
        - mock_copy: Mocks the copying of files to the target directory.

        Asserts:
        --------
        - A `FileNotFoundError` is raised with the correct message when a file does not exist.
        - `mock_exists` is called for each file to check its existence (3 calls in this case).
        - Only existing files are copied (2 calls to `mock_copy` for the first two files).
        """
        # Simulate local directory source with missing files
        self.mock_hash.return_value = "mock_hash_value"
        self.mock_exists.side_effect = [True, True, False]  # Third file is missing
        self.mock_location_type.return_value = "path"
        self.mock_source_type.return_value = "directory"

        # Test that get raises FileNotFoundError when a file doesn't exist
        with pytest.raises(FileNotFoundError, match="does not exist"):
            fake_additionalcode_local.get("/mock/local/dir")

        # Ensure the first two files were checked for existence and copying was attempted
        assert self.mock_exists.call_count == 3
        assert self.mock_copy.call_count == 2  # Only the first two files were copied

    def test_get_with_empty_file_list(self, fake_additionalcode_local):
        """Test that `get` raises a `ValueError` when the `files` attribute is empty in
        AdditionalCode.

        Fixtures:
        ---------
        - fake_additionalcode_local: An example AdditionalCode instance representing local code.

        Asserts:
        --------
        - A `ValueError` is raised with the correct message when `files` is an empty list.
        """
        expected_message = (
            "Cannot `get` an AdditionalCode object when AdditionalCode.files is empty"
        )
        fake_additionalcode_local.files = []

        with pytest.raises(ValueError) as exception_info:
            fake_additionalcode_local.get("/mock/local/dir")

        assert str(exception_info.value) == expected_message

    def test_cleanup_temp_directory(
        self, mock_path_resolve, fake_additionalcode_remote
    ):
        """Test that the temporary directory is cleaned up (deleted) after fetching
        remote additional code.

        Fixtures:
        ---------
        - fake_additionalcode_remote: An example AdditionalCode instance representing code in a remote repo
        - mock_location_type: Mocks AdditionalCode.source.location_type as "url".
        - mock_source_type: Mocks AdditionalCode.source.source_type as "repository".
        - mock_path_resolve: Mocks resolving the target directory.
        - mock_clone: Mocks cloning the repository and checking out the correct branch or commit.
        - mock_mkdtemp: Mocks the creation of a temporary directory for cloning the repo.
        - mock_rmtree: Mocks the cleanup (removal) of the temporary directory.

        Asserts:
        --------
        - The temporary directory is cleaned up (mock_rmtree is called once with the correct path).
        """
        self.mock_location_type.return_value = "url"
        self.mock_source_type.return_value = "repository"

        # Call get method
        fake_additionalcode_remote.get("/mock/local/dir")

        # Ensure the temporary directory is cleaned up after use
        self.mock_rmtree.assert_called_once_with("/mock/tmp/dir")
