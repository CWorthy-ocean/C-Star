import pytest
from unittest import mock
from pathlib import Path
from cstar.base import AdditionalCode
from cstar.base.datasource import DataSource


# Set up fixtures
@pytest.fixture
def remote_additional_code():
    """Pytest fixture that provides an instance of the AdditionalCode class representing
    a remote repository.

    This fixture simulates additional code retrieved from a remote Git
    repository. It sets up the following attributes:

    - `location`: The URL of the remote repository
    - `checkout_target`: The specific branch, tag, or commit to checkout
    - `subdir`: A subdirectory within the repository where files are located
    - `files`: A list of files to be included from the repository

    This fixture can be used in tests that involve handling or manipulating code
    fetched from a remote Git repository.

    Returns:
        AdditionalCode: An instance of the AdditionalCode class with preset
        remote repository details.
    """
    return AdditionalCode(
        location="https://github.com/test/repo.git",
        checkout_target="test123",
        subdir="test/subdir",
        files=["test_file_1.F", "test_file_2.py", "test_file_3.opt"],
    )


@pytest.fixture
def local_additional_code():
    """Pytest fixture that provides an instance of the AdditionalCode class representing
    code located on the local filesystem.

    This fixture simulates additional code stored in a local directory. It sets
    up the following attributes:

    - `location`: The path to the local directory containing the code
    - `subdir`: A subdirectory within the local directory where the files are located
    - `files`: A list of files to be included from the local directory

    This fixture can be used in tests that involve handling or manipulating
    code that resides on the local filesystem.

    Returns:
        AdditionalCode: An instance of the AdditionalCode class with preset
        local directory details.
    """
    return AdditionalCode(
        location="/some/local/directory",
        subdir="some/subdirectory",
        files=["test_file_1.F", "test_file_2.py", "test_file_3.opt"],
    )


def test_init(remote_additional_code):
    """Test that an AdditionalCode object is initialized with the correct attributes."""
    assert remote_additional_code.source.location == "https://github.com/test/repo.git"
    assert remote_additional_code.checkout_target == "test123"
    assert remote_additional_code.subdir == "test/subdir"
    assert remote_additional_code.files == [
        "test_file_1.F",
        "test_file_2.py",
        "test_file_3.opt",
    ]


def test_defaults():
    """Test that a minimal AdditionalCode object is initialized with correct default
    values."""
    additional_code = AdditionalCode(location="test/location")

    assert additional_code.source.location == "test/location"
    assert additional_code.subdir == ""
    assert additional_code.checkout_target is None
    assert len(additional_code.files) == 0


def test_repr_remote(remote_additional_code):
    """Test that the __repr__ method returns the correct string for the example remote
    AdditionalCode instance defined in the above fixture."""
    expected_repr = """AdditionalCode(
location = 'https://github.com/test/repo.git',
subdir = 'test/subdir'
checkout_target = 'test123',
files = ['test_file_1.F',
         'test_file_2.py',
         'test_file_3.opt']
)"""
    assert repr(remote_additional_code) == expected_repr


def test_repr_local(local_additional_code):
    """Test that the __repr__ method returns the correct string for the example local
    AdditionalCode instance defined in the above fixture."""

    expected_repr = """AdditionalCode(
location = '/some/local/directory',
subdir = 'some/subdirectory'
checkout_target = None,
files = ['test_file_1.F',
         'test_file_2.py',
         'test_file_3.opt']
)"""
    assert repr(local_additional_code) == expected_repr


@mock.patch("pathlib.Path.exists", side_effect=[True, True, True])
def test_repr_with_working_path(mock_exists, local_additional_code):
    """Test that the __repr__ method contains the correct substring when working_path
    attr is defined.

    Fixtures:
    ------
    - mock_exists: Patches Path.exists() to ensure exists_locally property used in __repr__ returns True.
    - local_additional_code: An example AdditionalCode instance representing local code
    """

    local_additional_code.working_path = Path("/mock/local/dir")
    assert "State: <working_path = /mock/local/dir,exists_locally = True>" in repr(
        local_additional_code
    )


def test_str_remote(remote_additional_code):
    """Test that the __str__ method returns the correct string for the example remote
    AdditionalCode instance defined in the above fixture."""

    expected_str = """AdditionalCode
--------------
Location: https://github.com/test/repo.git
subdirectory: test/subdir
Working path: None
Exists locally: False (get with AdditionalCode.get())
Files:
    test_file_1.F
    test_file_2.py
    test_file_3.opt"""

    assert str(remote_additional_code) == expected_str


def test_str_with_template_file(local_additional_code):
    """Test that the __str__ method contains the correct substring when an additional
    code filename has the '_TEMPLATE' suffix."""
    # Simulate template files with "_TEMPLATE"
    local_additional_code.files = ["file1_TEMPLATE", "file2"]
    local_additional_code.working_path = Path("/mock/local/dir")

    assert "      (file1 will be used by C-Star based on this template)" in str(
        local_additional_code
    )


def test_str_local(local_additional_code):
    """Test that the __str__ method returns the correct string for the example local
    AdditionalCode instance defined in the above fixture."""

    expected_str = """AdditionalCode
--------------
Location: /some/local/directory
subdirectory: some/subdirectory
Working path: None
Exists locally: False (get with AdditionalCode.get())
Files:
    test_file_1.F
    test_file_2.py
    test_file_3.opt"""

    assert str(local_additional_code) == expected_str


@mock.patch("pathlib.Path.exists", side_effect=[True, True, True])
def test_exists_locally_all_files_exist(mock_exists, remote_additional_code):
    """Test that the AdditionalCode.exists_locally() property works correctly when all
    files exist locally.

    Fixtures:
    ------
    - mock_exists: Patches Path.exists() to ensure exists_locally property used in __repr__ returns True.
    - remote_additional_code: An example AdditionalCode instance representing code in a remote repo
    """
    remote_additional_code.working_path = Path("/mock/local/dir")
    assert remote_additional_code.exists_locally is True
    assert mock_exists.call_count == len(remote_additional_code.files)


@mock.patch("pathlib.Path.exists", side_effect=[True, True, False])
def test_exists_locally_some_files_missing(mock_exists, remote_additional_code):
    """Test that the AdditionalCode.exists_locally() property works correctly when a
    file is missing.

    Fixtures:
    ------
    - mock_exists: Patches Path.exists() to ensure exists_locally property used in __repr__ returns True.
    - remote_additional_code: An example AdditionalCode instance representing code in a remote repo
    """

    remote_additional_code.working_path = Path("/mock/local/dir")
    assert remote_additional_code.exists_locally is False
    assert mock_exists.call_count == len(remote_additional_code.files)


def test_exists_locally_no_working_path(remote_additional_code):
    """Test that the AdditionalCode.exists_locally() property correctly returns False
    when the working_path attribute is not set."""
    remote_additional_code.working_path = None
    assert remote_additional_code.exists_locally is False


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
    - test_get_with_template_files
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
        - Path.resolve: Mocked to simulate resolving paths.
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

        # Set up common mocks for Path.resolve and DataSource attributes
        self.patch_resolve = mock.patch.object(Path, "resolve")
        self.mock_resolve = self.patch_resolve.start()

        self.patch_location_type = mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        )
        self.mock_location_type = self.patch_location_type.start()

        self.patch_source_type = mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        )
        self.mock_source_type = self.patch_source_type.start()

    def teardown_method(self):
        """Stop all the patches after each test to clean up the mock environment."""
        mock.patch.stopall()

    def test_get_from_local_directory(self, local_additional_code):
        """Test the `get` method when fetching additional code from elsewhere on the
        current filesystem.

        Fixtures:
        ---------
        - local_additional_code: An example AdditionalCode instance representing local code.
        - mock_location_type: Mocks AdditionalCode.source.location_type as "path"
        - mock_source_type: Mocks AdditionalCode.source.source_type as "directory"
        - mock_resolve: Mocks resolving the target directory, returning a mocked resolved path.
        - mock_mkdir: Mocks the creation of the target directory if it doesn’t exist.
        - mock_copy: Mocks the copying of files from the source to the target directory.

        Asserts:
        --------
        - The target directory is created (mock_mkdir is called once with correct parameters).
        - The correct number of files are copied (mock_copy is called for each file).
        - Each file is copied from the correct source path to the correct target path.
        - The `working_path` is set to the target directory path after the operation.
        """
        # Set specific mock return values for this test
        self.mock_location_type.return_value = "path"
        self.mock_source_type.return_value = "directory"
        self.mock_resolve.return_value = Path("/mock/local/dir")

        # Call the get() method to simulate fetching additional code from a local directory
        # to another (mock) local dir:
        local_additional_code.get("/mock/local/dir")

        # Ensure the directory is created
        self.mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # Ensure that all files are copied
        assert self.mock_copy.call_count == len(local_additional_code.files)
        for f in local_additional_code.files:
            src_file_path = (
                Path(f"/some/local/directory/{local_additional_code.subdir}") / f
            )
            tgt_file_path = Path("/mock/local/dir") / Path(f).name
            self.mock_copy.assert_any_call(src_file_path, tgt_file_path)

        # Ensure that the working_path is set correctly
        assert local_additional_code.working_path == Path("/mock/local/dir")

    def test_get_from_remote_repository(self, remote_additional_code):
        """Test the `get` method when fetching additional code from a remote Git
        repository.

        Fixtures:
        ---------
        - remote_additional_code: An example AdditionalCode instance representing code in a remote repo
        - mock_location_type: Mocks AdditionalCode.source.location_type as "url"
        - mock_source_type: Mocks AdditionalCode.source.source_type  as "repository"
        - mock_resolve: Mocks resolving the target directory.
        - mock_clone: Mocks cloning the repository and checking out the correct branch or commit.
        - mock_mkdir: Mocks the creation of the target directory (if needed).
        - mock_mkdtemp: Mocks the creation of a temporary directory for cloning the repo into.
        - mock_copy: Mocks the copying of files from the temporary clone to the target directory.
        - mock_rmtree: Mocks the cleanup (removal) of the temporary directory.

        Asserts:
        --------
        - The repository is cloned and checked out at the correct target
        - The temporary directory is created (mock_mkdtemp is called once).
        - The target directory is created (mock_mkdir is called once with the correct parameters).
        - The correct number of files are copied (mock_copy is called for each file).
        - Each file is copied from the cloned repository to the correct target path.
        - The temporary directory is cleaned up (mock_rmtree is called once).
        - The `working_path` is set to the target directory path after the operation.
        """
        # Set specific return values for this test
        self.mock_location_type.return_value = "url"
        self.mock_source_type.return_value = "repository"
        self.mock_resolve.return_value = Path("/mock/local/dir")

        # Call get method
        remote_additional_code.get("/mock/local/dir")

        # Ensure the repository is cloned and checked out
        self.mock_clone.assert_called_once_with(
            source_repo=remote_additional_code.source.location,
            local_path="/mock/tmp/dir",
            checkout_target=remote_additional_code.checkout_target,
        )

        # Ensure the temporary directory is created
        self.mock_mkdtemp.assert_called_once()

        # Ensure that all files are copied
        assert self.mock_copy.call_count == len(remote_additional_code.files)
        for f in remote_additional_code.files:
            src_file_path = Path(f"/mock/tmp/dir/{remote_additional_code.subdir}") / f
            tgt_file_path = Path("/mock/local/dir") / Path(f).name
        self.mock_copy.assert_any_call(src_file_path, tgt_file_path)

        # Ensure that the working_path is set correctly
        assert remote_additional_code.working_path == Path("/mock/local/dir")

    # Test failures:

    def test_get_raises_if_checkout_target_none(self, remote_additional_code):
        """Test that `get` raises a `ValueError` when no checkout_target provided for a
        remote repo.

        Fixtures:
        ---------
        - remote_additional_code: An example AdditionalCode instance representing code in a remote repo
        - mock_location_type: Mocks AdditionalCode.source.location_type as "url"
        - mock_source_type: Mocks AdditionalCode.source.source_type as "repository"

        Asserts:
        --------
        - A `ValueError` is raised with the correct message when `checkout_target` is `None`.
        - The repository is not cloned (mock_clone is not called).
        - No files are copied (mock_copy is not called).
        """

        # Simulate a remote repository source but without checkout_target
        remote_additional_code.checkout_target = None  # This should raise a ValueError
        self.mock_location_type.return_value = "url"
        self.mock_source_type.return_value = "repository"

        # Test that calling get raises ValueError
        with pytest.raises(ValueError, match="checkout_target is None"):
            remote_additional_code.get("/mock/local/dir")

        # Ensure no cloning or copying happens
        self.mock_clone.assert_not_called()
        self.mock_copy.assert_not_called()

    def test_get_raises_if_source_incompatible(self, remote_additional_code):
        """Test that `get` raises a `ValueError` when the source location and type are
        incompatible.

        Fixtures:
        ---------
        - remote_additional_code: An example AdditionalCode instance representing code in a remote repo
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
            remote_additional_code.get("/mock/local/dir")

        expected_message = (
            "Invalid source for AdditionalCode. "
            + "AdditionalCode.source.location_type and "
            + "AdditionalCode.source.source_type should be "
            + "'url' and 'repository', or 'path' and 'repository', or"
            + "'path' and 'directory', not"
            + "'url' and 'directory'"
        )

        assert str(exception_info.value) == expected_message

    def test_get_raises_if_missing_files(self, local_additional_code):
        """Test that `get` raises a `FileNotFoundError` when a file is missing in a
        local directory.

        Fixtures:
        ---------
        - local_additional_code: An example AdditionalCode instance representing local code.
        - mock_location_type: Mocks AdditionalCode.source.location_type as "path".
        - mock_source_type: Mocks AdditionalCode.source.source_type as "directory".
        - mock_resolve: Mocks resolving the target directory.
        - mock_exists: Mocks file existence checks, simulating the third file as missing.
        - mock_copy: Mocks the copying of files to the target directory.

        Asserts:
        --------
        - A `FileNotFoundError` is raised with the correct message when a file does not exist.
        - `mock_exists` is called for each file to check its existence (3 calls in this case).
        - Only existing files are copied (2 calls to `mock_copy` for the first two files).
        """
        # Simulate local directory source with missing files
        self.mock_exists.side_effect = [True, True, False]  # Third file is missing
        self.mock_resolve.return_value = Path("/mock/local/dir")
        self.mock_location_type.return_value = "path"
        self.mock_source_type.return_value = "directory"

        # Test that get raises FileNotFoundError when a file doesn't exist
        with pytest.raises(FileNotFoundError, match="does not exist"):
            local_additional_code.get("/mock/local/dir")

        # Ensure the first two files were checked for existence and copying was attempted
        assert self.mock_exists.call_count == 3
        assert self.mock_copy.call_count == 2  # Only the first two files were copied

    def test_get_with_template_files(self, local_additional_code):
        """Test that `get` correctly handles files with the '_TEMPLATE' filename suffix.

        Fixtures:
        ---------
        - local_additional_code: An example AdditionalCode instance representing local code.
        - mock_location_type: Mocks AdditionalCode.source.location_type as "path".
        - mock_source_type: Mocks AdditionalCode.source.source_type as "directory".
        - mock_resolve: Mocks resolving the target directory.
        - mock_copy: Mocks the copying of template files and renaming them.

        Asserts:
        --------
        - `mock_copy` is called the correct number of times (2 original files + 2 renamed files).
        - Each template file is copied and renamed (from "fileX_TEMPLATE" to "fileX").
        - The `modified_files` attribute is correctly updated with the renamed files.
        """
        self.mock_location_type.return_value = "path"
        self.mock_source_type.return_value = "directory"
        self.mock_resolve.return_value = Path("/mock/local/dir")

        # Simulate template files with "_TEMPLATE"
        local_additional_code.files = ["file1_TEMPLATE", "file2_TEMPLATE"]
        local_additional_code.working_path = Path("/mock/local/dir")

        # Call get method
        local_additional_code.get("/mock/local/dir")

        print(f"DEBUG: All copy calls: {self.mock_copy.call_args_list}")
        # Ensure that the template files were copied and renamed
        assert self.mock_copy.call_count == 4  # 2 original files + 2 template renames
        self.mock_copy.assert_any_call(
            Path("/mock/local/dir/file1_TEMPLATE"), Path("/mock/local/dir/file1")
        )
        self.mock_copy.assert_any_call(
            Path("/mock/local/dir/file2_TEMPLATE"), Path("/mock/local/dir/file2")
        )
        assert local_additional_code.modified_files == ["file1", "file2"]

    def test_get_with_empty_file_list(self, local_additional_code):
        """Test that `get` raises a `ValueError` when the `files` attribute is empty in
        AdditionalCode.

        Fixtures:
        ---------
        - local_additional_code: An example AdditionalCode instance representing local code.

        Asserts:
        --------
        - A `ValueError` is raised with the correct message when `files` is an empty list.
        """
        expected_message = (
            "Cannot `get` an AdditionalCode object when AdditionalCode.files is empty"
        )
        local_additional_code.files = []

        with pytest.raises(ValueError) as exception_info:
            local_additional_code.get("/mock/local/dir")

        assert str(exception_info.value) == expected_message

    def test_cleanup_temp_directory(self, remote_additional_code):
        """Test that the temporary directory is cleaned up (deleted) after fetching
        remote additional code.

        Fixtures:
        ---------
        - remote_additional_code: An example AdditionalCode instance representing code in a remote repo
        - mock_location_type: Mocks AdditionalCode.source.location_type as "url".
        - mock_source_type: Mocks AdditionalCode.source.source_type as "repository".
        - mock_resolve: Mocks resolving the target directory.
        - mock_clone: Mocks cloning the repository and checking out the correct branch or commit.
        - mock_mkdtemp: Mocks the creation of a temporary directory for cloning the repo.
        - mock_rmtree: Mocks the cleanup (removal) of the temporary directory.

        Asserts:
        --------
        - The temporary directory is cleaned up (mock_rmtree is called once with the correct path).
        """
        self.mock_location_type.return_value = "url"
        self.mock_source_type.return_value = "repository"
        self.mock_resolve.return_value = Path("/mock/local/dir")

        # Call get method
        remote_additional_code.get("/mock/local/dir")

        # Ensure the temporary directory is cleaned up after use
        self.mock_rmtree.assert_called_once_with("/mock/tmp/dir")
