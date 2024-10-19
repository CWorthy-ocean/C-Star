import pytest
from unittest import mock
from pathlib import Path
from cstar.base import AdditionalCode
from cstar.base.datasource import DataSource


@pytest.fixture
def remote_additional_code():
    return AdditionalCode(
        location="https://github.com/test/repo.git",
        checkout_target="test123",
        subdir="test/subdir",
        files=["test_file_1.F", "test_file_2.py", "test_file_3.opt"],
    )


@pytest.fixture
def local_additional_code():
    return AdditionalCode(
        location="/some/local/directory",
        subdir="some/subdirectory",
        files=["test_file_1.F", "test_file_2.py", "test_file_3.opt"],
    )


def test_init(remote_additional_code):
    assert remote_additional_code.source.location == "https://github.com/test/repo.git"
    assert remote_additional_code.checkout_target == "test123"
    assert remote_additional_code.subdir == "test/subdir"
    assert remote_additional_code.files == [
        "test_file_1.F",
        "test_file_2.py",
        "test_file_3.opt",
    ]


def test_defaults():
    additional_code = AdditionalCode(location="test/location")

    assert additional_code.source.location == "test/location"
    assert additional_code.subdir == ""
    assert additional_code.checkout_target is None
    assert len(additional_code.files) == 0


@mock.patch("pathlib.Path.exists", side_effect=[True, True, True])
def test_exists_locally_all_files_exist(mock_exists, remote_additional_code):
    remote_additional_code.working_path = Path("/mock/local/dir")
    assert remote_additional_code.exists_locally is True
    assert mock_exists.call_count == len(remote_additional_code.files)


@mock.patch("pathlib.Path.exists", side_effect=[True, True, False])
def test_exists_locally_some_files_missing(mock_exists, remote_additional_code):
    remote_additional_code.working_path = Path("/mock/local/dir")
    assert remote_additional_code.exists_locally is False
    assert mock_exists.call_count == len(remote_additional_code.files)


def test_exists_locally_no_working_path(remote_additional_code):
    remote_additional_code.working_path = None
    assert remote_additional_code.exists_locally is False


class TestAdditionalCodeGet:
    def setup_method(self):
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
        # Stop all the patches
        mock.patch.stopall()

    def test_get_from_local_directory(self, local_additional_code):
        # Set specific return values for this test
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


################################################################################

# def test_get_raises_value_error_if_checkout_target_none(self, additional_code):
#     # Simulate a remote repository source but without checkout_target
#     additional_code.source.location_type = "url"
#     additional_code.source.source_type = "repository"
#     additional_code.checkout_target = None  # This should raise a ValueError

#     # Test that calling get raises ValueError
#     with pytest.raises(ValueError, match="checkout_target is None"):
#         additional_code.get("/mock/local/dir")

#     # Ensure no cloning or copying happens
#     self.mock_clone.assert_not_called()
#     self.mock_copy.assert_not_called()

# def test_get_raises_file_not_found_error(self, additional_code):
#     # Simulate local directory source with missing files
#     self.mock_exists.side_effect = [True, True, False]  # Third file is missing
#     additional_code.source.location_type = "path"
#     additional_code.source.source_type = "directory"
#     additional_code.working_path = Path("/mock/local/dir")

#     # Test that get raises FileNotFoundError when a file doesn't exist
#     with pytest.raises(FileNotFoundError, match="does not exist"):
#         additional_code.get("/mock/local/dir")

#     # Ensure the first two files were checked for existence and copying was attempted
#     assert self.mock_exists.call_count == 3
#     assert self.mock_copy.call_count == 2  # Only the first two files were copied

# def test_get_template_files(self, additional_code):
#     # Simulate template files with "_TEMPLATE"
#     additional_code.files = ["file1_TEMPLATE", "file2_TEMPLATE"]
#     additional_code.working_path = Path("/mock/local/dir")

#     # Call get method
#     additional_code.get("/mock/local/dir")

#     # Ensure that the template files were copied and renamed
#     assert self.mock_copy.call_count == 4  # 2 original files + 2 template renames
#     self.mock_copy.assert_any_call(Path("/mock/local/dir/file1_TEMPLATE"), Path("/mock/local/dir/file1"))
#     self.mock_copy.assert_any_call(Path("/mock/local/dir/file2_TEMPLATE"), Path("/mock/local/dir/file2"))

# def test_cleanup_temp_directory(self, additional_code):
#     # Simulate remote repository source
#     additional_code.source.location_type = "url"
#     additional_code.source.source_type = "repository"
#     additional_code.checkout_target = "v1.0.0"

#     # Call get method
#     additional_code.get("/mock/local/dir")

#     # Ensure the temporary directory is cleaned up after use
#     self.mock_rmtree.assert_called_once_with("/mock/tmp/dir")
