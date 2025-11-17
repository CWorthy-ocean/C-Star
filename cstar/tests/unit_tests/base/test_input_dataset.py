from datetime import datetime
from pathlib import Path
from textwrap import dedent
from unittest import mock

from cstar.io.source_data import SourceData
from cstar.io.staged_data import StagedData
from cstar.tests.unit_tests.fake_abc_subclasses import FakeInputDataset


class TestInputDatasetInit:
    """Test class for the initialization of the InputDataset class."""

    def test_local_init(self, fakeinputdataset_local):
        """Test initialization of an InputDataset with a local source."""
        ind = fakeinputdataset_local
        assert ind.source.basename == "local_file.nc", (
            "Expected basename to be 'local_file.nc'"
        )
        assert isinstance(ind, FakeInputDataset), (
            "Expected an instance of FakeInputDataset"
        )
        assert ind.start_date == datetime(2024, 10, 22, 12, 34, 56)
        assert ind.end_date == datetime(2024, 12, 31, 23, 59, 59)

    def test_remote_init(self, fakeinputdataset_remote):
        """Test initialization of an InputDataset with a remote source."""
        ind = fakeinputdataset_remote
        assert ind.source.basename == "remote_file.nc", (
            "Expected basename to be 'remote_file.nc'"
        )
        assert ind.source.file_hash == "abc123", "Expected file_hash to be 'abc123'"
        assert isinstance(ind, FakeInputDataset), (
            "Expected an instance of FakeInputDataset"
        )
        assert ind


class TestStrAndRepr:
    """Test class for the __str__ and __repr__ methods on an InputDataset."""

    def test_local_str(self, fakeinputdataset_local):
        """Test the string representation of a local InputDataset."""
        expected_str = dedent(
            """\
    ----------------
    FakeInputDataset
    ----------------
    Source location: some/local/source/path/local_file.nc
    Source file hash: test_target
    start date: 2024-10-22 12:34:56
    end date: 2024-12-31 23:59:59
    Local copy: None"""
        )

        assert str(fakeinputdataset_local) == expected_str

    def test_local_repr(self, fakeinputdataset_local):
        """Test the repr representation of a local InputDataset."""
        expected_repr = dedent(
            """\
    FakeInputDataset(
    location = 'some/local/source/path/local_file.nc',
    file_hash = 'test_target',
    start_date = datetime.datetime(2024, 10, 22, 12, 34, 56),
    end_date = datetime.datetime(2024, 12, 31, 23, 59, 59)
    )"""
        )
        assert repr(fakeinputdataset_local) == expected_repr

    def test_remote_repr(self, fakeinputdataset_remote):
        """Test the repr representation of a remote InputDataset."""
        expected_repr = dedent(
            """\
    FakeInputDataset(
    location = 'http://example.com/remote_file.nc',
    file_hash = 'abc123',
    start_date = datetime.datetime(2024, 10, 22, 12, 34, 56),
    end_date = datetime.datetime(2024, 12, 31, 23, 59, 59)
    )"""
        )
        assert repr(fakeinputdataset_remote) == expected_repr

    def test_remote_str(self, fakeinputdataset_remote):
        """Test the string representation of a remote InputDataset."""
        expected_str = dedent(
            """\
    ----------------
    FakeInputDataset
    ----------------
    Source location: http://example.com/remote_file.nc
    Source file hash: abc123
    start date: 2024-10-22 12:34:56
    end date: 2024-12-31 23:59:59
    Local copy: None"""
        )
        assert str(fakeinputdataset_remote) == expected_str

    @mock.patch.object(
        FakeInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )  # Mock exists_locally
    @mock.patch.object(FakeInputDataset, "working_copy", new_callable=mock.PropertyMock)
    def test_str_with_working_copy(
        self,
        mock_working_copy,
        mock_exists_locally,
        fakeinputdataset_local,
    ):
        """Test the string output when the dataset has been staged."""
        mock_staged = mock.create_autospec(StagedData)
        mock_staged.path = "/some/local/path"
        mock_working_copy.return_value = mock_staged
        fakeinputdataset_local.working_copy.path = Path("/some/local/path")

        # Simulate exists_locally being True
        mock_exists_locally.return_value = True
        assert "Local copy: /some/local/path" in str(fakeinputdataset_local), (
            f"substring not in {str(fakeinputdataset_local)}"
        )

    @mock.patch.object(
        FakeInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )  # Mock exists_locally
    @mock.patch.object(FakeInputDataset, "working_copy", new_callable=mock.PropertyMock)
    def test_repr_with_working_copy(
        self,
        mock_working_copy,
        mock_exists_locally,
        fakeinputdataset_local,
    ):
        """Test the repr output when the dataset has been staged."""
        mock_staged = mock.create_autospec(StagedData)
        mock_staged.path = "/some/local/path"
        mock_working_copy.return_value = mock_staged
        fakeinputdataset_local.working_copy.path = Path("/some/local/path")

        # Simulate exists_locally being True
        mock_exists_locally.return_value = True
        assert "State: <working_copy = /some/local/path>" in repr(
            fakeinputdataset_local
        ), f"substring not in {repr(fakeinputdataset_local)}"


class TestExistsLocally:
    """Test class for the 'exists_locally' property."""

    def test_exists_locally_when_exists(
        self, fakeinputdataset_remote, stagedfile_remote_source
    ):
        """Test exists_locally property when `working_path` attr set and `changed_from_source` is `False`."""
        mock_staged = stagedfile_remote_source(
            source=fakeinputdataset_remote.source,
            path="some/dir",
            changed_from_source=False,
        )
        with mock.patch(
            "cstar.base.input_dataset.InputDataset.working_copy",
            new_callable=mock.PropertyMock,
            return_value=mock_staged,
        ):
            assert fakeinputdataset_remote.exists_locally

    def test_exists_locally_when_modified(
        self, fakeinputdataset_remote, stagedfile_remote_source
    ):
        """Test exists_locally property when `working_path` attr set and `changed_from_source` is `True`."""
        mock_staged = stagedfile_remote_source(
            source=fakeinputdataset_remote.source,
            path="some/dir",
            changed_from_source=True,
        )
        with mock.patch(
            "cstar.base.input_dataset.InputDataset.working_copy",
            new_callable=mock.PropertyMock,
            return_value=mock_staged,
        ):
            assert not fakeinputdataset_remote.exists_locally

    def test_exists_locally_when_no_working_copy(self, fakeinputdataset_remote):
        """Test exists_locally property when `working_path` attr unset."""
        with mock.patch(
            "cstar.base.input_dataset.InputDataset.working_copy",
            new_callable=mock.PropertyMock,
            return_value=None,
        ):
            assert not fakeinputdataset_remote.exists_locally


def test_to_dict(fakeinputdataset_remote):
    """Test the InputDataset.to_dict method, using a remote InputDataset as an example.

    Fixtures
    --------
    fakeinputdataset_remote: FakeInputDataset instance for remote files.

    Asserts
    -------
    - The dictionary returned matches a known expected dictionary
    """
    assert fakeinputdataset_remote.to_dict() == {
        "location": "http://example.com/remote_file.nc",
        "file_hash": "abc123",
        "start_date": "2024-10-22 12:34:56",
        "end_date": "2024-12-31 23:59:59",
    }


class TestInputDatasetGet:
    """Test class for the InputDataset.get method."""

    @mock.patch.object(
        FakeInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )
    @mock.patch.object(SourceData, "stage")
    def test_get_when_file_exists(
        self,
        mock_stage,
        mock_exists_locally,
        fakeinputdataset_local,
        mock_path_resolve,
    ):
        """Test the InputDataset.get method skips a target file that already exists."""
        # Hardcode the resolved path for local_dir
        local_dir_resolved = Path("/resolved/local/dir")

        # Mock `exists_locally` to return True
        mock_exists_locally.return_value = True

        # Call the `get` method
        fakeinputdataset_local.get(local_dir_resolved)
        mock_stage.assert_not_called()
