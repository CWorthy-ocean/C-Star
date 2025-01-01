import pytest
from unittest import mock
from pathlib import Path
from textwrap import dedent
from cstar.base import InputDataset
from cstar.base.datasource import DataSource


class MockInputDataset(InputDataset):
    """Mock subclass of the InputDataset abstract base class.

    Since InputDataset is an abstract base class, this mock class is needed to allow
    instantiation for testing purposes. It inherits from InputDataset without adding any
    new behavior, serving only to allow tests to create and manipulate instances.
    """

    pass


@pytest.fixture
def local_input_dataset():
    """Fixture to provide a mock local InputDataset instance.

    This fixture patches properties of the DataSource class to simulate a local dataset,
    initializing it with relevant attributes like location, start date, and end date.

    Mocks
    -----
    - Mocked DataSource.location_type property returning 'path'
    - Mocked DataSource.source_type property returning 'netcdf'
    - Mocked DataSource.basename property returning 'local_file.nc'

    Yields
    ------
    MockInputDataset: Instance representing a local input dataset for testing.
    """
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
        mock.patch.object(
            DataSource, "basename", new_callable=mock.PropertyMock
        ) as mock_basename,
    ):
        mock_location_type.return_value = "path"
        mock_source_type.return_value = "netcdf"
        mock_basename.return_value = "local_file.nc"

        dataset = MockInputDataset(
            location="some/local/source/path/local_file.nc",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def remote_input_dataset():
    """Fixture to provide a mock remote InputDataset instance.

    This fixture patches properties of the DataSource class to simulate a remote dataset,
    initializing it with attributes such as URL location, file hash, and date range.

    Mocks
    -----
    - Mocked DataSource.location_type property returning 'url'
    - Mocked DataSource.source_type property returning 'netcdf'
    - Mocked DataSource.basename property returning 'remote_file.nc'

    Yields
    ------
    MockInputDataset: Instance representing a remote input dataset for testing.
    """

    # Using context managers to patch properties on DataSource
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
        mock.patch.object(
            DataSource, "basename", new_callable=mock.PropertyMock
        ) as mock_basename,
    ):
        # Mock property return values for a remote file (URL)
        mock_location_type.return_value = "url"
        mock_source_type.return_value = "netcdf"
        mock_basename.return_value = "remote_file.nc"

        # Create the InputDataset instance; it will use the mocked DataSource
        dataset = MockInputDataset(
            location="http://example.com/remote_file.nc",
            file_hash="abc123",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        # Yield the dataset for use in the test
        yield dataset


class TestInputDatasetInit:
    """Unit tests for initializing InputDataset objects.

    This class contains tests for initializing local and remote `InputDataset`
    objects and verifying their attributes, as well as validating required parameters.

    Tests
    -----
    test_local_init:
        Tests the initialization of a local `InputDataset` instance.
    test_remote_init:
        Tests the initialization of a remote `InputDataset` instance.
    test_remote_requires_file_hash:
        Verifies that a remote `InputDataset` raises a `ValueError` if the file hash is not provided.
    """

    def test_local_init(self, local_input_dataset):
        """Test initialization of a local InputDataset.

        Fixtures
        --------
        local_input_dataset: MockInputDataset instance for local files.

        Mocks
        -----
        - Mocked `location_type` attribute of `DataSource`, returning 'path'.
        - Mocked `basename` attribute of `DataSource`, returning 'local_file.nc'.

        Asserts
        -------
        - The `location_type` is "path".
        - The `basename` is "local_file.nc".
        - The dataset is an instance of MockInputDataset.
        """

        assert (
            local_input_dataset.source.location_type == "path"
        ), "Expected location_type to be 'path'"
        assert (
            local_input_dataset.source.basename == "local_file.nc"
        ), "Expected basename to be 'local_file.nc'"
        assert isinstance(
            local_input_dataset, MockInputDataset
        ), "Expected an instance of MockInputDataset"

    def test_remote_init(self, remote_input_dataset):
        """Test initialization of a remote InputDataset.

        Fixtures
        --------
        remote_input_dataset: MockInputDataset instance for remote files.

        Mocks
        -----
        - Mocked `location_type` attribute of `DataSource`, returning 'url'.
        - Mocked `basename` attribute of `DataSource`, returning 'remote_file.nc'.
        - Mocked `file_hash` attribute of `DataSource`, returning 'abc123'.

        Asserts
        -------
        - The `location_type` is "url".
        - The `basename` is "remote_file.nc".
        - The `file_hash` is set to "abc123".
        - The dataset is an instance of MockInputDataset.
        """
        assert (
            remote_input_dataset.source.location_type == "url"
        ), "Expected location_type to be 'url'"
        assert (
            remote_input_dataset.source.basename == "remote_file.nc"
        ), "Expected basename to be 'remote_file.nc'"
        assert (
            remote_input_dataset.source.file_hash == "abc123"
        ), "Expected file_hash to be 'abc123'"
        assert isinstance(
            remote_input_dataset, MockInputDataset
        ), "Expected an instance of MockInputDataset"

    def test_remote_requires_file_hash(self, remote_input_dataset):
        """Test that a remote InputDataset raises an error when the file hash is
        missing.

        This test confirms that a ValueError is raised if a remote dataset is created without a required file hash.

        Fixtures
        --------
        remote_input_dataset: MockInputDataset instance for remote files.

        Mocks
        -----
        - Mocked remote InputDataset without a file hash.

        Asserts
        -------
        - A ValueError is raised if the `file_hash` is missing for a remote dataset.
        - The exception message matches the expected error message.
        """
        with pytest.raises(ValueError) as exception_info:
            MockInputDataset("http://example.com/remote_file.nc")

        expected_message = (
            "Cannot create InputDataset for \n http://example.com/remote_file.nc:\n "
            + "InputDataset.source.file_hash cannot be None if InputDataset.source.location_type is 'url'.\n"
            + "A file hash is required to verify files downloaded from remote sources."
        )

        assert str(exception_info.value) == expected_message


class TestLocalHash:
    """Tests for the `local_hash` property of the `InputDataset`.

    This test suite verifies the behavior of the `local_hash` property, ensuring
    it correctly computes, caches, and handles edge cases such as multiple files
    or a missing `working_path` attribute.

    Tests
    -----
    - `test_local_hash_single_file`: Verifies the calculation of `local_hash` for a single file.
    - `test_local_hash_cached`: Ensures cached hash values are used when available.
    - `test_local_hash_no_working_path`: Confirms that `local_hash` returns `None` when no working path is set.
    - `test_local_hash_multiple_files`: Validates `local_hash` computation for multiple files.
    """

    def setup_method(self):
        """Set up common mocks for `local_hash` tests."""
        # Patch resolve
        self.patcher_resolve = mock.patch("pathlib.Path.resolve")
        self.mock_resolve = self.patcher_resolve.start()
        self.mock_resolve.return_value = Path("/resolved/local/path")

        # Patch _get_sha256_hash
        self.patcher_get_hash = mock.patch("cstar.base.input_dataset._get_sha256_hash")
        self.mock_get_hash = self.patcher_get_hash.start()
        self.mock_get_hash.return_value = "mocked_hash"

        # Patch exists_locally
        self.patcher_exists_locally = mock.patch(
            "cstar.base.input_dataset.InputDataset.exists_locally",
            new_callable=mock.PropertyMock,
        )
        self.mock_exists_locally = self.patcher_exists_locally.start()
        self.mock_exists_locally.return_value = True

    def teardown_method(self):
        """Stop all patches."""
        mock.patch.stopall()

    def test_local_hash_single_file(self, local_input_dataset):
        """Test `local_hash` calculation for a single file.

        This test ensures that the `local_hash` property calculates the hash correctly
        for a single file in the `working_path`.

        Mocks
        -----
        - Mocked `Path.resolve` to simulate resolved paths.
        - Mocked `_get_sha256_hash` to simulate hash computation.

        Asserts
        -------
        - The `local_hash` matches the expected hash for the file.
        - `_get_sha256_hash` is called with the resolved path.
        """
        local_input_dataset._local_file_hash_cache = None
        local_input_dataset.working_path = Path("/some/local/path")

        # Ensure the resolve method is invoked
        self.mock_resolve.return_value = Path("/resolved/local/path")

        result = local_input_dataset.local_hash

        # Debug: Confirm resolve was invoked
        print(f"Resolved path: {local_input_dataset.working_path.resolve()}")

        # Check that the result uses the resolved path
        assert result == {
            Path("/some/local/path"): "mocked_hash"
        }, f"Expected calculated local_hash, but got {result}"

        # Verify _get_sha256_hash was called with the resolved path
        self.mock_get_hash.assert_called_once_with(Path("/some/local/path"))

    def test_local_hash_cached(self, local_input_dataset):
        """Test `local_hash` when the hash is cached.

        This test ensures that if the `_local_file_hash_cache` is already set,
        the `local_hash` property uses the cached value without recomputing.

        Asserts
        -------
        - The `local_hash` property returns the cached value.
        - `_get_sha256_hash` is not called.
        """

        cached_hash = {Path("/resolved/local/path"): "cached_hash"}
        local_input_dataset._local_file_hash_cache = cached_hash

        result = local_input_dataset.local_hash

        assert result == cached_hash, "Expected the cached hash to be returned."
        self.mock_get_hash.assert_not_called()

    def test_local_hash_no_working_path(self, local_input_dataset):
        """Test `local_hash` when no working path is set.

        This test ensures that the `local_hash` property returns `None` when the
        `working_path` attribute is not defined, indicating no valid local file exists.

        Asserts
        -------
        - The `local_hash` property returns `None` when `working_path` is `None`.
        - `_get_sha256_hash` is not called.
        """

        local_input_dataset.working_path = None

        result = local_input_dataset.local_hash

        assert (
            result is None
        ), "Expected local_hash to be None when working_path is not set."
        self.mock_get_hash.assert_not_called()

    def test_local_hash_multiple_files(self, local_input_dataset):
        """Test `local_hash` calculation for multiple files.

        This test ensures that the `local_hash` property correctly computes and returns
        SHA256 hashes for multiple files when `working_path` is a list of paths.

        Mocks
        -----
        - `Path.resolve`: Mocked to return predefined resolved paths for each file.
        - `_get_sha256_hash`: Mocked to return a consistent hash value for testing.

        Asserts
        -------
        - The `local_hash` property returns a dictionary mapping each file path to its
          corresponding hash.
        - `_get_sha256_hash` is called for each resolved path in `working_path`.
        """

        local_input_dataset._local_file_hash_cache = None
        local_input_dataset.working_path = [
            Path("/some/local/path1"),
            Path("/some/local/path2"),
        ]

        self.mock_resolve.side_effect = [
            Path("/resolved/local/path1"),
            Path("/resolved/local/path2"),
        ]

        result = local_input_dataset.local_hash

        assert result == {
            Path("/some/local/path1"): "mocked_hash",
            Path("/some/local/path2"): "mocked_hash",
        }, f"Expected calculated local_hash for multiple files, but got {result}"

        self.mock_get_hash.assert_has_calls(
            [
                mock.call(Path("/resolved/local/path1")),
                mock.call(Path("/resolved/local/path2")),
            ],
            any_order=True,
        )


class TestStrAndRepr:
    """Tests for string and representation methods of the `InputDataset` class.

    This test class verifies the correctness of the `__str__` and `__repr__` methods
    for both local and remote datasets, as well as scenarios where the `working_path` is
    defined or missing.

    Tests
    -----
    - `test_local_str`: Ensures the `__str__` method for a local dataset produces the
      expected string output.
    - `test_local_repr`: Ensures the `__repr__` method for a local dataset produces the
      expected representation string.
    - `test_remote_str`: Ensures the `__str__` method for a remote dataset produces the
      expected string output.
    - `test_remote_repr`: Ensures the `__repr__` method for a remote dataset produces the
      expected representation string.
    - `test_str_with_working_path`: Verifies the `__str__` output when the `working_path`
      attribute is defined.
    - `test_repr_with_working_path`: Verifies the `__repr__` output when the `working_path`
      attribute is defined.
    """

    def test_local_str(self, local_input_dataset):
        """Test the string representation of a local InputDataset."""
        expected_str = dedent("""\
            ----------------
            MockInputDataset
            ----------------
            Source location: some/local/source/path/local_file.nc
            start_date: 2024-10-22 12:34:56
            end_date: 2024-12-31 23:59:59
            Working path: None ( does not yet exist. Call InputDataset.get() )
        """).strip()
        assert str(local_input_dataset) == expected_str

    def test_local_repr(self, local_input_dataset):
        """Test the repr representation of a local InputDataset."""
        expected_repr = dedent("""\
        MockInputDataset(
        location = 'some/local/source/path/local_file.nc',
        file_hash = None,
        start_date = datetime.datetime(2024, 10, 22, 12, 34, 56),
        end_date = datetime.datetime(2024, 12, 31, 23, 59, 59)
        )
        """).strip()
        actual_repr = repr(local_input_dataset)
        assert (
            actual_repr == expected_repr
        ), f"Expected:\n{expected_repr}\nBut got:\n{actual_repr}"

    def test_remote_repr(self, remote_input_dataset):
        """Test the repr representation of a remote InputDataset."""
        expected_repr = dedent("""\
        MockInputDataset(
        location = 'http://example.com/remote_file.nc',
        file_hash = 'abc123',
        start_date = datetime.datetime(2024, 10, 22, 12, 34, 56),
        end_date = datetime.datetime(2024, 12, 31, 23, 59, 59)
        )
        """).strip()
        actual_repr = repr(remote_input_dataset)
        assert (
            actual_repr == expected_repr
        ), f"Expected:\n{expected_repr}\nBut got:\n{actual_repr}"

    def test_remote_str(self, remote_input_dataset):
        """Test the string representation of a remote InputDataset."""
        expected_str = dedent("""\
            ----------------
            MockInputDataset
            ----------------
            Source location: http://example.com/remote_file.nc
            Source file hash: abc123
            start_date: 2024-10-22 12:34:56
            end_date: 2024-12-31 23:59:59
            Working path: None ( does not yet exist. Call InputDataset.get() )
        """).strip()
        assert str(remote_input_dataset) == expected_str

    @mock.patch.object(
        MockInputDataset, "local_hash", new_callable=mock.PropertyMock
    )  # Mock local_hash
    @mock.patch.object(
        MockInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )  # Mock exists_locally
    def test_str_with_working_path(
        self, mock_exists_locally, mock_local_hash, local_input_dataset
    ):
        """Test the string output when the working_path attribute is defined."""
        local_input_dataset.working_path = Path("/some/local/path")
        mock_local_hash.return_value = {"mocked_path": "mocked_hash"}

        # Simulate exists_locally being True
        mock_exists_locally.return_value = True
        assert "Working path: /some/local/path" in str(local_input_dataset)
        assert "(exists)" in str(local_input_dataset)

        # Simulate exists_locally being False
        mock_exists_locally.return_value = False
        assert "Working path: /some/local/path" in str(local_input_dataset)
        assert " ( does not yet exist. Call InputDataset.get() )" in str(
            local_input_dataset
        )

    @mock.patch.object(
        MockInputDataset, "local_hash", new_callable=mock.PropertyMock
    )  # Mock local_hash
    @mock.patch.object(
        MockInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )  # Mock exists_locally
    def test_repr_with_working_path(
        self, mock_exists_locally, mock_local_hash, local_input_dataset
    ):
        """Test the repr output when the working_path attribute is defined."""
        local_input_dataset.working_path = Path("/some/local/path")
        mock_local_hash.return_value = {"mocked_path": "mocked_hash"}

        # Simulate exists_locally being True
        mock_exists_locally.return_value = True
        assert (
            "State: <working_path = /some/local/path, local_hash = {'mocked_path': 'mocked_hash'}>"
            in repr(local_input_dataset)
        )

        # Simulate exists_locally being False
        mock_exists_locally.return_value = False
        mock_local_hash.return_value = None
        assert "State: <working_path = /some/local/path (does not exist)>" in repr(
            local_input_dataset
        )
