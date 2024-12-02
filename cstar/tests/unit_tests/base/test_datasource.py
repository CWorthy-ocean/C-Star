import pytest
from pathlib import Path
from cstar.base.datasource import DataSource


# Mock paths for local file tests
@pytest.fixture
def mock_netcdf_file_path(tmp_path):
    """Fixture to return a temporary empty file with a .nc extension."""
    file_path = tmp_path / "testfile.nc"
    file_path.touch()  # create an empty file for testing
    return file_path


@pytest.fixture
def mock_yaml_file_path(tmp_path):
    """Fixture to return a temporary empty file with a .yaml extension."""
    file_path = tmp_path / "testfile.yaml"
    file_path.touch()  # create an empty file for testing
    return file_path


@pytest.fixture
def mock_directory_path(tmp_path):
    """Fixture to return a temporary empty directory."""
    dir_path = tmp_path / "test_dir"
    dir_path.mkdir()
    return dir_path


@pytest.fixture
def mock_git_path(tmp_path):
    """Fixture to return a temporary mocked git repository."""
    git_dir = tmp_path / "test_repo.git"
    git_dir.mkdir()
    (git_dir / ".git").mkdir()  # create a .git subdirectory
    return git_dir


# Tests for the `location_type` property
def test_location_type_url():
    """Check the DataSource.location_type property correctly returns 'url' for a remote
    data source."""

    data_source = DataSource("https://example.com/data.nc")
    assert data_source.location_type == "url"


def test_location_type_path(mock_netcdf_file_path):
    """Check the DataSource.location_type property correctly returns 'path' for a local
    data file."""

    data_source = DataSource(mock_netcdf_file_path)
    assert data_source.location_type == "path"


def test_location_type_invalid():
    """Test the DataSource.location_type property raises a ValueError for an invalid
    location."""

    with pytest.raises(ValueError):
        data_source = DataSource("invalid_location")
        data_source.location_type


# Tests for the `source_type` property
def test_source_type_netcdf(mock_netcdf_file_path):
    """Test the DataSource.source_type property correctly returns 'netcdf' for a mock
    '.nc' file."""

    data_source = DataSource(mock_netcdf_file_path)
    assert data_source.source_type == "netcdf"


def test_source_type_directory(mock_directory_path):
    """Test the DataSource.source_type property correctly returns 'directory' for a
    temporary directory."""

    data_source = DataSource(mock_directory_path)
    assert data_source.source_type == "directory"


def test_source_type_repository(mock_git_path):
    """Test the DataSource.source_type property correctly returns 'repository' for a
    mock git repository."""

    data_source = DataSource(mock_git_path)
    assert data_source.source_type == "repository"


def test_source_type_yaml(mock_yaml_file_path):
    """Test the DataSource.source_type property correctly returns 'yaml' for a mock
    '.yaml' file."""
    data_source = DataSource(mock_yaml_file_path)
    assert data_source.source_type == "yaml"


def test_source_type_unsupported_extension(tmp_path):
    """Test the DataSource.source_type property raises a ValueError for an unsupported
    file."""

    unsupported_file = tmp_path / "data.unsupported"
    unsupported_file.touch()
    data_source = DataSource(unsupported_file)
    with pytest.raises(ValueError):
        data_source.source_type


def test_basename(mock_netcdf_file_path):
    """Test the DataSource.basename property correctly returns the filename and
    extension."""
    data_source = DataSource(mock_netcdf_file_path)
    assert data_source.basename == "testfile.nc"


def test_str(mock_netcdf_file_path):
    """Test the string representation of DataSource."""

    expected_str = f"""DataSource
----------
 location: {mock_netcdf_file_path.resolve()}
 basename: testfile.nc
 location type: path
 source type: netcdf"""
    data_source = DataSource(mock_netcdf_file_path)

    assert str(data_source) == expected_str


def test_repr(mock_netcdf_file_path):
    """Test the repr representation of DataSource."""
    data_source = DataSource(mock_netcdf_file_path)
    expected_repr = f"""DataSource(location={data_source.location!r})"""
    assert repr(data_source) == expected_repr


def test_init_with_path_object(mock_netcdf_file_path):
    """Test the DataSource.location property when initialized with a Path object."""
    data_source = DataSource(Path(mock_netcdf_file_path))
    assert data_source.location == str(mock_netcdf_file_path)
