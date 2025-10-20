from pathlib import Path
from unittest import mock

import pytest

from cstar.io.constants import (
    FileEncoding,
    LocationType,
    SourceClassification,
    SourceType,
)
from cstar.io.source_data import (
    SourceData,
    SourceDataCollection,
    _location_as_path,
    _SourceInspector,
    get_remote_header,
)


class TestHelperFunctions:
    def test_get_remote_header_reads_bytes(self):
        """Tests that `get_remote_header` fetches the expected number of bytes from a URL"""
        fake_bytes = b"headerdata"

        fake_raw = mock.Mock()
        fake_raw.read.return_value = fake_bytes

        fake_response = mock.Mock()
        fake_response.raw = fake_raw

        # Clear cache here so test always runs fresh
        get_remote_header.cache_clear()

        with mock.patch(
            "cstar.io.source_data.requests.get", return_value=fake_response
        ) as mock_get:
            result = get_remote_header("http://example.com/file", 10)

        mock_get.assert_called_once_with(
            "http://example.com/file", stream=True, allow_redirects=True
        )
        fake_raw.read.assert_called_once_with(10)
        assert result == fake_bytes

    @pytest.mark.parametrize(
        "location, expected",
        [
            # plain string path
            ("some/dir/file.txt", Path("some/dir/file.txt")),
            # Path object
            (Path("/absolute/path/to/file.txt"), Path("/absolute/path/to/file.txt")),
            # http URL
            ("http://example.com/data/file.txt", Path("/data/file.txt")),
            # https URL
            ("https://example.com/foo/bar.csv", Path("/foo/bar.csv")),
        ],
    )
    def test_location_as_path(self, location, expected):
        """Tests the _location_as_path function correctly converts various location strings to Path objects."""
        result = _location_as_path(location)
        assert result == expected


class TestSourceInspector:
    def test_location_type_http(self):
        """Tests location_type returns HTTP for a remote address"""
        inspector = _SourceInspector("http://example.com/file.txt")
        assert inspector.location_type is LocationType.HTTP

    def test_location_type_path(self, tmp_path):
        """Tests that location_type returns PATH for a local path"""
        file_path = tmp_path / "data.txt"
        file_path.write_text("hello")
        inspector = _SourceInspector(str(file_path))
        assert inspector.location_type is LocationType.PATH

    def test_location_type_invalid(self, tmp_path):
        """Tests that location_type raises if invalid"""
        bad_path = tmp_path / "does_not_exist.txt"
        inspector = _SourceInspector(str(bad_path))
        with pytest.raises(ValueError):
            _ = inspector.location_type

    def test_is_repository_true(self):
        """Tests that is_repository returns True if git ls-remote output non-empty."""
        inspector = _SourceInspector("http://example.com/repo.git")
        with mock.patch(
            "cstar.io.source_data._run_cmd", return_value="fake output"
        ) as mock_cmd:
            assert inspector._is_repository is True
            mock_cmd.assert_called_once_with(
                "git ls-remote http://example.com/repo.git", raise_on_error=True
            )

    def test_is_repository_false(self):
        """Tests that is_repository returns False if git ls-remote exits with an error."""
        inspector = _SourceInspector("http://example.com/not_a_repo")
        with mock.patch(
            "cstar.io.source_data._run_cmd", side_effect=RuntimeError("bad repo")
        ) as mock_cmd:
            assert inspector._is_repository is False
            mock_cmd.assert_called_once_with(
                "git ls-remote http://example.com/not_a_repo", raise_on_error=True
            )

    def test_source_type_repository(self):
        """Tests that source_type returns REPOSITORY if _is_repository True"""
        inspector = _SourceInspector("http://example.com/repo.git")
        with mock.patch.object(
            _SourceInspector, "_is_repository", new_callable=mock.PropertyMock
        ) as mock_repo:
            mock_repo.return_value = True
            assert inspector.source_type is SourceType.REPOSITORY

    def test_source_type_http_file(self):
        """Tests that source_type returns FILE for a remote file"""
        inspector = _SourceInspector("http://example.com/file.txt")
        with (
            mock.patch.object(
                _SourceInspector,
                "_is_repository",
                new_callable=mock.PropertyMock,
                return_value=False,
            ),
            mock.patch.object(
                _SourceInspector,
                "location_type",
                new_callable=mock.PropertyMock,
                return_value=LocationType.HTTP,
            ),
            mock.patch.object(
                _SourceInspector,
                "_http_is_html",
                new_callable=mock.PropertyMock,
                return_value=False,
            ),
        ):
            assert inspector.source_type is SourceType.FILE

    def test_source_type_http_html_invalid(self):
        """Tests that source_type raises if remote location is not a valid source type."""
        inspector = _SourceInspector("http://example.com/page")
        with (
            mock.patch.object(
                _SourceInspector,
                "_is_repository",
                new_callable=mock.PropertyMock,
                return_value=False,
            ),
            mock.patch.object(
                _SourceInspector,
                "location_type",
                new_callable=mock.PropertyMock,
                return_value=LocationType.HTTP,
            ),
            mock.patch.object(
                _SourceInspector,
                "_http_is_html",
                new_callable=mock.PropertyMock,
                return_value=True,
            ),
        ):
            with pytest.raises(ValueError):
                _ = inspector.source_type

    def test_source_type_local_file(self, tmp_path):
        """Tests that source_type returns FILE for a local filepath"""
        file_path = tmp_path / "data.txt"
        file_path.write_text("hello")
        inspector = _SourceInspector(str(file_path))
        assert inspector.source_type is SourceType.FILE

    def test_source_type_local_directory(self, tmp_path):
        """Tests that source_type returns DIRECTORY for a local directory"""
        inspector = _SourceInspector(str(tmp_path))
        assert inspector.source_type is SourceType.DIRECTORY

    def test_source_type_invalid(self):
        """Tests that source_type raises if location is not a valid source type."""
        inspector = _SourceInspector("not_a_real_source")
        with (
            mock.patch.object(
                _SourceInspector,
                "_is_repository",
                new_callable=mock.PropertyMock,
                return_value=False,
            ),
            mock.patch.object(
                _SourceInspector,
                "location_type",
                new_callable=mock.PropertyMock,
                return_value=None,
            ),
        ):
            with pytest.raises(ValueError):
                _ = inspector.source_type

    def test_http_is_html_true(self):
        """Tests that http_is_html returns True if location is a HTML-format page"""
        inspector = _SourceInspector("http://example.com/page")
        fake_response = mock.Mock()
        fake_response.headers = {"Content-Type": "text/html; charset=UTF-8"}
        with mock.patch(
            "cstar.io.source_data.requests.head", return_value=fake_response
        ):
            assert inspector._http_is_html is True

    def test_http_is_html_false(self):
        """Tests that http_is_html returns False if location is not a HTML-format page"""
        inspector = _SourceInspector("http://example.com/file")
        fake_response = mock.Mock()
        fake_response.headers = {"Content-Type": "application/octet-stream"}
        with mock.patch(
            "cstar.io.source_data.requests.head", return_value=fake_response
        ):
            assert inspector._http_is_html is False

    def test_file_encoding_na_when_not_a_file(self):
        """Tests that file_encoding is NA if location does not point to a file"""
        inspector = _SourceInspector("http://example.com/notafile")
        # Patch source_type to be something other than FILE
        with mock.patch.object(
            _SourceInspector, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type:
            mock_source_type.return_value = SourceType.DIRECTORY
            assert inspector.file_encoding is FileEncoding.NA

    def test_file_encoding_http_text(self):
        """Tests that file_encoding returns TEXT for a remote text file"""
        inspector = _SourceInspector("http://example.com/file.txt")

        fake_bytes = b"hello world"
        with (
            mock.patch(
                "cstar.io.source_data.get_remote_header", return_value=fake_bytes
            ) as mock_header,
            mock.patch(
                "cstar.io.source_data.charset_normalizer.from_bytes"
            ) as mock_csn,
            mock.patch.object(
                _SourceInspector, "source_type", new_callable=mock.PropertyMock
            ) as mock_source_type,
            mock.patch.object(
                _SourceInspector, "location_type", new_callable=mock.PropertyMock
            ) as mock_loc_type,
        ):
            mock_source_type.return_value = SourceType.FILE
            mock_loc_type.return_value = LocationType.HTTP

            mock_csn.return_value.best.return_value = "utf-8"

            result = inspector.file_encoding

        assert result is FileEncoding.TEXT
        mock_header.assert_called_once_with("http://example.com/file.txt", 512)

    def test_file_encoding_http_binary(self):
        """Tests that file_encoding returns BINARY for a remote binary file"""
        inspector = _SourceInspector("http://example.com/file.bin")

        fake_bytes = b"\x00\x01\x02"
        with (
            mock.patch(
                "cstar.io.source_data.get_remote_header", return_value=fake_bytes
            ),
            mock.patch(
                "cstar.io.source_data.charset_normalizer.from_bytes"
            ) as mock_csn,
            mock.patch.object(
                _SourceInspector, "source_type", new_callable=mock.PropertyMock
            ) as mock_source_type,
            mock.patch.object(
                _SourceInspector, "location_type", new_callable=mock.PropertyMock
            ) as mock_loc_type,
        ):
            mock_source_type.return_value = SourceType.FILE
            mock_loc_type.return_value = LocationType.HTTP

            mock_csn.return_value.best.return_value = None

            result = inspector.file_encoding

        assert result is FileEncoding.BINARY

    def test_file_encoding_path_text(self, tmp_path):
        """Tests that file_encoding returns TEXT for a local text file."""
        file_path = tmp_path / "text.txt"
        file_path.write_bytes(b"hello world")

        inspector = _SourceInspector(str(file_path))

        with (
            mock.patch(
                "cstar.io.source_data.charset_normalizer.from_bytes"
            ) as mock_csn,
            mock.patch.object(
                _SourceInspector, "source_type", new_callable=mock.PropertyMock
            ) as mock_source_type,
            mock.patch.object(
                _SourceInspector, "location_type", new_callable=mock.PropertyMock
            ) as mock_loc_type,
        ):
            mock_source_type.return_value = SourceType.FILE
            mock_loc_type.return_value = LocationType.PATH

            mock_csn.return_value.best.return_value = "utf-8"

            result = inspector.file_encoding

        assert result is FileEncoding.TEXT

    def test_file_encoding_path_binary(self, tmp_path):
        """Tests that file_encoding returns BINARY for a local binary file."""
        file_path = tmp_path / "bin.bin"
        file_path.write_bytes(b"\x00\x01\x02")

        inspector = _SourceInspector(str(file_path))

        with (
            mock.patch(
                "cstar.io.source_data.charset_normalizer.from_bytes"
            ) as mock_csn,
            mock.patch.object(
                _SourceInspector, "source_type", new_callable=mock.PropertyMock
            ) as mock_source_type,
            mock.patch.object(
                _SourceInspector, "location_type", new_callable=mock.PropertyMock
            ) as mock_loc_type,
        ):
            mock_source_type.return_value = SourceType.FILE
            mock_loc_type.return_value = LocationType.PATH

            mock_csn.return_value.best.return_value = None

            result = inspector.file_encoding

        assert result is FileEncoding.BINARY

    def test_file_encoding_invalid_location_type(self):
        """Tests that file_encoding raises for an invalid location."""
        inspector = _SourceInspector("invalid://weird")

        with (
            mock.patch.object(
                _SourceInspector, "source_type", new_callable=mock.PropertyMock
            ) as mock_source_type,
            mock.patch.object(
                _SourceInspector, "location_type", new_callable=mock.PropertyMock
            ) as mock_loc_type,
        ):
            mock_source_type.return_value = SourceType.FILE
            mock_loc_type.return_value = None  # Something invalid

            with pytest.raises(ValueError):
                _ = inspector.file_encoding


class TestSourceData:
    def test_init_sets_attributes_and_classification(self):
        """Tests that SourceData.__init__ assigns attributes and calls `classify()``"""
        # Arrange
        fake_location = "http://example.com/file.txt"
        fake_identifier = "abc123"
        fake_classification = SourceClassification.REMOTE_TEXT_FILE

        # Patch _SourceInspector so we don't run real classification logic
        with mock.patch("cstar.io.source_data._SourceInspector") as mock_inspector:
            mock_instance = mock_inspector.return_value
            mock_instance.classify.return_value = fake_classification

            # Act
            src = SourceData(location=fake_location, identifier=fake_identifier)

        assert src.location == fake_location
        assert src.identifier == fake_identifier
        assert src._classification == fake_classification

        # Also check inspector was called with the right location
        mock_inspector.assert_called_once_with(fake_location)
        mock_instance.classify.assert_called_once()

    @pytest.mark.parametrize(
        "classification, expected_file_hash, expected_checkout_target",
        [
            (SourceClassification.LOCAL_TEXT_FILE, "id123", None),
            (SourceClassification.LOCAL_BINARY_FILE, "id123", None),
            (SourceClassification.REMOTE_TEXT_FILE, "id123", None),
            (SourceClassification.REMOTE_BINARY_FILE, "id123", None),
            (SourceClassification.REMOTE_REPOSITORY, None, "id123"),
        ],
    )
    def test_identifier_synonyms(
        self,
        mocksourcedata_factory,
        classification,
        expected_file_hash,
        expected_checkout_target,
    ):
        """Tests that context-specific uses of `identifier` (file hash/checkout target) are correct."""
        src = mocksourcedata_factory(
            classification=classification,
            location="some/location",
            identifier="id123",
        )

        assert src.file_hash == expected_file_hash
        assert src.checkout_target == expected_checkout_target

    def test_checkout_hash_for_repository(self, mocksourcedata_factory):
        """Tests that the checkout_hash property calls _get_hash_from_checkout_target if location is a repo."""
        fake_location = "https://github.com/test/repo.git"
        fake_identifier = "abc123"

        src = mocksourcedata_factory(
            classification=SourceClassification.REMOTE_REPOSITORY,
            location=fake_location,
            identifier=fake_identifier,
        )

        with mock.patch(
            "cstar.io.source_data._get_hash_from_checkout_target",
            return_value="deadtofu",
        ) as mock_get_hash:
            result = src.checkout_hash

        assert result == "deadtofu"
        mock_get_hash.assert_called_once_with(fake_location, fake_identifier)

    def test_checkout_hash_none_if_not_repo(self, mocksourcedata_factory):
        """Tests that the checkout_hash property is None if location is not a git repo."""
        src = mocksourcedata_factory(
            classification=SourceClassification.LOCAL_TEXT_FILE,
            location="foo.txt",
            identifier="id123",
        )
        assert src.checkout_hash is None

    def test_checkout_hash_none_if_no_identifier(self, mocksourcedata_factory):
        """Tests that the checkout_hash property is None if SourceData.identifier is None."""
        src = mocksourcedata_factory(
            classification=SourceClassification.REMOTE_REPOSITORY,
            location="https://github.com/test/repo.git",
            identifier=None,
        )
        assert src.checkout_hash is None

    def test_stager_property_caches_value(self, mocksourcedata_factory):
        """Tests that the `stager` property caches its first result and avoids repeat calls to get_stager()."""
        src = mocksourcedata_factory(
            classification=SourceClassification.LOCAL_TEXT_FILE,
            location="foo.txt",
            identifier="id123",
        )

        # First access: should call _select_stager
        first = src.stager
        # Second access: should reuse the same object, not create a new one
        second = src.stager
        assert first is second


class TestSourceDataCollection:
    def test_len_getitem_iter(self, mock_sourcedatacollection):
        """Tests the additional dunder methods on the SourceDataCollection"""
        coll = mock_sourcedatacollection()
        assert len(coll) == 2
        first = coll[0]
        assert isinstance(first.location, str)
        assert list(iter(coll)) == coll.sources

    def test_invalid_source_type_raises(self, mocksourcedata_factory):
        """Tests that SourceDataCollection cannot be initialized with invalid sources."""
        bad = mocksourcedata_factory(
            classification=SourceClassification.REMOTE_REPOSITORY,
            location="http://example.com/repo.git",
            identifier="id1",
        )
        with pytest.raises(TypeError):
            SourceDataCollection([bad])

    def test_append_and_locations(
        self, mock_sourcedatacollection, mocksourcedata_factory
    ):
        """Tests that SourceDataCollection.append() adds a new SourceData instance to the collection."""
        coll = mock_sourcedatacollection()
        good = mocksourcedata_factory(
            classification=SourceClassification.LOCAL_TEXT_FILE,
            location="foo.txt",
            identifier="idx",
        )
        coll.append(good)
        assert good in coll.sources
        assert "foo.txt" in coll.locations

    def test_append_invalid_raises(
        self, mock_sourcedatacollection, mocksourcedata_factory
    ):
        """Tests that SourceDataCollection.append() raises if called with an invalid argument."""
        coll = mock_sourcedatacollection()
        bad = mocksourcedata_factory(
            classification=SourceClassification.REMOTE_REPOSITORY,
            location="http://example.com/repo.git",
            identifier="badid",
        )
        with pytest.raises(TypeError):
            coll.append(bad)

    def test_sources_property(self, mock_sourcedatacollection):
        """Tests that SourceDataCollection.sources returns the list of underlying SourceData items."""
        coll = mock_sourcedatacollection()
        assert coll.sources == coll._sources

    def test_from_locations_success_and_length_mismatch(self):
        """Tests SourceDataCollection.from_locations classmethod with valid/mismatched input for `locations` and `identifiers`."""
        fake_classification = SourceClassification.LOCAL_TEXT_FILE
        # Patch SourceData so we don't hit inspector logic
        with mock.patch("cstar.io.source_data.SourceData", autospec=True) as mock_src:
            # Make every fake SourceData instance have _classification set
            instance = mock_src.return_value
            instance._classification = fake_classification
            # Success case
            coll = SourceDataCollection.from_locations(
                ["foo.txt", "bar.txt"], ["id1", "id2"]
            )
            assert isinstance(coll, SourceDataCollection)
            assert len(coll) == 2
            assert mock_src.call_count == 2

            # Length mismatch
            with pytest.raises(ValueError):
                SourceDataCollection.from_locations(["foo.txt"], ["id1", "id2"])
