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
from cstar.io.staged_data import StagedData, StagedDataCollection
from cstar.io.stager import (
    LocalBinaryFileStager,
    LocalTextFileStager,
    RemoteBinaryFileStager,
    RemoteRepositoryStager,
    RemoteTextFileStager,
)


class TestHelperFunctions:
    def test_get_remote_header_reads_bytes(self):
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
        result = _location_as_path(location)
        assert result == expected


class TestSourceInspector:
    def test_location_type_http(self):
        inspector = _SourceInspector("http://example.com/file.txt")
        assert inspector.location_type is LocationType.HTTP

    def test_location_type_path(self, tmp_path):
        file_path = tmp_path / "data.txt"
        file_path.write_text("hello")
        inspector = _SourceInspector(str(file_path))
        assert inspector.location_type is LocationType.PATH

    def test_location_type_invalid(self, tmp_path):
        bad_path = tmp_path / "does_not_exist.txt"
        inspector = _SourceInspector(str(bad_path))
        with pytest.raises(ValueError):
            _ = inspector.location_type

    def test_is_repository_true(self):
        inspector = _SourceInspector("http://example.com/repo.git")
        with mock.patch(
            "cstar.io.source_data._run_cmd", return_value="fake output"
        ) as mock_cmd:
            assert inspector._is_repository is True
            mock_cmd.assert_called_once_with(
                "git ls-remote http://example.com/repo.git", raise_on_error=True
            )

    def test_is_repository_false(self):
        inspector = _SourceInspector("http://example.com/not_a_repo")
        with mock.patch(
            "cstar.io.source_data._run_cmd", side_effect=RuntimeError("bad repo")
        ) as mock_cmd:
            assert inspector._is_repository is False
            mock_cmd.assert_called_once_with(
                "git ls-remote http://example.com/not_a_repo", raise_on_error=True
            )

    def test_source_type_repository(self):
        inspector = _SourceInspector("http://example.com/repo.git")
        with mock.patch.object(
            _SourceInspector, "_is_repository", new_callable=mock.PropertyMock
        ) as mock_repo:
            mock_repo.return_value = True
            assert inspector.source_type is SourceType.REPOSITORY

    def test_source_type_http_file(self):
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
        file_path = tmp_path / "data.txt"
        file_path.write_text("hello")
        inspector = _SourceInspector(str(file_path))
        assert inspector.source_type is SourceType.FILE

    def test_source_type_local_directory(self, tmp_path):
        inspector = _SourceInspector(str(tmp_path))
        assert inspector.source_type is SourceType.DIRECTORY

    def test_source_type_invalid(self):
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
        inspector = _SourceInspector("http://example.com/page")
        fake_response = mock.Mock()
        fake_response.headers = {"Content-Type": "text/html; charset=UTF-8"}
        with mock.patch(
            "cstar.io.source_data.requests.head", return_value=fake_response
        ):
            assert inspector._http_is_html is True

    def test_http_is_html_false(self):
        inspector = _SourceInspector("http://example.com/file")
        fake_response = mock.Mock()
        fake_response.headers = {"Content-Type": "application/octet-stream"}
        with mock.patch(
            "cstar.io.source_data.requests.head", return_value=fake_response
        ):
            assert inspector._http_is_html is False

    def test_file_encoding_na_when_not_a_file(self):
        inspector = _SourceInspector("http://example.com/notafile")
        # Patch source_type to be something other than FILE
        with mock.patch.object(
            _SourceInspector, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type:
            mock_source_type.return_value = SourceType.DIRECTORY
            assert inspector.file_encoding is FileEncoding.NA

    def test_file_encoding_http_text(self):
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


# class TestSourceData:

#     def test_init_sets_attributes_and_classification(self):
#         # Arrange
#         fake_location = "http://example.com/file.txt"
#         fake_identifier = "abc123"
#         fake_classification = SourceClassification.REMOTE_TEXT_FILE

#         # Patch _SourceInspector so we don't run real classification logic
#         with mock.patch(
#             "cstar.io.source_data._SourceInspector"
#         ) as mock_inspector:
#             mock_instance = mock_inspector.return_value
#             mock_instance.classify.return_value = fake_classification

#             # Act
#             src = SourceData(location=fake_location, identifier=fake_identifier)

#         assert src.location == fake_location
#         assert src.identifier == fake_identifier
#         assert src._classification == fake_classification

#         # Also check inspector was called with the right location
#         mock_inspector.assert_called_once_with(fake_location)
#         mock_instance.classify.assert_called_once()


#     @pytest.mark.parametrize(
#         "classification, expected_file_hash, expected_checkout_target",
#         [
#             (SourceClassification.LOCAL_TEXT_FILE, "id123", None),       # file → file_hash
#             (SourceClassification.LOCAL_BINARY_FILE, "id123", None),     # file → file_hash
#             (SourceClassification.REMOTE_TEXT_FILE, "id123", None),      # remote file
#             (SourceClassification.REMOTE_BINARY_FILE, "id123", None),    # remote file
#             (SourceClassification.REMOTE_REPOSITORY, None, "id123"),     # repo → checkout_target
#         ],
#     )
#     def test_identifier_synonyms(
#         self, classification, expected_file_hash, expected_checkout_target
#     ):
#         fake_location = "some/location"
#         fake_identifier = "id123"

#         # Patch out inspector so classification is predictable
#         with mock.patch("cstar.io.source_data._SourceInspector") as mock_inspector:
#             mock_instance = mock_inspector.return_value
#             mock_instance.classify.return_value = classification

#             src = SourceData(location=fake_location, identifier=fake_identifier)

#         assert src.identifier == fake_identifier
#         assert src.file_hash == expected_file_hash
#         assert src.checkout_target == expected_checkout_target


class TestSourceData:
    def test_init_sets_attributes_and_classification(self):
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
        mock_source_data_factory,
        classification,
        expected_file_hash,
        expected_checkout_target,
    ):
        src = mock_source_data_factory(
            classification=classification,
            location="some/location",
            identifier="id123",
        )

        assert src.file_hash == expected_file_hash
        assert src.checkout_target == expected_checkout_target

    def test_checkout_hash_for_repository(self, mock_source_data_factory):
        fake_location = "https://github.com/test/repo.git"
        fake_identifier = "abc123"

        src = mock_source_data_factory(
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

    def test_checkout_hash_none_if_not_repo(self, mock_source_data_factory):
        src = mock_source_data_factory(
            classification=SourceClassification.LOCAL_TEXT_FILE,
            location="foo.txt",
            identifier="id123",
        )
        assert src.checkout_hash is None

    def test_checkout_hash_none_if_no_identifier(self, mock_source_data_factory):
        src = mock_source_data_factory(
            classification=SourceClassification.REMOTE_REPOSITORY,
            location="https://github.com/test/repo.git",
            identifier=None,
        )
        assert src.checkout_hash is None

    # def test_checkout_target_for_repository(self, mock_source_data_factory):
    #     src = mock_source_data_factory(
    #         classification=SourceClassification.REMOTE_REPOSITORY,
    #         location="https://github.com/test/repo.git",
    #         identifier="abc123",
    #     )
    #     assert src.checkout_target == "abc123"

    # def test_checkout_target_for_non_repository(self, mock_source_data_factory):
    #     src = mock_source_data_factory(
    #         classification=SourceClassification.REMOTE_TEXT_FILE,
    #         location="http://example.com/file.txt",
    #         identifier="abc123",
    #     )
    #     assert src.checkout_target is None

    @pytest.mark.parametrize(
        "classification, expected_stager_cls",
        [
            (SourceClassification.REMOTE_REPOSITORY, RemoteRepositoryStager),
            (SourceClassification.REMOTE_BINARY_FILE, RemoteBinaryFileStager),
            (SourceClassification.REMOTE_TEXT_FILE, RemoteTextFileStager),
            (SourceClassification.LOCAL_BINARY_FILE, LocalBinaryFileStager),
            (SourceClassification.LOCAL_TEXT_FILE, LocalTextFileStager),
        ],
    )
    def test_select_stager_returns_expected_class(
        self, mock_source_data_factory, classification, expected_stager_cls
    ):
        src = mock_source_data_factory(
            classification=classification,
            location="some/location",
            identifier="id123",
        )
        stager = src._select_stager()
        assert isinstance(stager, expected_stager_cls)

    def test_select_stager_invalid_classification_raises(
        self, mock_source_data_factory
    ):
        # Fake classification not handled by match-case
        class DummyClassification:
            value = None

        src = mock_source_data_factory(
            classification=None,  # override classification manually
            location="some/location",
            identifier="id123",
        )
        src._classification = "not-a-real-classification"

        with pytest.raises(ValueError):
            _ = src._select_stager()

    def test_stager_property_caches_value(self, mock_source_data_factory):
        src = mock_source_data_factory(
            classification=SourceClassification.LOCAL_TEXT_FILE,
            location="foo.txt",
            identifier="id123",
        )

        # First access: should call _select_stager
        first = src.stager
        # Second access: should reuse the same object, not create a new one
        second = src.stager
        assert first is second

    def test_stage_returns_mockstageddata(self, tmp_path, mock_source_data_factory):
        src = mock_source_data_factory(
            classification=SourceClassification.LOCAL_TEXT_FILE,
            location="foo.txt",
            identifier="id123",
        )

        target_dir = tmp_path / "staged"
        target_dir.mkdir()

        staged = src.stage(target_dir)

        # Check it's our mock staged data
        assert isinstance(staged, StagedData)
        # Path should be target_dir / basename
        assert staged.path == target_dir / Path(src.location).name
        # Should hold a reference to the source
        assert staged.source is src


class TestSourceDataCollection:
    def test_len_getitem_iter(self, mock_sourcedatacollection):
        coll = mock_sourcedatacollection()
        assert len(coll) == 2
        first = coll[0]
        assert isinstance(first.location, str)
        assert list(iter(coll)) == coll.sources

    def test_invalid_source_type_raises(self, mock_source_data_factory):
        bad = mock_source_data_factory(
            classification=SourceClassification.REMOTE_REPOSITORY,
            location="http://example.com/repo.git",
            identifier="id1",
        )
        with pytest.raises(TypeError):
            SourceDataCollection([bad])

    def test_append_and_locations(
        self, mock_sourcedatacollection, mock_source_data_factory
    ):
        coll = mock_sourcedatacollection()
        good = mock_source_data_factory(
            classification=SourceClassification.LOCAL_TEXT_FILE,
            location="foo.txt",
            identifier="idx",
        )
        coll.append(good)
        assert good in coll.sources
        assert "foo.txt" in coll.locations

    def test_append_invalid_raises(
        self, mock_sourcedatacollection, mock_source_data_factory
    ):
        coll = mock_sourcedatacollection()
        bad = mock_source_data_factory(
            classification=SourceClassification.REMOTE_REPOSITORY,
            location="http://example.com/repo.git",
            identifier="badid",
        )
        with pytest.raises(TypeError):
            coll.append(bad)

    def test_sources_property(self, mock_sourcedatacollection):
        coll = mock_sourcedatacollection()
        assert coll.sources == coll._sources

    def test_from_locations_success_and_length_mismatch(self):
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

    def test_stage_returns_staged_collection(self, tmp_path, mock_sourcedatacollection):
        coll = mock_sourcedatacollection()
        staged = coll.stage(tmp_path)

        assert isinstance(staged, StagedDataCollection)
        assert len(staged.items) == len(coll.sources)
        assert {s.source for s in staged.items} == set(coll.sources)
        for staged_item in staged.items:
            assert str(tmp_path) in str(staged_item.path)
