from unittest import mock

import cstar
from cstar.base.additional_code import AdditionalCode
from cstar.io.constants import SourceClassification
from cstar.io.source_data import SourceDataCollection


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
        with mock.patch.object(
            cstar.io.source_data._SourceInspector,
            "classify",
            side_effect=[
                SourceClassification.REMOTE_REPOSITORY,
                SourceClassification.REMOTE_TEXT_FILE,
                SourceClassification.REMOTE_TEXT_FILE,
                SourceClassification.REMOTE_TEXT_FILE,
            ],
        ):
            ac = AdditionalCode(
                location="https://github.com/test/repo.git",
                checkout_target="test123",
                subdir="test/subdir",
                files=["test_file_1.F", "test_file_2.py", "test_file_3.opt"],
            )

        assert ac.source.locations == [
            "https://raw.githubusercontent.com/test/repo/test123/test/subdir/test_file_1.F",
            "https://raw.githubusercontent.com/test/repo/test123/test/subdir/test_file_2.py",
            "https://raw.githubusercontent.com/test/repo/test123/test/subdir/test_file_3.opt",
        ]


class TestExistsLocallyAndGet:
    """Test class for the `exists_locally` property of the AdditionalCode class."""

    def test_exists_locally_when_exists(
        self, additionalcode_remote, stageddatacollection_remote_files
    ):
        """Test exists_locally property when `working_copy` attr set and `changed_from_source` is `False`."""
        ac = additionalcode_remote()
        ac._working_copy = stageddatacollection_remote_files(
            paths=[f"/some/local/dir/{s.basename}" for s in ac.source],
            sources=ac.source.sources,
            changed_from_source=False,
        )

        assert ac.exists_locally

    def test_exists_locally_when_modified(
        self, additionalcode_remote, stageddatacollection_remote_files
    ):
        """Test exists_locally property when `working_copy` attr set and `changed_from_source` is `True`."""
        ac = additionalcode_remote()
        ac._working_copy = stageddatacollection_remote_files(
            paths=[f"/some/local/dir/{s.basename}" for s in ac.source],
            sources=ac.source.sources,
            changed_from_source=True,
        )
        assert not ac.exists_locally

    def test_exists_locally_when_no_working_copy(self, additionalcode_remote):
        """Test exists_locally property when `working_copy` attr unset."""
        with mock.patch(
            "cstar.base.additional_code.AdditionalCode.working_copy",
            new_callable=mock.PropertyMock,
            return_value=None,
        ):
            assert not additionalcode_remote().exists_locally

    def test_get(self, additionalcode_remote, stageddatacollection_remote_files):
        """Tests that the `get` method correctly calls `stage` and sets `working_copy`"""
        ac = additionalcode_remote()
        staged = stageddatacollection_remote_files()

        with mock.patch.object(
            SourceDataCollection, "stage", return_value=staged
        ) as mock_stage:
            ac.get("/some/local/dir")
            mock_stage.assert_called_once()
            assert ac.working_copy == staged
