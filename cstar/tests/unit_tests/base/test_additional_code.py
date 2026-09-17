from pathlib import Path
from unittest import mock

import pytest

import cstar
from cstar.base.additional_code import AdditionalCode
from cstar.io.constants import SourceClassification
from cstar.io.source_data import SourceDataCollection
from cstar.io.staged_data import StagedDataCollection


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


class TestAttach:
    """Test class for the `AdditionalCode.attach` method.

    Uses real local directories under `tmp_path` (rather than the mocked
    `additionalcode_local` fixture) so that `SourceData.file_hash` is `None`,
    matching real local-file `AdditionalCode` usage where no checksum is
    provided.
    """

    def _make_additional_code(
        self, tmp_path: Path, files: list[str]
    ) -> tuple[AdditionalCode, Path]:
        source_dir = tmp_path / "source"
        subdir = "subdir"
        (source_dir / subdir).mkdir(parents=True)
        for f in files:
            (source_dir / subdir / f).write_text("source content")

        ac = AdditionalCode(location=str(source_dir), subdir=subdir, files=files)
        return ac, tmp_path / "staged"

    def test_attach_happy_path(self, tmp_path):
        """Confirms `attach` sets `working_copy` and `exists_locally` when all files
        are already present at `local_dir`.
        """
        files = ["file1.opt", "file2.F"]
        ac, target_dir = self._make_additional_code(tmp_path, files)
        target_dir.mkdir()
        for f in files:
            (target_dir / f).write_text("staged content")

        ac.attach(target_dir)

        assert isinstance(ac.working_copy, StagedDataCollection)
        assert ac.working_copy.paths == [target_dir / f for f in files]
        assert ac.exists_locally

    def test_attach_raises_and_lists_all_missing_files(self, tmp_path):
        """Confirms `attach` raises a single FileNotFoundError naming every missing
        file when more than one is absent.
        """
        files = ["file1.opt", "file2.F", "file3.py"]
        ac, target_dir = self._make_additional_code(tmp_path, files)
        target_dir.mkdir()
        (target_dir / files[0]).write_text("staged content")
        # files[1] and files[2] are left missing

        with pytest.raises(FileNotFoundError) as exc_info:
            ac.attach(target_dir)

        message = str(exc_info.value)
        assert str(target_dir / files[1]) in message
        assert str(target_dir / files[2]) in message
