################################################################################
import logging
from pathlib import Path
from unittest import mock

import pytest

import cstar.base.external_codebase as external_codebase
from cstar.tests.unit_tests.fake_abc_subclasses import FakeExternalCodeBase


def test_codebase_str(fakeexternalcodebase):
    """Test the string representation of the `ExternalCodeBase` class.

    Fixtures
    --------
    fakeexternalcodebase : FakeExternalCodeBase
        A mock instance of `ExternalCodeBase` with a predefined environment and repository configuration.

    Mocks
    -----
    local_config_status : PropertyMock
        Mocked to test different states of the local configuration, such as valid,
        wrong repo, right repo/wrong hash, and repo not found.

    Asserts
    -------
    str
        Verifies that the expected string output matches the actual string representation
        under various configurations of `local_config_status`.
    """
    # Define the expected output
    expected_str = (
        "FakeExternalCodeBase\n"
        "--------------------\n"
        "source_repo : https://github.com/test/repo.git (default)\n"
        "checkout_target : test_target (default)"
    )

    # Compare the actual result with the expected result
    assert expected_str in str(fakeexternalcodebase), (
        f"EXPECTED: \n{expected_str}, GOT: \n{str(fakeexternalcodebase)}"
    )


def test_codebase_repr(fakeexternalcodebase):
    """Test the repr representation of the `ExternalCodeBase` class."""
    result_repr = repr(fakeexternalcodebase)
    expected_repr = (
        "FakeExternalCodeBase("
        + "\nsource_repo = 'https://github.com/test/repo.git',"
        + "\ncheckout_target = 'test_target'"
        + "\n)"
    )

    assert result_repr == expected_repr


class TestToDict:
    def test_to_dict(self, fakeexternalcodebase):
        """Confirms that the correct key/value pairs are assigned by ExternalCodeBase.to_dict"""
        result = fakeexternalcodebase.to_dict()
        assert result["source_repo"] == fakeexternalcodebase.source.location
        assert result["checkout_target"] == fakeexternalcodebase.source.checkout_target


class TestSetup:
    def test_init_assigns_preconfigured_codebase_to_working_copy(
        self, mocksourcedata_remote_repo, stagedrepository, tmp_path
    ):
        """Confirms that, if the codebase is already configured (e.g. in a different session), working_copy is set."""
        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value={"TEST_ROOT": tmp_path},
            ),
            mock.patch(
                "cstar.base.external_codebase.StagedRepository",
                return_value=stagedrepository(path=tmp_path),
            ),
            mock.patch(
                "cstar.base.external_codebase.SourceData",
                return_value=mocksourcedata_remote_repo(),
            ),
        ):
            fecb = FakeExternalCodeBase(configured=True)
        assert fecb.working_copy.path == tmp_path

    def test_get_skips_if_already_staged(self, fakeexternalcodebase, caplog):
        """Confirms that `get()` skips its logic if the codebase is staged"""
        fakeexternalcodebase._working_copy = mock.Mock(
            spec=external_codebase.StagedRepository
        )
        caplog.set_level(logging.INFO, logger=fakeexternalcodebase.log.name)
        fakeexternalcodebase.get(target_dir=Path("/tmp/foo"))

        assert "already staged" in caplog.text
        assert fakeexternalcodebase.working_copy is not None

    def test_get_default_target_dir_and_stage_called(
        self, fakeexternalcodebase, tmp_path, caplog
    ):
        """Tests that `get()` invokes default staging location if no target provided.

        The default location is <C-star's root directory>/<codebase repository's basename>
        """
        staged_repo = mock.Mock(spec=external_codebase.StagedRepository)
        fakeexternalcodebase.source.stage = mock.Mock(return_value=staged_repo)

        with mock.patch(
            "cstar.base.external_codebase.cstar_sysmgr._environment"
        ) as mock_env:
            mock_env.package_root = tmp_path
            fakeexternalcodebase._working_copy = None
            caplog.set_level(logging.INFO, logger=fakeexternalcodebase.log.name)
            fakeexternalcodebase.get()

        assert fakeexternalcodebase.working_copy == staged_repo
        fakeexternalcodebase.source.stage.assert_called_once()
        assert "defaulting to" in caplog.text


class TestConfigure:
    def test_configure_raises_without_working_copy(
        self, fakeexternalcodebase_with_mock_get
    ):
        """Confirms the `configure` method raises if `working_copy` is None."""
        fakeexternalcodebase_with_mock_get._working_copy = None
        with pytest.raises(FileNotFoundError):
            fakeexternalcodebase_with_mock_get.configure()

    def test_configure_logs_if_already_configured(
        self, fakeexternalcodebase_with_mock_get, caplog
    ):
        """Confirms that a message is logged if `configure()` is called unnecessarily"""
        fakeexternalcodebase_with_mock_get._working_copy = mock.Mock(
            spec=external_codebase.StagedRepository
        )
        with mock.patch.object(
            type(fakeexternalcodebase_with_mock_get),
            "is_configured",
            new_callable=mock.PropertyMock,
        ) as prop:
            prop.return_value = True
            caplog.set_level(
                logging.INFO,
                logger=fakeexternalcodebase_with_mock_get.log.name,
            )
            fakeexternalcodebase_with_mock_get.configure()

        assert "correctly configured" in caplog.text

    def test_configure_calls_subclass_configure_if_not_configured(
        self, fakeexternalcodebase_with_mock_get
    ):
        """Confirms that ExternalCodeBase defers to the abstractmethod `_configure` after validation."""
        fakeexternalcodebase_with_mock_get._working_copy = mock.Mock(
            spec=external_codebase.StagedRepository
        )
        with mock.patch.object(
            type(fakeexternalcodebase_with_mock_get),
            "is_configured",
            new_callable=mock.PropertyMock,
        ) as prop:
            prop.return_value = False
            with mock.patch.object(
                fakeexternalcodebase_with_mock_get, "_configure"
            ) as do_configure:
                fakeexternalcodebase_with_mock_get.configure()
        do_configure.assert_called_once()

    def test_setup_calls_get_and_configure(
        self, fakeexternalcodebase_with_mock_get, tmp_path
    ):
        """Confirms that `setup` calls both methods it wraps."""
        with (
            mock.patch.object(fakeexternalcodebase_with_mock_get, "get") as fake_get,
            mock.patch.object(
                fakeexternalcodebase_with_mock_get, "configure"
            ) as fake_conf,
        ):
            fakeexternalcodebase_with_mock_get.setup(tmp_path)
        fake_get.assert_called_once_with(tmp_path)
        fake_conf.assert_called_once()
