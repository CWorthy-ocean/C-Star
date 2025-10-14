################################################################################
import logging
from pathlib import Path
from unittest import mock

import pytest

import cstar.base.external_codebase as external_codebase
from cstar.tests.unit_tests.fake_abc_subclasses import FakeExternalCodeBase


def test_codebase_str(fake_externalcodebase):
    """Test the string representation of the `ExternalCodeBase` class.

    Fixtures
    --------
    fake_externalcodebase : FakeExternalCodeBase
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
    assert expected_str in str(fake_externalcodebase), (
        f"EXPECTED: \n{expected_str}, GOT: \n{str(fake_externalcodebase)}"
    )


def test_codebase_repr(fake_externalcodebase):
    """Test the repr representation of the `ExternalCodeBase` class."""
    result_repr = repr(fake_externalcodebase)
    expected_repr = (
        "FakeExternalCodeBase("
        + "\nsource_repo = 'https://github.com/test/repo.git',"
        + "\ncheckout_target = 'test_target'"
        + "\n)"
    )

    assert result_repr == expected_repr
    pass


class TestToDict:
    def test_roundtrip_to_dict(self, fake_externalcodebase):
        result = fake_externalcodebase.to_dict()
        assert result["source_repo"] == fake_externalcodebase.source.location
        assert result["checkout_target"] == fake_externalcodebase.source.checkout_target


class TestGet:
    def test_init_assigns_preconfigured_codebase_to_working_copy(
        self, mock_sourcedata_remote_repo, fake_stagedrepository, tmp_path
    ):
        with (
            mock.patch(
                "cstar.system.environment.CStarEnvironment.environment_variables",
                new_callable=mock.PropertyMock,
                return_value={"TEST_ROOT": tmp_path},
            ),
            mock.patch(
                "cstar.base.external_codebase.StagedRepository",
                return_value=fake_stagedrepository(path=tmp_path),
            ),
            mock.patch(
                "cstar.base.external_codebase.SourceData",
                return_value=mock_sourcedata_remote_repo(),
            ),
        ):
            fecb = FakeExternalCodeBase(configured=True)
        assert fecb.working_copy.path == tmp_path

    def test_get_skips_if_already_staged(self, fake_externalcodebase, caplog):
        fake_externalcodebase._working_copy = mock.Mock(
            spec=external_codebase.StagedRepository
        )
        caplog.set_level(logging.INFO, logger=fake_externalcodebase.log.name)
        fake_externalcodebase.get(target_dir=Path("/tmp/foo"))

        assert "already staged" in caplog.text
        assert fake_externalcodebase.working_copy is not None

    def test_get_default_target_dir_and_stage_called(
        self, fake_externalcodebase, tmp_path, caplog
    ):
        staged_repo = mock.Mock(spec=external_codebase.StagedRepository)
        fake_externalcodebase.source.stage = mock.Mock(return_value=staged_repo)

        with mock.patch(
            "cstar.base.external_codebase.cstar_sysmgr._environment"
        ) as mock_env:
            mock_env.package_root = tmp_path
            fake_externalcodebase._working_copy = None
            caplog.set_level(logging.INFO, logger=fake_externalcodebase.log.name)
            fake_externalcodebase.get()

        assert fake_externalcodebase.working_copy == staged_repo
        fake_externalcodebase.source.stage.assert_called_once()
        assert "defaulting to" in caplog.text


class TestConfigure:
    def test_configure_raises_without_working_copy(self, mock_externalcodebase):
        mock_externalcodebase._working_copy = None
        with pytest.raises(FileNotFoundError):
            mock_externalcodebase.configure()

    def test_configure_logs_if_already_configured(self, mock_externalcodebase, caplog):
        mock_externalcodebase._working_copy = mock.Mock(
            spec=external_codebase.StagedRepository
        )
        with mock.patch.object(
            type(mock_externalcodebase), "is_configured", new_callable=mock.PropertyMock
        ) as prop:
            prop.return_value = True
            caplog.set_level(logging.INFO, logger=mock_externalcodebase.log.name)
            mock_externalcodebase.configure()

        assert "correctly configured" in caplog.text

    def test_configure_calls__configure_if_not_configured(self, mock_externalcodebase):
        mock_externalcodebase._working_copy = mock.Mock(
            spec=external_codebase.StagedRepository
        )
        with mock.patch.object(
            type(mock_externalcodebase), "is_configured", new_callable=mock.PropertyMock
        ) as prop:
            prop.return_value = False
            with mock.patch.object(mock_externalcodebase, "_configure") as do_configure:
                mock_externalcodebase.configure()
        do_configure.assert_called_once()


class TestSetup:
    def test_setup_calls_get_and_configure(self, mock_externalcodebase, tmp_path):
        with (
            mock.patch.object(mock_externalcodebase, "get") as fake_get,
            mock.patch.object(mock_externalcodebase, "configure") as fake_conf,
        ):
            mock_externalcodebase.setup(tmp_path)
        fake_get.assert_called_once_with(tmp_path)
        fake_conf.assert_called_once()
