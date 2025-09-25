from unittest import mock

################################################################################


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
        "checkout_target : test_target (corresponding to hash test123) (default)\n"
    )

    # Compare the actual result with the expected result
    assert expected_str in str(fake_externalcodebase)

    # Assuming fake_externalcodebase is an instance of a class
    with mock.patch.object(
        type(fake_externalcodebase),
        "local_config_status",
        new_callable=mock.PropertyMock,
    ) as mock_local_config_status:
        mock_local_config_status.return_value = 0
        assert (
            "(Environment variable TEST_ROOT is present, points to the correct repository remote, and is checked out at the correct hash)"
            in str(fake_externalcodebase)
        )

        mock_local_config_status.return_value = 1
        assert (
            "(Environment variable TEST_ROOT is present but does not point to the correct repository remote [unresolvable])"
            in str(fake_externalcodebase)
        )

        # Change the return value again
        mock_local_config_status.return_value = 2
        assert (
            "(Environment variable TEST_ROOT is present, points to the correct repository remote, but is checked out at the wrong hash)"
            in str(fake_externalcodebase)
        )

        # Final test with return value 3
        mock_local_config_status.return_value = 3
        assert (
            "(Environment variable TEST_ROOT is not present and it is assumed the external codebase is not installed locally)"
            in str(fake_externalcodebase)
        )


def test_codebase_repr(fake_externalcodebase):
    """Test the repr representation of the `ExternalCodeBase` class."""
    result_repr = repr(fake_externalcodebase)
    expected_repr = (
        "FakeExternalCodeBase("
        + "\nsource_repo = 'https://github.com/test/repo.git',"
        + "\ncheckout_target = 'test_target'"
        + "\n)"
        + "\nState: <local_config_status = 3>"
    )

    assert result_repr == expected_repr
    pass
