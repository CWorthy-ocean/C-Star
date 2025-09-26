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
