import pooch
import pytest
import shutil
import zipfile
from typing import Callable
from pathlib import Path

from cstar.tests.config import ROMS_TOOLS_DATA_DIRECTORY, CSTAR_TEST_DATA_DIRECTORY
# TEST ALL THESE FIXTURES


@pytest.fixture
def fetch_roms_tools_source_data(request) -> Callable[[str | Path], None]:
    def _fetch_roms_tools_source_data(symlink_path: str | Path) -> None:
        """docstring"""

        if isinstance(symlink_path, str):
            symlink_path = Path(symlink_path)

        # cache_dir = Path(pooch.os_cache("roms_tools_datasets_for_cstar_test_case"))
        pup_test_data = pooch.create(
            path=ROMS_TOOLS_DATA_DIRECTORY,
            base_url="https://github.com/CWorthy-ocean/roms-tools-test-data/raw/main/",
            # The registry specifies the files that can be fetched
            registry={
                "GLORYS_NA_2012.nc": "b862add892f5d6e0d670c8f7fa698f4af5290ac87077ca812a6795e120d0ca8c",
                "ERA5_NA_2012.nc": "d07fa7450869dfd3aec54411777a5f7de3cb3ec21492eec36f4980e220c51757",
                "TPXO_global_test_data.nc": "457bfe87a7b247ec6e04e3c7d3e741ccf223020c41593f8ae33a14f2b5255e60",
                "CESM_BGC_2012.nc": "e374d5df3c1be742d564fd26fd861c2d40af73be50a432c51d258171d5638eb6",
                "CESM_BGC_SURFACE_2012.nc": "3c4d156adca97909d0fac36bf50b99583ab37d8020d7a3e8511e92abf2331b38",
            },
        )
        pup_test_data.fetch("GLORYS_NA_2012.nc")
        pup_test_data.fetch("TPXO_global_test_data.nc")
        pup_test_data.fetch("ERA5_NA_2012.nc")
        pup_test_data.fetch("CESM_BGC_2012.nc")
        pup_test_data.fetch("CESM_BGC_SURFACE_2012.nc")

        symlink_path.symlink_to(ROMS_TOOLS_DATA_DIRECTORY)

        def cleanup():
            if symlink_path.is_symlink():
                print(f"removing {symlink_path}")
                symlink_path.unlink()

        request.addfinalizer(cleanup)

    return _fetch_roms_tools_source_data


@pytest.fixture
def fetch_remote_test_case_data() -> Callable[[], None]:
    def _fetch_remote_test_case_data() -> None:
        """docstring"""

        test_case_repo_url = (
            "https://github.com/CWorthy-ocean/cstar_blueprint_test_case/"
        )
        checkout_target = "d7996aea8d4fd4ca4148b34d2d898f019c90a8ff"

        # Construct the URL of this commit as a zip archive:
        archive_url = f"{test_case_repo_url.rstrip('/')}/archive/{checkout_target}.zip"

        # Create a cache dir with pooch
        # cache_dir = Path(pooch.os_cache("cstar_test_case_data"))

        # Download the zip with pooch
        zip_path = pooch.retrieve(
            url=archive_url,
            known_hash="e291bc79dd5f0ab88f486974075f3defe2076ff3706787131dae7fd0f01358b5",
            fname=f"{checkout_target}.zip",  # Name of the cached file
            path=CSTAR_TEST_DATA_DIRECTORY,  # Set the cache directory (customize as needed)
        )

        # Unzip the files into a subdirectory `extract_dir` of `cache_dir`
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            extract_list = zip_ref.namelist()
            extract_dir = CSTAR_TEST_DATA_DIRECTORY / extract_list[0]
            zip_ref.extractall(CSTAR_TEST_DATA_DIRECTORY)

        # Copy the contents of the subdirectory up one and remove it and the zip file
        shutil.copytree(extract_dir, CSTAR_TEST_DATA_DIRECTORY, dirs_exist_ok=True)
        shutil.rmtree(extract_dir)
        Path(zip_path).unlink()

    return _fetch_remote_test_case_data
