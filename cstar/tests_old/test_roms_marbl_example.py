# mypy: ignore-errors

import cstar
from unittest.mock import patch

##
from pathlib import Path
import shutil


## Delete output of any previous run of this script
for oldfiles in [
    "local_input_files/",
    "local_additional_code/",
    "roms_marbl_example_case/",
    "roms_marbl_local_case/",
    "test_blueprint.yaml",
    "modified_test_blueprint.yaml",
    "test_blueprint_local.yaml",
]:
    oldpath = Path(oldfiles)
    if oldpath.exists():
        if oldpath.is_dir():
            shutil.rmtree(oldpath)
        else:
            oldpath.unlink()

## First step makes/runs Case using URLs to point to input datasets
roms_marbl_remote_case = cstar.Case.from_blueprint(
    blueprint=(cstar.base.environment._CSTAR_ROOT)
    + "/../examples/cstar_blueprint_roms_marbl_example.yaml",
    caseroot="roms_marbl_example_case/",
    start_date="20120103 12:00:00",
    end_date="20120103 12:30:00",
)

# patch will automatically respond "y" to any call for input
with patch("builtins.input", return_value="y"):
    roms_marbl_remote_case.setup()

roms_marbl_remote_case.persist("test_blueprint.yaml")
roms_marbl_remote_case.build()
roms_marbl_remote_case.pre_run()
roms_marbl_remote_case.run()
roms_marbl_remote_case.post_run()

print("Test complete with remote input dataset files")

## Second step modifies the yaml created above to use available local input datasets and additional code
rmr_dir = Path(roms_marbl_remote_case.caseroot)
# Move the input datasets to a new location
shutil.move(rmr_dir / "input_datasets/ROMS", "local_input_files")

# Move the additional code to a new location

lac_dir = Path.cwd() / "local_additional_code/"
lac_dir.mkdir(parents=True, exist_ok=True)
shutil.copytree(
    rmr_dir / "additional_code/ROMS/namelists",
    lac_dir / "additional_code/ROMS/namelists",
)
shutil.copytree(
    rmr_dir / "additional_code/ROMS/source_mods",
    lac_dir / "additional_code/ROMS/source_mods",
)

# Modify the blueprint file to point to local paths whenever we have the files:
with open("test_blueprint.yaml") as f:
    test_blueprint = f.readlines()

for i, line in enumerate(test_blueprint):
    id_url_prefix = (
        "https://github.com/CWorthy-ocean/input_datasets_roms_marbl_example/raw/main/"
    )
    ac_url = "https://github.com/dafyddstephenson/roms_marbl_example.git"
    if id_url_prefix in line:
        fileurl = line.split()[-1]  # Just isolate URL from e.g. source: URL
        filepath = Path.cwd() / "local_input_files" / Path(fileurl).name
        if filepath.exists():
            test_blueprint[i] = line.replace(fileurl, str(filepath))
    elif ac_url in line:
        test_blueprint[i] = line.replace(str(ac_url), str(lac_dir))

with open("modified_test_blueprint.yaml", "w") as f:
    f.writelines(test_blueprint)

## Third step creates and runs Case with local input datasets and additional code

roms_marbl_local_case = cstar.Case.from_blueprint(
    blueprint="modified_test_blueprint.yaml",
    caseroot="roms_marbl_local_case",
    start_date="20120103 12:00:00",
    end_date="20120103 12:30:00",
)

# patch will automatically respond "y" to any call for input
with patch("builtins.input", return_value="y"):
    roms_marbl_local_case.setup()
    roms_marbl_local_case.persist("test_blueprint_local.yaml")
    roms_marbl_local_case.build()
    roms_marbl_local_case.pre_run()
    roms_marbl_local_case.run()
    roms_marbl_local_case.post_run()

print("Test complete with local input dataset files")
