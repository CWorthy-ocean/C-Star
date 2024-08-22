import cstar
import shutil
from pathlib import Path
from unittest.mock import patch

## Delete output of any previous run of this script
for oldfiles in ["local_input_files/",
                 "roms_marbl_example_case/",
                 "roms_marbl_local_case/",
                 "test_blueprint.yaml",
                 "modified_test_blueprint.yaml",
                 "test_blueprint_local.yaml"]:
    
    oldpath=Path(oldfiles)
    if oldpath.exists():
        if oldpath.is_dir():
            shutil.rmtree(oldpath)
        else:
            oldpath.unlink()
    
## First test uses URLs to point to input datasets
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

## Second test modifies the yaml created above to use available local input datasets

# Move the input datasets to a new location
shutil.move(roms_marbl_remote_case.caseroot+'/input_datasets/ROMS/','local_input_files')

# Modify the blueprint file to point to local paths whenever we have the files:
with open('test_blueprint.yaml') as f:
    test_blueprint=f.readlines()

for i,line in enumerate(test_blueprint):
    url_prefix="https://github.com/CWorthy-ocean/input_datasets_roms_marbl_example/raw/main/"
    if url_prefix in line:
        fileurl=line.split()[-1] # Just isolate URL from e.g. source: URL
        filepath=Path(f"{Path.cwd()}/local_input_files/{Path(fileurl).name}")
        if filepath.exists():
            test_blueprint[i]=line.replace(fileurl,str(filepath))

with open('modified_test_blueprint.yaml', 'w') as f:
    f.writelines(test_blueprint)
##

roms_marbl_local_case = cstar.Case.from_blueprint(
    blueprint="modified_test_blueprint.yaml",
    caseroot="roms_marbl_local_case",
    start_date="20120103 12:00:00",
    end_date="20120103 12:30:00")

# patch will automatically respond "y" to any call for input
with patch("builtins.input", return_value="y"):
    roms_marbl_local_case.setup()
    roms_marbl_local_case.persist("test_blueprint_local.yaml")
    roms_marbl_local_case.build()
    roms_marbl_local_case.pre_run()
    roms_marbl_local_case.run()
    roms_marbl_local_case.post_run()

print("Test complete with local input dataset files")
    
