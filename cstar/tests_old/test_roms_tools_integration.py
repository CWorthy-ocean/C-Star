import cstar
from unittest.mock import patch

from pathlib import Path
import shutil


################################################################################

## Delete output of any previous run of this script
for oldfiles in ["local_additional_code/",
                 "local_input_files/",                 
                 "roms_tools_local_case",
                 "roms_tools_remote_case",
                 "modified_test_blueprint.yaml",
                 "test_blueprint_local.yaml",
                 "test_rt_blueprint.yaml"]:
    
    oldpath=Path(oldfiles)
    if oldpath.exists():
        if oldpath.is_dir():
            shutil.rmtree(oldpath)
        else:
            oldpath.unlink()



## First step makes/runs Case using URLs to point to input dataset yaml files:

roms_marbl_remote_case = cstar.Case.from_blueprint(
    blueprint=(cstar.base.environment._CSTAR_ROOT)
    + "/../tests/cstar_blueprint_roms_tools_example.yaml",
    caseroot="roms_tools_remote_case/",
    start_date="20120101 12:00:00",
    end_date="20120101 12:30:00",
)

# patch will automatically respond "y" to any call for input
with patch("builtins.input", return_value="y"):
    roms_marbl_remote_case.setup()
    roms_marbl_remote_case.persist("test_rt_blueprint.yaml")
    roms_marbl_remote_case.build()
    roms_marbl_remote_case.pre_run()
    roms_marbl_remote_case.run()
    roms_marbl_remote_case.post_run()

print("Test complete with remote input dataset yaml files")


## Second step modifies the yaml created above to use available local input dataset yamls and additional code
rmr_dir=Path(roms_marbl_remote_case.caseroot)
# Move the input datasets to a new location
local_filepath=Path("local_input_files")
local_filepath.mkdir(exist_ok=True)
for f in (rmr_dir/"input_datasets/ROMS").glob("*.yaml"):
    f.rename(local_filepath/f.name)


# Move the additional code to a new location

lac_dir=Path.cwd()/"local_additional_code/"
lac_dir.mkdir(parents=True,exist_ok=True)
shutil.copytree(rmr_dir/"additional_code/ROMS/namelists" , lac_dir/"additional_code/ROMS/namelists")
shutil.copytree(rmr_dir/"additional_code/ROMS/source_mods" , lac_dir/"additional_code/ROMS/source_mods")

# Modify the blueprint file to point to local paths whenever we have the files:
with open('test_rt_blueprint.yaml') as f:
    test_blueprint=f.readlines()

for i,line in enumerate(test_blueprint):
    id_url_prefix="https://github.com/dafyddstephenson/roms_marbl_example/raw/main/roms_tools_yaml_files/"
    ac_url="https://github.com/dafyddstephenson/roms_marbl_example.git"
    if id_url_prefix in line:
        fileurl=line.split()[-1] # Just isolate URL from e.g. source: URL
        filepath=Path.cwd()/"local_input_files"/Path(fileurl).name
        if filepath.exists():
            test_blueprint[i]=line.replace(fileurl,str(filepath))
    elif ac_url in line:
             test_blueprint[i]=line.replace(str(ac_url),str(lac_dir))

with open('modified_test_blueprint.yaml', 'w') as f:
    f.writelines(test_blueprint)

## Third step creates and runs Case with local input datasets and additional code

roms_marbl_local_case = cstar.Case.from_blueprint(
    blueprint="modified_test_blueprint.yaml",
    caseroot="roms_tools_local_case",
    start_date="20120101 12:00:00",
    end_date="20120101 12:30:00")

# patch will automatically respond "y" to any call for input
with patch("builtins.input", return_value="y"):
    roms_marbl_local_case.setup()
    roms_marbl_local_case.persist("test_blueprint_local.yaml")
    roms_marbl_local_case.build()
    roms_marbl_local_case.pre_run()
    roms_marbl_local_case.run()
    roms_marbl_local_case.post_run()

print("Test complete with local input dataset yaml files")
    
