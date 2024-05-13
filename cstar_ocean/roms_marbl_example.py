import os
import pooch
import numpy as np
import xarray as xr
from core import InitialConditions

runpath=os.getcwd()
#### INITIAL CONDITIONS
# Example usage
ICP = pooch.create(
    path=runpath,
    base_url='https://github.com/dafyddstephenson/roms_marbl_example/blob/main/INPUT/',
    #registry={'MARBL_rst.20120103120000.nc':'fc3bbd039256edc89c898efda0eebc5c53773995598d59310bc6d57f454a6ddd'}
    registry={'MARBL_rst.20120103120000.nc':None}
    )

# FIXME: For some reason Pooch throws an error saying the expected hash does not match, showing a different actual hash each time?

ic = InitialConditions(
#    source=['htrtps://github.com/dafyddstephenson/roms_marbl_example/blob/main/INPUT/roms_ini_MARBL.nc',],
    source=ICP,
    grid='roms_grd.nc',
    times=xr.cftime_range(start="2012-01-03 11:59:24",end="2012-01-03 12:00:00",freq="36S"),
    timesteps=np.array([2398,2399],dtype=int)
    )

BCP = pooch.create(
    path=runpath,
    base_url='https://github.com/dafyddstephenson/roms_marbl_example/blob/main/INPUT/',
    #registry={'roms_bry_2012.nc'     :'c3b0e14aae6dd5a0d54703fa04cf95960c1970e732c0a230427bf8b0fbbd8bf1',
    #          'roms_bry_bgc_MARBL.nc':'897a8df8ed45841a98b3906f2dd07750decc5c2b50095ba648a855c869c7d3ee'}
    # FIXME : see above
    registry={'roms_bry_2012.nc'      : None,
              'roms_bry_bgc_MARBL.nc' : None}

#TODO: bgc and physics BCs are on different time arrays - account for this possibly with multiple BC objects?
    # Will need some attrs to distinguish between them (variables?) and some logic for the blueprint needing multiple BCs
    # Think with forcing every variable can have its own time array so this will become a bigger issue

    
#bc = BoundaryConditions(
#    source=BCP,
#    grid='roms_grd.nc',
#    times


############################## scratch space below

print(ic.is_restart)  # Should print True if timestep > 0
#ic.get("/desired/path/for/initial_conditions.nc")


# Define the components and input_files for your blueprint
components = ["ROMS", "MARBL"]
input_files = ["config.ini", "data.nc"]
grid = 'roms marbl grid object'
inputdata = ['initial conditions','boundary conditions','forcing']
# Create the blueprint
#roms_marbl_blueprint = Blueprint("roms_marbl_example", components, grid, inputdata)


# Paths where instances are managed
path_all_instances = "/path/to/instances"
path_scratch = "/path/to/scratch"

# Create an instance from the blueprint
#instance = roms_marbl_blueprint.create_instance("instance_001", '.')
