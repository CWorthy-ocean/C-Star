import os
import pooch
import numpy as np
import xarray as xr
#from core import *
import cstar_ocean as cstar

runpath=os.getcwd()+'/roms_marbl_example/'
extdir=os.getcwd()+'/externals/'
inputpath=runpath+'INPUT/'
rme_input_base_url='https://github.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/raw/main/INPUT/';

#### INITIAL CONDITIONS
ICP = pooch.create(
    path=inputpath,
    base_url=rme_input_base_url,
    registry={'MARBL_rst.20120103120000.nc':'fc3bbd039256edc89c898efda0eebc5c53773995598d59310bc6d57f454a6ddd'}
    )

ic = cstar.InitialConditions(
    source=ICP,
    grid='roms_grd.nc')
#    times=xr.cftime_range(start="2012-01-03 11:59:24",end="2012-01-03 12:00:00",freq="36S"),
#    timesteps=np.array([2398,2399],dtype=int)


#### BOUNDARY CONDITIONS
BCP = pooch.create(
    path=inputpath,
    base_url=rme_input_base_url,
    registry={'roms_bry_2012.nc'     :'c3b0e14aae6dd5a0d54703fa04cf95960c1970e732c0a230427bf8b0fbbd8bf1',
              'roms_bry_bgc_MARBL.nc':'897a8df8ed45841a98b3906f2dd07750decc5c2b50095ba648a855c869c7d3ee'}
    )
bc = cstar.BoundaryConditions(
    source=BCP,
    grid='roms_grd.nc')#,

#### SURFACE FORCING
SFP = pooch.create(
    path=inputpath,
    base_url=rme_input_base_url,
    registry={'roms_frc.201201.nc':'923049a9c2ab9ce77fa4a0211585e6848a12e87bf237e7aa310f693c3ac6abfa',
              'roms_frc.201202.nc':'5a5d99cdfaacdcda7b531916f6af0f7cef4aea595ea634dac809226ea2a8a4fe',
              'roms_frc.201203.nc':'8251bd08d435444da7c38fe11eba082365ee7b68453b6dc61460ddcb72c07671',
              'roms_frc.201204.nc':'0b62ab974bd718af1d421a715dc2b0968f65ec99856513f2ee988d996ff3d059',
              'roms_frc.201205.nc':'b82797f91c0741245e58b90f787c9597f342faa49c45ebb27e2df964006d6df5',
              'roms_frc.201206.nc':'8cf6f2413ae45dddc1680a19aea0d40a04def82366d626a7fe33dfe5eef7ea7f',
              'roms_frc.201207.nc':'4ec7284f2bdc222b961483af5f6a01ecd6feea5236bb57d2101171f38ea8653b',
              'roms_frc.201208.nc':'4eec008592337e0da87c2fac8c41a1400cc7067fcdc146a665db5b3a74213828',
              'roms_frc.201209.nc':'feb5718c45c4d0874919367fbadfca6784dfddaa2b193ef767a37d92a554eed4',
              'roms_frc.201210.nc':'74538789218a2815c5a5532756e1282958d22026da7513ced0131febfce1012b',
              'roms_frc.201211.nc':'c79d4b2a9d1c41f9c603454c2b023995a6c3ea78c01d17b7428257c3c66f8750',
              'roms_frc.201212.nc':'477d1c0f2abcb0d5227594777521ce30d30c2376f5a8b2f08c25e25a77fd1fa5'}
    )

sf = cstar.SurfaceForcing(
    source=SFP,
    grid='roms_grd.nc')

blueprint_src_repo='https://github.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example.git';
blueprint_src_hash='f3c3541';

mc = cstar.ModelCode(source_repo=blueprint_src_repo,
               checkout_target=blueprint_src_hash,\
               target_path=runpath,
               retrieval_commands=[\
                    "git clone --no-checkout --filter=blob:none {self.source_repo} {self.target_path}",\
                    "git -C {self.target_path} sparse-checkout init --cone",\
                    "git -C {self.target_path} sparse-checkout set code",\
                    "git -C {self.target_path} fetch origin",\
                    "git -C {self.target_path} checkout {self.checkout_target}"])
                                   
#####
roms_component=cstar.Component("ROMS" ,source_repo="https://github.com/dafyddstephenson/ucla-roms.git",checkout_target="7fd149280d9c1f30882fade2a3897247f8cc4bbd")
marbl_component=cstar.Component("MARBL",checkout_target="marbl0.45.0")

roms_marbl_blueprint=cstar.Blueprint(
    name='roms_marbl_example',
    components=[roms_component,marbl_component],
    grid='roms_grd.nc',
    initial_conditions=ic,
    boundary_conditions=bc,
    surface_forcing=sf,
    model_code=mc)
    
roms_marbl_instance=roms_marbl_blueprint.create_instance(
    instance_name='roms_marbl_instance',
    path='/Users/dafyddstephenson/Code/C-Star/cstar_ocean/cstar_ocean')

#roms_marbl_instance.persist()


############################## scratch space below

#print(ic.is_restart)  # Should print True if timestep > 0
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

