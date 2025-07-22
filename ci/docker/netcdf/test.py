from netCDF4 import Dataset
import numpy as np

# Create a new NetCDF file
with Dataset('test.nc', 'w', format='NETCDF4') as ncfile:
    # Create dimensions
    ncfile.createDimension('x', 10)
    ncfile.createDimension('y', 5)
    # Create variables
    data_var = ncfile.createVariable('data', 'i4', ('x', 'y'))

    # Write data to the variable
    data_out = np.arange(50).reshape(10, 5)
    data_var[:] = data_out

    # Add some metadata (attributes)
    ncfile.title = "My test file"
    data_var.units = "unknown"

print("*** Successfully created a simple NetCDF file named test.nc")

