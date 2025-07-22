#include <stdio.h>
#include <netcdf.h>

/* Handle errors by printing an error message and exiting with a
 * non-zero status. */
#define ERR(e) {printf("Error: %s\n", nc_strerror(e)); return 2;}

int main() {
  int ncid, varid, retval;

  /* Create the file. */
  if ((retval = nc_create("simple.nc", NC_CLOBBER, &ncid)))
    ERR(retval);

  /* Define a variable. */
  if ((retval = nc_def_var(ncid, "data", NC_INT, 0, NULL, &varid)))
    ERR(retval);

  /* End define mode. */
  if ((retval = nc_enddef(ncid)))
    ERR(retval);

  /* Close the file. */
  if ((retval = nc_close(ncid)))
    ERR(retval);

  printf("*** Successfully created a simple NetCDF file named simple.nc\n");
  return 0;
}
