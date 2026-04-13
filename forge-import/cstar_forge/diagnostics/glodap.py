import os
from glob import glob
from subprocess import check_call
import urllib

import numpy as np
import xarray as xr

try:
    cache_dir = os.environ['TMPDIR']
except:
    cache_dir = os.environ['HOME']

known_products = [
    "GLODAPv2.2016b_MappedClimatologies",
]

depth_bnds = xr.DataArray(
    np.array(
        [
            [-5.0, 5.0],
            [5.0, 15.0],
            [15.0, 25.0],
            [25.0, 40.0],
            [40.0, 62.5],
            [62.5, 87.5],
            [87.5, 112.5],
            [112.5, 137.5],
            [137.5, 175.0],
            [175.0, 225.0],
            [225.0, 275.0],
            [275.0, 350.0],
            [350.0, 450.0],
            [450.0, 550.0],
            [550.0, 650.0],
            [650.0, 750.0],
            [750.0, 850.0],
            [850.0, 950.0],
            [950.0, 1050.0],
            [1050.0, 1150.0],
            [1150.0, 1250.0],
            [1250.0, 1350.0],
            [1350.0, 1450.0],
            [1450.0, 1625.0],
            [1625.0, 1875.0],
            [1875.0, 2250.0],
            [2250.0, 2750.0],
            [2750.0, 3250.0],
            [3250.0, 3750.0],
            [3750.0, 4250.0],
            [4250.0, 4750.0],
            [4750.0, 5250.0],
            [5250.0, 5750.0],
        ]
    ),
    dims=("depth", "bnds"),
)


def _ensure_datafiles(product_name="GLODAPv2.2016b_MappedClimatologies"):
    """
    get data files from website and return dictionary

    product_name='GLODAPv2.2016b_MappedClimatologies'
    Variables returned = 'Cant', 'NO3', 'OmegaA', 'OmegaC', 'PI_TCO2', 'PO4',
                          'TAlk', 'TCO2', 'oxygen', 'pHts25p0', 'pHtsinsitutp',
                          'salinity', 'silicate', 'temperature',

    Alternative to default:
    product_name='GLODAPv2_Mapped_Climatologies'
    Variables returned = 'OmegaAinsitu',  'OmegaCinsitu',  'nitrate',  'oxygen',
                          'pHts25p0',  'pHtsinsitu',  'phosphate',  'salinity',
                          'silicate',  'talk',  'tco2',  'theta',
    """

    url = "https://www.nodc.noaa.gov/archive/arc0107/0162565/2.2/data/0-data/mapped"

    filename = (
        "GLODAPv2_Mapped_Climatology.tar.gz"
        if product_name == "GLODAPv2_Mapped_Climatologies"
        else f"{product_name}.tar.gz"
    )

    files = sorted(glob(f"{cache_dir}/{product_name}/*.nc"))
    if not files:
        os.makedirs(cache_dir, exist_ok=True)
        local_file = f"{cache_dir}/{filename}"
        urllib.request.urlretrieve(f"{url}/{filename}", local_file)
        check_call(["gunzip", local_file])
        check_call(["tar", "-xvf", local_file.replace(".gz", ""), "-C", cache_dir])
        files = sorted(glob(f"{cache_dir}/{product_name}/*.nc"))

    return {f.split(".")[-2]: f for f in files}


def open_glodap(product="GLODAPv2.2016b_MappedClimatologies"):
    """return GLODAP dataset"""
    assert product in known_products

    obs_files = _ensure_datafiles(product)
    ds_list = []
    for varname, file_in in obs_files.items():
        ds = xr.open_dataset(file_in)
        depth = "Depth" if "Depth" in ds else "depth"
        ds_list.append(ds[[depth, varname]])
    ds = xr.merge(ds_list)
    ds = ds.rename({"Depth": "depth"})
    ds = ds.rename({"depth_surface": "depth"}).set_coords("depth")
    ds = ds.rename(
        {
            "TAlk": "ALK",
            "TCO2": "DIC",
            "oxygen": "O2",
            "silicate": "SiO3",
            "temperature": "TEMP",
            "salinity": "SALT",
        }
    )
    for v in ds.data_vars:
        if 'units' in ds[v].attrs and ds[v].attrs['units'] == 'micro-mol kg-1':
            ds[v].attrs['units'] = 'µmol kg$^{-1}$'

    ds.DIC.attrs['long_name'] = 'DIC'
    ds.ALK.attrs['long_name'] = 'Alkalinity'

    ds["area"] = compute_grid_area(ds)
    ds["depth_bnds"] = depth_bnds
    ds["dz"] = depth_bnds.diff("bnds").squeeze()
    if 'Comment' in ds.attrs:
        del ds.attrs['Comment']
    return ds


def lat_weights_regular_grid(lat):
    """
    Generate latitude weights for equally spaced (regular) global grids.
    Weights are computed as sin(lat+dlat/2)-sin(lat-dlat/2) and sum to 2.0.
    """
    dlat = np.abs(np.diff(lat))
    np.testing.assert_almost_equal(dlat, dlat[0])
    w = np.abs(np.sin(np.radians(lat + dlat[0] / 2.0)) - np.sin(np.radians(lat - dlat[0] / 2.0)))

    if np.abs(lat[0]) > 89.9999:
        w[0] = np.abs(1.0 - np.sin(np.radians(np.pi / 2 - dlat[0])))

    if np.abs(lat[-1]) > 89.9999:
        w[-1] = np.abs(1.0 - np.sin(np.radians(np.pi / 2 - dlat[0])))

    return w


def compute_grid_area(ds, check_total=True):
    """Compute the area of grid cells.

    Parameters
    ----------

    ds : xarray.Dataset
      Input dataset with latitude and longitude fields

    check_total : Boolean, optional
      Test that total area is equal to area of the sphere.

    Returns
    -------

    area : xarray.DataArray
       DataArray with area field.

    """

    radius_earth = 6.37122e6  # m, radius of Earth
    area_earth = 4.0 * np.pi * radius_earth ** 2  # area of earth [m^2]e

    lon_name = "lon"
    lat_name = "lat"

    weights = lat_weights_regular_grid(ds[lat_name])
    area = weights + 0.0 * ds[lon_name]  # add 'lon' dimension
    area = (area_earth / area.sum(dim=(lat_name, lon_name))) * area

    if check_total:
        np.testing.assert_approx_equal(np.sum(area), area_earth)

    return xr.DataArray(
        area, dims=(lat_name, lon_name), attrs={"units": "m^2", "long_name": "area"}
    )
