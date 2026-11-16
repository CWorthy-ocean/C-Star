Forge accepts CONUS404, the 4 km North American reanalysis, as a physics surface forcing source (``source: {name: CONUS404}``), streamed from its store on Amazon S3.
- A CONUS404 item with ``correct_radiation: true``, ``climatology: true`` or ``regrid_method: scipy`` is refused when the blueprint loads.
- Used alone on a grid that reaches past CONUS404's footprint, Forge stops before writing the surface forcing and asks for a ``blend``.
