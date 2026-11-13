"""The ``compress`` application: compress the data files of a finished run in place.

`detect` groups a run's data files into file types from their headers,
`blueprint` holds the per-type settings (the ``compress`` blueprint), `run`
applies them to a run, and `app` registers the application.
"""

from cstar.applications.compress import app as app
