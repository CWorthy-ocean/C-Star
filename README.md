# C-Star
Computational Systems for Tracking Ocean Carbon

## Setup
### First time setup
- Clone this repository and run (from the command line) the script `setup_cstar` in the `setup_cstar` directory, providing your system as an argument (e.g. `./setup_cstar osx_arm64_gnu`). For a list of supported systems, run `setup_cstar --help`.
- The setup script will obtain and compile any external code, and also make an environment on your machine to use when running C-Star in future. To activate this environment, run the command `cstar_env` after completing the setup (note you will have to restart your shell).

### Obtaining C-Star configurations
- With the C-Star environment active (see above), use the command `cstar_get_config` to obtain and compile a C-Star configuration. For a list of available configurations, run `cstar_get_config --help`.
- C-Star configurations are saved to `${CSTAR_ROOT}/configurations`
- For help with a specific configuration, see its README file (e.g. `${CSTAR_ROOT}/configurations/roms_marbl_example/README.md`)


## See Also
- [ROMS-tools](https://github.com/CWorthy-ocean/roms-tools)
