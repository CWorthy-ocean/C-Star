```{include} disclaimer.md
```

# Terminology

*Below is a glossary of key terms.*

```{glossary}
Simulation
  The primary object of `C-Star`. It contains all the necessary information for a user to run a reproducible Earth system simulation. Specific subclasses are applied to different simulation systems. Currently C-Star exclusively supports UCLA-ROMS via the ROMSSimulation subclass.

blueprint
  An on-disk representation of a {term}`Somulation <Simulation>`, stored in a `yaml` file. A `Simulation` can be exported to, or created from, a blueprint file. 

External Codebase
  An object that describes a non-python dependency such as the source code of an Earth system model. It does not contain any information relative to a specific instance of this codebase, such as a specific model simulation. Typically it will simply point (via the `source_repo` attribute) to the repository in which a development team hosts the codebase, and accepts a `checkout_target` parameter instructing C-Star on which version of the code to use.

Additional Code
  An object that describes code associated with the {term}`base model <Base Model>`, this time containing code that is necessary to run the base model in a particular configuration of interest. This may include runtime namelist files specifying parameter values and paths to input data, or compile-time option files or source-code modifications. This is effectively code that would not normally be included with a model, but may be required in order to run it.

Input Dataset
  An object that describes any non-plaintext file needed to run our {term}`base model <Base Model>` in a particular configuration. These are typically netCDF files describing the domain, initial conditions, surface and boundary forcing necessary for a particular simulation using the base model.

Discretization
  An object that contains any information related to the discretization of a {term}`simulation <Simulation>`, such as time step and CPU distribution.
```






