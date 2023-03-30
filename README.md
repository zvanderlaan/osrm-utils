## Overview

The **osrmutils** package contains reusable Python functions that are useful for making queries to an OSRM server.  Note that a valid OSRM server must be set up separately (see https://github.com/Project-OSRM/osrm-backend for details) and the queries are based on the official OSRM API documentation: http://project-osrm.org/

## Typical Install

The **osrmutils** package is intended to be used frequently in other repos and can be installed via pip. It is recommended to install this package in a conda or virtual environment, as shown below usuing the provided environment.yml file.

```
# create conda development environment using environment.yml
$ conda env create --name myenv -f environment.yml
$ conda activate myenv
```

The easiest way to install this package is to "pip install" via Github.  This repo uses tags to indicate the software version (using standard semantic versioning X.X.X), which mean you can pip install a specific version of the package.  For example, to install **osrmutils** tagged "v0.1.0": 

```(myenv)$ python -m pip install git+ssh://git@github.com/zvanderlaan/osrm-utils.git@v0.1.0#egg=osrmutils```

Alternatively you could clone the repo run pip install locally: 

``` 
    (myenv)$ git clone git@github.com:cattworks-zv/reid-utils.git
    (myenv)$ cd reid-utils
    (myenv)$ python -m pip install .
```

## Sample Usage

```
osrm_server = 'http://router.project-osrm.org'
lats = [38.99755, 38.9998385183186, 39.00617]  
longs = [-77.027213, -77.03219, -77.03883]
timestamps = [1680088000, 1680088020, 1680088045]
max_querysize = 100

# Returns two pandas dataframes, one containing snapped tracepoints (df_tp) and the other with routing details (df_rte)
df_tp, df_rte = osrmutils.mapmatch_custom(osrm_server, lats, longs, timestamps, max_matching_size=max_querysize)
```

