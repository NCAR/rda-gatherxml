# src

This directory contains the source code for ``gatherxml`` and it's modules, as well as the source code for the other content metadata utilities in the "gatherxml" suite.

- ``cmd_util.cpp`` is a setuid wrapper that calls an underscore executable as user "rdadata"
   - in the setuid bin directory, the following utilities are sym-linked to ``cmd_util``:
      - ``gathermxl``
      - ``dsgen``
      - ``dcm``
      - ``gsi``
      - ``iinv``
      - ``rcm``
      - ``scm``

- ``_gatherxml.cpp``
   - scans a data file and extracts content metadata by calling the appropriate module, which depends on the data format (-f flag) that is passed to ``gatherxml``
   - needs ``gatherxml.con`` to run
   - the following are the possible modules called by ``gatherxml`` (these should not be executed explicitly; users should only execute ``gatherxml``):
      - ``_ascii2xml.cpp`` - scans proprietary ASCII data files
      - ``_bufr2xml.cpp``- scans WMO BUFR data files
      - ``_fix2xml.cpp`` - scans "cyclone fix" data files (e.g. CXML, HURDAT, etc.)
      - ``_grid2xml.cpp`` - scans gridded data files (e.g. WMO GRIB1, GRIB2, DSS binary formats, other proprietary formats) that are not in netCDF (see ``_nc2xml.cpp``) or HDF (see ``_hdf2xml.cpp``)
      - ``_nc2xml.cpp`` - scans netCDF-3 data files (for netCDF-4 wrapped in HDF5, use ``_hdf2xml.cpp``)
      - ``_obs2xml.cpp`` - scans observations data files (e.g. DSS binary formats, NCDC ASCII formats, etc.) that are not in netCDF (see ``_nc2xml.cpp``) or HDF (see ``_hdf2xml.cpp``)
      - ``_prop2xml.cpp`` - scans data files in proprietary ASCII or Binary formats
      
- ``_dcm.cpp``
   - deletes data file content metadata from the database when data files are deleted
- ``_rcm.cpp``
   - renames data file content metadata when data files are renamed
- ``_scm.cpp``
   - summarizes content metadata and populates the metadata database at both the data file and dataset levels
- ``_dsgen.cpp``
   - regenerates a dataset description page
- ``_gsi.cpp``
   - generates the geospatial-representation images on the detailed metadata pages for observations datasets
- ``_iinv.cpp``
   - inserts detailed information (byte offsets, byte lengths, product, level, parameter, valid date, etc.) into the inventory database
   
   - - - -
- ``libgatherxml``
   - library of routines that are specific to ``gatherxml``
   
