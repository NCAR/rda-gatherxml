# rda-gatherxml
Codes to support the RDA metadata gathering utilities.

- \_gatherxml.cpp

   code that runs when the command _gatherxml_ is executed
   
- \_ascii2xml.cpp

   code that scans proprietary ASCII data files
   
- \_bufr2xml.cpp

   code that scans WMO BUFR data files
   
- \_grid2xml.cpp

   code that scans gridded data files that are not in netCDF (see \_nc2xml.cpp) or HDF (see \_hdf2xml.cpp) (e.g. WMO GRIB1, GRIB2, DSS binary formats, other proprietary formats)

- \_hdf2xml.cpp

   code that scans HDF4 and HDF5 data files

- \_nc2xml.cpp

   code that scans netCDF-3 data files (for netCDF-4 wrapped in HDF5, use \_hdf2xml.cpp)
