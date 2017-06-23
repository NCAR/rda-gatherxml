# rda-gatherxml
Codes to support the RDA metadata gathering utilities.

- _gatherxml.cpp

   code that runs when the command "gatherxml" is executed
   
- _ascii2xml.cpp

   code that scans proprietary ASCII data files

- _hdf2xml.cpp

   code that scans HDF4 and HDF5 files

- _nc2xml.cpp

   code that scans netCDF-3 files (for netCDF-4 wrapped in HDF5, use _hdf2xml.cpp)
