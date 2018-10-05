# rda-gatherxml

``gatherxml`` is an RDA utility to scan data files, extract file content metadata, and populate the metadata database, which supports user-facing data services.

It is also a suite of content metadata utilities, which includes:
   - ``gatherxml`` (described above)
   - ``scm`` (summarize content metadata) for populating the metadata database (automatically called by ``gatherxml``)
   - ``iinv`` (insert inventory) for populating the detailed inventory database (automatically called by ``gatherxml``)
   - ``dsgen`` (dataset generator) for generating the dataset description page (automatically called by ``gatherxml``)
   - ``gsi`` (generate spatial images) for creating the geospatial-representation images on the detailed metadata pages for observations datasets (automatically called by ``gatherxml``)
   - ``dcm`` (delete content metadata) for deleting metadata when data files are removed
   - ``rcm`` (rename content metadata) for renaming content metadata when data files are renamed

- \_gatherxml.cpp

   - code that runs when the command _gatherxml_ is executed - scans a data file and extracts content metadata
   
   - needs _gatherxml.conf_ to run
   
   - the following are called by _gatherxml_, depending on the format indicated by _gatherxml's_ -f flag (these should not be executed explicitly; users should only execute _gatherxml_):
   
      - \_ascii2xml.cpp

         code that scans proprietary ASCII data files
   
      - \_bufr2xml.cpp

         code that scans WMO BUFR data files
      
      - \_fix2xml.cpp
   
         code that scans "cyclone fix" data files (e.g. CXML, HURDAT, etc.)
   
      - \_grid2xml.cpp

         code that scans gridded data files (e.g. WMO GRIB1, GRIB2, DSS binary formats, other proprietary formats) that are not in netCDF (see \_nc2xml.cpp) or HDF (see \_hdf2xml.cpp)

      - \_hdf2xml.cpp

         code that scans HDF4 and HDF5 data files

      - \_nc2xml.cpp

         code that scans netCDF-3 data files (for netCDF-4 wrapped in HDF5, use \_hdf2xml.cpp)
   
      - \_obs2xml.cpp

         code that scans observations data files (e.g. DSS binary formats, NCDC ASCII formats, etc.) that are not in netCDF (see \_nc2xml.cpp) or HDF (see \_hdf2xml.cpp)
      
- \_dcm.cpp

   code that runs when the command _dcm_ is executed - deletes data file content metadata from the database
   
- \_rcm.cpp

   code that runs when the command _rcm_ is executed - renames content metadata when the data file is renamed
   
- \_scm.cpp

   code that runs when the command _scm_ is executed - summarizes content metadata for a file and/or a dataset
