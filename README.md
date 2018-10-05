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
