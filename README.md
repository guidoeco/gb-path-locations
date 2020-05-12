# GB Path Locations

The British railway timetable works on TIPLOCs but where many of these are isn't always clear. This project creates a `location-

## Overview

This is a set of scripts that uses Open Data sources to guess the locations of most of the TIPLOCs (TIming Point LOCations) used in the railway timetable in Britain. This data consists of:

### Mirrored OpenData reference feeds

This uses these OpenData feeds:

 * Network Rail BPLAN
 * Network Rail CIF timetable data
 * National Public Transport Access Node (NaPTAN)
 * Network Freedom of Information request
 * OpenStreetMap rail data 

### Mapping files
Maps between these dataset is then in the `data` directory in TSV

## Creating the TIPLOC report

Once the dependencies to create the `locations-report.tsv` and `missing-report.tsv` files run the script:

    $ ./run.sh

## Dependencies

These are environment and project dependencies.

### Environment dependencies

These scripts were built and run on a Debian type Linux system (Debian/Ubuntu/Mint) and python3.

To manage the python dependencies this uses miniconda3 installed in the userhome directory. Create a virtual environment called `venv` in the project directory:

    $ ~/miniconda3/bin/virtualenv venv

Then activate the python virtual environment `venv`:

    $ source venv/bin/activate

Then install the required GDAL dependencies and python3 packages:

    $ sudo apt install libgdal-dev ogr libspatialindex-dev 
    $ sudo apt install jq curl osmium-tool osmctools
    $ pip install pygdal=="`gdal-config --version`.*"
    $ pip install geopandas requests geojson xmltodict lxml rtree

### Project Dependencies

This also requires a working Apache Solr docker installation loaded with NaPTAN and a copy of the working timetable. Details about are in the [wagtail](https://github.com/anisotropi4/wagtail/) github repository.

If hostname of a remote Apache Solr server is `solrsvr` set this using the `SOLRHOST` environment variable:

    $ export SOLRHOST=solrsvr
