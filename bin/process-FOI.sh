#!/bin/sh

if [ ! -s TIPLOC_Eastings_and_Northings.json ]; then
    URL=https://wiki.openraildata.com/images/8/89
    FILE=TIPLOC_Eastings_and_Northings.xlsx
    curl -L "${URL}/${FILE}.gz" -o output/${FILE}.gz
    gzip -df output/${FILE}.gz
    easting2GeoJSON.sh output/${FILE}
fi
