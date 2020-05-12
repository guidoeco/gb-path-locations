#!/bin/sh

export PATH=./bin:${PATH}

for i in shape-file output/archive
do
    if [ ! -d ${i} ]; then
        mkdir -p ${i}
    fi
done

# FOI reference
echo process FOI
if [ ! -s TIPLOC_Eastings_and_Northings.json ]; then
    process-FOI.sh
fi

# NaPTAN reference
echo process NaPTAN
if [ ! -s NaPTAN-All.jsonl ]; then
    process-naptan.py
fi

# Update and filter OSM for rail data
echo update OSM
if [ ! -s great-britain-rail-all.osm ]; then
    update-OSM.sh
fi

# Process OSM data
echo process OSM
if [ ! -s OSM-All.jsonl ]; then
    process-osm.py
fi

# Get locations
echo process BPLAN
if [ ! -f Geography-LOC.json ]; then
    process-BPLAN.sh
fi

echo process locations
wtt-map2.py
