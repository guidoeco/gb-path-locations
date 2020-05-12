#!/bin/sh

if [ ! -d output/archive ]; then
    mkdir -p ouput/archive
fi

CONTINENT=europe
REGION=great-britain

URL=http://download.geofabrik.de/${CONTINENT}
FILENAME=${REGION}-latest.osm.pbf
if [ ! -s  output/${FILENAME} ]; then
    curl -L -o output/${FILENAME} ${URL}/${FILENAME}
fi


if [ ! -f data/${REGION}.poly ]; then
    echo missing data/${REGION}.poly polygon file
    exit 1
fi

if [ x"$(find output/${REGION}-latest.osm.pbf -mmin -15)" = x ]; then
    osmupdate output/${FILENAME} ${REGION}-update.osm.pbf -B=data/${REGION}.poly --verbose --keep-tempfiles
    mv output/${FILENAME} output/archive
    mv ${REGION}-update.osm.pbf output/${FILENAME}
    rm ${REGION}-rail-all.osm
else
    echo "${FILENAME} is less than 15 minutes old"
fi

if [ ! -s ${REGION}-rail-all.osm ]; then
    osmium tags-filter output/${FILENAME} /*railway* /*train* /rail /*tiploc /*TIPLOC --overwrite -o ${REGION}-rail-all.osm
fi
