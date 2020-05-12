#!/bin/sh

for i in data shape-file
do
    if [ ! -d ${i} ]; then
        mkdir ${i}
    fi
done

URL='https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural'

FILE=ne_110m_admin_0_countries_lakes.zip

if [ ! -s shape-file/${FILE} ]; then
    curl -L "${URL}/${FILE}" -o shape-file/${FILE}
    (cd shape-file; unzip ${FILE})
fi

URL="https://wiki.openraildata.com/index.php?title=BPLAN_Geography_Data"

if [ ! -f full-file-list.txtx ]; then
    curl -L ${URL} | htmltojson.py --depth 9 --stdout | jq -rc 'select(.td?) | .td[2].a.href' > output/full-file-list.txt
fi

FILEPATH=$(tail -1 output/full-file-list.txt | sed 's/^\/*//')

URL="https://wiki.openraildata.com"
if [ ! -f Geography-LOC.json ]; then
    echo "${URL}${FILEPATH}"
    curl -L -o output/Geography-full.gz "${URL}/${FILEPATH}"
    cp data/Geography-LOC-header.tsv output/Geography-LOC.tsv
    gzip -cd output/Geography-full.gz | egrep ^LOC >> output/Geography-LOC.tsv
    easting2GeoJSON.sh output/Geography-LOC.tsv Easting Northing Geography-LOC
fi
