#!/bin/sh

if [ ! -d lookup ]; then
    mkdir lookup
fi

for LOCATION in $(cat locations.txt)
do
    FILENAME=$(echo ${LOCATION} | sed 's/%20/_/g')
    curl --silent 'http://joseph:8983/solr/StopArea/select?omitHeader=true&rows=99&q='${LOCATION} | jq -c '.response.docs[] | {Name, StopAreaCode} + {location: ("https://www.openstreetmap.org/#map=19/" + (._location_ | split(",") | join("/")))}' | jq -sc '.' | json2tsv | sed 's/^/'${FILENAME}'\t/'
done > lookup-report.tsv
