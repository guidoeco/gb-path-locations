#!/bin/sh
# Convert xlsx spreadsheet with EASTING and NORTHING data to GeoJSON

FILEPATH=$1
FILENAME=$(basename ${FILEPATH})
FILESTUB=$(echo $FILENAME | sed 's/\..*$//; s/ /_/g')

echo ${FILENAME}

EASTING=$2
NORTHING=$3
TAB=$4

EASTING=${EASTING:-"EASTING"}
NORTHING=${NORTHING:-"NORTHING"}
TAB=${TAB:-TIPLOC}

echo ${EASTING} ${NORTHING} ${TAB}

cat > /tmp/convert-to-gps.vrt.$$ <<EOF
<OGRVRTDataSource>
 <OGRVRTWarpedLayer>
  <OGRVRTLayer name="${TAB}">
   <SrcDataSource>${FILEPATH}</SrcDataSource>
   <GeometryType>wkbPoint</GeometryType>
   <LayerSRS>EPSG:27700</LayerSRS>
   <GeometryField name="output_layer" encoding="PointFromColumns" x="${EASTING}" y="${NORTHING}"/>
  </OGRVRTLayer>
  <TargetSRS>EPSG:4326</TargetSRS>
 </OGRVRTWarpedLayer>
</OGRVRTDataSource>
EOF

ogr2ogr -f GeoJSON -lco GEOMETRY=AS_XY "${FILESTUB}".json /tmp/convert-to-gps.vrt.$$

rm /tmp/convert-to-gps.vrt.$$
