#!/usr/bin/env python3

import sys
import json
import argparse
from geojson import FeatureCollection
import pandas as pd
import geopandas as gp
from osgeo import gdal, ogr

pd.set_option('display.max_columns', None)

ARGPARSER = argparse.ArgumentParser(description='Reformats an OSM format file to a summary jsonl format')

ARGPARSER.add_argument('inputfile', type=str, nargs='?', help='name of file to convert', default='great-britain-rail-all.osm')

ARGS = ARGPARSER.parse_args()
FILENAME = ARGS.inputfile

def trim_f(this_float):
    return round(float(this_float), 6)

def get_locationstr(p):
    if p.type == 'Point':
        v = p.coords[0]
        return '{},{}'.format(str(trim_f(v[1])), str(trim_f(v[0])))
    return ['{},{}'.format(str(trim_f(v[1])), str(trim_f(v[0])))
            for v in p.coords]

def get_pairs(this_list):
    r = [trim_f(i) for i in this_list]
    return list(zip(r[1::2], r[::2]))

def get_tiploc(this_df):
    this_series = pd.Series(index=this_df.index, dtype='object', name='TIPLOC')
    idx1 = this_series.notna()
    if 'ref:tiploc' in this_df.columns:
        idx1 = this_df['ref:tiploc'].apply(lambda v: v is not None)
        this_series[idx1] = this_df.loc[idx1, 'ref:tiploc']
    if 'naptan:AtcoCode' in this_df.columns:
        idx2 = this_df['naptan:AtcoCode'].apply(lambda v: v is not None)
        this_series[idx2 & ~idx1] = this_df.loc[idx2 & ~idx1, 'naptan:AtcoCode'].str[4:]
    this_series[this_series.isna()] = None
    return this_series

def clean_json(this_df):
    return [{k: v for k, v in m.items() if v != ''} for m in this_df]

def get_geoframe(osm_ds):
    data = []
    for n in range(osm_ds.GetLayerCount()):
        layer = osm_ds.GetLayer(n)
        layer_name = layer.GetName()
        for feature in layer:
            json_data = json.loads(feature.ExportToJson())
            json_data['properties'].update({'layer': layer_name})
            for i in [k for k, v in json_data['properties'].items() if v is None]:
                json_data['properties'].pop(i)
            data.append(json_data)
    collection = FeatureCollection(data)
    return gp.GeoDataFrame.from_features(collection['features'])

gdal.SetConfigOption('OSM_CONFIG_FILE', 'data/osmconfig.ini')
OSM_DS = ogr.Open(FILENAME, 0)
if OSM_DS is None:
    raise OSError(2, 'No such file or directory: \'{}\''.format(FILENAME))

GDF = get_geoframe(OSM_DS).fillna('')
GDF['_location_'] = GDF['geometry'].centroid.apply(get_locationstr)
GDF['TIPLOC'] = get_tiploc(GDF)

DATA = clean_json(GDF.drop(columns='geometry').fillna('').to_dict(orient='records'))
with open('OSM-All.jsonl', 'w') as fout:
    fout.write('\n'.join([json.dumps(i) for i in DATA]))
