#!/usr/bin/env python3

import json
import numpy as np
import pandas as pd
import geopandas as gp
import sys
import os
os.environ['SOLRHOST'] = 'joseph'

from app.solr import get_group, get_query, get_facet
pd.set_option('display.max_columns', None)

def get_url(tiploc):
    if LOCATIONS.loc[tiploc].any():
        (latitude, longitude) = LOCATIONS.loc[tiploc]['_location_'].split(',')
        print('https://www.openstreetmap.org/#map=19/{}/{}'.format(latitude, longitude))
    return None

def print_url(tiploc):
    print(get_url(tiploc))

def get_counts(name, field):
    r = get_facet(name, facet_fl=field)
    return dict(zip(r[::2], r[1::2]))

def get_facetkeys(name, field):
    r = get_facet(name, facet_fl=field)
    return r[::2]

def get_found():
    try:
        return LOCATIONS['_location_'].notna().sum()
    except KeyError:
        pass
    return 0

def get_missing():
    return {k: COUNTS[k] for k, v in LOCATIONS.items() if not v}

COUNTS = get_counts('PATH', 'TIPLOC')
LOCATIONS = pd.DataFrame(index=get_facetkeys('PATH', 'TIPLOC'), dtype='object', columns=['type', '_location_'])

LOCATIONS.index.name='TIPLOC'
NAMES = pd.DataFrame(get_query('TR', fl='TIPLOC,TPS_Description')).rename(columns={'TPS_Description': 'Description'}).set_index('TIPLOC')
LOCATIONS = LOCATIONS.join(NAMES)

N, _ = LOCATIONS.shape

print('TIPLOCs: {} of {}'.format(N - get_found(), N))

## Decode GeoJson object:
# {"type": "FeatureCollection",
#  "name": "TIPLOC",
#  "crs": {
#    "type": "name",
#    "properties": {
#      "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
#    }
#  },
#  "features": [...]}

def get_locationstr(p):
    if p.type == 'Point':
        v = p.coords[0]
        return '{},{}'.format(str(trim_f(v[1])), str(trim_f(v[0])))
    return ['{},{}'.format(str(trim_f(v[1])), str(trim_f(v[0])))
            for v in p.coords]

def trim_f(this_float):
    return round(float(this_float), 6)

def match_TIPLOC(idk, this_df):
    this_df = this_df.set_index('TIPLOC')
    this_idk = this_df[this_df.index.isin(idk)].index
    return this_df.loc[this_idk, ['type', 'Description', '_location_']]

COLUMNS = ['type', 'Description', '_location_']

FOI = gp.read_file('TIPLOC_Eastings_and_Northings.json')
FOI['_location_'] = FOI['geometry'].centroid.apply(get_locationstr)
FOI['type'] = 'FOI'

IDX1 = LOCATIONS[LOCATIONS['_location_'].isna()].index
DF1 = match_TIPLOC(IDX1, FOI.rename(columns={'NAME': 'Description'}))
LOCATIONS.loc[DF1.index, COLUMNS] = DF1

print('TIPLOCs: {} of {}'.format(N - get_found(), N))

BPLAN = gp.read_file('Geography-LOC.json')
BPLAN['_location_'] = BPLAN['geometry'].centroid.apply(get_locationstr)
BPLAN = BPLAN.rename(columns={'Location Code': 'TIPLOC', 'Location name': 'Description'})
BPLAN['type'] = 'BPLAN'
COUNTRIES = gp.GeoDataFrame.from_file('shape-file/ne_110m_admin_0_countries_lakes.shp')
GBPOLYGON = COUNTRIES.loc[COUNTRIES['ISO_A2'] == 'GB', 'geometry']
IDX2 = LOCATIONS[LOCATIONS['_location_'].isna()].index
DF2 = match_TIPLOC(IDX2, gp.clip(BPLAN, GBPOLYGON))
LOCATIONS.loc[DF2.index, COLUMNS] = DF2

print('TIPLOCs: {} of {}'.format(N - get_found(), N))

NAPTAN = pd.read_json('NaPTAN-All.jsonl', dtype='object', lines=True)
NAPTAN['type'] = 'NaPTAN'
IDX3 = LOCATIONS[LOCATIONS['_location_'].isna()].index
DF3 = match_TIPLOC(IDX3, NAPTAN.rename(columns={'Name': 'Description'}))
LOCATIONS.loc[DF3.index, COLUMNS] = DF3

print('TIPLOCs: {} of {}'.format(N - get_found(), N))

OSM = pd.read_json('OSM-All.jsonl', dtype='object', lines=True)
OSM = OSM.dropna(subset=['TIPLOC'])
OSM = OSM.drop_duplicates(subset='TIPLOC')
OSM['type'] = 'OSM'

IDX4 = LOCATIONS[LOCATIONS['_location_'].isna()].index
DF4 = match_TIPLOC(IDX4, OSM.rename(columns={'name': 'Description'}))
LOCATIONS.loc[DF4.index, COLUMNS] = DF4

print('TIPLOCs: {} of {}'.format(N - get_found(), N))

MAP = pd.read_csv('data/TIPLOC-map.tsv', dtype='object', sep='\t')
MAP = MAP.dropna(subset=['TIPLOC'])
MAP['type'] = 'NaPTAN_MAP'
MAP = MAP.join(NAPTAN[['AtcoCode', '_location_']].set_index('AtcoCode'), on='StopAreaCode')

IDX5 = LOCATIONS[LOCATIONS['_location_'].isna()].index
DF5 = match_TIPLOC(IDX5, MAP.rename(columns={'Name': 'Description'}))
LOCATIONS.loc[DF5.index, COLUMNS] = DF5

print('TIPLOCs: {} of {}'.format(N - get_found(), N))

WIKIPEDIA = pd.read_csv('data/wikipedia-map.tsv', dtype='object', sep='\t')
WIKIPEDIA = WIKIPEDIA.set_index('TIPLOC')
LOCATIONS.loc[WIKIPEDIA.index, ['type', 'Description', '_location_']] = WIKIPEDIA[['type', 'Description', '_location_']]

print('TIPLOCs: {} of {}'.format(N - get_found(), N))

OVERLAP = pd.read_csv('data/overlap-map.tsv', dtype='object', sep='\t')
OVERLAP = OVERLAP.dropna(subset=['mapped TIPLOC'])
for _, location in OVERLAP.iterrows():
    LOCATIONS.loc[location['TIPLOC'], 'Description'] = location['Description']
    LOCATIONS.loc[location['TIPLOC'], 'type'] = 'overlapA'
    try:
        LOCATIONS.loc[location['TIPLOC'], '_location_'] = LOCATIONS.loc[location['mapped TIPLOC'], '_location_']
    except KeyError:
        LOCATIONS.loc[location['TIPLOC'], 'type'] = 'overlapB'
        LOCATIONS.loc[location['TIPLOC'], '_location_'] = BPLAN.set_index('TIPLOC').loc[location['mapped TIPLOC'], '_location_']

print('TIPLOCs: {} of {}'.format(N - get_found(), N))

IDX0 = LOCATIONS[LOCATIONS['Description'].isna()].index
LOCATIONS.loc[IDX0, 'Description'] = NAMES.loc[IDX0, 'Description']
LOCATIONS.fillna('').reset_index().to_csv('locations-report.tsv', sep='\t', index=False)

def get_transport(tiploc):
    UUIDS = get_query('PATH', search_str='TIPLOC:{}'.format(tiploc), fl='UUID')
    search_str = ' OR '.join(['UUID:{}'.format(i['UUID']) for i in UUIDS[:4]])
    this_type = set([i['Headcode'] for i in get_query('BS', search_str, fl='Headcode', nrows=4) if i])
    if this_type:
        return this_type.pop()
    return '____'

MISSING = pd.DataFrame(index=LOCATIONS[LOCATIONS['_location_'].isna()].index)
try:
    TRANSPORT.empty
except NameError:
    TRANSPORT = pd.Series([get_transport(i) for i in MISSING.index], index=MISSING.index, name='Transport')

q = ' OR '.join(['TIPLOC:{}'.format(i) for i in list(MISSING.index)])
DF5 = pd.DataFrame(get_query('TR', q))
DF5 = DF5.set_index('TIPLOC').drop(columns=['_version_'])
MISSING = DF5.join(pd.Series(COUNTS, name='count')).fillna('')
MISSING = MISSING.join(TRANSPORT).sort_values('count', ascending=False)
MISSING['k'] = pd.Series(MISSING.index, index=MISSING.index).str[:4]
MISSING.to_csv('missing-report.tsv', sep='\t')

print(MISSING['count'].sum() / sum(COUNTS.values()))
