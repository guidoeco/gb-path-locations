#!/usr/bin/env python3

import json
import numpy as np
import pandas as pd
import sys
from app.solr import get_facet,  get_query

def get_counts(name, field):
    r = get_facet(name, facet_fl=field)
    return dict(zip(r[::2], r[1::2]))

def trim_f(this_float):
    return round(float(this_float), 5)

def get_counts(name, field):
    r = get_facet(name, facet_fl=field)
    return dict(zip(r[::2], r[1::2]))

STOPTYPES = get_counts('StopPoint', 'StopClassification.StopType')
with open('output/StopTypes.tsv', 'w') as fout:
    fout.write('StopType\t#\n')
    fout.write('\n'.join(['{}\t{}'.format(k,v) for k, v in STOPTYPES.items()]))

FIELDS = {'id': 'id',
          '_location_': '_location_',
          'Status': 'Status',
          'AtcoCode': 'AtcoCode',
          'AdministrativeAreaRef': 'AdministrativeAreaRef',
          'Place.NptgLocalityRef': 'node',
          'StopAreas.StopAreaRef.Status': 'StopAreaStatus',
          'StopAreas.StopAreaRef.value': 'StopAreaRef',
          'Descriptor.CommonName': 'Name',
          'Descriptor.Street': 'Street',
          'StopClassification.StopType': 'StopType',
          'StopClassification.OffStreet.Rail.AnnotatedRailRef.TiplocRef': 'TIPLOC',
          'StopClassification.OffStreet.Rail.AnnotatedRailRef.CrsRef': 'CRS',
          'StopClassification.OffStreet.Rail.AnnotatedRailRef.StationName': 'StationName',
          'PlusbusZones.PlusbusZoneRef.Status': 'PlusBusZoneStatus',
          'PlusbusZones.PlusbusZoneRef.value': 'PlusBusZoneName',
          'Place.MainNptgLocalities.NptgLocalityRef.value': 'PlaceLocatityRefs',
          'Place.Suburb': 'Suburb',
          'Place.Town': 'Town',
          'AlternativeDescriptors.Descriptor.Status': 'AlternativeStatus',
          'AlternativeDescriptors.Descriptor.CommonName': 'AlternativeName',
          'AlternativeDescriptors.Descriptor.Street': 'AlternativeStreet',
          'StopClassification.OffStreet.Rail.AnnotatedRailRef.StationName.value': 'StationStopName',
          'Descriptor.Indicator': 'Platform',
          'StopClassification.OffStreet.Air.AnnotatedAirRef.IataRef': 'IataRef',
          'StopClassification.OffStreet.Air.AnnotatedAirRef.Name': 'AirportName',
          'StopClassification.OffStreet.Ferry.AnnotatedFerryRef.FerryRef': 'FerryRef',
          'StopClassification.OffStreet.Ferry.AnnotatedFerryRef.Name': 'FerryName',
          'NaptanCode': 'NaptanCode',
          'Descriptor.Landmark': 'Landmark',
          'Descriptor.ShortCommonName': 'CommonName'}


POINTS = get_query('StopPoint', 'AtcoCode:9*', fl=','.join(FIELDS.keys()))
#DATA = get_query('StopPoint', 'AtcoCode:9*') #, fl=','.join(FIELDS.keys()))

df1 = pd.DataFrame(POINTS)
df1 = df1.rename(columns=FIELDS).fillna('')
df1['TIPLOC'] = df1['AtcoCode'].str[4:]
df1['type'] = 'Point'

FIELDS = {'id': 'id',
          '_location_': '_location_',
          'AdministrativeAreaRef': 'AdministrativeAreaRef',
          'Name': 'Name',
          'ParentStopAreaRef.Status': 'StopAreaStatus',
          'ParentStopAreaRef.value': 'ParentAtcoCode',
          'Status': 'Status',
          'StopAreaCode': 'AtcoCode',
          'StopAreaType': 'StopAreaType'}

AREAS = get_query('StopArea', 'ParentStopAreaRef.value:9* OR StopAreaCode:9*', fl=','.join(FIELDS.keys()))
df2 = pd.DataFrame(AREAS)
df2 = df2.rename(columns=FIELDS).fillna('')
df2['TIPLOC'] = df2['ParentAtcoCode'].str[4:]
idx1 = df2['TIPLOC'] == ''
df2.loc[idx1, 'TIPLOC'] = df2.loc[idx1, 'AtcoCode'].str[4:]
df2['type'] = 'Area'

def clean_json(this_df):
    return [{k: v for k, v in m.items() if isinstance(v, list) or v != ''} for m in this_df]

DATA = clean_json(df1.append(df2).fillna('').to_dict(orient='records'))
with open('NaPTAN-All.jsonl', 'w') as fout:
    fout.write('\n'.join([json.dumps(i) for i in DATA]))
