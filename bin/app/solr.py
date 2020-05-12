"""solr: set of `solr` helper functions based on the `pysolr` package"""
import json
from json.decoder import JSONDecodeError as SolrError
import os
from time import sleep
import requests
from requests.exceptions import HTTPError, ConnectionError

CONNECTIONS = {}
SOLRHOST = os.environ.get('SOLRHOST', 'localhost')

def get_api(name='', api='', api_type='collections', hostname=SOLRHOST):
    """get_api: get method for v2 Solr api"""
    this_url = 'http://{}:8983/api/{}/{}/{}'\
               .format(hostname, api_type, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.get(this_url)
    this_request.raise_for_status()
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    this_data.pop('responseHeader', None)
    return this_data

def post_api(data, name='', api='', api_type='collections', hostname=SOLRHOST):
    """post_api: post method with v2 Solr api"""
    this_url = 'http://{}:8983/api/{}/{}/{}'\
               .format(hostname, api_type, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.post(this_url, data)
    this_request.raise_for_status()
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    this_data.pop('responseHeader', None)
    return this_data

def delete_api(name='', api='', this_type='collections', hostname=SOLRHOST):
    """delete_api: delete method with v2 Solr api"""
    this_url = 'http://{}:8983/api/{}/{}/{}'.format(hostname, this_type, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.delete(this_url)
    this_request.raise_for_status()
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    this_data.pop('responseHeader', None)
    return this_data

def get_solr(name='', api='', response_header=False, hostname=SOLRHOST):
    """get_solr: get method with v1 Solr api"""
    this_url = 'http://{}:8983/solr/{}/{}'.format(hostname, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.get(this_url)
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    if response_header:
        return this_data
    this_data.pop('responseHeader', None)
    return this_data

def post_solr(data, name='', api='', response_header=False, hostname=SOLRHOST):
    """post_solr: post data method with v1 Solr` api"""
    this_url = 'http://{}:8983/solr/{}/{}'.format(hostname, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.post(this_url, data)
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    if response_header:
        return this_data
    this_data.pop('responseHeader', None)
    return this_data

def raw_query(name, search_str='*:*', sort='id asc', nrows=10,
              facet_fl=None, group_fl=None, ngroup=1024, **rest):
    """raw_query: return Solr raw query data for a connection"""
    data = {'q': search_str, 'sort': sort, 'rows': nrows, 'indent': 'off'}
    if group_fl:
        data = {**data,
                'group': 'true',
                'group.field': group_fl,
                'group.limit': ngroup}
    if facet_fl:
        data = {**data,
                'facet': 'true',
                'facet.field': facet_fl,
                'facet.limit': ngroup,
                'rows': nrows}
    if rest:
        data = {**data, **rest}
    this_response = post_solr(data, name, api='select')
    return this_response

def get_query(name, search_str='*:*', sort='id asc', limitrows=False, nrows=10, **rest):
    """get_query: return Solr query data for `solr` connection"""
    if not ping_name(name):
        raise ValueError('"{}" is not a Solr collection or core'.format(name))
    nrows = nrows if limitrows else get_count(name)
    this_response = raw_query(name,
                              q=search_str,
                              sort=sort,
                              nrows=nrows,
                              **rest)
    this_data = this_response.pop('response')
    return this_data['docs']

def get_group(name, group_fl, search_str='*:*',
              ngroup=1024, sort='id asc', **rest):
    """get_group: return Solr query grouped by group_fl data"""
    if not ping_name(name):
        raise ValueError('"{}" is not a Solr collection or core'.format(name))
    nrows = get_count(name)
    this_response = raw_query(name,
                              q=search_str,
                              sort=sort,
                              nrows=nrows,
                              group_fl=group_fl,
                              ngroup=ngroup,
                              **rest)
    this_data = this_response.pop('grouped')[group_fl]['groups']
    return {i['groupValue']:
            [{k: v for k, v in j.items() if k != group_fl}
             for j in i['doclist']['docs']]
            for i in this_data}

def get_facet(name, facet_fl, search_str='*:*', nrows=0, ngroup=512, **rest):
    """get_facet: return Solr facet grouped by facet_fl field"""
    if not ping_name(name):
        raise ValueError('"{}" is not a Solr collection or core'.format(name))
    ngroup = get_count(name)
    this_response = raw_query(name,
                              q=search_str,
                              nrows=nrows,
                              ngroup=ngroup,
                              facet_fl=facet_fl,
                              **rest)
    this_data = this_response.pop('facet_counts')['facet_fields'][facet_fl]
    return this_data

def get_count(name, search_str='*:*', **rest):
    """get_count: return Solr document count for name"""
    if not ping_name(name):
        raise ValueError('"{}" is not a Solr collection or core'.format(name))
    this_response = raw_query(name, q=search_str, start=0, nrows=0, **rest)
    this_data = this_response.pop('response')
    return this_data.pop('numFound')

def clean_query(this_object):
    """clean_query: remove `_version_` key"""
    this_object.pop('_version_', None)
    return this_object

def get_cores():
    """get_cores: return a list of the Solr core names"""
    this_data = get_api(api_type='cores')
    this_status = this_data.get('status')
    return set(this_status.keys())

def type_error_solr(this_response, this_schema):
    """type_error_solr: parse dtype schema response for field name"""
    error_text = this_response['error']['msg']
    if 'Error adding field' in error_text:
        print(error_text)
        print(error_text.split('\''))
        this_schema = {i['name']: i['type'] for i in this_schema}
        (_, this_field, _, this_data, *_) = error_text.split('\'')
        this_type = this_schema.get(this_field, None)
        raise ValueError('Error: cannot post data "{}" to field "{}" type "{}"'\
                         .format(this_data, this_field, this_type))

def post_data(data, name):
    """post_data_api: post data using v1 Solr API"""
    return post_solr(json.dumps(data),
                     name,
                     api='update/json/docs?commit=true',
                     response_header=True)

def update_data(data, name):
    """post_data_api: post data using v1 Solr API"""
    update_data = [{k: v if k == 'id' else {'set': v} for k, v in i.items()} for i in data]
    return post_solr(json.dumps(update_data),
                     name,
                     api='update/json?commit=true',
                     response_header=True)

def get_names():
    """get_names: return a set of Solr collection or core names"""
    try:
        return get_collections()
    except requests.exceptions.HTTPError:
        pass
    try:
        return get_cores()
    except ConnectionError as error:
        print(error)
    return set()

def get_collections():
    """get_collections: return set of collection names"""
    this_data = get_api()
    return set(i for i in this_data['collections'])

def usr_dtype(this_str):
    """usr_dtype: test for system `dtypes`"""
    excluded = {'_root_', '_version_', '_text_', '_nest_path_'}
    return not this_str in excluded

def get_schema(name, solr_mode='collections', all_fields=False):
    """get_schema: return dict for Solr schema for excluding required and unstored fields"""
    try:
        this_error = None
        this_data = get_api(name, 'schema/fields', solr_mode)
    except HTTPError as error:
        this_error = error
    if isinstance(this_error, HTTPError):
        raise ValueError('"{}" is not a Solr collection or core'.format(name))
    if all_fields:
        return this_data['fields']
    return [i for i in this_data['fields'] if usr_dtype(i['name']) and not i.get('required')]

def get_fullschema(name, solr_mode='collections', all_fields=False):
    """get_schema: return dict for Solr schema for excluding required and unstored fields"""
    try:
        this_error = None
        this_fields = get_api(name, 'schema/fields', solr_mode)
        this_copyfields = get_api(name, 'schema/copyfields', solr_mode)
    except HTTPError as error:
        this_error = error
    if isinstance(this_error, HTTPError):
        raise ValueError('"{}" is not a Solr collection or core'.format(name))
    if all_fields:
        return this_data['fields']
    return {'fields': [i for i in this_fields['fields'] if usr_dtype(i['name']) and not i.get('required')], **this_copyfields}


def solr_field(name=None, type='string', multiValued=False, stored=True, docValues=False):
    """solr_field: convert python dict structure to Solr field structure"""
    if not name:
        raise TypeError('solar() missing 1 required positional \
        argument: "name"')
    lookup_bool = {True: 'true', False: 'false'}
    return {'name': name, 'type': type,
            'multiValued': lookup_bool[multiValued],
            'stored': lookup_bool[stored],
            'docValues': lookup_bool[docValues]}

def set_schema(name, solr_mode='collections', *v):
    """set_schema: add or replace the Solr schema for name from list of dict
    `v` containing `name` and `type` keys"""
    fields = []
    for i in v:
        if isinstance(i, list):
            fields += i
        else:
            fields.append(i)
    schema_fields = None
    try:
        schema_fields = {i['name']: i['type'] for i in get_schema(name, solr_mode)}
    except ValueError:
        pass
    data = {'add-field': [], 'replace-field': []}
    for field in fields:
        if field['name'] in schema_fields:
            data['replace-field'].append(solr_field(**field))
            continue
        data['add-field'].append(solr_field(**field))
    return post_solr(json.dumps(data), name, api='schema')

def wait_for_success(function, error, *rest):
    """wait_for_success: poll for successful completion of function"""
    for i in range(128):
        try:
            if function(*rest):
                return True
        except error:
            pass
        print({'waiting': function.__name__, 'count': i})
        sleep(1.0)
    return False

def check_missing_status(name, solr_mode='collections', status='false'):
    """check_missing_status: test if `add-unknown-fields-to-the-schema`
    parameter is set"""
    this_result = get_api(name, 'config/updateRequestProcessorChain', solr_mode)
    this_data = this_result['config']['updateRequestProcessorChain']
    return  all(i['default'] == status
                for i in this_data
                if i['name'] == 'add-unknown-fields-to-the-schema')

def create_collection(name, shards=1, replication=1, set_schema=True):
    """create_collection: create Solr collection"""
#def create_collection(name, hostname=SOLRHOST, shards=3, replication=2):
    print('create collection {}'.format(name))
    data = {'create': {'name': name,
                       'numShards': shards,
                       'replicationFactor':replication,
                       'waitForFinalState': 'true'}}
    this_response = post_api(json.dumps(data))
    print(this_response)
    print('created collection {}'.format(name))
    if set_schema:
        data = {'set-user-property': {'update.autoCreateFields': 'false',
                                  'waitForFinalState': 'true'}}
        post_api(json.dumps(data), name, 'config')
        print('autoCreateFields {} true'.format(name))

def get_configs():
    return get_api('configs', '', api_type='cluster')

def delete_config(name):
    if name == '_default_':
        raise ValueError('Error: cannot delete "_default_" config')
    these_configs = get_api('configs', '', api_type='cluster').pop('configSets', None)
    if not these_configs: return
    for this_name in [name, '{}.AUTOCREATED'.format(name)]:
        if this_name in these_configs:
            delete_api('configs', this_name, 'cluster')

def delete_schema(name):
    """remove_schema: remove field definitions for Solr schema `name`"""
    this_schema = get_fullschema(name)
    fields = [{'name': i['name']} for i in this_schema['fields']]
    copyfields = this_schema['copyFields']
    data = {'delete-field': fields}
    for i in copyfields:
        data = {'delete-copy-field': i}
        post_solr(json.dumps(data), name, api='schema')
    return post_solr(json.dumps(data), name, api='schema')

def delete_collection(name, drop_schema=True, drop_config=True):
    """delete_collection: delete collection and optionally drop schema"""
    print('delete collection {}'.format(name))
    if drop_schema and get_schema(name):
        print('delete schema {}'.format(name))
        delete_schema(name)
        print('deleted schema {}'.format(name))
    delete_api(name)
    wait_for_success(lambda v: not ping_name(v), ValueError, name)
    if drop_config:
        delete_config(name)
        print('deleted config {}'.format(name))
    print('deleted collection {}'.format(name))

def ping_name(name, solr_mode='cores'):
    """ping_name: check if collection or core exists"""
    this_api = 'admin/ping' if solr_mode == 'cores' else 'admin/ping?distrib=true'
    this_error = None
    try:
        this_data = get_solr(name, api=this_api)
    except (HTTPError, SolrError, ConnectionError) as error:
        this_error = error
    if isinstance(this_error, ConnectionError):
        raise ConnectionError('Error: Solr is not running')
    if this_error:
        return False
    return this_data.get('status') == 'OK'

def get_solrmode():
    this_error = None
    try:
        _ = get_collections()
    except (ConnectionError, HTTPError) as error:
        this_error = error
    if isinstance(this_error, ConnectionError):
        raise ConnectionError('Error: Solr is not running')
    if isinstance(this_error, HTTPError):
        return 'cores'
    return 'collections'
