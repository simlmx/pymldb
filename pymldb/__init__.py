#
# pymldb
# Nicolas Kructhen, 2015-05-28
# Mich, 2016-01-26
# Copyright (c) 2013 Datacratic. All rights reserved.
#

import pandas as pd
pd.set_option('display.width', 150)
pd.set_option('display.max_columns', 999)
import requests
import json
from pymldb.util import add_repr_html_to_response
from IPython.display import display


def unescapeSpecialParams(params):
    params = dict(params)  # copy (not deep)
    for p in params.keys():
        if p.endswith('_'):
            params[p[:-1]] = params.pop(p)
    return params


def decorate_response(fn):
    def inner(*args, **kwargs):
        result = add_repr_html_to_response(fn(*args, **kwargs))
        if result.status_code < 200 or result.status_code >= 400:
            raise ResourceError(result)
        return result
    return inner


class ResourceError(Exception):
    def __init__(self, r):
        try:
            message = json.dumps(r.json(), indent=2)
        except:
            message = r.content
        super(ResourceError, self).__init__(
            "'%d %s' response to '%s %s'\n\n%s" %
            (r.status_code, r.reason, r.request.method, r.request.url, message)
        )
        self.result = r


class _Connection(object):
    """Original Connection object from pymldb"""
    def __init__(self, host="http://localhost"):
        if not host.startswith("http"):
            raise Exception("URIs must start with 'http'")
        if host[-1] == '/':
            host = host[:-1]
        self.uri = host

    @decorate_response
    def get(self, url, **kwargs):
        params = {}
        for k, v in kwargs.iteritems():
            if type(v) in [dict, list]:
                v = json.dumps(v)
            params[str(k)] = v
        return requests.get(self.uri + url, params=params)

    @decorate_response
    def put(self, url, payload=None):
        if payload is None:
            payload = {}
        return requests.put(self.uri + url, json=payload)

    @decorate_response
    def post(self, url, payload=None):
        if payload is None:
            payload = {}
        return requests.post(self.uri + url, json=payload)

    @decorate_response
    def delete(self, url):
        return requests.delete(self.uri + url)

    def query(self, sql):
        resp = self.get('/v1/query', q=sql, format="table").json()
        if len(resp) == 0:
            return pd.DataFrame()
        else:
            return pd.DataFrame.from_records(resp[1:], columns=resp[0],
                                             index="_rowName")


class Connection(_Connection):
    """My custom version with shortcuts of pymldb's Connection object"""
    debug=False

    def put(self, url, payload):
        # if in debug mode, will print the queries we are passing
        if self.debug:
            for key in payload['params'].iterkeys():
                if key.endswith('Data'):
                    self.log(key)
                    self.log(payload['params'][key])
        return super(Connection, self).put(url, payload)

    def quick(self, verb, url, type_, **params):
        return getattr(self, verb.lower())(
            url, {'type': type_, 'params': params})

    def quick_post(self, url, type_, **params):
        return self.quick('post', url, type_, **params)

    def quick_put(self, url, type_, **params):
        return self.quick('put', url, type_, **params)

    def query(self, query, **kwargs):
        # TODO I think this is now fixed in pymldb itself
        # default to dataframe
        if 'format' not in kwargs:
            kwargs['format']='dataframe'
        format_ = kwargs.pop('format')
        is_dataframe = format_ == 'dataframe'
        # if pandas is not there
        if is_dataframe and not 'pd' in globals():
            is_dataframe = False
            format_ = 'table'
        if is_dataframe:
            format_ = 'table'

        x = self.get('/v1/query', q=query, format=format_, **kwargs).json()
        if is_dataframe:
            if len(x) == 0:
                return pd.DataFrame()
            else:
                return pd.DataFrame.from_records(x[1:], columns=x[0],
                                                 index='_rowName')
        else:
            return x

    def log(self, x):
        # _parent = super(SimonMLDB, self)
        # if hasattr(_parent, 'log'):
            # add a breakline in case of multiline, it's clearner
        #     x = str(x)
        #     prefix = '\n' if '\n' in x else ''
        #     return _parent.log(prefix + x)
        # else:
        if isinstance(x, basestring):
            print(x)
        else:
            display(x)

    def log_summary(self, dataset, limit=5, order=None, count=False, where=None):
        self.log('summary of ' + dataset)
        if count:
            self.log('nb of rows: ')
            self.log(self.query('select count(*) as cnt from ' + dataset))
        query = """SELECT * FROM """ + dataset
        if where is not None:
            query += '\nWHERE ' + where
        if order is not None:
            query += '\nORDER BY ' + order
        query += '\nLIMIT ' + str(limit)
        self.log(self.query(query))

    def save_dataset(self, name):
        return self.post('/v1/procedures', {
            'type': 'export.csv',
            'params': {
                'exportData': 'select * from ' + name,
                'dataFileUrl': 'file://' + name + '.csv.gz',
            }
        })

    def delete_all_entities(self):
        """ careful with this! """
        for ent_type in ['functions', 'procedures', 'datasets']:
            for ent in self.get('/v1/' + ent_type).json():
                url = '/v1/{}/{}'.format(ent_type, ent)
                self.log('deleting ' + url)
                self.delete(url)
