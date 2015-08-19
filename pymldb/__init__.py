#
# pymldb
# Copyright (c) 2013 Datacratic. All rights reserved.
#

from pymldb import resource
import pandas as pd
from pprint import pprint


class Color:
    GREEN = '\033[32m'
    RED = '\033[31m'
    ORANGE = '\033[33m'
    WHITE = '\033[37m'
    RESET = '\033[0m'

def color(text, color):
    return getattr(Color, color.upper()) + str(text) + Color.RESET


def unescapeSpecialParams(params):
    params = dict(params)  # copy (not deep)
    for p in params.keys():
        if p.endswith('_'):
            params[p[:-1]] = params.pop(p)
    return params


class Connection(object):

    def __init__(self, host="http://localhost"):
        if not host.startswith("http"):
            raise Exception("URIs must start with 'http'")
        self.host = host.strip("/")
        self.v1 = resource.Resource(self.host).v1

    def query(self, sql, raise_on_error=True):
        resp_json = self.v1.query.get(params=dict(q=sql, format="aos"),
                                 raise_on_error=raise_on_error)
        if len(resp_json) == 0:
            return pd.DataFrame()
        else:
            return pd.DataFrame.from_records(resp_json, index="_rowName")

    # a few shortcuts
    @property
    def functions(self):
        return self.v1.functions

    @property
    def procedures(self):
        return self.v1.procedures

    @property
    def datasets(self):
        return self.v1.datasets

    # a few other shortcuts to create dataset/functions/procedures
    def _create_thing(self, what, id, type_, **params):
        params = unescapeSpecialParams(params)
        thing = getattr(self, what)(id)
        try:
            print '\n', color('DELETE', 'red'), color(thing, 'white')
            thing.delete()
        except resource.ResourceError:
            pass
        jsonBlob = {'type': type_, 'params': params}
        print '\n', color('PUT', 'orange'), color(thing, 'white')
        # pprint(jsonBlob)
        res = thing.put_json(jsonBlob)
        # print '\nResult:'
        pprint(res)
        return thing

    def create_dataset(self, id, type_=None, **params):
        """ you can also pass a config dictionary """
        if type(id) == dict:
            return self.create_dataset(id['id'], id['type'], **id['params'])
        elif type_ is None:
            raise ValueError('you need a type_ for you dataset')
        return self._create_thing('datasets', id, type_, **params)

    def create_function(self, id, type_, **params):
        return self._create_thing('functions', id, type_, **params)

    def create_procedure(self, id, type_, **params):
        if 'outputDataset' in params:
            ds = self.datasets(params['outputDataset']['id'])
            print '\n', color('DELETE', 'red'), color(ds, 'white')
            ds.delete()
        return self._create_thing('procedures', id, type_, **params)



# IPython Magic system

def load_ipython_extension(ipython, *args):
    from pymldb.magic import dispatcher
    dispatcher("init http://localhost")
    ipython.register_magic_function(dispatcher, 'line_cell', magic_name="mldb")

def unload_ipython_extension(ipython):
    pass
