import os
import re
import datetime
import logging
from copy import deepcopy
from collections import Mapping, MutableMapping
import json

import yaml
import elasticsearch
import shotgun_api3 as sg

__all__ = [
    'ShotgunAPIWrapper',
    'convertStrToDatetime',
    'addNumberSign',
    'getBaseEntity',
    'prettyJson',
    'chunks',
    'combine_dict',
    'update_dict',
    'EncodedDict',
    'DeepDict',
    'sortMultiEntityFieldsByID',
    'get_dict_diff',
    'get_deep_keys',
    'has_deep_key',
    'get_deep_item',
    'set_deep_item',
    'del_deep_item',
    'Config',
    'History',
]


LOG = logging.getLogger(__name__)


class ShotgunAPIWrapper(sg.Shotgun):
    _make_call_results = None

    def _make_call(self, *args, **kwargs):
        # TODO Needed?
        self._make_call_results = super(ShotgunAPIWrapper, self)._make_call(*args, **kwargs)
        return self._make_call_results

    def _transform_inbound(self, data):
        # Skip transforming inbound data so it correctly matches for our proxy
        return data


def convertStrToDatetime(dateStr):
    return datetime.datetime(*map(int, re.split('[^\d]', dateStr)[:-1]))


def sortMultiEntityFieldsByID(schema, entity):
    """
    Sort all multi-entity fields in an entity by their ID.

    Args:
        schema (dict): Shotgun Schema
        entity (dict): Entity dictionary

    Returns:
        dict: entity with fields sorted
    """
    result = {}
    entitySchema = schema[entity['type']]
    for field, val in entity.items():
        if field in ['id', 'type']:
            result[field] = val
            continue

        fieldSchema = entitySchema[field]
        dataType = fieldSchema['data_type']['value']
        if dataType == 'multi_entity':
            val = sorted(val, key=lambda e: e['id'])
        result[field] = val
    return result

def addNumberSign(num):
    if num > 0:
        num = '+' + str(num)
    elif num < 0:
        num = '-' + str(abs(num))
    return num


def getBaseEntity(entity):
    """
    Remove extra information from an entity dict
    keeping only type and id
    """
    if entity is None:
        return entity
    return dict([(k, v) for k, v in entity.items() if k in ['id', 'type']])


def get_dict_diff(a, b):
    """
    Get the differences between a, and b
    Supports nested dictionaries as well.

    >>> a = dict(
    ...     myBool = True,
    ...     myDict = {1:'a', 2:'b'},
    ... )
    >>> b = dict(
    ...     myBool = False,
    ...     myDict = {3:'c'},
    ...     myString = 'hi'
    ... )
    >>> get_dict_diff(b, a)
    {'myString': 'hi', 'myDict': {3: 'c'}, 'myBool': False}
    >>> a['myBool'] = False
    >>> get_dict_diff(b, a)
    {'myString': 'hi', 'myDict': {3: 'c'}}
    """
    diff = {}
    for k, a_value in a.items():
        if k in b.keys():
            b_value = b[k]
            if a_value == b_value:
                continue
            else:
                # Check for a nested dict
                # If so, compare values inside it
                if isinstance(a_value, MutableMapping):
                    # set any nested differences
                    nested_diff = get_dict_diff(a_value, b_value)
                    if not nested_diff:
                        continue
                    diff[k] = nested_diff

        # If it hasn't been added to the diff as a nested diff
        # add it now
        if k not in diff:
            diff[k] = a_value

    return diff


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def combine_dict(a, b, copy=True):
    """
    Return a dict that is the result of recursively
    updating dict `a` with dict `b`. Performs a deep
    copy to avoid altering the given objects.
    """
    result = deepcopy(a)
    update_dict(result, b, copy=copy)
    return result


def prettyJson(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))


def update_dict(a, b, copy=True):
    """
    Update dictionary A with B recursively.
    This means that dictionary values are not
    simply replaced, but updated as well.

    `copy` - if True, uses a copy of B before updating
        so that if A is changed it will not affect any
        elements of B

    >>> a = dict(
    ...     myBool = True,
    ...     myDict = {1:'a', 2:'b'},
    ... )
    >>> b = dict(
    ...     myBool = False,
    ...     myDict = {3:'c'},
    ...     myString = 'hi'
    ... )
    >>> update_dict(a, b)
    >>> a
    {'myDict': {1: 'a', 2: 'b', 3: 'c'}, 'myBool': False, 'myString': 'hi'}
    """
    if copy:
        b = deepcopy(b)
    for k in b.keys():
        if isinstance(b[k], Mapping) and k in a and isinstance(a[k], MutableMapping):
            # update existing key
            update_dict(a[k], b[k])
        else:
            # assign new key
            a[k] = b[k]
    return a


class EncodedDict(MutableMapping):
    """
    This is an abstract class for any dict-like classes
    that support holding data that is encoded and
    decoded on get and set. This means that data
    is changed at access time, so the EncodedDict provides
    a standardized way to get at the raw data, if needed.

    Subclasses must store the core data in `_data`,
    or override the `data` property to return
    the raw data, which must not be a copy
    """
    def __init__(self, *args, **kwargs):
        self._data = dict(*args, **kwargs)

    def __getitem__(self, key):
        return self.encode(self._data.__getitem__(key))

    def __setitem__(self, key, value):
        return self._data.__setitem__(key, value)

    def __delitem__(self, key):
        return self._data.__delitem__(key)

    def __contains__(self, key):
        return self._data.__contains__(key)

    def __iter__(self):
        return self._data.__iter__()

    def __len__(self):
        return self._data.__len__()

    def encode(self, value):
        """
        Encode and return the given value.
        Override this method to implement custom encoding
        """
        return value

    def __repr__(self):
        """
        Return the repr of the encoded dictionary
        """
        return dict(self).__repr__()

    @property
    def data(self):
        return self._data


class DeepDict(EncodedDict):
    """
    Basic dictionary that allows you to get child items
    at any depth using a dot syntax, eg. 'my.deep.key'.

    All keys must be non-empty strings that do not contain '.'

    When setting values, the child-most dictionary must
    already exist

    >>> d = DeepDict({1:2})
    >>> d = DeepDict(
    ...     a=4,
    ...     b=5,
    ...     c=dict(
    ...         d=dict(
    ...             e=10
    ...         )
    ...     )
    ... )
    >>> d['c.d']
    {'e': 10}
    >>> d['c.d.e']
    10
    >>> d['c.d.e'] = 3
    >>> d
    {'a': 4, 'c': {'d': {'e': 3}}, 'b': 5}
    >>> d.has_key('c.d.e')
    True

    >>> # overwrite existing integer value with another nested layer
    >>> d['c.d.e.f'] = 7
    >>> d
    {'a': 4, 'c': {'d': {'e': 3}}, 'b': 5}

    >>> # add new nested item directly
    >>> d['1.2.3'] = 5
    >>> d
    {'a': 4, '1': {'2': {'3': 5}}, 'c': {'d': {'e': 3}}, 'b': 5}

    >>> d.keys()
    ['a', '1', 'c', 'b']
    >>> d.deep_keys()
    ['a', '1.2.3', 'c.d.e', 'b']

    >>> del d['c.d']
    >>> d
    {'a': 4, '1': {'2': {'3': 5}}, 'c': {}, 'b': 5}

    >>> # access raw data using deep key
    >>> d.get_raw('1.2.3')
    5
    """
    def __getitem__(self, k):
        return self.encode(get_deep_item(self.data, k))

    def __contains__(self, key):
        return has_deep_key(self._data, key)

    def __setitem__(self, k, v):
        set_deep_item(self.data, k, v)

    def __delitem__(self, k):
        del_deep_item(self.data, k)

    def has_key(self, key):
        return self.__contains__(key)

    def deep_keys(self):
        return get_deep_keys(self)

    def get_raw(self, key):
        return get_deep_item(self.data, key)


def get_deep_keys(dict):
    keys = []
    for k, v in dict.items():
        if isinstance(v, Mapping):
            deepKeys = get_deep_keys(v)
            keys.extend(['{0}.{1}'.format(k, deepK) for deepK in deepKeys])
        else:
            keys.append(k)
    return keys


def has_deep_key(dict, key):
    keys = key.split('.', 1)
    if keys[0] in dict:
        if len(keys) == 1:
            return True
        v = dict[keys[0]]
        if isinstance(v, Mapping):
            return has_deep_key(v, keys[1])
    return False


def get_deep_item(d, k, sep='.'):
    """
    Return the value for `k` from the dictionary `d`,
    by splitting the key and searching recursively
    """
    if not isinstance(k, basestring):
        raise KeyError('expected string, got {0}: {1}'.format(type(k).__name__, k))
    val = d
    # recursively look for dictionary values, then
    # return the last value
    for key in k.split(sep):
        if key and isinstance(val, Mapping) and key in val:
            val = val.__getitem__(key)
        else:
            raise KeyError(k)
    return val


def set_deep_item(d, k, v, sep='.'):
    """
    Recurse into the dictionary `d` by splitting
    key `k` by `sep` and setting dictionary values appropriately.
    Will create or override intermediate key values if they
    are not dictionaries
    """
    if not isinstance(k, basestring):
        raise KeyError('expected string, got {0}: {1}'.format(type(k).__name__, k))
    # split and validate key
    keys = k.split(sep)
    for key in keys:
        if not key:
            raise KeyError(k)
    # loop through and get/create dictionary
    # items for all but the last key
    val = d
    for key in keys[:-1]:
        if key not in val:
            # create new dictionary item for key
            val[key] = {}
        val = dict.__getitem__(val, key)
        # force into being a dictionary
        if not isinstance(val, MutableMapping):
            val = {}
    val.__setitem__(keys[-1], v)


def del_deep_item(d, k, sep='.'):
    """
    Recurse into the dictionary `d` by splitting
    key `k` by `sep` and deleting the value at the last key
    """
    if not isinstance(k, basestring):
        raise KeyError('expected string, got {0}: {1}'.format(type(k).__name__, k))
    keys = k.split(sep)
    val = d
    for key in keys[:-1]:
        if isinstance(val, MutableMapping) and key in val:
            val = dict.__getitem__(val, key)
        else:
            raise KeyError(k)
    val.__delitem__(keys[-1])


class Config(DeepDict):
    """
    Main configuration dictionary for the shotgunCache
    """
    _history = None

    @classmethod
    def loadFromYaml(cls, yamlPath):
        result = yaml.load(open(yamlPath, 'r').read())
        return cls(result)

    def createShotgunConnection(self, raw=True, **kwargs):
        cls = ShotgunAPIWrapper if raw else sg.Shotgun
        kw = self['shotgun'].copy()
        kw.update(kwargs)
        sgConn = cls(
            **kw
        )
        return sgConn

    def createElasticConnection(self, **kwargs):
        kw = self['elasticsearch_connection'].copy()
        kw.update(kwargs)
        elastic = elasticsearch.Elasticsearch(**kw)
        return elastic

    @property
    def history(self):
        if self._history is None:
            self._history = History(self['history_file'])
        return self._history
    @history.setter
    def history(self, value):
        self._history = value


class History(DeepDict):
    """
    Used to track the history state of the cache
    Loads and saves to a yaml file
    """
    def __init__(self, historyFilePath):
        path = os.path.expanduser(historyFilePath)
        path = os.path.abspath(path)
        self.historyFilePath = path
        super(History, self).__init__({})
        self.load()

    def load(self):
        if os.path.exists(self.historyFilePath):
            result = yaml.load(open(self.historyFilePath, 'r').read())
        else:
            LOG.info("No existing history file at {0}".format(self.historyFilePath))
            result = {}
        self._data = result
        return result

    def save(self):
        with open(self.historyFilePath, 'w') as f:
            yaml.dump(dict(self), f, default_flow_style=False, indent=4)
