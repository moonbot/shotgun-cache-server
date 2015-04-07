import shotgun_api3 as sg
from copy import deepcopy
from collections import Mapping, MutableMapping

__all__ = [
    # 'get_elastic_type',
    # 'get_elastic_index_for_entity_type',
    'ShotgunConnector',
    'getBaseEntity',
]

# def get_elastic_type(type):
#     return type.lower()

# def get_elastic_index_for_entity_type(sgType):
#     return config['esIndexFormat_entity'].format(type=sgType.lower())

class ShotgunAPIWrapper(sg.Shotgun):
    def _transform_inbound(self, data):
        # Skip transforming inbound data so it correctly matches for our proxy
        # TODO - maybe reimplement this elsewhere for elastic?
        return data

class ShotgunConnector(object):
    def __init__(self, config):
        self.config = config

    def getInstance(self, raw=True, **kwargs):
        cls = ShotgunAPIWrapper if raw else sg.Shotgun
        kw = self.config.copy()
        kw.update(kwargs)
        sgConn = cls(
            **kw
        )
        return sgConn

def getBaseEntity(entity):
    """
    Remove extra information from an entity dict
    keeping only type and id
    """
    if entity is None:
        return entity
    return dict([(k, v) for k, v in entity.items() if k in ['id', 'type']])

def combine_dict(a, b, copy=True):
    """
    Return a dict that is the result of recursively
    updating dict `a` with dict `b`. Performs a deep
    copy to avoid altering the given objects.
    """
    result = deepcopy(a)
    update_dict(result, b, copy=copy)
    return result

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
