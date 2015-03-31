from config import config

__all__ = [
    'get_elastic_type',
    'get_elastic_index_for_entity_type',
]

def get_elastic_type(type):
    return type.lower()

def get_elastic_index_for_entity_type(sgType):
    return config['esIndexFormat_entity'].format(type=sgType.lower())
