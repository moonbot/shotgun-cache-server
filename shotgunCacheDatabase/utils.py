import shotgun_api3 as sg

__all__ = [
    # 'get_elastic_type',
    # 'get_elastic_index_for_entity_type',
    'ShotgunConnector',
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

    def getInstance(self, raw=True):
        cls = ShotgunAPIWrapper if raw else sg.Shotgun
        sgConn = cls(
            **self.config
        )
        return sgConn
