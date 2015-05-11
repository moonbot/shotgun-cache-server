import logging
import gevent
import difflib

import rethinkdb as r

import utils

__all__ = [
    'FieldValidator'
]

LOG = logging.getLogger(__name__)


class FieldValidator(object):
    def __init__(self, config, entityConfigManager, entityConfigs, filters, filterOperator, fields, allCachedFields=False):
        super(FieldValidator, self).__init__()
        self.config = config
        self.entityConfigManager = entityConfigManager
        self.entityConfigs = entityConfigs
        self.filters = filters
        self.fields = fields
        self.filterOperator = filterOperator
        self.allCachedFields = allCachedFields

        self.shotgunPool = utils.ShotgunConnectionPool(config, config['import']['max_shotgun_connections'])
        self.rethinkPool = utils.RethinkConnectionPool(config, config['import']['max_rethink_connections'])

    def start(self, raiseExc=True):
        LOG.info("Starting Validate Fields")
        return self.run()

    def run(self):
        greenlets = [gevent.spawn(self.get_fields_for_config, c) for c in self.entityConfigs]
        gevent.joinall(greenlets)
        result = [g.value for g in greenlets]
        return result

    def get_fields_for_config(self, entityConfig):
        if self.allCachedFields:
            fields = entityConfig['fields'].keys()
        else:
            fields = self.fields[:]
        fields.append('id')
        fields.append('type')
        fields = list(set(fields))

        LOG.debug("Getting fields from Shotgun for type: {0}".format(entityConfig.type))
        with self.shotgunPool.get() as sg:
            shotgunResult = sg.find(
                entityConfig.type,
                filter_operator=self.filterOperator,
                filters=self.filters,
                fields=fields,
                order=[{'field_name': 'id', 'direction': 'asc'}]
            )

        # Convert any nested entities to base entities (type and id only)
        self.strip_nested_entities(entityConfig, shotgunResult)

        # Group by id's to match with cache
        # Group into a dictionary with the id as key
        shotgunMap = dict([(e['id'], e) for e in shotgunResult])

        LOG.debug("Getting fields from cache for type: {0}".format(entityConfig.type))

        # Have to batch requests to shotgun in groups of 1024
        cacheMatches = []
        LOG.debug("Getting total match count from cache for type: {0}".format(entityConfig.type))
        with self.rethinkPool.get() as conn:
            cacheMatches = list(r.table(entityConfig['table'])
                                .filter(lambda e: e['id'] in shotgunMap.keys())
                                .pluck(fields)
                                .run(conn))

        # Check for missing ids
        missingFromCache = []
        missingFromShotgun = []
        # print("cacheMatches: {0}".format(cacheMatches)) # TESTING
        cacheMap = dict([(e['id'], e) for e in cacheMatches])
        if len(cacheMap) != len(shotgunMap):
            cacheIDSet = set(cacheMap)
            shotgunIDSet = set(shotgunMap.keys())

            missingIDsFromCache = cacheIDSet.difference(shotgunIDSet)
            missingFromCache = dict([(_id, cacheMap[_id]) for _id in missingIDsFromCache])

            missingIDsFromShotgun = shotgunIDSet.difference(cacheIDSet)
            missingFromShotgun = dict([(_id, shotgunMap[_id]) for _id in missingIDsFromShotgun])

        # Compare the data for each
        failed = False
        diffs = []
        for _id, shotgunData in shotgunMap.items():
            if _id not in cacheMap:
                continue
            cacheData = cacheMap[_id]

            # Sort the nested entities by ID
            # Their sort order is not enforced by shotgun
            # So we can't count on it staying consistent
            shotgunData = utils.sort_multi_entity_fields_by_id(self.entityConfigManager.schema, shotgunData)
            cacheData = utils.sort_multi_entity_fields_by_id(self.entityConfigManager.schema, cacheData)

            shotgunJson = utils.pretty_json(shotgunData)
            cacheJson = utils.pretty_json(cacheData)
            if shotgunJson != cacheJson:
                diff = difflib.unified_diff(
                    str(shotgunJson).split('\n'),
                    str(cacheJson).split('\n'),
                    lineterm="",
                    n=5,
                )
                # Skip first 3 lines
                header = '{type}:{id}\n'.format(type=entityConfig.type, id=_id)
                [diff.next() for x in range(3)]
                diff = header + '\n'.join(diff)
                diffs.append(diff)

        result = {
            'entityType': entityConfig.type,
            'failed': failed,
            'shotgunMatchCount': len(shotgunMap),
            'cacheMatchCount': len(cacheMap),
            'missingFromCache': missingFromCache,
            'missingFromShotgun': missingFromShotgun,
            'diffs': diffs,
        }
        return result

    def strip_nested_entities(self, entityConfig, entities):
        # Strip extra data from nested entities so
        # only type and id remains
        for entity in entities:
            entitySchema = self.entityConfigManager.schema[entityConfig.type]
            for field, val in entity.items():
                if field not in entitySchema:
                    continue
                if field in ['type', 'id']:
                    continue
                fieldDataType = entitySchema[field].get('data_type', {}).get('value', None)
                if fieldDataType == 'multi_entity':
                    val = [utils.getBaseEntity(e) for e in val]
                    entity[field] = val
                elif fieldDataType == 'entity':
                    val = utils.getBaseEntity(val)
                    entity[field] = val
