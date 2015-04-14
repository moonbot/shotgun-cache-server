import os
import time
import logging
import multiprocessing
import difflib
import Queue

import utils

__all__ = [
    'DataValidator'
]

LOG = logging.getLogger(__name__)


class DataValidator(object):
    def __init__(self, config, entityConfigManager, entityConfigs, filters, filterOperator, fields, allCachedFields=False):
        super(DataValidator, self).__init__()
        self.config = config
        self.entityConfigManager = entityConfigManager
        self.entityConfigs = entityConfigs
        self.filters = filters
        self.fields = fields
        self.filterOperator = filterOperator
        self.allCachedFields = allCachedFields

        self.workQueue = multiprocessing.JoinableQueue()
        self.resultQueue = multiprocessing.Queue()
        self.processes = []
        self.results = []

    def start(self, raiseExc=True):
        LOG.info("Starting Validate Counts")
        self.launchWorkers()
        self.run()
        self.terminateWorkers()

        if raiseExc:
            failed = []
            for result in self.results:
                if result['failed']:
                    failed.append(result)

            if len(failed):
                raise RuntimeError("Validation Failed, {0} cached entity type(s) do not match".format(len(failed)))

        return self.results

    def launchWorkers(self):
        processCount = min(len(self.entityConfigs), self.config['validate_counts.processes'])
        LOG.debug("Launching {0} validate workers".format(processCount))
        for n in range(processCount):
            worker = DataValidateWorker(
                self.workQueue,
                self.resultQueue,
                self.config,
                self.entityConfigManager,
                self.entityConfigs,
                filters=self.filters,
                filterOperator=self.filterOperator,
                fields=self.fields,
                allCachedFields=self.allCachedFields,
            )
            proc = multiprocessing.Process(target=worker.start)
            proc.start()
            self.processes.append(proc)

    def run(self):
        LOG.debug("Adding items to validate queue")
        for config in self.entityConfigs:
            data = {'configType': config.type}
            self.workQueue.put(data)
        self.workQueue.join()

        results = []
        while True:
            try:
                result = self.resultQueue.get(False)
            except Queue.Empty:
                break
            else:
                if result:
                    results.append(result)
        self.results = results

    def terminateWorkers(self):
        LOG.debug("Terminating validate workers")
        for proc in self.processes:
            proc.terminate()
        self.processes = []


class ValidateWorker(object):
    def __init__(self, workQueue, resultQueue, config, entityConfigManager, entityConfigs, **kwargs):
        super(ValidateWorker, self).__init__()
        self.workQueue = workQueue
        self.resultQueue = resultQueue
        self.config = config
        self.entityConfigManager = entityConfigManager
        self.entityConfigs = dict([(c.type, c) for c in entityConfigs])

        for k, v in kwargs.items():
            setattr(self, k, v)

        self.sg = None
        self.elastic = None

    def start(self):
        self.sg = self.config.createShotgunConnection(convert_datetimes_to_utc=False)
        self.elastic = self.config.createElasticConnection()
        self.run()

    def run(self):
        raise NotImplemented()


class DataValidateWorker(ValidateWorker):
    def stripNestedEntities(self, entityConfig, entities):
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

    def run(self):
        workerPID = os.getpid()
        LOG.debug("Data Validate Worker Running: {0}".format(workerPID))
        while True:
            try:
                work = self.workQueue.get()
            except Queue.Emtpy:
                continue
                time.sleep(0.1)

            entityConfig = self.entityConfigs[work['configType']]

            # Make sure we are filtering based how the cached entity
            # is setup, then add the additional filters
            entityFilters = entityConfig.get('filters', [])

            # Combine the cache filters with additional filters
            filters = [
                {
                    'filter_operator': 'all',
                    'filters': entityFilters
                },
                {
                    'filter_operator': self.filterOperator,
                    'filters': self.filters
                }
            ]

            if self.allCachedFields:
                fields = entityConfig['fields'].keys()
            else:
                fields = self.fields[:]
            fields.append('id')
            fields.append('type')
            fields = list(set(fields))

            LOG.debug("Getting data from Shotgun for type: {0}".format(work['configType']))
            shotgunResult = self.sg.find(
                entityConfig.type,
                filter_operator='all',
                filters=filters,
                fields=fields,
                order=[{'field_name': 'id', 'direction': 'asc'}]
            )

            # Convert any nested entities to base entities (type and id only)
            self.stripNestedEntities(entityConfig, shotgunResult)

            # Group by id's to match with cache
            # Group into a dictionary with the id as key
            shotgunMap = dict([(e['id'], e) for e in shotgunResult])

            LOG.debug("Getting data from cache for type: {0}".format(work['configType']))

            # Have to batch requests to shotgun in groups of 1024
            cacheMatches = []
            for ids in utils.chunks(shotgunMap.keys(), 1024):
                searchBody = {
                    'query': {
                        'terms': {
                            'id': ids
                        }
                    }
                }

                LOG.debug("Getting total match count from cache for type: {0}".format(work['configType']))
                cacheHitResults = self.elastic.count(
                    index=entityConfig['index'],
                    doc_type=entityConfig['doc_type'],
                    body=searchBody,
                )
                cacheHits = cacheHitResults['count']

                cacheResult = self.elastic.search(
                    index=entityConfig['index'],
                    doc_type=entityConfig['doc_type'],
                    _source=fields,
                    body=searchBody,
                    size=cacheHits,
                )
                _cacheMatches = cacheResult['hits']['hits']
                cacheMatches.extend(_cacheMatches)

            # Check for missing ids
            missingFromCache = []
            missingFromShotgun = []
            cacheMap = dict([(t['_source']['id'], t['_source']) for t in cacheMatches])
            if len(cacheMap) != len(shotgunMap):
                cacheIDSet = set(cacheMap)
                shotgunIDSet = set(shotgunMap.keys())

                missingIDsFromCache = cacheIDSet.difference(shotgunIDSet)
                missingFromCache = dict([(_id, cacheMap[_id]) for _id in missingIDsFromCache])

                missingIDsFromShotgun = shotgunIDSet.difference(cacheIDSet)
                missingFromShotgun = dict([(_id, cacheMap[_id]) for _id in missingIDsFromShotgun])

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
                shotgunData = utils.sortMultiEntityFieldsByID(self.entityConfigManager.schema, shotgunData)
                cacheData = utils.sortMultiEntityFieldsByID(self.entityConfigManager.schema, cacheData)

                shotgunJson = utils.prettyJson(shotgunData)
                cacheJson = utils.prettyJson(cacheData)
                if shotgunJson != cacheJson:
                    diff = difflib.unified_diff(
                        str(shotgunJson).split('\n'),
                        str(cacheJson).split('\n'),
                        lineterm="",
                        n=5,
                    )
                    # Skip first 3 lines
                    header = '{type}:{id}\n'.format(type=work['configType'], id=_id)
                    [diff.next() for x in range(3)]
                    diff = header + '\n'.join(diff)
                    diffs.append(diff)

            result = {
                'work': work,
                'entityType': work['configType'],
                'failed': failed,
                'shotgunMatchCount': len(shotgunMap),
                'cacheMatchCount': len(cacheMap),
                'missingFromCache': missingFromCache,
                'missingFromShotgun': missingFromShotgun,
                'diffs': diffs,
            }
            self.resultQueue.put(result)

            self.workQueue.task_done()
