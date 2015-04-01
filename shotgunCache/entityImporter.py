import os
import logging
import zmq
import math
import traceback

__all__ = [
    'EntityImporter',
]

LOG = logging.getLogger(__name__)
# LOG.level = 10

SG = None

class EntityImporter(object):
    def __init__(self, zmqPullUrl, zmqPostUrl, shotgunConnector, entityConfigs, batchSize=250):
        super(EntityImporter, self).__init__()
        self.shotgunConnector = shotgunConnector
        self.zmqPullUrl = zmqPullUrl
        self.zmqPostUrl = zmqPostUrl
        self.batchSize = batchSize
        self.entityConfigs = dict([(c.type, c) for c in entityConfigs])

        self.sg = None
        self.incomingContext = None
        self.incomingSocket = None
        self.outgoingContext = None
        self.outgoingSocket = None

    def start(self):
        self.sg = self.shotgunConnector.getInstance()

        self.incomingContext = zmq.Context()
        self.incomingSocket = self.incomingContext.socket(zmq.PULL)
        self.incomingSocket.connect(self.zmqPullUrl)

        self.outgoingContext = zmq.Context()
        self.outgoingSocket = self.outgoingContext.socket(zmq.PUSH)
        self.outgoingSocket.connect(self.zmqPostUrl)

        self.run()

    def run(self):
        LOG.debug("Running Entity Import Loop")
        # Exited using terminate
        # TODO
        # Maybe add a signal catch to shutdown nicely

        while True:
            work = self.incomingSocket.recv_pyobj()

            try:
                if work['type'] == 'getPageCount':
                    LOG.debug("Getting counts for type '{0}' on process {1}".format(work['configType'], os.getpid()))
                    entityConfig = self.entityConfigs[work['configType']]
                    entityCount = self.getEntityCount(entityConfig)
                    pageCount = int(math.ceil(entityCount / float(self.batchSize)))
                    result = {
                        'type': 'counts',
                        'data': {
                            'entityCount': entityCount,
                            'pageCount': pageCount,
                        },
                        'work': work,
                    }
                    self.outgoingSocket.send_pyobj(result)

                elif work['type'] == 'getEntities':
                    LOG.debug("Importing Entities for type '{0}' on page {1} on process {2}".format(work['configType'], work['page'], os.getpid()))
                    entityConfig = self.entityConfigs[work['configType']]
                    entities = self.getEntities(entityConfig, work['page'])
                    result = {
                        'type': 'entitiesImported',
                        'data': {
                            'entities': entities,
                        },
                        'work': work,
                    }
                    self.outgoingSocket.send_pyobj(result)

                    # TODO
                    # Handle typeComplete in main controller
                else:
                    raise ValueError("Unknown work type: {0}".format(work['type']))

            except Exception, e:
                result = {
                    'type': 'exception',
                    'data': {
                        'exc': e,
                        'traceback': traceback.format_exc()
                    },
                    'work': work
                }
                self.outgoingSocket.send_pyobj(result)

    def getEntities(self, entityConfig, page):
        try:
            kwargs = dict(
                entity_type=entityConfig.type,
                fields=entityConfig.get('fields', {}).keys(),
                filters=entityConfig.get('filters', []),
                filter_operator=entityConfig.get('filterOperator', 'all'),
                order=[{'column': 'id', 'direction': 'asc'}],
                limit=self.batchSize,
                page=page
            )
            result = self.sg.find(**kwargs)
        except Exception:
            # TODO
            # print command?
            LOG.exception("Type: {entity_type}, filters: {filters}, fields: {fields} filterOperator: {filter_operator}".format(**kwargs))
            raise

        return result

    def getEntityCount(self, entityConfig):
        result = self.sg.summarize(
            entity_type=entityConfig.type,
            filters=entityConfig.get('filters', []),
            filter_operator=entityConfig.get('filterOperator', 'all'),
            summary_fields=[{'field': 'id', 'type': 'count'}],
        )
        return result['summaries']['id']
