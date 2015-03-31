
import mbotenv

import multiprocessing

import elasticsearch
import shotgun_api3 as sg_api

LOG = mbotenv.get_logger(__name__)
LOG.level = 10

class DBWorker(multiprocessing.Process):
    def __init__(self, queue, config):
        super(DBWorker, self).__init__()
        self.queue = queue
        self.config = config
        self._init_shotgun()
        self._init_elastic()

    def _init_shotgun(self):
        self.sg = sg_api.Shotgun(**self.config['shotgun'])
        LOG.debug('Connected to Shotgun')
        return self._sg

    def _init_elastic(self):
        config = self.config['elastic']
        self.elastic = elasticsearch.Elasticsearch(**config['elastic'])
        LOG.debug('Connected to Elastic')

    def run(self):
        while True:
            work = self.queue.get()
            print("work: {0}".format(work)) # TESTING

# TODO
# Integrate code below

# def entityChanged(self, entityType, entityID, eventData):
#     LOG.debug("Entity Changed")
#     data = {'type': entityType, 'id': entityID, 'data': eventData}
#     if not self.initialized:
#         self.startupQueue.put(data)
#         return
#     self.workQueue.put(data)
# self.context = zmq.Context()
# self.socket = self.context.socket(zmq.SUB)
# self.socket.connect(config['shotgunEventsZMQHost'])
# self.socket.setsockopt(zmq.SUBSCRIBE, "")
# self.poller = zmq.Poller()
# self.poller.register(self.socket, zmq.POLLIN)

# def run(self):
#     # self.importEntities()
#     while True:
#         socks = dict(poller.poll())
#         if self.socket in socks and socks[self.socket] == zmq.POLLIN:
#             message = self.socket.recv()
#             print 'Received message: {0}'.format(message)

#         while
#         # I need some way to batch load multiple events
#         event = self.socket.recv()
#         print("event: {0}".format(event)) # TESTING





# class ShotgunElasticConnection(object):
#     def __init__(self):
#         super(ShotgunElasticConnection, self).__init__()
#         self.es = elasticsearch.Elasticsearch()
#         self.sg = hourglass.sg.instance()

#         self.context = zmq.Context()
#         self.socket = self.context.socket(zmq.SUB)
#         self.socket.connect(config['shotgunEventsZMQHost'])
#         self.socket.setsockopt(zmq.SUBSCRIBE, "")
#         self.poller = zmq.Poller()
#         self.poller.register(self.socket, zmq.POLLIN)

#     def importEntities(self, sgTypes=None, force=False):
#         if sgTypes is None:
#             sgTypes = self.getAvailableConfigs()

#         for sgType in sgTypes:
#             typeConfig = self.getConfigForType(sgType)
#             esType = utils.get_elastic_type(sgType)
#             mappings = self.buildMappings(esType, typeConfig)
#             body = {
#                 'mappings': mappings
#             }
#             index = utils.get_elastic_index_for_entity_type(sgType)

#             LOG.debug("Creating Index")
#             if self.es.indices.exists(index=index):
#                 if force:
#                     self.es.indices.delete(index=index)
#                 else:
#                     raise IOError("Index already exists")

#             self.es.indices.create(
#                 index=index,
#                 body=body
#             )

#             LOG.debug("Creating Entities")
#             self.createEntities(sgType, esType, index, typeConfig)

#     def getAvailableConfigs(self):
#         result = []
#         configFolderPath = os.path.abspath(config['entityConfigPath'])
#         if not os.path.exists(configFolderPath):
#             raise IOError("Config folder path doesn't exist: {0}".format(configFolderPath))
#         for item in os.listdir(configFolderPath):
#             if not item.startswith('_') and item.endswith('.json'):
#                 result.append(item.split('.', 1)[0])
#         return result

#     def getConfigForType(self, sgType):
#         configFolderPath = os.path.abspath(config['entityConfigPath'])
#         configPath = os.path.join(configFolderPath, sgType + '.json')
#         if not os.path.exists(configPath):
#             raise IOError("Config path doesn't exist: {0}".format(configPath))
#         with open(configPath, 'r') as f:
#             result = json.loads(f.read())
#         return result

#     def buildMappings(self, esType, typeConfig):
#         result = {}
#         typResult = result.setdefault(esType, copy.deepcopy(typeConfig.get('defaultMapping', {})))
#         # result = {}

#         for field, settings in typeConfig.get('fields', {}).items():
#             if 'mapping' in settings:
#                 LOG.debug("Found custom mapping for field: {0}".format(field))
#                 prop = typResult.setdefault('properties', {})
#                 prop[field] = settings['mapping']

#         return result

#     def generateEntityConfigFiles(self, sgTypes, force=False):
#         schema = self.sg.schema_read()
#         for sgType in sgTypes:
#             if sgType not in schema:
#                 raise ValueError("Missing shotgun entity type: {0}".format(sgType))

#             destFolderPath = os.path.abspath(config['entityConfigPath'])
#             destPath = os.path.join(destFolderPath, '{type}.json'.format(type=sgType))
#             if not force and os.path.exists(destPath):
#                 LOG.debug("Entity Config already exists for: {0}".format(destPath))
#                 return

#             entityConfig = OrderedDict()

#             defaultMapping = config.get('defaultMapping', None)
#             if defaultMapping:
#                 entityConfig['defaultMapping'] = defaultMapping

#             typeSchema = schema[sgType]
#             # print envtools.format_dict(typeSchema)
#             fields = typeSchema.keys()
#             fieldsConfig = entityConfig['fields'] = OrderedDict()
#             for field in sorted(fields):
#                 fieldConfig = {}
#                 fieldSchema = typeSchema[field]

#                 fieldDataType = fieldSchema.get('data_type', {}).get('value', None)
#                 if fieldDataType == 'multi_entity':
#                     fieldConfig['mapping'] = {'type': 'nested', 'include_in_parent': True}
#                 elif fieldDataType == 'image':
#                     # Don't store these yet
#                     # TODO binary support
#                     continue

#                 fieldsConfig[field] = fieldConfig

#             if not os.path.exists(destFolderPath):
#                 os.makedirs(destFolderPath)
#             with open(destPath, 'w') as f:
#                 f.write(json.dumps(entityConfig, indent=4))
#             LOG.debug("Created Entity Config Template: {0}".format(destPath))

#     def createEntities(self, sgType, esType, index, typeConfig):

#         fieldsToLoad = [f for f in typeConfig.get('fields', [])]

#         result = []
#         page = 1
#         while True:
#             filterOperator = typeConfig.get('filterOperator', 'all')
#             filters = typeConfig.get('filters', [])
#             limit = 250
#             try:
#                 sgEntities = self.sg.find(sgType, filters, fieldsToLoad, filter_operator=filterOperator, limit=limit, page=page)
#             except Exception, e:
#                 LOG.exception("Type: {0}, filters: {1}, fieldsToLoad: {2} filterOperator: {3} limit: {4} page: {5}".format(sgType, filters, fieldsToLoad, filterOperator, limit, page))
#                 raise

#             if not len(sgEntities):
#                 break

#             result.extend(sgEntities)

#             requests = []
#             for sgEntity in sgEntities:
#                 header = {
#                     "index": {
#                         '_index': index,
#                         '_type': esType,
#                         '_id': sgEntity['id'],
#                     }
#                 }

#                 body = sgEntity
#                 requests.extend([header, body])

#             LOG.debug("Importing {0} {1} entities from page {2} - {3} Total".format(len(requests) / 2, sgType, page, len(requests) / 2 + page * limit))
#             responses = self.es.bulk(body=requests)

#             if responses['errors']:
#                 for response in responses['items']:
#                     if 'error' not in response.get('index', {}):
#                         continue
#                     if response['index']['error']:
#                         LOG.exception(response['index']['error'])
#                 raise IOError("Errors occurred creating entities")

#             page += 1

# if __name__ == '__main__':
#     conn = ShotgunElasticConnection()
#     # First we generate our configs
#     # db.generateEntityConfigFiles(['Note'])
#     conn.generateEntityConfigFiles(
#         [
#             # 'Task',
#             # 'HumanUser',
#             # 'Project',
#             # 'Version',
#             # 'Note',
#             # 'Reply',
#             'HumanUser',  # TODO, limit these to only certain types
#             # 'Step',
#             # 'Asset',
#             # 'Shot',
#             # 'Department',
#             # 'Status',
#             # 'TimeLog',
#             # 'Ticket',
#             # 'ClientUser',
#             # 'Sequence',
#             # 'Scene',
#             # 'Tool'
#         ]
#     )

#     # Before importing, we should double check the configs
#     conn.run()
#     # conn.importEntities(['EventLogEntry'], force=True)

#     # db.importEntities(["HumanUser"], force=True)
