import time
import logging
import rethinkdb as r

import utils

__all__ = [
    'MessageQueueMonitor',
]

LOG = logging.getLogger(__name__)


class MessageQueueMonitor(object):
    """
    Handle messages posted from external sources such as the
    rebuild command or the shotgunCacheProxy
    """
    def __init__(self, controller, config, workQueue):
        super(MessageQueueMonitor, self).__init__()
        self.controller = controller
        self.config = config
        self.workQueue = workQueue
        self.rethink = self.config.create_rethink_connection()

    def start(self):
        tableList = r.table_list().run(self.rethink)
        if 'message_queue' not in tableList:
            r.table_create('message_queue').run(self.rethink)

        self.run()

    def run(self):
        LOG.debug("Watching message queue")

        # exchange = repubsub.Exchange('message_queue', **self.config['rethink'])
        # First load the list of messages
        query = r.table('message_queue').filter(lambda msg: msg['topic'].match('^server.'))
        changesQuery = query.changes()

        pastMessages = list(query.run(self.rethink))
        LOG.debug("Number of old messages in message queue: {0}".format(len(pastMessages)))
        for msg in pastMessages:
            # TODO
            # handle any past messages that actually need to be considered
            LOG.debug("Removing old message: {0}".format(msg['id']))
            r.table('message_queue').get(msg['id']).delete().run(self.rethink)

        for change in changesQuery.run(self.rethink):
            msg = change['new_val']
            if msg is None or 'id' not in msg:
                continue

            LOG.debug("New Message:\n{0}".format(utils.pretty_json(msg)))
            r.table('message_queue').get(msg['id']).delete().run(self.rethink)

            handleName = 'handle_' + msg['topic'].split('server.', 1)[1].replace('.', '_')

            try:
                handler = getattr(self, handleName)
            except AttributeError:
                raise AttributeError("No handler for msg topic: {0}".format(handleName))
            handler(msg)

    def handle_update_cache(self, msg):
        LOG.debug("Forcing cache update")

        exc = None
        st = time.time()
        try:
            self.controller.monitor.process_new_events()
        except Exception, exc:
            pass
        duration = time.time() - st

        # TODO - who cleans these messages up?
        resultMsg = {
            'topic': 'cli.update_cache',
            'payload': {
                'id': msg['id'],
                'exc': str(exc),
                'duration': duration,
            },
            'updated_on': r.now()
        }

        r.table('message_queue').insert(resultMsg).run(self.rethink)

        if exc is not None:
            raise

        LOG.debug("Finished update cache for message: {0}".format(msg['id']))

    def handle_rebuild(self, msg):
        configTypes = msg['payload']['configTypes']
        configs = [self.controller.entityConfigManager.get_config_for_type(t) for t in configTypes]

        LOG.info("Rebuilding entities for {0}".format(', '.join(configTypes)))
        exc = None
        st = time.time()
        try:
            self.controller.importer.import_entities(configs)
        except Exception, exc:
            # Store the exception so we can send it back
            # we'll reraise it later here
            pass
        duration = time.time() - st

        # TODO - who cleans these messages up?
        resultMsg = {
            'topic': 'cli.rebuild',
            'payload': {
                'id': msg['id'],
                'exc': str(exc),
                'duration': duration,
            },
            'updated_on': r.now()
        }

        r.table('message_queue').insert(resultMsg).run(self.rethink)

        if exc is not None:
            raise

        LOG.debug("Finished rebuild for message: {0}".format(msg['id']))
