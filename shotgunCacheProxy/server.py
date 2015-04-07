#!/usr/bin/python
"""WSGI server example"""
from __future__ import print_function
from gevent.wsgi import WSGIServer
import os
import json
import urlparse

import mbotenv

import geventhttpclient.httplib
from gevent import monkey
monkey.patch_all()
from geventhttpclient.httplib import HTTPConnection
geventhttpclient.httplib.patch()

import shotgun_api3
import yaml

SCRIPT_DIR = os.path.dirname(__file__)
print("SCRIPT_DIR: {0}".format(SCRIPT_DIR)) # TESTING
CONFIG_PATH = os.path.dirname(os.path.abspath(SCRIPT_DIR)) + "/config.yaml"
print("CONFIG_PATH: {0}".format(CONFIG_PATH)) # TESTING

"""
What information do I need to connect?
  -


"""

HEADERS = {
    "content-type" : "application/json; charset=utf-8",
    "connection" : "keep-alive",
    "user-agent": "shotgun-json (3.0.18.dev); Python 2.7 (Mac)",
}
SG = None
SG_CONN = None
config = yaml.load(open(CONFIG_PATH, 'r').read())
SG = shotgun_api3.Shotgun(**config['shotgun'])

import debug

from geventconnpool import ConnectionPool
from gevent import socket

class _SGConnPool(ConnectionPool):
    def _new_connection(self):
        SG._connection = None
        conn = SG._get_connection()
        return conn
        # return socket.create_connection(('test.example.org', 2485))

SGConnPool = _SGConnPool(30)



@debug.timeIt()
def application(env, start_response):
    print('----- START ---------')
    global SG, SG_CONN
    contentLength = env['CONTENT_LENGTH']
    rawData = env['wsgi.input'].read(contentLength)
    SG_CONN = SG._get_connection()

    print('get connection')
    SG._connection = None
    conn = SG._get_connection()
    print('got connection')

    headers = {'user-agent': env['HTTP_USER_AGENT']}
    headers.update(HEADERS)

    url = urlparse.urlunparse((
        SG.config.scheme,
        SG.config.server,
        SG.config.api_path,
        None,
        None,
        None
    ))

    print('start conn')
    with SGConnPool.get() as conn:
        resp, content = conn.request(
            url,
            method=env['REQUEST_METHOD'],
            body=rawData,
            headers=headers,
        )
    print('stop conn')
    start_response(
        '{0} {1}'.format(resp.status, resp.reason),
        [('Content-Type', 'application/json')]
    )
    print('----- STOP ---------')
    return content





if __name__ == '__main__':
    print('Serving on 8080...')
    WSGIServer(('', 8080), application).serve_forever()





# if not env['PATH_INFO'] == '/api3/json':
#     raise ValueError("Shotgun Cache only supports api on at path /api3/json")

#     start_response('200 OK', [('Content-Type', 'text/html')])
#     return ["<b>hello world</b>"]

# contentLength = env['CONTENT_LENGTH']
# print("contentLength: {0}".format(contentLength)) # TESTING
# rawData = env['wsgi.input'].read(contentLength)
# print("rawData: {0}".format(rawData)) # TESTING
# data = json.loads(rawData)
# print("data: {0}".format(data)) # TESTING

# result = {}
# if data['method_name'] == 'info':
#     result = {
#         "version":[5,4,11],
#         "s3_uploads_enabled": True,
#         'from proxy': True,
#         "totango_site_id":"435",
#         "totango_site_name":"com_shotgunstudio_moonbotstudios"
#     }

# elif data['method_name'] == 'read':
#     result = {
#       "results": {
#         "entities": [
#           {
#             "type": "Task",
#             "id": 99
#           },
#           {
#             "type": "Task",
#             "id": 100
#           },
#           {
#             "type": "Task",
#             "id": 101
#           },
#           {
#             "type": "Task",
#             "id": 102
#           },
#           {
#             "type": "Task",
#             "id": 107
#           }
#         ]
#       }
#     }

# jsonStr = json.dumps(result)
# start_response('200 OK', [('Content-Type', 'application/json')])
# return [jsonStr]


# # else:

# #     return
# #     start_response('404 Not Found', [('Content-Type', 'text/html')])
# #     return ['<h1>Not Found</h1>']