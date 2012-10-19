"""
Expose the Bloomberg Desktop API as an XML RPC server.
"""
from bbg3 import BbgTerminal
from socket import gethostname
from SimpleXMLRPCServer import SimpleXMLRPCServer
from xmlrpclib import Binary, ServerProxy
import pickle
import logging
import sys

_logger = logging.getLogger(__name__)


def terminal_as_server(hostport=None):
    class BbgServer(object):
        def execute_request(self, brequest):
            request = pickle.loads(brequest.data)
            response = BbgTerminal.execute_request(request)
            return Binary(pickle.dumps(response))
    methods = BbgServer()
    hostport = hostport or (gethostname(), 3030)
    server = SimpleXMLRPCServer(hostport)
    server.register_instance(methods)
    _logger.info('starting server on %s:%s' % hostport)
    server.serve_forever()


class Client(object):
    def __init__(self, url):
        self.url = url
        self.proxy = ServerProxy(url, allow_none=1)

    def execte_request(self, request):
        brequest = Binary(pickle.dumps(request))
        bresponse = self.proxy.execute_request(brequest)
        return pickle.loads(bresponse.data)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--hostport', help='hostport to bind server to')
    args = parser.parse_args()

    if args.hostport:
        hostport = tuple( args.hostport.split(':') )
    else:
        hostport = None
    # start the server
    terminal_as_server(hostport)




