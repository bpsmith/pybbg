Client Server example
=====================

Running the server
==================

> python.exe service.py --hostport HOST:PORT

Running the client
==================
> from bbg3 import BbgHistoricalDataRequest
> from service import Client
> req = BbgHistoricalDataRequest(['msft us equity', 'intc us equity'], ['px_open', 'px_close'])
> client = Client('http://HOST:PORT')
> res = client.execute_request(req)
> print res
