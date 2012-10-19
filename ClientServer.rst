Client Server example
=====================

Server
==================

Running from command prompt ::

> python.exe service.py --hostport HOST:PORT

Client
===============================

Running from IPython::

> from bbg3 import HistoricalDataRequest
> from service import Client
> req = HistoricalDataRequest(['msft us equity', 'intc us equity'], ['px_open', 'px_last'])
> client = Client('http://HOST:PORT')
> res = client.execute_request(req)
> print res.response
