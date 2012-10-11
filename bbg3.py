"""
methods for using the bloomberg COM API v3 from python

Written by Brian P. Smith (brian.p.smith@gmail.com)
"""
from pythoncom import PumpWaitingMessages
from win32com.client import DispatchWithEvents, constants, CastTo
from collections import defaultdict, OrderedDict
from datetime import datetime
import pandas
import numpy as np
import pywintypes


def _convert_value(v):
    """ Convert the Bloomberg COM value to a python type """
    if isinstance(v, pywintypes.TimeType):
        return datetime(year=v.year, month=v.month, day=v.day, hour=v.hour, minute=v.minute, second=v.second)
    elif isinstance(v, basestring):
        if v.startswith('#N/A'):
            return np.nan
        return str(v)
    else:
        return v


def _get_value(element, name):
    if element.HasElement(name):
        v = element.GetElement(name).Value
        return _convert_value(v)
    else:
        return np.nan


def _read_table(element):
    """ Transform the fieldData child for a bulk data element to a frame """
    cols = None
    data = []
    for i in range(element.NumValues):
        row = element.GetValue(i)
        if cols is None:
            cols = [ row.GetElement(_).Name for _ in range(row.NumElements) ]
        data.append( [ _convert_value( row.GetElement(_).Value ) for _ in range(row.NumElements) ] )
    if cols:
        cdata = zip(*data)
        return pandas.DataFrame( dict( zip(cols, cdata) ), columns=cols)
    else:
        return None


class ResponseHandler(object):
    def OnProcessEvent(self, evt):
        try:
            evt = CastTo(evt, 'Event')
            if evt.EventType == constants.RESPONSE:
                self.process_event(evt)
                self.waiting = False
            elif evt.EventType == constants.PARTIAL_RESPONSE:
                self.process_event(evt)
            else:
                self.process_admin_event(evt)
        except Exception, e:
            import sys
            self.waiting = False
            self.exc_info = sys.exc_info()
            raise

    def process_event(self, evt):
        raise NotImplementedError()

    def process_admin_event(self, evt):
        pass

    def verify_security_response(self, sd):
        """ check to see if the securityData element has exceptions related to security request or field data requests """
        if sd.HasElement('securityError'):
            ecat = sd.GetElement('securityError').GetElement('category').Value
            emsg = sd.GetElement('securityError').GetElement('message').Value
            raise Exception('SecurityError: %s - %s: %s' % (sd.GetElement('security').Value, ecat, emsg))
        elif sd.HasElement('fieldExceptions') and sd.GetElement('fieldExceptions').NumValues:
            fldexarr = sd.GetElement('fieldExceptions')
            bfr = []
            for i in range(fldexarr.NumValues):
                fldex = fldexarr.GetValue(i)
                fldid = fldex.GetElement('fieldId').Value
                ecat = fldex.GetElement('errorInfo').GetElement('category').Value
                emsg = fldex.GetElement('errorInfo').GetElement('message').Value
                bfr.append('FieldError: %s - %s: %s' % (fldid, ecat, emsg))
            raise Exception('\n'.join(bfr))


class ReferenceDataResponseHandler(ResponseHandler):

    def process_event(self, evt):
        iter = evt.CreateMessageIterator()
        while iter.Next():
            msg = iter.Message
            #print msg.Print
            if msg.AsElement.HasElement('responseError'):
                raise Exception(msg.AsElement.GetValue('message'))

            sdarray = msg.GetElement('securityData')
            for i in range(sdarray.NumValues):
                sd = sdarray.GetValue(i)
                self.verify_security_response(sd)
                secid, fd = sd.GetElement('security').Value, sd.GetElement('fieldData')
                self.results['security'].append(secid)
                [ self.results[f].append( _get_value(fd, f) ) for f in self.fields]


class HistoricalResponseHandler(ResponseHandler):

    def process_event(self, evt):
        iter = evt.CreateMessageIterator()
        while iter.Next():
            msg = iter.Message
            if msg.AsElement.HasElement('responseError'):
                raise Exception(msg.AsElement.GetValue('message'))

            sd = msg.GetElement('securityData')
            self.verify_security_response(sd)
            secid, fd = sd.GetElement('security').Value, sd.GetElement('fieldData')
            dmap = defaultdict(list)
            for j in range(fd.NumValues):
                period = fd.GetValue(j)
                dmap['date'].append( _get_value(period, 'date') )
                [ dmap[f].append( _get_value(period, f) ) for f in self.fields ]

            idx = dmap.pop('date')
            frame = pandas.DataFrame(dmap, columns=self.fields, index=idx)
            frame.index.name = 'date'
            self.results[secid] = frame


class IntradayBarResponseHandler(ResponseHandler):

    def process_event(self, evt):
        iter = evt.CreateMessageIterator()
        while iter.Next():
            msg = iter.Message
            #print msg.Print
            if msg.AsElement.HasElement('responseError'):
                raise Exception(msg.AsElement.GetValue('message'))

            bars = msg.GetElement('barData').GetElement('barTickData')
            r = self.results
            for i in range(bars.NumValues):
                bar = bars.GetValue(i)
                # time, open, high, low, close, volume
                ts = bar.GetElement(0).Value
                r['time'].append( datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute) )
                r['open'].append( bar.GetElement(1).Value )
                r['high'].append( bar.GetElement(2).Value )
                r['low'].append( bar.GetElement(3).Value )
                r['close'].append( bar.GetElement(4).Value )
                r['volume'].append( bar.GetElement(5).Value )
                r['events'].append( bar.GetElement(6).Value )


class BulkReferenceDataResponseHandler(ResponseHandler):

    def process_event(self, evt):
        iter = evt.CreateMessageIterator()
        while iter.Next():
            msg = iter.Message
            #print msg.Print
            if msg.AsElement.HasElement('responseError'):
                raise Exception(msg.AsElement.GetValue('message'))

            sdarray = msg.GetElement('securityData')
            for i in range(sdarray.NumValues):
                sd = sdarray.GetValue(i)
                self.verify_security_response(sd)
                secid, fd = sd.GetElement('security').Value, sd.GetElement('fieldData')
                for j in range(fd.NumValues):
                    bulkfld = fd.GetElement(j)
                    frame = _read_table(bulkfld)
                    frame['security'] = secid
                    if frame is not None:
                        if self.results is None:
                            self.results = frame
                        else:
                            self.results = pandas.concat([self.results, frame])


def get_data_bbg_historical(symbols, flds, start=None, end=None, period='DAILY'):
    """
    Get historical data from bloomberg. Retrieve the flds for the specified start to end date for the given symbol. Currently
    all results are returned with DatetimeIndex without a frequency being set.

    symbols: Bloomberg identifier(s)
    flds: list of bloomberg fields to retrieve
    start: start date
    end: end date
    period: periodicity of data, either ('DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'SEMI-ANNUAL', 'YEARLY')
    """
    if isinstance(symbols, basestring):
        symbols = [symbols]
    elif not isinstance(symbols, (list, tuple)):
        raise TypeError('symbols must be list or tuple')

    if isinstance(flds, basestring):
        flds = [flds]

    from pandas.io.data import _sanitize_dates
    start, end = _sanitize_dates(start, end)

    session = DispatchWithEvents('blpapicom.ProviderSession.1', HistoricalResponseHandler)
    session.Start()
    if not session.OpenService('//blp/refdata'):
        raise Exception('failed to open service')

    rfd = session.GetService('//blp/refdata')
    request = rfd.CreateRequest('HistoricalDataRequest')

    # configure historical request
    [ request.GetElement('securities').AppendValue(s) for s in symbols ]
    [ request.GetElement('fields').AppendValue(fld) for fld in flds ]
    request.Set('startDate', start.strftime('%Y%m%d'))
    request.Set('endDate', end.strftime('%Y%m%d'))
    request.Set('periodicitySelection', period)

    # event loop
    cid = session.SendRequest(request)
    session.exc_info = None
    session.waiting = True
    session.fields = flds
    session.results = defaultdict(list)

    while session.waiting:
        PumpWaitingMessages()
    session.Stop()

    if session.exc_info is not None:
        raise session.exc_info[1], None, session.exc_info[2]
    else:
        data = OrderedDict()
        idx = None

        if True:
            # Return the frame with security as column
            frames = []
            for k, v in session.results.iteritems():
                v = v.reset_index()
                v.insert(1, 'security', k)
                frames.append(v)
            result = pandas.concat(frames, ignore_index=1)
            result = result.set_index(['date', 'security'])
            return result
        else:
            # Return the frame with security as a column header
            for secid in symbols:
                frame = session.results[secid]
                [ data.setdefault( (secid, s.name), s) for k,s in frame.iteritems() ]
                if idx is None:
                    idx = frame.index
                else:
                    idx = idx.union(idx)
            return pandas.DataFrame(data, index=idx, columns=pandas.MultiIndex.from_tuples(data.keys(), names=['security', 'fields']))


def get_data_bbg_live(symbols, flds, replace_na=True, overrides=None):
    """
    Get the live data for the specified fields from Bloomberg. if replace_na is true then convert all #N/As to numpy NaN's.

    symbols: Bloomberg identifier(s)
    flds: list of bloomberg fields to retrieve
    overrides - map of bloomberg key and the override value
    """
    if isinstance(symbols, basestring):
        symbols = [symbols]
    elif not isinstance(symbols, (list, tuple)):
        raise TypeError('symbols must be list or tuple')

    if isinstance(flds, basestring):
        flds = [flds]

    session = DispatchWithEvents('blpapicom.ProviderSession.1', ReferenceDataResponseHandler)
    session.Start()
    if not session.OpenService('//blp/refdata'):
        raise Exception('failed to open service')

    rfd = session.GetService('//blp/refdata')
    request = rfd.CreateRequest('ReferenceDataRequest')

    # configure reference data request
    [ request.GetElement('securities').AppendValue(s) for s in symbols ]
    [ request.GetElement('fields').AppendValue(fld) for fld in flds ]
    if overrides:
        for k, v in overrides.iteritems():
            o = request.GetElement('overrides').AppendElment()
            o.SetElement('fieldId', k)
            o.SetElement('value', v)

    # event loop
    cid = session.SendRequest(request)
    session.exc_info = None
    session.waiting = True
    session.fields = flds
    session.results = defaultdict(list)

    while session.waiting:
        PumpWaitingMessages()
    session.Stop()

    if session.exc_info is not None:
        raise session.exc_info[1], None, session.exc_info[2]
    else:
        idx = session.results.pop('security')
        return pandas.DataFrame( session.results, columns=flds, index=idx)


def get_data_bbg_intraday_bar(symbol, interval, start, end=None, event='TRADE'):
    """
    Retrieve the summary intervals for intraday data covering five event types.

    Parameters
    ----------
    symbol : Bloomberg identifier
    interval : bar interval in minutes
    start : start datetime
    end : end datetime
    event : event type to request (TRADE,BID,ASK,BEST_BID,BEST_ASK)

    Returns:
    df: DataFrame with columns for timestamp, open, high, low, close, volume, events

    """
    assert isinstance(symbol, basestring), 'get_data_bbg_intraday_bar accepts only a single security'
    end = end or datetime.now()
    session = DispatchWithEvents('blpapicom.ProviderSession.1', IntradayBarResponseHandler)
    session.Start()
    if not session.OpenService('//blp/refdata'):
        raise Exception('failed to open service')

    rfd = session.GetService('//blp/refdata')
    request = rfd.CreateRequest('IntradayBarRequest')
    # configure reference data request
    request.Set('security', symbol)
    request.Set('interval', interval)
    request.Set('eventType', event)
    request.Set('startDateTime', session.CreateDatetime(start.year, start.month, start.day, start.hour, start.minute) )
    request.Set('endDateTime', session.CreateDatetime(end.year, end.month, end.day, end.hour, end.minute) )

    # event loop
    cid = session.SendRequest(request)
    session.exc_info = None
    session.waiting = True
    session.event = event
    session.results = defaultdict(list)

    while session.waiting:
        PumpWaitingMessages()
    session.Stop()

    if session.exc_info is not None:
        raise session.exc_info[1], None, session.exc_info[2]
    else:
        idx = session.results.pop('time')
        return pandas.DataFrame( session.results, columns=['open', 'high', 'low', 'close', 'volume', 'events'], index=idx)


def get_data_bbg_bulk(symbols, fld, overrides=None):
    """
    Special method to receive 'bulk' data items. (item which has results returned in a table). This method will read the table and return it as a data frame.

    Parameters:
    symbols - bloomberg security identifier(s)
    fld - bloomberg bulk field
    overrides - dict of bloomberg overrides
    """
    if isinstance(symbols, basestring):
        symbols = [symbols]
    elif not isinstance(symbols, (list, tuple)):
        raise TypeError('symbols must be list or tuple')

    assert isinstance(fld, basestring), 'must specify a single bulk field'

    session = DispatchWithEvents('blpapicom.ProviderSession.1', BulkReferenceDataResponseHandler)
    session.Start()
    if not session.OpenService('//blp/refdata'):
        raise Exception('failed to open service')

    rfd = session.GetService('//blp/refdata')
    request = rfd.CreateRequest('ReferenceDataRequest')

    # configure reference data request
    [ request.GetElement('securities').AppendValue(s) for s in symbols ]
    request.GetElement('fields').AppendValue(fld)
    if overrides:
        for k, v in overrides.iteritems():
            o = request.GetElement('overrides').AppendElment()
            o.SetElement('fieldId', k)
            o.SetElement('value', v)

    # event loop
    cid = session.SendRequest(request)
    session.exc_info = None
    session.waiting = True
    session.fields = [ fld ]
    session.results = None

    while session.waiting:
        PumpWaitingMessages()
    session.Stop()

    if session.exc_info is not None:
        raise session.exc_info[1], None, session.exc_info[2]
    else:
        return session.results


if __name__ == '__main__':
    # 5 days ago
    d = pandas.datetools.BDay(-5).apply(datetime.now())
    m = pandas.datetools.BMonthBegin(-3).apply(datetime.now())

    # Retrieve reference data
    securities = ['IBM US EQUITY', 'VOD LN EQUITY', 'DELL US EQUITY']
    fields = ['NAME', 'PX_LAST', 'EQY_WEIGHTED_AVG_PX', 'CRNCY', 'PRICING_SOURCE']
    print get_data_bbg_live(securities, fields)

    # Retrieve reference data with overrides
    overrides = {'VWAP_START_DT' : d.strftime('%Y%m%d'), 'VWAP_END_DT' : d.strftime('%Y%m%d'), 'VWAP_START_TIME' : '9:30', 'VWAP_END_TIME' : '10:30'}
    print get_data_bbg_live(securities, fields, overrides=overrides)

    # Retrieve bulk data field
    print get_data_bbg_bulk('csco us equity', 'EQY_DVD_HIST') # single security
    print get_data_bbg_bulk(['msft us equity', 'csco us equity'], 'BDVD_PR_EX_DTS_DVD_AMTS_W_ANN') # multi-security
    print get_data_bbg_bulk('eurusd curncy', 'DFLT_VOL_SURF_MID')  # vol surface
    print get_data_bbg_bulk('eurusd curncy', 'FWD_CURVE')  # fx forward curve

    # Retrive historical data
    print get_data_bbg_historical(['msft us equity', 'intc us equity'], ['px_last', 'px_open'], start=d)
    print get_data_bbg_historical(['msft us equity', 'intc us equity'], ['px_last', 'px_open'], start=m, period='WEEKLY')

    # Retrive 60 minute intraday bar data
    print get_data_bbg_intraday_bar('msft us equity', 60, start=d)

