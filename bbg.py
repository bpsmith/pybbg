"""
methods for using the old bloomberg COM API from python

Written by Brian P. Smith (brian.p.smith@gmail.com)
"""
from datetime import datetime
from win32com.client import Dispatch
import numpy as np
import pandas
import pywintypes


def _convert_value(v, replace_na=True):
    if isinstance(v, pywintypes.TimeType):
        return datetime(year=v.year, month=v.month, day=v.day)
    elif isinstance(v, float):
        return v
    elif isinstance(v, basestring):
        if v.startswith('#N/A'):
            return np.nan
        return str(v)
    else:
        return np.nan


def get_data_bbg_historical(symbol, flds, start=None, end=None):
    """
    Get historical data from bloomberg. Retrieve the flds for the specified start to end date for the given symbol.

    symbol: Bloomberg identifier
    flds: list of bloomberg fields to retrieve
    """
    bbg = Dispatch('Bloomberg.Data.1')

    from pandas.io.data import _sanitize_dates
    start, end = _sanitize_dates(start, end)

    data = bbg.BLPGetHistoricalData(symbol, flds, pywintypes.Time(start.timetuple()), pywintypes.Time(end.timetuple()))
    # Convert to datetime64 and ensure nan's for strings
    cdata = zip(*[ map(_convert_value, r[0]) for r in data ])
    flds = ['Date'] + list(flds)
    frame = pandas.DataFrame(dict((cname, cdata[i]) for i, cname in enumerate(flds)), columns=flds)
    return frame.set_index('Date')



def get_data_bbg_live(symbols, flds, replace_na=True):
    """
    Get the live data for the specified fields from Bloomberg. if replace_na is true then convert all #N/As to numpy NaN's.

    symbols: Bloomberg identifier(s)
    flds: list of bloomberg fields to retrieve
    """
    bbg = Dispatch('Bloomberg.Data.1')

    if isinstance(symbols, basestring):
        symbols = [symbols]
    elif not isinstance(symbols, (list, tuple)):
        raise TypeError('symbols must be list or tuple')

    if isinstance(flds, basestring):
        flds = [flds]

    frames = []
    for symbol in symbols:
        data = bbg.BLPSubscribe(symbol, flds)[0]
        data = [_convert_value(v, replace_na) for v in data ]
        frame = pandas.DataFrame(dict((n, data[i]) for i, n in enumerate(flds)), columns=flds, index=[symbol])
        frames.append(frame)
    return pandas.concat(frames)




