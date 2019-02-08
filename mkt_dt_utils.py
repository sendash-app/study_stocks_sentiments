import numpy as np
import pandas as pd

from datetime import datetime, time, timedelta
import pandas_market_calendars as mcal
from pandas.tseries.offsets import BDay
import pytz

def TimeConvert( inDateTime, OutZone):
    to_zone = pytz.timezone(OutZone)

    return inDateTime.astimezone(to_zone)

def IsMarketOpen(DateTimeObj, ExchangeName, debugmode = False):
    mkt = mcal.get_calendar(ExchangeName)
    tDateTime = TimeConvert(DateTimeObj, 'UTC')
    tDate = tDateTime.date()
    dateRange = pd.bdate_range( start = tDate - BDay(1), end = tDate + BDay(1))
    mkt_hours = mkt.schedule( start_date = dateRange[0], end_date = dateRange[-1])

    if debugmode:
        print(f'Current time UTC: {tDateTime}')
        print(mkt_hours)

    return mkt.open_at_time( schedule = mkt_hours, timestamp = tDateTime, include_close = True)

def MarketDateAdj( DateObj, IntBusinessDays , ExchangeName):
    mkt = mcal.get_calendar(ExchangeName)
    holidays = mkt.holidays()

    inDay = DateObj.date()
    outDay = inDay + BDay(IntBusinessDays)

    while outDay in holidays.holidays:
        outDay += BDay( np.sign(IntBusinessDays) * 1)

    return outDay

def days_hours_mins_secs( TimeDeltaObj):
    '''
    Note that in Python 3 // is for integer division
    '''
    td = TimeDeltaObj
    hours, remainder = divmod( td.seconds, 3600)
    minutes, seconds = divmod( remainder, 60)

    return td.days, hours, minutes, seconds

def GetTimeToMktOpen( DateTimeObj, ExchangeName, debugmode = False):
    # let's standardize time to UTC
    dt_now = TimeConvert(DateTimeObj, 'UTC')
    mkt = mcal.get_calendar(ExchangeName)
    sch = mkt.schedule( start_date = dt_now.date(),
                           end_date = MarketDateAdj(dt_now, 1, ExchangeName))

    close_time = sch['market_close'][0]

    # determine today's open or next day's open
    l_which_open = [h > dt_now for h in sch['market_open']]
    if l_which_open[0]:
        open_time = sch['market_open'][0]
    else:
        open_time = sch['market_open'][1]

    if IsMarketOpen(DateTimeObj, ExchangeName):
        # Show Time to Market Close
        tdelta = close_time.to_pydatetime() - dt_now

        if debugmode :
            print( f'--- Market is Open ---\nClose Time is {close_time}, Time Now is {dt_now}')

        return { 'status': 'open', 'd-h-m-s': days_hours_mins_secs(tdelta)}
    else:
        # Show Time to Next Market Open
        tdelta = open_time.to_pydatetime() - dt_now
        if debugmode :
            print( f'--- Market is Closed ---\nNext Open Time is {open_time}, Time Now is {dt_now}')
            print( f'\n--- Market Open Time ---\n{sch["market_open"]}')

        return { 'status': 'closed', 'd-h-m-s': days_hours_mins_secs(tdelta)}
