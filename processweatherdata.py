#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
process weather data from the regional transportation data archive
"""
import pandas as pd

# weather data
nws19 = pd.read_csv("D:/weather/NWS_19_aug19.csv",
                  names = ['weather','windchill_c','ob_url','windchill_f','dewpoint_string','relative_humidity',
                           'SourceID','dewpoint_f','location','dewpoint_c','latitude','wind_mph','temp_f','station_id',
                           'windchill_string','temp_c','wind_string','pressure_in','wind_kt','temperature_string','wind_dir',
                           'wind_degrees','observation_time','longitude','observation_time_rfc822','icon_url_name',
                           'image','privacy_policy_url','suggested_pickup_period','disclaimer_url','copyright_url','visibility_mi',
                           'two_day_history_url','icon_url_base','credit_URL','suggested_pickup','credit','pressure_mb',
                           'pressure_string','wind_gust_kt','wind_gust_mph','heat_index_c','heat_index_f','heat_index_string'])

nws18 = pd.read_csv("D:/weather/NWS_18.csv",
                  names = ['weather','ob_url','dewpoint_string','relative_humidity',
                           'SourceID','dewpoint_f','location','dewpoint_c','latitude','wind_mph','temp_f','station_id',
                           'temp_c','wind_string','pressure_in','wind_kt','temperature_string','wind_dir',
                           'wind_degrees','observation_time','longitude','observation_time_rfc822','icon_url_name',
                           'image','privacy_policy_url','suggested_pickup_period','disclaimer_url','copyright_url','visibility_mi',
                           'two_day_history_url','icon_url_base','credit_URL','suggested_pickup','credit',
                           'pressure_string','pressure_mb','windchill_c','windchill_f','windchill_string',
                           'mean_wave_degrees','wind_gust_mph','wind_gust_dir','wind_gust_kt','pressure_tendency_mb',
                           'heat_index_c','heat_index_f','heat_index_string'])

# I am getting the right time period for the NWS data to do the matching, which is 2018 9,10,11,12 and 2019 1,2,3,4,5
nws18['datetime'] = pd.to_datetime(nws18['observation_time_rfc822'], utc=True)
nws18.datetime = nws18.datetime.dt.tz_convert('US/Central')
nws18['year'] = nws18.datetime.dt.year
nws18['month'] = nws18.datetime.dt.month

# we convert it to UTC and back again?? (some are in CST but some are in EST)
nws19['datetime'] = pd.to_datetime(nws19['observation_time_rfc822'], utc=True)
nws19.datetime = nws19.datetime.dt.tz_convert('US/Central')
nws19['year'] = nws19.datetime.dt.year
nws19['month'] = nws19.datetime.dt.month

# looking for just 2018 sept, oct, nov to match the trips sample
nws_timeperiod = nws18[(nws18['year']==2018) & (nws18['month'].isin([9,10,11,12]))]
nws_timeperiod19 = nws19[(nws19['year']==2019) & (nws19['month'].isin([1,2,3,4,5]))]

nws_timeperiod.to_csv("D:/weather/NWS18selection.csv")
# we're missing most december days - need to get the data again but not on the server yet
nws_timeperiod19.to_csv("D:/weather/NWS19selection.csv")