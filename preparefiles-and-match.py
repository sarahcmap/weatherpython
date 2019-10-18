#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: sbuchhorn
Last updated: 10/2/2019
Description: Starting with trip and weather data, match weather data to each trip
https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude/43211266#43211266
"""

import geopy.distance
import pandas as pd
import bisect
import numpy as np
import time


################################################
# Trips - read in and filter dates
trips = pd.read_csv("D:/weather/fromnickaug19/trips_time_location.csv")
trips = trips[trips['travdate'] >= '2019-01-01']


################################################
#  Weather
weatherdata = pd.read_csv("D:/weather/NWS19selection.csv")
weatherdatatest = weatherdata[['latitude', 'longitude', 'datetime', 'heat_index_c', 'heat_index_f',
                               'month', 'relative_humidity', 'temp_c', 'temp_f', 'weather', 'wind_gust_mph',
                               'wind_mph', 'windchill_c', 'windchill_f']]


################################################
# Weather Stations - get unique stations and save to refer to later
stations = pd.read_csv("D:/weather/2019_try2/stations19.csv")
stations = stations[['latitude', 'longitude', 'stationid']]
stationsdict = dict(zip(stations.index, stations.stationid))


################################################
# Trips + Weather Stations - match trip coords with weather station
places = trips[['latitude', 'longitude']]
uniplaces = places.drop_duplicates(['latitude','longitude']).reset_index(drop=True)
uniplaces['placeid'] = uniplaces.index

# get from file
closest_stationdf = pd.read_csv("D:/weather/2019_try2/closest_station19.csv")

# get station distances
closest_stationdf['placeid'] = closest_stationdf['placeid:stationid'].apply(lambda x: x.split(",")[0].strip("("))
closest_stationdf['stationid'] = closest_stationdf['placeid:stationid'].apply(lambda x: x.split(",")[1].strip(")"))
closest_stationdf['stationid'] = closest_stationdf['stationid'].astype(int).map(stationsdict)   # this means that all of the duplicates are still in this list

# need to know which stations are in 10,20,30, and >30 miles.
closest_stationdf.loc[(closest_stationdf['distance'] >= 30), 'distscore'] = 4
closest_stationdf.loc[(closest_stationdf['distance'] < 30), 'distscore'] = 3
closest_stationdf.loc[(closest_stationdf['distance'] < 20), 'distscore'] = 2
closest_stationdf.loc[(closest_stationdf['distance'] < 10), 'distscore'] = 1

# convert placeid to int
closest_stationdf['placeid'] = closest_stationdf['placeid'].astype('int64')

# i want to get this where the placeid is the index, the dist score is the header, and the station ids are the entries
binlist = closest_stationdf[closest_stationdf['distscore'] < 4].groupby(['placeid','distscore'])['stationid'].apply(list)
binlist = binlist.reset_index()
binlist['stationid'] = binlist.stationid.apply(set)
binlist = binlist.pivot(index='placeid',columns='distscore',values='stationid')

# need to get stationid into weatherdatatest via lat/long (which station goes with which observation)
stationreadings = stations.merge(weatherdatatest, on=['latitude', 'longitude']) # so we'll just want one of these rows for weather data
stationreadings['datetime'] = pd.to_datetime(stationreadings['datetime'], utc=True)
stationreadings['datetimecentral'] = stationreadings.datetime.dt.tz_convert('US/Central')


################################################
# Add placeid to trips
# get placeid into trips via lat/long
tripswplaceid = trips.merge(uniplaces, on=['latitude', 'longitude'])

# prepare time, add station categories
trips2 = tripswplaceid[tripswplaceid['deptime'] != "-1"]
trips2['deptimedt'] = pd.to_datetime(trips2['deptime'], utc=False)
trips2['deptimedtcentral'] = pd.to_datetime(trips2['deptimedt']).dt.tz_localize('US/Central')
trips2.reset_index(inplace=True)
trips2 = trips2.merge(binlist,left_on='placeid',right_on=binlist.index)
trips2[[1,2,3]] = trips2[[1,2,3]].fillna(999)
trips2.loc[trips2[1] == 999, [1]] = trips2[1].apply(lambda x: [x])
trips2.loc[trips2[2] == 999, [2]] = trips2[2].apply(lambda x: [x])
trips2.loc[trips2[3] == 999, [3]] = trips2[3].apply(lambda x: [x])

# adding weather columns in
for x in ['stationid','heat_index_c','heat_index_f','relative_humidity','temp_c','temp_f','weather',
         'wind_gust_mph','wind_mph','windchill_c','windchill_f','score']:
    trips2[x] = np.nan
trips2['datetimecentral'] = pd.datetime(2099, 9, 9, 9, 9, 9, 9)

# remove reference to initial df so we don't get warnings
trips3 = trips2.copy()


################################################
# Weather Assignment

def getReading(stationlist, timebefore, timeafter, score, timem, i):
    choices = stationreadings[stationreadings['stationid'].isin(stationlist)].sort_values('datetimecentral',
                                                                                  ascending=True)
    choicesseries = choices['datetimecentral']
    choiceslist = [x for x in choicesseries]
    t = bisect.bisect_left(choiceslist, timem)
    # t is the id of the entry from choiceslist one after timetomatch
    readingb = choices[choices['datetimecentral'] == choiceslist[t - 1]]
    if len(readingb) > 1:
        readingb = readingb.iloc[[0]]
    try:    # normal
        readinga = choices[choices['datetimecentral'] == choiceslist[t]]
        if len(readinga) > 1:
            readinga = readinga.iloc[[0]]

        timediffb = timem - readingb['datetimecentral']
        # print(timediffb)
        if (timediffb < timebefore).bool():
            readingb.index = [i]
            readingb['score'] = score
            plugin = readingb[['stationid',
                               'heat_index_c', 'heat_index_f', 'relative_humidity', 'temp_c', 'temp_f',
                               'weather', 'wind_gust_mph', 'wind_mph', 'windchill_c', 'windchill_f',
                               'datetimecentral',
                               'score']]
            return plugin

        timediffa = readinga['datetimecentral'] - timem
        # print(timediffa)
        if (timediffa < timeafter).bool():
            readinga.index = [i]
            readinga['score'] = score
            plugin = readinga[['stationid',
                               'heat_index_c', 'heat_index_f', 'relative_humidity', 'temp_c', 'temp_f',
                               'weather', 'wind_gust_mph', 'wind_mph', 'windchill_c', 'windchill_f',
                               'datetimecentral',
                               'score']]
            return plugin

    except IndexError:
        # proceed with only before reading
        timediffb = timem - readingb['datetimecentral']
        # print(timediffb)
        if (timediffb < timebefore).bool():
            readingb.index = [i]
            readingb['score'] = score
            plugin = readingb[['stationid',
                               'heat_index_c', 'heat_index_f', 'relative_humidity', 'temp_c', 'temp_f',
                               'weather', 'wind_gust_mph', 'wind_mph', 'windchill_c', 'windchill_f', 'datetimecentral',
                               'score']]
            return plugin

    return pd.Series()


def readingProcess(start, end):
    starttime = time.time()

    for i in range(start, end):
        one = list(trips3.loc[i][1])
        two = list(trips3.loc[i][2])
        three = list(trips3.loc[i][3])
        print(one, two, three)

        # this is wasteful - improve!
        if 999 in one:
            one.remove(999)
        if 999 in two:
            two.remove(999)
        if 999 in three:
            three.remove(999)

        timem = trips3.loc[i]['deptimedtcentral']

        while True:
            if len(one) > 0:
                plugin = getReading(one, '01:00:00', '00:30:00', 'A', timem, i)
                if not plugin.empty:
                    trips3.update(plugin)
                    break
                if plugin.empty:
                    # print('oneempty')
                    one.clear()

            if len(two) > 0:
                plugin = getReading(two, '01:00:00', '00:30:00', 'B', timem, i)
                if not plugin.empty:
                    trips3.update(plugin)
                    break
                if plugin.empty:
                    # print('twoempty')
                    two.clear()

            if len(three) > 0:
                plugin = getReading(three, '01:00:00', '00:30:00', 'C', timem, i)
                if not plugin.empty:
                    trips3.update(plugin)
                    break
                if plugin.empty:
                    # print('threeempty')
                    three.clear()

            plugin = pd.Series('F',index=[i],name='score')
            trips3.update(plugin)
            print('no matches')
            break

    trips3.to_csv("D:/weather/2019_try3/2019results4.csv")
    print(time.time() - starttime)


readingProcess(0, len(trips3))
# troubleshooting
readingProcess(160, 175)

################################################
# Post-processing
# reading back in and out to get the tz correct (currently, datetimecentral is actually showing UTC)
tripswdata = pd.read_csv("D:/weather/2019_try3/2019results4.csv")
tripswdata.datetimecentral = pd.to_datetime(tripswdata['datetimecentral'], utc=True)
tripswdata.datetimecentral = tripswdata.datetimecentral.dt.tz_convert('US/Central')
tripswdata.to_csv("D:/weather/2019_try3/2019results_central4.csv")

# validation - should know time difference and miles for each trip
tripswdata.placeid.describe()
tripswdata.stationid.value_counts()
tripswdata['deptimedtcentral'] = pd.to_datetime(tripswdata['deptimedtcentral'], utc=True)
tripswdata['deptimedtcentral'] = tripswdata.deptimedtcentral.dt.tz_convert('US/Central')
tripswdata['timediff'] = tripswdata['deptimedtcentral'] - tripswdata['datetimecentral']
tripswdata['hours'] = tripswdata['timediff'].astype('timedelta64[h]')

tripswdata.groupby('stationid').agg({'hours': 'mean'})

tripswdata.to_csv("D:/weather/2019_try3/tripswdataval4.csv")



