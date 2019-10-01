"""
find appropriate weather data for each trip
"""

# https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude/43211266#43211266
import geopy.distance
import pandas as pd
import bisect
import numpy as np
import time
import re

#trips = pd.read_csv("C:/Users/sbuchhorn/Desktop/hhts/weather/sample_datasets/MDT_201811_one_hh_per_zip.csv")
trips = pd.read_csv("D:/weather/fromnickaug19/trips_time_location.csv")
# clean up - we only want dates on or after 9/3/18 (Labor Day)
trips = trips[trips['travdate'] >= '2018-09-03'] # there are 990 trips before the start date
# there are now 125,958 trips
# just doing 2019 - 65,691 trips
trips = trips[trips['travdate'] >= '2019-01-01']


# weatherdata = pd.read_csv("C:/Users/sbuchhorn/Desktop/hhts/weather/nws_fall2018.csv")
weatherdata = pd.read_csv("D:/weather/NWS19selection.csv")
weatherdatatest = weatherdata[['latitude','longitude','datetime','heat_index_c','heat_index_f',
                               'month','relative_humidity','temp_c','temp_f','weather','wind_gust_mph',
                               'wind_mph','windchill_c','windchill_f']]

# get unique stations and save to refer to later
stations = weatherdata[['latitude','longitude']].dropna()
stations = stations.drop_duplicates(['latitude','longitude']).reset_index(drop=True)
stations['stationid'] = stations.index

# some of these "unique stations" are actually the same.  assign the same ID to those within 1/2 mile.
stationduplicate = {}
for i in range(0,len(stations)):
    for x in range (0,len(stations)):
        distance = geopy.distance.distance(stations.loc[i], stations.loc[x]).miles
        stationduplicate[(i,x)] = distance
stationdupdf = pd.DataFrame.from_dict(stationduplicate,orient='index')
stationdupdf = stationdupdf[(stationdupdf[0] > 0) & (stationdupdf[0] < 0.25)]
stationdupdf.reset_index(inplace=True)
stationdupdf['left'] = 0
stationdupdf['right'] = 0
for i,row in stationdupdf.iterrows():
    stationdupdf.iloc[i,2] = stationdupdf['index'][i][:][0]
    stationdupdf.iloc[i,3] = stationdupdf['index'][i][:][1]

for i in range(0,len(stationdupdf) + 1):
    try:
        y = stationdupdf.loc[i,'right']
        stationdupdf = stationdupdf[stationdupdf['left'] != y]
    except KeyError:
        continue

stations = stations.merge(stationdupdf[['left','right']],left_on='stationid',right_on='right',how="outer")
stations.loc[stations['right'] >= 0,'stationid'] = stations['left']


# stations.to_csv("C:/Users/sbuchhorn/Desktop/stations.csv")
stations.to_csv("D:/weather/2019_try2/stations19.csv")
stations = stations[['latitude','longitude','stationid']]

# match trip coords with weather station
places = trips[['latitude','longitude']]
uniplaces = places.drop_duplicates(['latitude','longitude']).reset_index(drop=True)
uniplaces['placeid'] = uniplaces.index

stationdistance = {}

# for 2019, 23918 uniplaces, 54 unique stations,
# first round: started 8:30AM, finished 8:54AM
# second round: 21 min
starttime = time.time()
for i in range(0, len(uniplaces)):
    for x in range(0,len(stations)):
        distance = geopy.distance.distance(uniplaces.loc[i],stations.loc[x]).miles
        stationdistance[(i,x)] = distance
    # i will match the placeid
print(time.time() - starttime)


closest_stationdf = pd.DataFrame.from_dict(stationdistance, orient='index')
closest_stationdf['placeid:stationid'] = closest_stationdf.index
closest_stationdf.rename({0:'distance'},axis=1,inplace=True)
closest_stationdf.to_csv("D:/weather/2019_try2/closest_station19.csv")

# get just the stations that are 30 miles or less away from place
closestmini = closest_stationdf[closest_stationdf['distance'] <= 30] # should it be 20??
closestmini.to_csv("D:/weather/2019_try2/closest_station19mini.csv")

#if using from file:
closest_stationdf = pd.read_csv("D:/weather/2019_try2/closest_station19mini.csv")

# or get the closest station - what a mess! WIP
# TODO fix this
closest_stationdf.loc[:,'placeid'] = closest_stationdf['placeid:stationid'].str.split(pat="\,")
closest_stationdf.loc[:,'placeid'] = closest_stationdf['placeid'].str[:1]
closest_stationdf.loc[:,'stationid'] = closest_stationdf['placeid:stationid'].str.split(pat="\,")
closest_stationdf.loc[:,'stationid'] = closest_stationdf['stationid'].str[1:2]
closest_stationdf['placeid'] = closest_stationdf.placeid.astype(str).str.replace("\(","")
closest_stationdf['stationid'] = closest_stationdf.stationid.astype(str).str.replace(")","").replace("[","")



closest_stationdf.sort_values('distance').drop_duplicates('placeid')

# need to get stationid into weatherdatatest via lat/long (which station goes with which observation)
stationreadings = stations.merge(weatherdatatest,on=['latitude','longitude']) # so we'll just want one of these rows for weather data
stationreadings['datetime'] = pd.to_datetime(stationreadings['datetime'], utc=True)
stationreadings['datetimecentral'] = stationreadings.datetime.dt.tz_convert('US/Central')
# 679 have null lat long


# need to get placeid into trips via lat/long
tripswplaceid = trips.merge(uniplaces,on=['latitude','longitude'])


# now, get station id in with the trips table
# if you want just the closest one
tripswstation = tripswplaceid.merge(closest_stationdf,on='placeid')

#TODO: if you want the list of all within x miles

tripswstationdep = tripswstation[tripswstation['deptime'] != "-1"] # 2915 --> 2277 // 2019: 65691 to 50881
tripswstationdep['deptimedt'] = pd.to_datetime(tripswstationdep['deptime'], utc=False)
tripswstationdep['deptimedtcentral'] = pd.to_datetime(tripswstationdep['deptimedt']).dt.tz_localize('US/Central')
tripswstationdep.reset_index(inplace=True)
# adding weather columns in
for x in ['stationid','heat_index_c','heat_index_f','relative_humidity','temp_c','temp_f','weather',
         'wind_gust_mph','wind_mph','windchill_c','windchill_f']:
    tripswstationdep[x] = np.nan
tripswstationdep['datetimecentral'] = pd.datetime(2099,9,9,9,9,9,9)
# remove reference to initial df so we don't get warnings
tripswstationdep = tripswstationdep.copy()

######## start here #####################
starttime = time.time()
readingsdict = {}

for i in range(0, len(tripswstationdep)):
    if i % 10000 == 0:
        print(i)
    acceptablelist = tripswstationdep.loc[i]['stationidlist']
    if acceptablelist == 999:
        continue
    else:
        timetomatch = tripswstationdep.loc[i]['deptimedtcentral']
        # you can either match closest or closest time in a list

        # match closest time in a list implementation
        choices = stationreadings[stationreadings['stationid'].isin(acceptablelist)].sort_values('datetimecentral',ascending=True)

        # match closest station implementation
        choices = stationreadings[stationreadings['stationid'] == #closestid].sort_values('datetimecentral',ascending=True)

        choicesseries = choices['datetimecentral']
        choiceslist = [x for x in choicesseries]
        t = bisect.bisect_left(choiceslist, timetomatch)
        # t is the id of the entry from choiceslist one after timetomatch
        readings = choices[choices['datetimecentral'] == choiceslist[t-1]]
        if len(readings) > 1:
            readings = readings.iloc[[0]]
        #readings.to_clipboard(excel=True,sep='\t')
        readings.index = [i]
        plugin = readings[['stationid','heat_index_c','heat_index_f','relative_humidity','temp_c','temp_f',
                                          'weather','wind_gust_mph','wind_mph','windchill_c','windchill_f','datetimecentral']]
        # updates on idex
        tripswstationdep.update(plugin)


# tripswstationdep.to_csv("C:/Users/sbuchhorn/Desktop/results.csv")
tripswstationdep.to_csv("D:/weather/2019results.csv")
print(time.time() - starttime)
####### end here #########################


# reading back in and out to get the tz correct (currently, datetimecentral is actually showing UTC)
tripswdata = pd.read_csv("D:/weather/2019results.csv")
tripswdata.datetimecentral = pd.to_datetime(tripswdata['datetimecentral'],utc=True)
tripswdata.datetimecentral = tripswdata.datetimecentral.dt.tz_convert('US/Central')
tripswdata.to_csv("D:/weather/2019results2.csv")


# validation - should know time difference and miles for each trip
tripswdata.placeid.describe()
tripswdata.stationid.value_counts()
tripswdata['deptimedtcentral'] = pd.to_datetime(tripswdata['deptimedtcentral'],utc=True)
tripswdata['deptimedtcentral'] = tripswdata.deptimedtcentral.dt.tz_convert('US/Central')
tripswdata['timediff'] = tripswdata['deptimedtcentral'] - tripswdata['datetimecentral']
tripswdata['hours'] = tripswdata['timediff'].astype('timedelta64[h]')

tripswdata.groupby('stationid').agg({'hours':'mean'})

tripswdata.to_csv("D:/weather/tripswdataval.csv")

