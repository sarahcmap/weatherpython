#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
code to help get data from the regional transportation data archive
"""

import psycopg2

# connection and cursor
conn = psycopg2.connect(dbname="rtdap", user="cmap", password="cm@pcm@p", host="cmap-rtdap01", port="5434")
# gotta change the port for the proper year.  current year is port 5432.  refer to https://docs.pangaeatech.com/cmap/rtdap/inventory.html
cur = conn.cursor()

# Get table names
cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
for table in cur.fetchall():
    print(table)

# get attribute info for a table
cur.execute("""SELECT * FROM information_schema.columns
       WHERE table_schema = 'public'
       AND table_name = 'NwsObservation_2016'""")
for table in cur.fetchall():
    print(table)

# weather stuff is 'NwsObservation_2019' and 'NwsObservation_2018'
with psycopg2.connect(dbname="rtdap", user="cmap", password="cm@pcm@p", host="cmap-rtdap01", port="5434") as conn:
    with conn.cursor(name = 'testcursor') as cursor:
        cursor.itersize = 20000
        tablename = "\"NwsObservation_2016" + "\""
        query = """COPY
        (SELECT *
            FROM %s)
            TO STDOUT WITH CSV DELIMITER ','"""

# execute query and save to file
        with open(
            "D:/weather/NWS_16.csv", "w") as file:
            # I saved the 2019 in D:/weather/NWS_19_aug19.csv
            cursor.copy_expert(query % (tablename), file)