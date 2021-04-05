#! /usr/bin/env python3

import sys
from influxdb import InfluxDBClient

####################################

if len(sys.argv) < 2:
    print('Usage: ')
    print(sys.argv[0], ' <database name> [data file to ingest]', )

    print("\nIf no data file is specified, lines will be read from stdin until EOF\n")
    quit(1)

database = sys.argv[1]
datafilename = ""
if len(sys.argv) > 2:
    datafilename = sys.argv[2]

try:
    client = InfluxDBClient(host='localhost', port = 8086)

    # Just drop and recreate database. No errors if it doesn't exist.
    client.drop_database(database)
    client.create_database(database)
    client.switch_database(database)


    # Open the file or read from stdin
    if (datafilename):
        datafile = open(datafilename, 'r')
        lines = datafile.readlines()
        print('{} lines of data'.format(len(lines)), flush=True)
        client.write(lines, params={'db':database}, protocol='line')
        datafile.close()
    else:
        count = 0
        for line in sys.stdin:
            count += 1
            print(line, end='', flush=True)
            client.write(line, params={'db':database}, protocol='line')
        print("\n{} lines ingested\n".format(count))
except Exception as e:
    print("Is the influxdb database up and running?\n")
    print(e)
    quit(1)
