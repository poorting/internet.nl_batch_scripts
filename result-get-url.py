#! /usr/bin/env python3

import sys
import os
import csv
import requests
import json
import datetime
import time
import pandas as pd
import numpy as np
import math
import pprint
from lib import utils as ut

#############################################

pp = pprint.PrettyPrinter(indent=4)

if len(sys.argv) < 3 :
    print ('')
    print ('Get the URL pointing to results for specific domain(s) containing \'domain_part\'')
    print ('from a JSON results file from internet.nl')
    print ('Usage: ')
    print (sys.argv[0], ' <JSON results file> <domain_part>')
    quit(1)

# Create empty dataframe
domains = pd.DataFrame()
domainsFile=''
columns_to_add=[]

if len(sys.argv) > 2 :
    domainPattern = sys.argv[2]

filename = sys.argv[1]

if os.path.isdir(filename):
    print ('Usage: ')
    print (sys.argv[0], ' <JSON results file> <domain>')
    quit(1)

print('Processing {0}'.format(filename))
data = ut.openJSON(filename)

api_version = ut.getAPIversion(data)

url = 'url'
if api_version == '1.1' or api_version == '1.0':
    url='link'

df = ut.JSONtoDF(data, domains, columns_to_add)

dfMatch1 = df[df['domain'].str.contains(domainPattern)]

dfMatch = dfMatch1.sort_values(by=['domain'], ascending=True, inplace=False)

for i in range(0, len(dfMatch)):
        print('{0} ({1}): {2}'.format(dfMatch.iloc[i]['domain'],int(dfMatch.iloc[i]['score']), dfMatch.iloc[i][url]))
# All done
