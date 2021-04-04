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

if len(sys.argv) < 2 :
    print ('Usage: ')
    print (sys.argv[0], ' <JSON results file|JSON results directory> [domains xlsx file] [metadata_column_name[,md_col_name2, ...]]')
    quit(1)

# Create empty dataframe
domains = pd.DataFrame()
domainsFile=''
columns_to_add=[]

if len(sys.argv) > 2 :
    domainsFile = sys.argv[2]
    columns_to_add=['type']

if len(sys.argv) > 3 :
    columns_to_add = sys.argv[3].split(',')

filelist=[]
filename = sys.argv[1]

if os.path.isdir(filename):
    if not filename.endswith("/"):
        filename=filename+'/'
    with os.scandir(filename) as it:
        for entry in it:
            if not entry.name.startswith('.') and entry.is_file() and entry.name.endswith('.json'):
                    filelist.append('{0}{1}'.format(filename, entry.name))
else:
    filelist.append(filename)

try:
    if not domainsFile == '':
        domains = pd.read_excel(domainsFile, sheet_name=0, engine='openpyxl')
except Exception as e:
    print("error processing domains Excel file: {}".format(e))
    exit(1)

for fn in filelist:
    print('Processing {0}'.format(fn))
    data = ut.openJSON(fn)

    df = ut.JSONtoDF(data, domains, columns_to_add)

    df.sort_values(by=['score','domain'], ascending=False, inplace=True)
    # Write the DataFrame to a file of the same name but with xlsx extension
    fnr = fn[0:-5]+'.xlsx'
    print('Writing to {0}'.format(fnr))
    df.to_excel (fnr, index = None, header=True)


# All done
