#! /usr/bin/env python3

import sys
import stat
import os
import requests
import json
from lib import utils as ut
import pprint


#############################################
pp = pprint.PrettyPrinter(indent=4)

if len(sys.argv) < 2 :
    print ('Usage: ')
    print (sys.argv[0], ' <list|stat|get|del> [<limit|request_id>]' )
    print()
    print (sys.argv[0], ' list [limit]         - lists the requests, up to [limit] results. default/0 is all' )
    print (sys.argv[0], ' stat <request_id>    - gets the status of the request' )
    print (sys.argv[0], ' get <request_id>     - gets the results of the request' )
    print (sys.argv[0], ' del <request_id>     - cancels the request' )
    print()
    quit(1)


batchapi = 'https://batch.internet.nl/api/batch/v2/requests'

action = sys.argv[1]

if action in ['stat', 'get', 'del'] and len(sys.argv) < 3:
    print(' Missing request_id')
    quit(1)

request_id = 0
if len(sys.argv) > 2:
    # request_id for get/del/stat, limit of results returned for list
    request_id = sys.argv[2]

try:
    credentials = ut.readCredentials()
except Exception as e:
    print("error opening/processing credentials file: {}".format(e))
    exit(1)
if action == 'list':
    r = requests.get(batchapi, params={'limit':request_id} , auth=(credentials['login'], credentials['password']) )
elif action == 'stat':
    r = requests.get(batchapi+'/'+request_id, auth=(credentials['login'], credentials['password']) )
elif action == 'get':
    r = requests.get(batchapi+'/'+request_id+'/results', auth=(credentials['login'], credentials['password']) )
elif action == 'del':
    r = requests.patch(batchapi+'/'+request_id, auth=(credentials['login'], credentials['password']) )
else:
    print('No valid action specified: {}'.format(action))
    quit(1)

if r.status_code == 200 or r.status_code == 201:
    if action == 'list':
        requests = r.json()
        for request in requests['requests']:
            pp.pprint(request)
            print()
    elif action == 'stat':
        request = r.json()['request']
        pp.pprint(request)
    elif action == 'get':
        print(r.text)
    elif action == 'del':
        print('Result = {}'.format(r.status_code))
        pp.pprint(r.json())
else:
    print('Something went wrong! (Error = {})'.format(r.status_code))
    pp.pprint(r.json())

# All done
