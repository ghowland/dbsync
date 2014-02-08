#!/usr/bin/env python3
"""
One Off Script - Mess with JSON data values in tables
"""

import json
import pprint
import math

import AbsoluteImport
AbsoluteImport.RegisterPathPrefix('procblock', '../dropstar/dropstar/control/procblock/')
AbsoluteImport.RegisterPathPrefix('unidist', '../dropstar/dropstar/control/unidist/')
AbsoluteImport.RegisterPathPrefix('dbsync', '../dbsync/')

query_mysql = AbsoluteImport.Import('query_mysql_legacy', 'dbsync')

Query = query_mysql.Query

def SanitizeSQL(text):
  return text.replace("\\", "\\\\").replace("'", "\\'")

result = Query("SELECT * FROM some_table", host='qa', user='root', 
    password='SECRET', database='somedb')

for item in result:
  #DEBUG(ghowland): Can limit to only 1 item easily for testing
  # if item['id'] != 1001:
  #  continue

  data = json.loads(item['another_field'])
  olddata = data['example_field']
  data['example_field'] = str(int(math.ceil(int(data['example_field']) / 2.0)))
  newdata = data['example_field']

  print('Old: %s  New: %s' % (olddata, newdata))

  json_data = json.dumps(data)


  #print(item)
  sql = """UPDATE another_table SET another_field = '%s' WHERE id = %s""" % \
        (SanitizeSQL(json_data), item['id'])
  print('\n'+sql+'\n')

  Query(sql, host='qa', user='root', password='SECRET', database='somedb')

