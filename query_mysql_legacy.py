"""
Query Library for DBSync API
"""


import MySQLdb
import MySQLdb.cursors
import threading
import os
import sys
import imp
import gc
import time

import yaml

from AbsoluteImport import Import


log = Import('log', prefix='unidist').log
Log = log


# Default database connection: The OPs DB
#TODO(g): Wrap in startup-loader function for better error handling and options (path)
DEFAULT_DATA_PATH = __file__ + '/conf/database.yaml'
DEFAULT_DATA = yaml.load(open(DEFAULT_DATA_PATH))
DEFAULT_DB_HOST = DEFAULT_DATA['host']
DEFAULT_DB_USER = DEFAULT_DATA['user']
DEFAULT_DB_PASSWORD = DEFAULT_DATA['password']
DEFAULT_DB_DATABASE = DEFAULT_DATA['database']
DEFAULT_DB_PORT = DEFAULT_DATA['port']


# Global to store DB connections
#NOTE(g): We store connections to each database separately, even if
#   they go to the same DB host, so that we dont have to keep track
#   of which DB we are currently connected to.
DB_CONNECTION = {}
DB_CURSOR = {}


# Create a Global Write Lock
#NOTE(g): You have to grab this to do an UPDATE/INSERT, so we are
#   never doing these at exactly the same time.  Avoids all kinds
#   of problems immediately and can be refactored out later once
#   the project is up and running with all other things being stable.
GLOBAL_WRITE_LOCK = threading.Lock()



# SQL cache, by time and by event that knows things changed (because it just changed them)
SQL_CACHE = {}
SQL_CACHE_LAST_RESET_TIME = 0
SQL_CACHE_RESET_DELAY = 300 # 5 minutes


# Store the schema cache, because we call it so often
SCHEMA_CACHE = {}



class QueryFailure(Exception):
  """Failure to query the DB properly"""


def CloseAll():
  """Forcibly close all the MYSQLdb connections"""
  global DB_CONNECTION
  global DB_CURSOR
  
  # Loop through all our connection cache keys, and close them all
  keys = list(DB_CONNECTION.keys())
  for cache_key in keys:
    DB_CONNECTION[cache_key].close()
    del DB_CURSOR[cache_key]
    del DB_CONNECTION[cache_key]
  
  # Reload the MySQLdb module to try to clear more cache
  imp.reload(MySQLdb)
  
  # Force GC to run now and do whatever terrible things MySQLdb is doing to 
  #   close our connections and not allow new ones to open
  gc.collect()


def Connect(host, user, password, database, port, reset=False):
  """Connect to the specified MySQL DB"""
  global DB_CONNECTION
  global DB_CURSOR

  # Convert to proper empty DB
  if database == None:
    database = ''

  # Create the cache key (tuple), for caching the DB connection and cursor
  cache_key = (host, user, password, database, port)

  # Close any connection we have persistent, if resetting
  if reset and cache_key in DB_CURSOR:
    Log('Resetting cache key: %s' % str(cache_key))
    DB_CONNECTION[cache_key].close()
    del DB_CURSOR[cache_key]
    del DB_CONNECTION[cache_key]
    # Force module reload to clear out anything that is being incorrectly cached
    imp.reload(MySQLdb)
    #Log('Connections: %s  Cursors: %s' % (DB_CONNECTION, DB_CURSOR))

  # If no cached cursor/connection exists, create them
  if reset or cache_key not in DB_CURSOR:
    Log('Creating MySQL connection: %s' % str(cache_key))
    conn = MySQLdb.Connect(host, user, password, database, port=port, cursorclass=MySQLdb.cursors.DictCursor)
    cursor = conn.cursor()

    DB_CURSOR[cache_key] = cursor
    DB_CONNECTION[cache_key] = conn
    #Log('Connections: %s  Cursors: %s' % (DB_CONNECTION, DB_CURSOR))

  # Else, use the cache
  else:
    conn = DB_CONNECTION[cache_key]
    cursor = DB_CURSOR[cache_key]

  return (conn, cursor)


def Query(sql, host=DEFAULT_DB_HOST, user=DEFAULT_DB_USER, 
		password=DEFAULT_DB_PASSWORD, database=DEFAULT_DB_DATABASE, 
		port=DEFAULT_DB_PORT, clear_cache=False):
  """Execute and Fetch All results, or reutns last row ID inserted if INSERT."""
  global SQL_CACHE, SQL_CACHE_LAST_RESET_TIME, SQL_CACHE_RESET_DELAY

  # Ensure the entire SQL cache is cleared every N seconds
  if clear_cache or time.time() > SQL_CACHE_LAST_RESET_TIME + SQL_CACHE_RESET_DELAY:
    SQL_CACHE = {}

  # If this host/etc combo data key already has this SQL entry
  cache_key = (host, user, password, database, port)
  if cache_key in SQL_CACHE and sql in SQL_CACHE[cache_key]:
    return SQL_CACHE[cache_key][sql]

  # Ensure we have the cache key present, if its already here
  if cache_key not in SQL_CACHE:
    SQL_CACHE[cache_key] = {}


  # Try to reconnect and stuff
  success = False
  tries = 0
  last_error = None
  while tries <= 3 and success == False:
    tries += 1

    try:
      # Connect (will save connections)
      (conn, cursor) = Connect(host, user, password, database, port)

      # Query
      Log('Query: %s' % sql)
      cursor.execute(sql)
      
      #Log('Query complete, committing')
      
      # Force commit
      conn.commit()
      
      # Command didnt throw an exception
      success = True
    
    except MySQLdb.DatabaseError as exc:
      (error_code, error_text) = (exc.code, exc.message)
      last_error = '%s: %s (Attempt: %s): %s: %s: %s' % (error_code, error_text, tries, host, database, sql)
      Log(last_error)
      
      # Connect lost, reconnect
      if error_code in (2006, '2006'):
        Log('Lost connection: %s' % last_error)

        # Enforce we always close the connections, because 
        CloseAll()

        # Reset connection
        Connect(host, user, password, database, port, reset=True)
      else:
        Log('Unhandled MySQL query error: %s' % last_error)

  # If we made the query, get the result
  if success:
    if sql.upper()[:6] not in ('INSERT', 'UPDATE', 'DELETE'):
      result = cursor.fetchall()
    elif sql.upper()[:6] == 'INSERT':
      # This is 0 unless we were auto_incrementing, and then it is accurate
      result = cursor.lastrowid

    else:
      result = None

  # We failed, no result for you
  else:
    raise QueryFailure(str(last_error))

  # Cache the result
  SQL_CACHE[cache_key][sql] = result

  return result


def SanitizeSQL(sql):
  """Convert singled quotes to dual single quotes, so SQL doesnt terminate the string improperly"""
  sql = str(sql).replace("'", "''")
  
  return sql

def GetSchema(host=DEFAULT_DB_HOST, user=DEFAULT_DB_USER, 
    password=DEFAULT_DB_PASSWORD, database=DEFAULT_DB_DATABASE, 
    port=DEFAULT_DB_PORT, clear_cache=False):
  """Returns a dict of tables and fields in those tables for a given database"""
  global SCHEMA_CACHE

  cache_key = (host, user, password, database, port)

  # If we want to clear the cache, clear schema and data.  If the schema changed, the data did too
  if clear_cache:
    SCHEMA_CACHE[cache_key] = {}
    SQL_CACHE[cache_key] = {}


  elif cache_key in SCHEMA_CACHE:
    return SCHEMA_CACHE[cache_key]

  schema = {}

  sql = "SHOW TABLES"
  tables = Query(sql, host=host, user=user, password=password, database=database, port=port)
  for table_item in tables:
    key = list(table_item.keys())[0]
    table = table_item[key]

    schema[table] = {}

    field_order_dict = {}

    sql = "DESC %s" % table
    field_order = 0
    fields = Query(sql, host=host, user=user, password=password, database=database, port=port)
    for field in fields:
      schema[table][field['Field']] = field
      schema[table][field['Field']]['_Order'] = field_order
      field_order += 1
      field_order_dict[field_order] = field['Field']

    # Gather the CREATE table statement
    sql = "SHOW CREATE TABLE %s" % table
    table_create = Query(sql, host=host, user=user, password=password, database=database, port=port)
    schema[table]['__CREATE_SQL__'] = table_create[0]['Create Table']

    # Get the table PRIMARY KEY INDEX
    sequence = {}
    sql = 'SHOW INDEXES IN `%s`' % table
    result = Query(sql, host=host, user=user, password=password, database=database, port=port)
    for item in result:
      if item['Key_name'] == 'PRIMARY':
        sequence[item['Seq_in_index']] = item
    
    # Add the PRIMARY KEY keys by their sequence order (ensure its correct)
    schema[table]['__PRIMARY_KEYS__'] = []
    sequence_keys = list(sequence.keys())
    sequence_keys.sort()
    for sequence_key in sequence_keys:
      schema[table]['__PRIMARY_KEYS__'].append(sequence[sequence_key]['Column_name'])

    # Make a list of feilds in their order, for ease of use (needed all the time)
    schema[table]['__FIELD_ORDER__'] = []
    field_orders = list(field_order_dict.keys())
    for field_order in field_orders:
      schema[table]['__FIELD_ORDER__'].append(field_order_dict[field_order])


  # Save this Schema to the cache, so we dont keep rechecking the same cache_key, unless
  #   we need new results because we change something or its otherwise requests (time limit, user)
  SCHEMA_CACHE[cache_key] = schema


  return schema

