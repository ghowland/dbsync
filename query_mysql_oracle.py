"""
Query Library for dbsync API
"""


# import MySQLdb
# import MySQLdb.cursors
import mysql.connector
import mysql.connector.errors
import threading
import os
import sys
import imp

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


class QueryFailure(Exception):
  """Failure to query the DB properly"""

  def __init__(self, text, code=None):
    Exception.__init__(self, text)
    self.code = code


class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
  """Oracle mysql-connector does not provide DictCursor support, because they smoke crack."""
  def _row_to_python(self, rowdata, desc=None):
    row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
    if row:
      return dict(zip(self.column_names, row))
    return None


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
  #imp.reload(MySQLdb)
  imp.reload(mysql.connector)
  
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
    imp.reload(mysql.connector)
    #Log('Connections: %s  Cursors: %s' % (DB_CONNECTION, DB_CURSOR))

  # If no cached cursor/connection exists, create them
  if reset or cache_key not in DB_CURSOR:
    Log('Creating MySQL connection: %s' % str(cache_key))

    # conn = MySQLdb.Connect(host, user, password, database, port=port, cursorclass=MySQLdb.cursors.DictCursor)
    # cursor = conn.cursor()

    conn = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)
    cursor = conn.cursor(cursor_class=MySQLCursorDict)

    DB_CONNECTION[cache_key] = conn
    DB_CURSOR[cache_key] = cursor
    #Log('Connections: %s  Cursors: %s' % (DB_CONNECTION, DB_CURSOR))

  # Else, use the cache
  else:
    conn = DB_CONNECTION[cache_key]
    cursor = DB_CURSOR[cache_key]

  return (conn, cursor)


#PERFORMANCE(geoff): To deal with Oracle stupidity, I am keeping track of all SQL statements and will only
#   reset the connection 
SQL_QUERY_CACHE = {}


def Query(sql, host=DEFAULT_DB_HOST, user=DEFAULT_DB_USER, 
		password=DEFAULT_DB_PASSWORD, database=DEFAULT_DB_DATABASE, 
		port=DEFAULT_DB_PORT):
  """Execute and Fetch All results, or reutns last row ID inserted if INSERT.

  NOTE(geoff): This function is not thread-safe, because it modifies a global dictionary that isnt thread safe to manage Oracle cache bug.
  """
  global SQL_QUERY_CACHE

  # #WORKAROUND(geoff): Some problem with Oracle's MySQL driver, Im getting the same data on the same query, 
  # #   even though I know the data has changed in the database
  # imp.reload(mysql.connector)

  # Try to reconnect and stuff
  success = False
  tries = 0
  last_error = None
  while tries <= 3 and success == False:
    tries += 1

    try:
      # Connect (will save connections)
      #WORKAROUND(geoff): For the Oracle MySQL DB connector, I have to reset the connection each time or it caches the queries and I dont get updated data that has been committed
      #PERFORMANCE(geoff): Can only do a reset if we have made this exact same query before.  This could make this much faster...
      reset_connection = False
      if sql in SQL_QUERY_CACHE:
        reset_connection = True
        # Clear the caache
        SQL_QUERY_CACHE = {}
      else:
        SQL_QUERY_CACHE[sql] = True

      # Ensure we have a connection, reset if specified
      (conn, cursor) = Connect(host, user, password, database, port, reset=reset_connection)

      # Query
      #log('Query: %s' % sql)
      cursor.execute(sql)
      
      #Log('Query complete, committing')
      
      # Force commit
      if not sql.upper().startswith('SELECT') and not sql.upper().startswith('SHOW') and not sql.upper().startswith('DESC'): # This is only required in mysql-connector
        log('Commit')
        conn.commit()
      
      # Command didnt throw an exception
      success = True
    
    #except MySQLdb.DatabaseError as exc:
    except mysql.connector.errors.DatabaseError as exc:
      log('Exception thrown')
      log(dir())
      log(exc)
      #(error_code, error_text) = (exc.code, exc.message)
      (error_code, error_text) = (exc.errno, exc.msg)
      last_error = '%s: %s (Attempt: %s): %s: %s: %s' % (error_code, error_text, tries, host, database, sql)
      last_exception = exc
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
    if sql.split(' ')[0].upper() not in ('INSERT', 'UPDATE', 'DELETE', 'ALTER', 'CREATE', 'DROP'):
      result = cursor.fetchall()
    elif sql.split(' ')[0].upper() == 'INSERT':
      # This is 0 unless we were auto_incrementing, and then it is accurate
      result = cursor.lastrowid

    else:
      result = None

  # We failed, no result for you
  else:
    raise QueryFailure(str(last_error), code=last_exception.errno)

  return result


def SanitizeSQL(sql):
  """Convert singled quotes to dual single quotes, so SQL doesnt terminate the string improperly"""
  sql = str(sql).replace("'", "''")
  
  return sql


def GetSchema(host=DEFAULT_DB_HOST, user=DEFAULT_DB_USER, 
    password=DEFAULT_DB_PASSWORD, database=DEFAULT_DB_DATABASE, 
    port=DEFAULT_DB_PORT):
  """Returns a dict of tables and fields in those tables for a given database"""
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



  return schema

