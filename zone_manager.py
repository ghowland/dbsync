"""
Zone Manager

Handles all the Zone related issues.  Zones are separate areas of the same databaes. 
For instance, QA, Staging and Production all have the same databases in them, but they
have different users.
"""


import yaml
import re
import operator

# import query_mysql_oracle as query_mysql

from AbsoluteImport import Import

log = Import('log', prefix='unidist').log
#query_mysql = Import('query_mysql_oracle')
query_mysql = Import('query_mysql_legacy')


#TODO(g): Move this into a startup-loading function, to harder and provide better error handling or options (path)
ZONE_PATH = __file__ + '/conf/zones.yaml'
#print('Zone Path: %s' % ZONE_PATH)
ZONES = yaml.load(open(ZONE_PATH))
ZONE_APPROVERS_PATH = __file__ + '/conf/approvers.yaml'
ZONE_APPROVERS = yaml.load(open(ZONE_APPROVERS_PATH))
DATABASE_SETS_PATH = __file__ + '/conf/database_sets.yaml'
DATABASE_SETS = yaml.load(open(DATABASE_SETS_PATH))


def ReloadZoneInfo():
  """Do this dynamically, so we can refresh the data and not restart things or reload the module."""
  global ZONES, ZONE_APPROVERS

  ZONES = yaml.load(open(ZONE_PATH))
  ZONE_APPROVERS = yaml.load(open(ZONE_APPROVERS_PATH))


def GetZoneDataSetData(zone, database_set):
  """Returns the Zone info for a given Zone/Database Set.  Details connections and approval info."""
  global ZONES

  zone_info = ZONES[zone]
  database_set_info = zone_info[database_set]

  return database_set_info


def GetApprovers():
  """Returns the Approvers dictionary of lists of dicts."""
  global ZONE_APPROVERS

  return ZONE_APPROVERS


def ValidateZoneSourceAndTarget(zone_source, zone_target):
  """Returns valid tuple of zone source and target, or raises ComparisonException"""
  try:
    zone_source = int(zone_source)
  except TypeError as exc:
    raise ComparisonException('Zone Source is not an integer.  Valid Options: %s' % ZONES)

  try:
    zone_target = int(zone_target)
  except TypeError as exc:
    raise ComparisonException('Zone Target is not an integer.  Valid Options: %s' % ZONES)

  if zone_source not in ZONES:
    raise ComparisonException('Zone Source is not a valid option.  Valid Options: %s' % ZONES)

  if zone_target not in ZONES:
    raise ComparisonException('Zone Target is not a valid option.  Valid Options: %s' % ZONES)

  if zone_source == zone_target:
    raise ComparisonException('Zone Source and Target are the same.  They must be different: Options: %s' % ZONES)

  return (zone_source, zone_target)


def _FormatData(text, data):
  """Returns formatted text, using key/value from data dictionary"""
  for (key, value) in data.items():
    key_str = '%%(%s)s' % key
    if key_str in text:
      text = text.replace(key_str, value)

  return text


def GetZoneConnectionInfo(zone, database_set, instance):
  """Returns the connection information"""
  # print()
  # print('%s' % ZONES[zone][database_set])
  yaml_path = '%s/%s' % (__file__, DATABASE_SETS[database_set]['data'])
  data = yaml.load(open(yaml_path))
  # print(data[instance])

  host = _FormatData(ZONES[zone][database_set]['host'], data[instance])
  database = _FormatData(ZONES[zone][database_set]['database'], data[instance])
  user = _FormatData(ZONES[zone][database_set]['user'], data[instance])
  password = _FormatData(ZONES[zone][database_set]['password'], data[instance])
  port = _FormatData(str(ZONES[zone][database_set].get('port', 3306)), data[instance])

  # Pack in the data we need as well, as one validated dict
  data = {}
  #data['table_blacklist'] = ZONES[zone][database_set].get('table_blacklist', [])
  data['table_blacklist'] = ZONES[zone][database_set]['table_blacklist']

  # print()
  # print('Host: %s' % host)
  # print('Database: %s' % database)
  # print('User: %s' % user)
  # print('Password: %s' % password)
  # print()

  # Determine Query Module
  #TODO(g): For now its always MySQL
  query_module = query_mysql

  return (query_module, host, database, user, password, port, data)



def Query(zone, database_set, instance, sql):
  """Query the database in this zone.  Returns list of dicts.  Master DB is always used."""
  #print('Query: %s %s %s: %s' % (zone, database_set, instance, sql))

  # Get all our connection information
  (query_module, host, database, user, password, port, zone_data) = GetZoneConnectionInfo(zone, database_set, instance)

  result = query_module.Query(sql, host=host, user=user, password=password, database=database, port=int(port))

  return result


def GetSchema(zone, database_set, instance):
  """Query the database in this zone.  Returns list of dicts.  Master DB is always used."""
  #print('GetSchema: %s %s %s' % (zone, database_set, instance))

  # Get all our connection information
  (query_module, host, database, user, password, port, zone_data) = GetZoneConnectionInfo(zone, database_set, instance)

  schema = query_module.GetSchema(host=host, user=user, password=password, database=database, port=int(port))

  return schema


def CompareSchemas(schema_source, schema_target):
  """Returns the differences between source and target."""
  comparison = {'create':{}, 'drop':{}, 'alter':{}}

  # Check the Source tables
  for (table_name, source_data) in schema_source.items():
    # If we dont have this table, create it
    if table_name not in schema_target:
      comparison['create'][table_name] = source_data
    # Else, look for alters
    else:
      target_data = schema_target[table_name]

      #TODO(g): DELETE field/column has not yet been implemented...
      comparison['alter'][table_name] = {'add':{}, 'delete':{}, 'modify':{}}

      # Look through Source fields
      for (field, source_field_data) in source_data.items():
        # Ignore my custom schema fields
        if field in ['__CREATE_SQL__', '__FIELD_ORDER__']:
          continue

        if field not in target_data:
          comparison['alter'][table_name]['add'][field] = {'source':source_field_data, 'target':None}
        else:
          target_field_data = target_data[field]

          if target_field_data != source_field_data:
            comparison['alter'][table_name]['modify'][field] = {'source':source_field_data, 'target':target_field_data}

      # Look through Target fields
      for (field, field_data) in target_data.items():
        if field not in source_data:
          comparison['alter'][table_name]['delete'][field] = field_data


  # Check the Target tables to see what has been dropped from the source
  for (table_name, table_data) in schema_target.items():
    # If we dont have this table in the source, drop it
    if table_name not in schema_source:
      comparison['drop'][table_name] = table_data['__CREATE_SQL__']

  return comparison


def GenerateSchemaSyncCommands(zone_source, zone_target, database_set, instance, comparison):
  """Generate a Forward and Reverse command list to sync up source and target schemas.
  Reverse should be able to undo any of the schema changes performed.

  NOTE: All forward and reverse commands must be paired.  If a forward is added, a reverse 
      must be added, even if it is None/'', and so nothing will OCCUR.  This allows these 
      statements to be ordered pairs, so they can be re-ordered later on user discretion.
  """
  forward_commands = []
  reverse_commands = []

  if comparison['alter']:
    for (table, table_changes) in comparison['alter'].items():
      #print('alter: %s: %s' % (table, table_changes))

      if table_changes['add']:
        for (field, field_data) in table_changes['add'].items():
          sql = CreateAlterAddColumn(table, field, field_data)
          forward_commands.append(sql)

          sql = CreateAlterDropColumn(table, field, field_data)
          reverse_commands.append(sql)

      if table_changes['delete']:
        for (field, field_data) in table_changes['delete'].items():
          sql = CreateAlterDropColumn(table, field, field_data)
          forward_commands.append(sql)

          sql = CreateAlterAddColumn(table, field, field_data)
          reverse_commands.append(sql)
          
      if table_changes['modify']:
        for (field, field_data) in table_changes['modify'].items():
          sql = CreateAlterModifyColumn(table, field, field_data['source'], field_data['target'])
          forward_commands.append(sql)

          sql = CreateAlterModifyColumn(table, field, field_data['target'], field_data['source'])
          reverse_commands.append(sql)

  if comparison['drop']:
    for (table, table_create_sql) in comparison['drop'].items():
      sql = 'DROP TABLE %s' % table
      forward_commands.append(sql)

      # Add reverse to CREATE the table again
      #TODO(g): Do I also have to grab all the data in the table to re-INSERT it back in?  In the normal case, yes, so I need to think of another case to save this step.
      sql_statements = []
      sql_statements.append(table_create_sql)

      # Get all the rows in INSERT format to re-populate this table
      insert_rows = GetSql_InsertAll(zone_target, zone_source, database_set, instance, table)
      sql_statements += insert_rows

      reverse_commands.append(sql_statements)

  if comparison['create']:
    for (table, table_data) in comparison['create'].items():
      sql = table_data['__CREATE_SQL__']
      sql_statements = []
      sql_statements.append(sql)

      # Get all the rows in INSERT format to populate this table
      insert_rows = GetSql_InsertAll(zone_source, zone_target, database_set, instance, table)
      sql_statements += insert_rows
      forward_commands.append(sql_statements)

      # Add reverse to CREATE the table again
      #TODO(g): Do I also have to grab all the data in the table to re-INSERT it back in?  In the normal case, yes, so I need to think of another case to save this step.
      sql = 'DROP TABLE %s' % table
      reverse_commands.append(sql)


  # Return the forward and reverse
  return (forward_commands, reverse_commands)


def CreateAlterAddColumn(table, field, field_data):
  """Create ALTER statement to add this field"""
  allow_null = ''
  if field_data['Null'] != 'YES':
    allow_null = ' NOT NULL'

  primary_key = ''
  if field_data['Key'] == 'PRI':
    primary_key = ' PRIMARY KEY'

  auto_increment = ''
  if field_data['Extra']:
    auto_increment = ' %s' % field_data['Extra']

  sql = 'ALTER TABLE %s ADD COLUMN %s %s%s%s%s' % (table, field, field_data['Type'], allow_null, primary_key, auto_increment)

  return sql


def CreateAlterDropColumn(table, field, field_data):
  """Create ALTER statement to add this field"""
  sql = 'ALTER TABLE %s DROP COLUMN %s' % (table, field)

  return sql


def CreateAlterModifyColumn(table, field, source_data, target_data):
  """Create ALTER statement to add this field"""
  allow_null = ''
  if source_data['Null'] != 'YES':
    allow_null = ' NOT NULL'

  primary_key = ''
  if source_data['Key'] == 'PRI' and target_data['Key'] != 'PRI':
    primary_key = ' PRIMARY KEY'
  
  auto_increment = ''
  if source_data['Extra']:
    auto_increment = ' %s' % source_data['Extra']

  sql = 'ALTER TABLE %s MODIFY %s %s%s%s%s' % (table, field, source_data['Type'], allow_null, primary_key, auto_increment)

  # Index step
  if target_data['Key'] == 'PRI' and source_data['Key'] != 'PRI':
    index_sql = 'ALTER TABLE %s DROP PRIMARY KEY' % table

    # Add the indexing step before the ALTER, so it works
    sql = '%s ; %s' % (index_sql, sql)

  return sql


def SqlValue(table_schema, field, value):
  """Returns a prepared string for a SQL value.  Quoting and sanitization is applied."""
  #NOTE(g): Not just passing this in in case we want to access index or other data that
  #   is not in the field_info dictionary
  field_info = table_schema[field]

  if value == None:
    sql_value = 'NULL'
  elif field_info['Type'].split('(')[0].lower() in ('varchar', 'char', 'blob'):
    # Quote the value and turn any single quotes to double single quotes to escape them
    sql_value = "'%s'" % SanitizeSqlString(value)
  else:
    sql_value = str(value)

  return sql_value


def SanitizeSqlString(value):
  """Returns a prepared string for a SQL value.  Quoting and sanitization is applied.

  This is onyl for confirmed string values.
  """
  sql_value = str(value).replace("'", "''")

  return sql_value


def GetTableFieldsInOrder(table_schema):
  """Returns a list of strings, the fields in their table order"""
  # Get only the Field Dictionaries in a list, strip out other data
  field_dict_list = []
  for field in table_schema.keys():
    if type(table_schema[field]) == dict and '_Order' in table_schema[field]:
      field_dict_list.append(table_schema[field])

  # Get sorted field list
  field_dicts_sorted = sorted(field_dict_list, key=operator.itemgetter('_Order'))

  fields = []
  for field_info in field_dicts_sorted:
    fields.append(field_info['Field'])

  #print('Sorted Fields: %s' % fields)

  return fields


def CreateInsert(table_schema_source, table_schema_target, table, row):
  """Return str, INSERT statement for this table"""
  # Get the table fields in order
  fields = GetTableFieldsInOrder(table_schema_source)

  # List of SQL fields
  insert_sql_fields = []
  for field in fields:
    #NOTE(g): Back-quoting all field names, to avoid any reserved word conflicts
    insert_sql_fields.append('`%s`' % field)

  # List of SQL field values, properly quoted and escaped for their type
  insert_sql_values = []
  for field in fields:
    insert_sql_values.append(SqlValue(table_schema_source, field, row[field]))

  insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table, ', '.join(insert_sql_fields), ', '.join(insert_sql_values))

  return insert_sql


def CreateUpdate(table_schema_source, table_schema_target, table, source_row, target_row):
  """Return str, UPDATE statement for this row, so target becomes like source"""
  # Get the table fields in order
  fields = GetTableFieldsInOrder(table_schema_target)

  # Build SET clauses section (`count` = 5)
  set_clauses = []
  for field in fields:
    # If the row field values are different, UPDATE them.  Primary keys will always be skipped.
    #NOTE(g): First checking if the field doesnt exist.  I am assuming that this field will
    #   be created by the schema update, which occurs before this.
    if field not in target_row  or (field in source_row and source_row[field] != target_row[field]):
      sql_set = '`%s` = %s' % (field, SqlValue(table_schema_target, field, source_row[field]))
      set_clauses.append(sql_set)

  # If we have no sets to perform, for whatever reason, then we do not create an UPDATE statement
  if not set_clauses:
    return None

  # Build WHERE clauses sections (`id` = 1) for each primary key field
  where_clauses = []
  for field in table_schema_target['__PRIMARY_KEYS__']:
    where_clauses.append('`%s` = %s' % (field, SqlValue(table_schema_target, field, source_row[field])))

  update_sql = 'UPDATE %s SET %s WHERE %s' % (table, ', '.join(set_clauses), ' AND '.join(where_clauses))
  
  return update_sql


def CreateDelete(table_schema_source, table_schema_target, table, row):
  """Return str, DELETE statement for this row"""
  # Build WHERE clauses sections (`id` = 1) for each primary key field
  where_clauses = []
  for field in table_schema_source['__PRIMARY_KEYS__']:
    where_clauses.append('`%s` = %s' % (field, SqlValue(table_schema_source, field, row[field])))

  delete_sql = 'DELETE FROM %s WHERE %s' % (table, ' AND '.join(where_clauses))

  return delete_sql


def GetSql_InsertAll(zone_source, zone_target, database_set, instance, table, selected_schema_keys=None):
  """Returns list of strings for all INSERT statements to create all the rows in a table."""
  sql = 'SELECT * FROM %s' % table
  rows = Query(zone_source, database_set, instance, sql)

  # Get the schemas for the tables, each time so that they can be matches together
  (table_schema_source, table_schema_target) = GetTableSchema(zone_source, zone_target, database_set, instance, table, selected_schema_keys=selected_schema_keys)

  inserts = []
  for row_data in rows:
    insert_sql = CreateInsert(table_schema_source, table_schema_target, table, row_data)
    inserts.append(insert_sql)

  return inserts


def GetRowPrimaryKeyTuple(table_schema, row):
  """Returns a list of the primary keys for this row."""
  primary_key_data = []

  for field in table_schema['__PRIMARY_KEYS__']:
    primary_key_data.append(row[field])

  return tuple(primary_key_data)


def GetSql_DatabaseDiff(zone_source, zone_target, database_set, instance, selected_schema_keys=None):
  """Return all the difference data between source and target database."""
  diff = {}

  schema_source = GetSchema(zone_source, database_set, instance)

  for (table, schema_data) in schema_source.items():
    diff[table] = GetSql_TableRowDiff(zone_source, zone_target, database_set, instance, table, selected_schema_keys=selected_schema_keys)

  return diff


def SkipTableCheck(table, zone_data):
  """Check that this table isnt blacklisted"""

  # print(zone_data['table_blacklist'])
  # import sys
  # sys.exit(0)

  for black_item in zone_data['table_blacklist']:
    black_item_regex = black_item.replace('*', '.*?')

    results = re.findall(black_item_regex, table)

    # Match on the regex, skip this table, its black listed
    if results:
      return True

  return False



def GetSql_TableRowDiff(zone_source, zone_target, database_set, instance, table, selected_schema_keys=None):
  """Returns list of strings for all INSERT statements to create all the rows in a table."""
  # Difference between source and target data
  diff = {'insert':[], 'delete':[], 'update':[]}

  sql = 'SELECT * FROM %s' % table
  #TOOD(g): If a table is missing, going to have some issues here.  Work through this with testing.


  # Get the Query Module, needed for exception handling
  #TODO(g): Make this easier, so all the work doesnt have to be done every time?  This loads data and stuff...
  (query_module, _, _, _, _, _, zone_data) = GetZoneConnectionInfo(zone_source, database_set, instance)

  if SkipTableCheck(table, zone_data):
    return diff

  # Source Rows
  try:
    source_rows = Query(zone_source, database_set, instance, sql)
  except query_module.QueryFailure as e:
    # If we failed because the table doesnt exist (MySQL error code)
    if e.code == 1146:
      source_rows = []
    else:
      raise e

  # Target Rows
  try:
    target_rows = Query(zone_target, database_set, instance, sql)
  except query_module.QueryFailure as e:
    # If we failed because the table doesnt exist (MySQL error code)
    if e.code == 1146:
      target_rows = []
    else:
      raise e


  # Get the schemas for the tables, each time so that they can be matches together
  (table_schema_source, table_schema_target) = GetTableSchema(zone_source, zone_target, database_set, instance, table, selected_schema_keys=selected_schema_keys)
  table_schema = table_schema_source

  # Build dictionary for source based on primary keys
  source_rows_keyed = {}
  for row in source_rows:
    key = GetRowPrimaryKeyTuple(table_schema, row)
    source_rows_keyed[key] = row

  # Build dictionary for target based on primary keys
  target_rows_keyed = {}
  for row in target_rows:
    key = GetRowPrimaryKeyTuple(table_schema, row)
    target_rows_keyed[key] = row

  # Get all the different SQL statements: INSERT, UPDATE, DELETE
  diff_sql_statements = []

  # Compare source rows with target rows to find INSERT and UPDATE requirements
  for (row_key, row) in source_rows_keyed.items():
    # If the key isnt found in target, INSERT this row
    if row_key not in target_rows_keyed:
      primary_key = GeneratePrimaryKeyId(table_schema_source, row)
      select_key = GenerateDataKey('insert', zone_source, zone_target, database_set, instance, table, primary_key)
      if selected_schema_keys == None or select_key in selected_schema_keys:
        diff['insert'].append(row)
      else:
        print('Select key not found: %s - %s' % (select_key, selected_schema_keys))
    # Else, if the source row data is different than the target row data, UPDATE this row
    elif row != target_rows_keyed[row_key]:
      #NOTE(g): This is just to find differences, the selection process has not yet occurred, so it's
      #   OK to store updates that will not be applied because the Target table does not have the field
      #   that the Source table has, so the SQL will not be generated
      primary_key = GeneratePrimaryKeyId(table_schema_source, row)
      select_key = GenerateDataKey('update', zone_source, zone_target, database_set, instance, table, primary_key)
      if selected_schema_keys == None or select_key in selected_schema_keys:
        diff['update'].append((row, target_rows_keyed[row_key]))
      else:
        print('Select key not found: %s - %s' % (select_key, selected_schema_keys))

  # Compare target rows with source rows to find DELETE requirements
  for (row_key, row) in target_rows_keyed.items():
    # If the key isnt found in source, DELETE this row
    if row_key not in source_rows_keyed:
      primary_key = GeneratePrimaryKeyId(table_schema_source, row)
      select_key = GenerateDataKey('insert', zone_source, zone_target, database_set, instance, table, primary_key)
      if selected_schema_keys == None or select_key in selected_schema_keys:
        diff['delete'].append(target_rows_keyed[row_key])
      else:
        print('Select key not found: %s - %s' % (select_key, selected_schema_keys))

  return diff


def GetTableSchema(zone_source, zone_target, database_set, instance, table, selected_schema_keys=None):
  """Returns tuple (table_schema_source, table_schema_data).

  These are dictionaries that hold table schema info, and 

  If selected_schema_keys is not None, then only some of the fields are applied from the 
  source to the target, in the case that the target schema is missing fields the source
  schema has.

  TODO(g): Implement selected_schema_keys, so not all fields are applied to the target
  """
  schema_source = GetSchema(zone_source, database_set, instance)
  schema_target = GetSchema(zone_target, database_set, instance)

  # If the table exists in the source, get it
  if table in schema_source:
    table_schema_source = schema_source[table]
  # Else, if the source table doesnt exist, use the target schema, because the field differences dont matter
  else:
    table_schema_source = schema_target[table]

  # If the target table exists, use it
  if table in schema_target:
    table_schema_target = schema_target[table]
  # Else, use the source schema, because it doesnt exist in the target zone
  else:
    table_schema_target = table_schema_source

  return (table_schema_source, table_schema_target)


def CreateSQLFromDataDiff(zone_source, zone_target, database_set, instance, diff, selected_schema_keys=None):
  """Returns a forward and reverse list of SQL statements (strings)"""
  diff_sql_statements = []

  for (table, table_diff) in diff.items():
    # Get the schemas for the tables, each time so that they can be matches together
    (table_schema_source, table_schema_target) = GetTableSchema(zone_source, zone_target, database_set, instance, table, selected_schema_keys=selected_schema_keys)
    
    for row in table_diff['insert']:
      diff_sql_statements.append(CreateInsert(table_schema_source, table_schema_target, table, row))
    
    for row in table_diff['delete']:
      diff_sql_statements.append(CreateDelete(table_schema_source, table_schema_target, table, row))
    
    for (source_row, target_row) in table_diff['update']:
      update_result = CreateUpdate(table_schema_source, table_schema_target, table, source_row, target_row)
      # Only append the UPDATE result if it is not None, because Target fields may not exist, 
      #     so no SETs can be performed
      if update_result != None:
        diff_sql_statements.append(update_result)

  return diff_sql_statements


def SyncTargetZone(zone, database_set, instance, sync_commands, depth=0):
  """Processes all the commands(queries) to sync the zones using sync_commands SQL.

  sync_commands is a list of strings (SQL) and lists of strings (SQL).  When a 
  list item is encountered SyncZones calls itself recursively.  Depth should not be
  a problem as lists are only embedded one deep and not recursively embedded.
  """
  if depth == 0:
    log('Sync Target Zone: %s %s %s' % (zone, database_set, instance))

  # Process all the commands (SQL strings or lists of SQL strings)
  for command in sync_commands:
    # If this is an embedded list of commands, recurse and process them
    if type(command) == list:
      SyncTargetZone(zone, database_set, instance, command, depth=depth+1)

    # Else, make the query to satisfy this command
    else:
      log('  %s' % command)
      Query(zone, database_set, instance, command)


def GenerateSchemaDiffKeyDictionary(zone_source, zone_target, database_set, instance, diff):
  """Returns a dictionary with a key for each element, for schemas, to be generally referenced with other formats."""
  keys = {}

  # Build the prefix so that the "location" of this key is easily derived from text, which can be
  #   passed between different systems and stored.
  prefix = '%s.%s.%s.%s' % (zone_source, zone_target, database_set, instance)

  # Generate Schema Difference Keys
  # Alter
  for (table, table_diff) in diff['alter'].items():
    #TODO(g): Needed?  Im not sure, leaving cause this is complicated and re-implementation is likely
    # # Get the schemas for the tables, each time so that they can be matches together
    # (table_schema_source, table_schema_target) = GetTableSchema(zone_source, zone_target, database_set, instance, table, selected_schema_keys=selected_schema_keys)

    # Alter - Add Column
    for (field, field_data) in table_diff['add'].items():
      key = 'schema.alter.add.%s.%s.%s' % (prefix, table, field)
      keys[key] = field_data

    # Alter - Delete Column
    for (field, field_data) in table_diff['delete'].items():
      key = 'schema.alter.delete.%s.%s.%s' % (prefix, table, field)
      keys[key] = field_data

    # Alter - Modify Column
    for (field, field_data) in table_diff['modify'].items():
      key = 'schema.alter.modify.%s.%s.%s' % (prefix, table, field)
      keys[key] = field_data

  # Create
  for (table, table_diff) in diff['create'].items():
    #TODO(g): Needed?  Im not sure, leaving cause this is complicated and re-implementation is likely
    # # Get the schemas for the tables, each time so that they can be matches together
    # (table_schema_source, table_schema_target) = GetTableSchema(zone_source, zone_target, database_set, instance, table, selected_schema_keys=selected_schema_keys)

    key = 'schema.create.%s.%s.%s' % (prefix, table, field)
    keys[key] = field_data

  # Drop
  for (table, table_diff) in diff['drop'].items():
    #TODO(g): Needed?  Im not sure, leaving cause this is complicated and re-implementation is likely
    # # Get the schemas for the tables, each time so that they can be matches together
    # (table_schema_source, table_schema_target) = GetTableSchema(zone_source, zone_target, database_set, instance, table, selected_schema_keys=selected_schema_keys)

    key = 'schema.drop.%s.%s.%s' % (prefix, table, field)
    keys[key] = field_data


  return keys



def GenerateDataDiffKeyDictionary(zone_source, zone_target, database_set, instance, diff, only_table=None, selected_schema_keys=None):
  """Returns a dictionary with a key for each element, for data, to be generally referenced with other formats."""
  keys = {}


  # Generate Data Difference Keys
  for (table, table_diff) in diff.items():
    # If were only generating a diff for one table, skip the rest
    if only_table and table != only_table:
      continue

    # Get the schemas for the tables, each time so that they can be matches together
    (table_schema_source, table_schema_target) = GetTableSchema(zone_source, zone_target, database_set, instance, table, selected_schema_keys=selected_schema_keys)

    # Alter - Add Column
    for item in table_diff['insert']:
      primary_key = GeneratePrimaryKeyId(table_schema_source, item)
      key = GenerateDataKey('insert', zone_source, zone_target, database_set, instance, table, primary_key)
      keys[key] = item

    # Alter - Delete Column
    for item in table_diff['delete']:
      primary_key = GeneratePrimaryKeyId(table_schema_source, item)
      key = GenerateDataKey('delete', zone_source, zone_target, database_set, instance, table, primary_key)
      keys[key] = item

    # Alter - Modify Column.  Gets both items so the order for updating is obvious
    for (source_item, target_item) in table_diff['update']:
      primary_key = GeneratePrimaryKeyId(table_schema_source, source_item)
      key = GenerateDataKey('update', zone_source, zone_target, database_set, instance, table, primary_key)
      keys[key] = (source_item, target_item)


  return keys


def GenerateDataKey(operation, zone_source, zone_target, database_set, instance, table, primary_key):
  """operation is 'insert'/'update'/'delete'"""
  # Build the prefix so that the "location" of this key is easily derived from text, which can be
  #   passed between different systems and stored.
  prefix = '%s.%s.%s.%s' % (zone_source, zone_target, database_set, instance)

  # Build up the final key
  key = 'data.%s.%s.%s.%s' % (operation, prefix, table, primary_key)

  return key


def GeneratePrimaryKeyId(table_schema_source, row):
  """Generate the primary key identifier from the table schema and row data (dict)"""
  key_id = ''

  keys = table_schema_source['__PRIMARY_KEYS__']

  # Append all our keys with __ separator
  for key in keys:
    # Make value easy to pass, by compressing whitespace
    value = str(row[key]).replace(' ', '').replace('\n', '').replace('\t', '')

    # Append the value to the key
    key_id += '__%s' % SanitizeSqlString(value)

  return key_id


def ParsePrimaryKeyFromKeyId(zone, database_set, instance, table, key_id):
  """Returns a dict, with the field names as the keys, and their values as the data."""
  # print('ParsePrimaryKeyFromKeyId ::: ::: %s : %s' % (table, key_id))
  primary_key_values = {}

  schema = GetSchema(zone, database_set, instance)
  table_schema = schema[table]

  # Ignore first __ separator.  
  #TODO(g): Should I bother fixing that at this point?  Its stupid having the leading __ separator, separating nothing...
  key_values = key_id.split('__')[1:]

  # print('ParsePrimaryKeyFromKeyId ::: ::: %s : %s' % (key_values, table_schema['__PRIMARY_KEYS__']))

  # Set all the primary keys from the key values, for our result
  for count in range(0, len(table_schema['__PRIMARY_KEYS__'])):
    primary_key = table_schema['__PRIMARY_KEYS__'][count]
    primary_key_value = key_values[count]
    primary_key_values[primary_key] = primary_key_value

  return primary_key_values



def ParseChangeListKey(key):
  """Returns tuple of all the change list key parts.  Always includes subaction (None if normally not there).  
  All return tuple parts are strings.

  Returns: (kind, action, subaction, zone_source, zone_target, database_set, database, table, row)
  """
  # Default so subaction is always returned
  subaction = None
  try:
    (kind, action, zone_source, zone_target, database_set, database, table, row) = key.split('.')
  except ValueError:
    (kind, action, subaction, zone_source, zone_target, database_set, database, table, row) = key.split('.')

  return (kind, action, subaction, zone_source, zone_target, database_set, database, table, row)


def ParseChangeListKeyAsDist(key):
  """Returns dict of all the change list key parts.  Always includes subaction (None if normally not there).  

  NOTE:Returns duplicate of 'database' and 'instance' keys since I used them in different places and this 
      will help so no one has to remember when they changed.  That sucks, but can be fixed on cleanup if 
      there is time.  Things have to keep moving...

  'row' is the unprocessed key, in format:  "__id__id__id", where "id" is the primary key part(field) value.
      This can be processed with ParsePrimaryKeyFromKeyId() to get the result, and auto-processing would 
      be wasteful, though seems like it be a helpful programming feature, need to keep performance up.
  """
  (kind, action, subaction, zone_source, zone_target, database_set, database, table, row) = ParseChangeListKey(key)

  data = {'kind':kind, 'action':action, 'subaction':subaction, 'zone_source':zone_source, 
          'zone_target':zone_target, 'database_set':database_set, 'database':database, 'instance':database,
          'table':table, 'row':row}

  return data

