#!/usr/bin/env python3

"""
dbsync: Database Sync: Command Line Interface

Python3.x was required for consist Unicode processing, stored in the DB

This CLI tool allows scripting Database Syncing between Zones.  

dbsync_server.py will call into the same functions, but will be for interactive 
multi-stage, multi-user, transactions.  CLI is meant for automation or 
single-user interactive runs.
"""


__author__ = 'Geoff Howland <geoff@gmail.com>'


import sys
import os
import getopt
import traceback
import yaml

# # Custom script imports
# import zone_manager


import AbsoluteImport
AbsoluteImport.RegisterPathPrefix('procblock', '/Users/ghowland/projects/dropstar/dropstar/control/procblock/')
AbsoluteImport.RegisterPathPrefix('unidist', '/Users/ghowland/projects/dropstar/dropstar/control/unidist/')
AbsoluteImport.RegisterPathPrefix('dbsync', '/Users/ghowland/projects/dbsync/')


from AbsoluteImport import Import

log = Import('log', prefix='unidist').log
zone_manager = Import('zone_manager', prefix='dbsync')
print(zone_manager)


# Commands mappng their descriptions to auto-build Usage info
#NOTE(g): Value Tuple = (args, description)
COMMANDS = {
  'list':('', 'List Groups and their Zones'),
  'compare':('<database set> <souce_zone> <target_zone> [instance]', 'Compare a Database Set source and target Zone Database Instances, optional DB Instance'),
  'sync':('<database set> <souce_zone> <target_zone> [instance]', 'Sync a Database Set source and target Zone Database Instances, optional DB Instance'),
}


def GetDatabaseSetInstances(database_set, directory_prefix=None):
  """Returns dict of instances"""
  database_set_data = zone_manager.DATABASE_SETS

  yaml_path = '%s' % database_set_data[database_set]['data']

  if directory_prefix:
    yaml_path = '%s/%s' % (directory_prefix, yaml_path)

  data = yaml.load(open(yaml_path))

  return data


def ProcessCommand(command, args, options):
  """Process the command: routes to specific command functions
  
  Args:
    command: string, command to process - routing rule
    args: list of strings, args for the command
    options: dict of string/string, options for the command
  """
  #print('Process command: %s: %s: %s' % (command, options, args))
  
  #TODO(g): Whatever needs to be done gets set into result...
  zones = zone_manager.ZONES
  database_set_data = zone_manager.DATABASE_SETS
  output = ''

  # List available data...
  if command == 'list':
    result = '\nZones:\n\n'

    # Loop over Zones
    for (zone, database_sets) in zones.items():
      # Loop over Database Sets in each Zone
      for (database_set, database_data) in database_sets.items():
        # If this database set exists and has data, load it (baased on Database Set conf data)
        if database_set in database_set_data and 'data' in database_set_data[database_set]:
          data = yaml.load(open(database_set_data[database_set]['data']))
          data_str = ' Instances: %s' % len(data)
        else:
          data = None
          data_str = ''

        result += '   Zone: %-15s   Set: %-15s  Host: %-40s%s\n' % \
                  (zone, database_set, database_data['host'],
                   data_str)

  
 # Compare the source and target zone database set instances
  elif command == 'compare':
    # Test all the argument cases for failures
    if len(args) < 1:
      Usage('Compare: Missing 3 arguments: <database set> <source zone> <target zone>')
    elif len(args) < 2:
      Usage('Compare: Missing 2 arguments: <source zone> <target zone>')
    elif len(args) < 3:
      Usage('Compare: Missing 1 argument: <target zone>')
    elif len(args) > 4:
      Usage('Compare: Too many arguments.  3 are required, the 4th is optional')

    # Set the variables
    database_set = args[0]
    zone_source = args[1]
    zone_target = args[2]
    if len(args) > 3:
      instance = args[3]
    else:
      instance = None

    # Validate arguments
    if database_set not in database_set_data:
      Usage('Database set "%s" is not a valid dataset listed in: conf/database_sets.yaml' % database_set)
    if zone_source not in zones:
      Usage('Source Zone "%s" not a valid zone listed in: conf/zones.yaml' % zone_source)
    if zone_target not in zones:
      Usage('Target Zone "%s" not a valid zone listed in: conf/zones.yaml' % zone_target)

    # Get the Database Set data
    #yaml_path = '%s/%s' % (__file__, database_set_data[database_set]['data'])
    yaml_path = '%s' % database_set_data[database_set]['data']
    data = yaml.load(open(yaml_path))
    if instance != None and instance not in data:
      Usage('Instance "%s" not found in Database Set data: %s' % (instance, database_set_data[database_set]['data']))

    output += 'Comparing: %s %s %s %s' % (database_set, zone_source, zone_target, instance)

    #TEST: Attempt querying zone, naively...
    schema_source = zone_manager.GetSchema(zone_source, database_set, instance)
    schema_target = zone_manager.GetSchema(zone_target, database_set, instance)

    # print('schema source:\n%s' % str(schema_source))
    # print('\n\n');
    # print('schema target:\n%s' % str(schema_target))

    comparison = zone_manager.CompareSchemas(schema_source, schema_target)
    output += '\n\nComparison: \n'
    import pprint
    output += pprint.pformat(comparison)

    # Get the Schema Diff in a key-oriented dictionary format
    schema_diff_keys = zone_manager.GenerateSchemaDiffKeyDictionary(zone_source, zone_target, database_set, instance, comparison)
    output += '\n\nSchema Key Diff: \n'
    import pprint
    output += pprint.pformat(schema_diff_keys)


    # Generate Forward and Reverse commands to sync the Target DB to Source DB
    (forward_commands, reverse_commands) = zone_manager.GenerateSchemaSyncCommands(zone_source, \
                                              zone_target, database_set, instance, \
                                              comparison)

    sql_comparison_forward = zone_manager.GetSql_DatabaseDiff(zone_source, zone_target, database_set, instance)
    sql_comparison_reverse = zone_manager.GetSql_DatabaseDiff(zone_target, zone_source, database_set, instance)
    output += '\n\nSQL Comparison: \n'
    import pprint
    output += pprint.pformat(sql_comparison_forward)

    # Get the Schema Diff in a key-oriented dictionary format
    data_diff_keys = zone_manager.GenerateDataDiffKeyDictionary(zone_source, zone_target, database_set, instance, sql_comparison_forward)
    output += '\n\nData Key Diff: \n'
    import pprint
    output += pprint.pformat(data_diff_keys)


    forward_commands += zone_manager.CreateSQLFromDataDiff(zone_source, zone_target, database_set, instance, sql_comparison_forward)
    reverse_commands += zone_manager.CreateSQLFromDataDiff(zone_source, zone_target, database_set, instance, sql_comparison_reverse)

    # If we have any commands
    if forward_commands or reverse_commands:
      output += '\n\nCompare completed: \n\nForward: %s\n\nReverse: %s\n\n' % (pprint.pformat(forward_commands), \
                                                                          pprint.pformat(reverse_commands))
    else:
      output += '\n\nCompare completed: No work to do'


    result = output

 # Sync the source and target zone database set instances
  elif command == 'sync':
    # Test all the argument cases for failures
    if len(args) < 1:
      Usage('Compare: Missing 3 arguments: <database set> <source zone> <target zone>')
    elif len(args) < 2:
      Usage('Compare: Missing 2 arguments: <source zone> <target zone>')
    elif len(args) < 3:
      Usage('Compare: Missing 1 argument: <target zone>')
    elif len(args) > 4:
      Usage('Compare: Too many arguments.  3 are required, the 4th is optional')

    # Set the variables
    database_set = args[0]
    zone_source = args[1]
    zone_target = args[2]
    if len(args) > 3:
      instance = args[3]
    else:
      instance = None

    # Validate arguments
    if database_set not in database_set_data:
      Usage('Database set "%s" is not a valid dataset listed in: conf/database_sets.yaml' % database_set)
    if zone_source not in zones:
      Usage('Source Zone "%s" not a valid zone listed in: conf/zones.yaml' % zone_source)
    if zone_target not in zones:
      Usage('Target Zone "%s" not a valid zone listed in: conf/zones.yaml' % zone_target)

    # Get the Database Set data
    data = yaml.load(open(database_set_data[database_set]['data']))
    if instance != None and instance not in data:
      Usage('Instance "%s" not found in Database Set data: %s' % (instance, database_set_data[database_set]['data']))

    print('Syncing: %s %s %s %s' % (database_set, zone_source, zone_target, instance))

    # #TEST: Attempt querying zone, naively...
    schema_source = zone_manager.GetSchema(zone_source, database_set, instance)
    schema_target = zone_manager.GetSchema(zone_target, database_set, instance)

    # # print('schema source:\n%s' % str(schema_source))
    # # print('\n\n');
    # # print('schema target:\n%s' % str(schema_target))

    comparison = zone_manager.CompareSchemas(schema_source, schema_target)
    # #print('Comparison: \n')
    # #import pprint
    # #pprint.pprint(comparison)

    # # Generate Forward and Reverse commands to sync the Target DB to Source DB
    # (forward_commands, reverse_commands) = zone_manager.GenerateSchemaSyncCommands(comparison, zone_source, \
    #                                           zone_target, database_set, instance)

    # schema_source = zone_manager.GetSchema(zone_source, database_set, instance)
    # schema_target = zone_manager.GetSchema(zone_target, database_set, instance)

    # print('schema source:\n%s' % str(schema_source))
    # print('\n\n');
    # print('schema target:\n%s' % str(schema_target))

    # Generate Forward and Reverse commands to sync the Target DB to Source DB
    (forward_commands, reverse_commands) = zone_manager.GenerateSchemaSyncCommands(zone_source, \
                                              zone_target, database_set, instance, \
                                              comparison)

    sql_comparison_forward = zone_manager.GetSql_DatabaseDiff(zone_source, zone_target, database_set, instance)
    sql_comparison_reverse = zone_manager.GetSql_DatabaseDiff(zone_target, zone_source, database_set, instance)

    forward_commands += zone_manager.CreateSQLFromDataDiff(zone_source, zone_target, database_set, instance, sql_comparison_forward)
    reverse_commands += zone_manager.CreateSQLFromDataDiff(zone_source, zone_target, database_set, instance, sql_comparison_reverse)

    # # If we have any commands
    # if forward_commands or reverse_commands:
    #   result = 'Compare completed: \n\nForward: %s\n\nReverse: %s\n\n' % (pprint.pformat(forward_commands), \
    #                                                                       pprint.pformat(reverse_commands))
    # else:
    #   result = 'Compare completed: No work to do'

    import pprint
    print('Applying SQL:\n%s' % pprint.pformat(forward_commands))

    # Sync the zone instances with the Forward commands
    zone_manager.SyncTargetZone(zone_target, database_set, instance, forward_commands)

    result = 'Success'


  else:
    #NOTE(g): Running from CLI will test for this, so this is for API usage
    raise Exception('Unknown command: %s' % command)
  
  # Return whatever the result of the command was, so it can be used or formatted
  return result


def Usage(error=None, exit_code=None):
  """Print usage information, any errors, and exit.  
  If errors, exit code = 1, otherwise 0.
  """
  if error:
    print('\nerror: %s' % error)
    if exit_code == None:
      exit_code = 1
  else:
    if exit_code == None:
      exit_code = 0
  
  print()
  print('usage: %s [options] <command>' % os.path.basename(sys.argv[0]))
  print()
  print('Commands:')
  keys = list(COMMANDS.keys())
  keys.sort()
  for key in keys:
    (args, description) = COMMANDS[key]
    if args:
      command_str = '%s %s' % (key, args)
      print('  %s\n                          %s' % (command_str, description))
    else:
      print('  %-23s %s' % (key, description))

  #print('  other                Other commands go here')
  print()
  print('Options:')
  print()
  print('  -h, -?, --help          This usage information')
  print('  -v, --verbose           Verbose output')
  print()
  
  sys.exit(exit_code)


def Main(args=None):
  if not args:
    args = []
  
  long_options = ['help', 'verbose']
  
  try:
    (options, args) = getopt.getopt(args, '?hv', long_options)
  except Exception as exc:
    Usage(exc)
  
  # Dictionary of command options, with defaults
  command_options = {}
  command_options['verbose'] = False
  
  
  # Process out CLI options
  for (option, value) in options:
    # Help
    if option in ('-h', '-?', '--help'):
      Usage()
    
    # Verbose output information
    elif option in ('-v', '--verbose'):
      command_options['verbose'] = True
    
    # Invalid option
    else:
      Usage('Unknown option: %s' % option)
  
  
  # Ensure we at least have a command, it's required
  if len(args) < 1:
    Usage('No command sepcified')
  
  # Get the command
  command = args[0]
  
  # If this is an unknown command, say so
  if command not in COMMANDS:
    Usage('Command "%s" unknown.  Commands: %s' % (command, ', '.join(COMMANDS)))
  
  # If there are any command args, get them
  command_args = args[1:]

  try:
    result = ProcessCommand(command, command_args, command_options)

    # Do something with the result...
    print(result)

  except Exception as exc:
    error = 'Error:\n%s\n%s\n' % ('\n'.join(traceback.format_tb(exc.__traceback__)), str(exc))
    #Log(error)
    print(error)
    sys.exit(1)


if __name__ == '__main__':
  Main(sys.argv[1:])

