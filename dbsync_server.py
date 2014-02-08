#!/usr/bin/env python3

"""
Database Sync Server

Python3.x was required for consist Unicode processing, stored in the DB
"""


__author__ = 'Geoff Howland <geoff@gmail.com>'


import sys
import os
import socketserver
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from traceback import format_tb

import AbsoluteImport
from AbsoluteImport import Import

#TODO(ghowland):HARDCODED:PATH: Fix.  Sorry potential users.  Not ready for you yet unless you're ready to fix things up yourself...
AbsoluteImport.RegisterPathPrefix('unidist', '/Users/ghowland/projects/dropstar/dropstar/control/unidist/')

log = Import('log', prefix='unidist').log
Log = log
zone_manager = Import('zone_manager')
migrate = Import('migrate')


# Bind on this port
LISTEN_PORT = 6677
# Use this when testing, to not conflict with the "production" service
TEST_LISTEN_PORT = 7766


# Threaded mix-in
class AsyncXMLRPCServer(socketserver.ThreadingMixIn,SimpleXMLRPCServer):
  """Handles simultaneous requests via threads.  No code needed."""


class DatabaseSyncManager:
  """Sync Manager"""
  
  def Compare(self, zone_source, zone_target, database_set, instance):
    try:


      schema_source = zone_manager.GetSchema(zone_source, database_set, instance)
      schema_target = zone_manager.GetSchema(zone_target, database_set, instance)

      comparison = zone_manager.CompareSchemas(schema_source, schema_target)
      return comparison

    except Exception as exc:
      error = 'Error:\n%s\n%s\n' % ('\n'.join(format_tb(exc.__traceback__)), str(exc))
      print(error)
      Log(error)
      return {'[error]':error}
    


def Main(args=None):
  if not args:
    args = []
  
  # Instantiate and bind our listening port
  server = AsyncXMLRPCServer(('', LISTEN_PORT), SimpleXMLRPCRequestHandler, allow_none=True)
 
  # Register example object instance
  server.register_instance(DatabaseSyncManager())
 
  # Run!  Forever!
  #TODO(g): Switch to polling, and look for SIGTERM to quit nicely, finishing 
  #   any transactions in progress
  server.serve_forever()


if __name__ == '__main__':
  Main(sys.argv[1:])

