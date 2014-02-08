"""
Database Migration

Functions related to comparing and migrating database schema and data between zones.
"""


from AbsoluteImport import Import

log = Import('log', prefix='unidist').log
zone_manager = Import('zone_manager')
Query = zone_manager.Query



class ComparisonException(Exception):
	"""Failed to compare database zones for configuration reasons."""



def Compare(zone_source, zone_target, database=None, table=None):
	"""Returns dict of differences between databases.  Keyed on [database][table][field]

		Schema is stored in __schema key in the database level of the dictionary.
	"""
	(zone_source, zone_target) = zone_manager.ValidateZoneSourceAndTarget(zone_source, zone_target)

	source_schema = GetZoneDatabaseSchema(zone_source, database=database, table=table)
	target_schema = GetZoneDatabaseSchema(zone_target, database=database, table=table)


def GetZoneDatabaseHost(zone_source, database, replication_master=True):
	"""Returns the host of the database in the given zone.  

		The Replication Master is the default, but the Replication Slave will be
		returned instead if this is set to False.  Only 1 database hose is allowed
		to be the Replication Slave of note.
	"""


def GetZoneDatabaseSchema(zone_source, database=None, table=None):
	"""Returns a dict of DB schemas.  Keys: [database][table][field][desc info]"""
	databases = Query()


