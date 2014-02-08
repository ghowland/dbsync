## dbsync

dbsync is a diff and migration tool for databases.  Currently only supports MySQL.

It will handle migrating data between databases in different security/use zones.  
It is not about replication or performing backups.

Certain tables will be excluded, such as user_* tables, *_events, comments and other zone specific data.

Examples of zones:

- Production
- Developer QA
- Producer QA
- Testing/Staging QA
- External QA (for external contractors)

--

This is "as is" software.  I haven't finished all the features, they aren't documented yet, and it's not guaranteed to work or not destroy your data.

YOU HAVE BEEN WARNED!  REPEAT: THIS MAY DESTROY YOUR DATA!!!  <--- 3!!!

