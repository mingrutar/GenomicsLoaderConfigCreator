---
--- create_scheme.sql
---
--- sqlite3 genomicsdb_loader.db < create_scheme.sql
---- .read pre_fill.sql
---

PRAGMA foreign_keys = ON;
DROP TABLE IF EXISTS loader_config_tag;
DROP TABLE IF EXISTS template;
DROP TABLE IF EXISTS host;

CREATE TABLE IF NOT EXISTS host (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   hostname TEXT NOT NULL,
   ipaddress TEXT NULL,
   avalability INTEGER DEFAULT 1
 );
 --- loader config tags, with default_value 
 CREATE TABLE IF NOT EXISTS loader_config_tag (
    _id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    default_value TEXT NULL,
    tag_code TEXT NULL,
    user_definable INTEGER DEFAULT 0,
    CONSTRAINT name_uniq UNIQUE (name ) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS template (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   name Text NOT NULL,
   file_path Path NOT NULL,
   params TEXT NULL,
   extra TEXT NULL
 );
--
-- define loader_config <=> a .json file under loaders/
--con
CREATE TABLE IF NOT EXISTS loader_config_def (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   name TEXT NOT NULL,
   config TEXT NOT NULL,
   creation_ts INTEGER NOT NULL,
   CONSTRAINT name_ts_uniq UNIQUE (name) ON CONFLICT REPLACE
);
----
-- define a run, status = run through, canceled, ..?
-- currently only profile with time
----
CREATE TABLE IF NOT EXISTS run_def (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   loader_configs TEXT NOT NULL,
   profiler TEXT DEFAULT 'time',
   profiler_type TEXT DEFAULT 'time',
   creation_ts INTEGER NOT NULL
);
----
--- result and result_type describe the outcome
--- run_flag: 0x1-use mpirun, 0x2-user_assign_host ...
---
CREATE TABLE IF NOT EXISTS run_loader_host (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   run_def_id REFERENCES run_def(_id),
   run_flag INTEGER DEFAULT 0,
   loader_on_host TEXT NOT NULL, 
   start_time INTEGER NOT NULL,
   end_time INTEGER NOT NULL,
   status INTEGER NOT NULL,
   result TEXT NULL, 
   result_type TEXT NULL 
);

--- for analysis, add partition 1 and total size for convenience
CREATE TABLE IF NOT EXISTS time_result (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   run_id REFERENCES run_loader_host (_id),
   time_result TEXT NOT NULL,
   genome_result TEXT,
   partition_1_size INTEGER DEFAULT 0,
   db_size INTEGER DEFAULT 0,
   pidstat_path TEXT
);
