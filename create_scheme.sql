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
--
CREATE TABLE IF NOT EXISTS loader_config_def (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   name TEXT NOT NULL,
   creation_ts INTEGER NOT NULL,
   CONSTRAINT name_ts_uniq UNIQUE (name) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS loader_item_def (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   lc_id REFERENCES loader_config (_id)  ON DELETE CASCADE,
   tag_id REFERENCES loader_config_tag (_id),
   override_value TEXT NOT NULL,
   CONSTRAINT tag_lc_uniq UNIQUE (lc_id, tag_id) ON CONFLICT REPLACE
);
----
--- define a run, status = run through, canceled, ..?
--
--- run_flag: 0x1-use mpirun, 0x2-user_assign_host ...
----
CREATE TABLE IF NOT EXISTS run_def (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   name TEXT NOT NULL,
   run_flag INTEGER DEFAULT 0,
   creation_ts INTEGER NOT NULL,
   CONSTRAINT name_ts_uniq UNIQUE (name) ON CONFLICT REPLACE
);
----
--- result and result_type describe the outcome
---
CREATE TABLE IF NOT EXISTS run_loader_host (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   rd_id REFERENCES run_def (_id) ,
   lcd_id REFERENCES loader_config_def (_id) ,
   host_id REFERENCES host (_id) ,
   command TEXT NOT NULL,
   command_type TEXT NOT NULL,
   start_time INTEGER NOT NULL,
   end_time INTEGER NOT NULL,
   status INTEGER NOT NULL,
   result TEXT NULL, 
   result_type TEXT NULL, 
   CONSTRAINT lc_host_uniq UNIQUE (rd_id, lcd_id, host_id) ON CONFLICT REPLACE
);

--- for analysis, add partition 1 and total size for convenience
CREATE TABLE IF NOT EXISTS time_result (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   start_time INTEGER NOT NULL,
   command TEXT NOT NULL,
   user_time REAL NOT NULL,
   system_time REAL NOT NULL,
   pCPU INTEGER NOT NULL,
   wall_clock REAL NOT NULL,
   maximum_resident INTEGER NOT NULL,
   average_resident INTEGER NOT NULL,
   major_page_fault INTEGER NOT NULL,
   minor_page_fault INTEGER NOT NULL,
   voluntary_context_switches INTEGER NOT NULL,
   involuntary_context_switches INTEGER NOT NULL,
   exit_status INTEGER NOT NULL,
   partition_1_size INTEGER DEFAULT 0,
   db_size INTEGER DEFAULT 0,
   run_id REFERENCES run_loader_host (_id)
);
