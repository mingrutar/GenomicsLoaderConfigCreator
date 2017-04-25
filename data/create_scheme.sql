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
   availibility INTEGER DEFAULT 1,
   creation_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   CONSTRAINT host_uniq UNIQUE (hostname ) ON CONFLICT REPLACE
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
----
--- not in use yet
--- query config tags, with default_value
----
CREATE TABLE IF NOT EXISTS query_config_tag (
    _id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    default_value TEXT NULL,
    tag_code TEXT NULL,
    user_definable INTEGER DEFAULT 0,
    CONSTRAINT name_uniq UNIQUE (name) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS template (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   name Text NOT NULL,
   file_path Path NOT NULL,
   params TEXT NULL,
   extra TEXT NULL,
   CONSTRAINT name_uniq UNIQUE (name ) ON CONFLICT REPLACE
 );
--
-- define loader_config <=> a .json file under loaders/
--con
CREATE TABLE IF NOT EXISTS loader_config_def (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   name TEXT NOT NULL,
   config TEXT NOT NULL,
   creation_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   CONSTRAINT name_ts_uniq UNIQUE (name) ON CONFLICT REPLACE
);
----
-- define a run, status = run through, canceled, ..?
-- currently only profile with time
-- target_command: vcf2tiledb, gt_mpi_gather,...
-- loader_configs: for loader, ldnames, for query list of loader_def_id
--- tiledb_ws_root require for loading
--- additional: such as zlib
----
CREATE TABLE IF NOT EXISTS batch_def (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   name TEXT,
   target_cmd_path TEXT NOT NULL,
   loader_configs TEXT NOT NULL,
   -- for loader
   tiledb_ws_root TEXT,
   mpirun_def TEXT,
   -- for query, config_args,such as  seg_size
   pick_mode INTEGER DEFAULT 0,
   config_args TEXT,
   -- shared
   runtime_env_id REFERENCES runtime_env(_id),
   description TEXT,
   creation_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   CONSTRAINT name_ts_uniq UNIQUE (name) ON CONFLICT REPLACE
);
---
-- for now 
CREATE TABLE IF NOT EXISTS runtime_env (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   library_path TEXT,
   additional TEXT
);
----
-- was run_log
-- loader_batch_id and query_batch_id mutual exclusive
CREATE TABLE IF NOT EXISTS test_def (
  _id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id REFERENCES batch_def(_id) NOT NULL,
  num_parallel INTEGER DEFAULT 1,
  lcname TEXT DEFAULT '',
  full_cmd TEXT NOT NULL,
  tiledb_ws TEXT NOT NULL,
  query_config TEXT,
  creation_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
-- default profiling are [time, pidstat]
-- overrite_ws = false is not supported yet
CREATE TABLE IF NOT EXISTS run_def (
  _id INTEGER PRIMARY KEY AUTOINCREMENT,
  hostname TEXT NOT NULL,
  run_defs TEXT NOT NULL,
  overrite_ws INTEGER DEFAULT 1,
  profilers TEXT,
  description TEXT,
  creation_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
--- for analysis, add partition 1 and total size for convenience
CREATE TABLE IF NOT EXISTS test_result (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   test_def_id REFERENCES test_def (_id) NOT NULL,
   run_def_id REFERENCES run_def (_id) NOT NULL,
   cmd_version TEXT,
   time_result TEXT NOT NULL,
   target_stdout_path TEXT,
   target_stderr_path TEXT,
   partition_1_size INTEGER DEFAULT 0,
   db_size INTEGER DEFAULT 0,
   pidstat_path TEXT,
   other_profiling TEXT,
   genome_result TEXT,
   comment TEXT,
   creation_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
----
--- execution info, buid_tag is an abitrary value
----
CREATE TABLE IF NOT EXISTS exec_info (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   buid_tag TEXT NOT NULL,
   name TEXT NOT NULL,
   version TEXT DEAFULT '0.4',
   full_path TEXT NOT NULL,
   build_time INTEGER NULL,
   hash TEXT,
   other TEXT,
   creation_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
 );
----
--- operation is 'create' or 'upgrade'
----
CREATE TABLE IF NOT EXISTS scheme_info (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   version TEXT DEAFULT '0.2',
   operation TEXT DEFAULT 'create',
   creation_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
 );
