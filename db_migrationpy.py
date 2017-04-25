# db_migrade
#
queries = {
        'CREATE_VERSION_TABLE' : 'CREATE TABLE IF NOT EXISTS exec_info (_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
            'buid_tag TEXT NOT NULL,name TEXT NOT NULL,version TEXT NOT NULL,full_path TEXT NOT NULL,' \
            'build_time INTEGER NULL,hash TEXT,additional TEXT );',
    
          # for backwards compatibility only, add lcname. TODO remove when no longer needed        
        'SELECTALL_EXEC_DEF' : 'SELECT * FROM exec_def limit 1;',
        'ADD_LCNAME2EXEC_DEF' : 'ALTER table exec_def ADD column "lcname" "TEXT";',
        'SELECT_EXEC_DEF' : 'SELECT _id , full_cmd, tiledb_ws FROM exec_def WHERE run_def_id=%d;',
        'SELECT_EXEC_DEF_ALL' : 'SELECT _id , full_cmd, tiledb_ws FROM exec_def;',
        'UPDATE_EXEC_DEF' : 'UPDATE exec_def SET lcname="%s" WHERE _id = %s;'
}
    def check_version_table():
        self.db_name = db_name if db_name else self.DefaultDBName 
        #TODO: for now create db only
        data_handler = RunVCFData(self.db_name)
        data_handler.close()

        self.db_conn = sqlite3.connect(self.db_name)
        # mycursor = self.db_conn.cursor()
        # mycursor.execute(self.queries["CREATE_VERSION_TABLE"])
        # mycursor.close()
        
    def updateRunLogLCName(self, rundef_id=None) :
        mycursor = self.db_conn.cursor()
        mycursor.execute(self.queries["SELECTALL_EXEC_DEF"])
        if 'lcname' not in [ x[0] for x in mycursor.description ]:
            mycursor.execute(self.queries['ADD_LCNAME2EXEC_DEF'])

        mycursor2 = self.db_conn.cursor()
        stmt = self.queries["SELECT_EXEC_DEF"] % rundef_id if rundef_id else self.queries["SELECT_EXEC_DEF_ALL"]
        mycursor.execute(stmt)
        rows = list(mycursor.fetchall())
        q_count = 0
        l_count = 0
        ll = len('tiledb-ws_')
        for r in reversed(rows):
            if 'vcf2tiledb' in r[1]:
                lcname = r[1].split('-')[-1][:-5]
                l_count += 1
            else:
                sstmp = os.path.basename(r[2][1:-1])[ll:]
                lcname = sstmp.split('-')[0]
                q_count += 1
            update_query = self.queries['UPDATE_EXEC_DEF'] % (lcname, r[0])
            mycursor2.execute(update_query)
        self.db_conn.commit()
        mycursor.close()
        mycursor2.close()
        print("updateRunLogLCName change %d loader and %d query rec" % (l_count, q_count))


functions:
  load2other_host(orig_exec_def_id, host_id, remoe_orig_tiledbws=False)
  set_hosts({host_name, enable|disable})   
  {host_name, enable|disable} = get_hosts()
------
run_def (
  target_comand => target_command
 delete loader_def_id;
 add description TEXT
);
----
-- change from run_log => run_host_def
  add description
  profiler => extra_info
-- 
run_log (  => exec_def
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   run_def_id REFERENCES run_def(_id),
   num_parallel INTEGER DEFAULT 1,
   full_cmd TEXT NOT NULL,
   tiledb_ws TEXT NOT NULL,
   host_id REFERENCES host(_id),
   lcname TEXT DEFAULT '', 
   profiler TEXT DEFAULT 'time', => extra_info TEXT DEFAULT '{}',
   profiler_type TEXT DEFAULT 'time',
 +     tiledb_ws_status INTEGER DEFAULT 0,  0, not exist, 1 exist
 +  description TEXT,
   creation_ts INTEGER NOT NULL
);

--- for analysis, add partition 1 and total size for convenience
CREATE TABLE IF NOT EXISTS time_result (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   run_id REFERENCES run_host_def (_id),
   target_comand TEXT =>  exec_info REFERENCES exec_info(_id),

--- add
CREATE TABLE IF NOT EXISTS scheme_info (
   _id INTEGER PRIMARY KEY AUTOINCREMENT,
   version TEXT DEAFULT '',
   operation TEXT DEFAULT 'create',
   timestamp INTEGER NULL
 );
