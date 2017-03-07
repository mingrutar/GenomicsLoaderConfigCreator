import sqlite3
import os, os.path
import json
import time

class RunVCFData(object):
    DefaultDBName = 'genomicsdb_loader.db'

    __extra_data = {}

    queries = {
        "Host" : 'SELECT hostname FROM host WHERE avalability = 1;',
        "Template" : 'SELECT name, file_path, params, extra FROM template;',
        "LC_Tag" : 'SELECT name, type, default_value FROM loader_config_tag where user_definable=0;',
        "LC_OverrideTag" : 'SELECT name, type, default_value,tag_code FROM loader_config_tag where user_definable=1;',
        'AllUser_LC' : 'SELECT name, config, _id from loader_config_def;',
        'AllRuns' : 'SELECT _id, loader_configs from run_def;',

        'INSERT_LOADER' : "INSERT INTO loader_config_def (name, config, creation_ts) VALUES (\"%s\", \"%s\", %d);",
        'INSERT_RUN_DEF' : 'INSERT INTO run_def (loader_configs, target_comand, creation_ts) VALUES (\"%s\", \"%s\", %d);',
        'INSERT_RUN_LOG' : 'INSERT INTO run_log (run_def_id, num_parallel, full_cmd, tiledb_ws, host_id, creation_ts) VALUES (%d,%d,\"%s\",\"%s\",\"%s\", %d);',
        'INSERT_QUERY_RUN_DEF' : 'INSERT INTO run_def (run_loader_id, target_comand, creation_ts) VALUES (%d, \"%s\", %d);',

        'Run_Config' : 'SELECT loader_configs, _id FROM run_def where _id=%d;',
        'Last_Run_Config' : 'SELECT loader_configs, _id FROM run_def ORDER BY _id desc LIMIT 1;',
        'User_Config' : 'SELECT config FROM loader_config_def where name in (%s);',
        'User_Config_dict' : "SELECT name, config FROM loader_config_def where name in (%s);",
        'Time_Results' : 'SELECT tr.time_result, tr.genome_result, tr.pidstat_path, rl.lcname, rl.num_parallel FROM time_result tr, run_log rl where tr.run_id=rl._id and rl.run_def_id=%d order by rl._id desc;',

        # for backwards compatibility only, add lcname. TODO remove when no longer needed        
        'SELECTALL_RUN_LOG' : 'SELECT * FROM run_log limit 1;',
        'ADD_LCNAME2RUN_LOG' : 'ALTER table run_log ADD column "lcname" "TEXT";',
        'SELECT_RUN_LOG' : 'SELECT _id , full_cmd FROM run_log;',
        'UPDATE_RUN_LOG' : 'UPDATE run_log SET lcname="%s" WHERE _id = %s;'
          }
    
    def updateRunLogLCName(self) :
        mycursor = self.db_conn.cursor()
        mycursor.execute(self.queries["SELECTALL_RUN_LOG"])
        if 'lcname' not in [ x[0] for x in mycursor.description ]:
            mycursor.execute(self.queries['ADD_LCNAME2RUN_LOG'])

        mycursor2 = self.db_conn.cursor()
        mycursor.execute(self.queries["SELECT_RUN_LOG"])
        rows = list(mycursor.fetchall())
        for r in rows:
            lcname = r[1].split('-')[-1][:-5]
            update_query = self.queries['UPDATE_RUN_LOG'] % (lcname, r[0])
            mycursor2.execute(update_query)
        mycursor.close()
        mycursor2.close()
        self.db_conn.commit()

    def getRunConfigs(self, runid, bFillFlag=True):
        ''' fillFlag = 0 => no fill, = 1 => fill with '-'; = 2 => fill with default_value '''
        mycursor = self.db_conn.cursor()
        definable_tags = self.getUserDefinableConfigTags(mycursor)

        myrunid, cfg_names = self.getRunConfigNames(runid, mycursor)
        if cfg_names:
            query = self.queries['User_Config'] % ",".join( [ "\"%s\"" % x for x in cfg_names ] )
            lc_items = []
            for row in mycursor.execute(query):
                lc_items.append(dict(eval(row[0])))     # user overrided tags
            
            if bFillFlag > 0:                # need to fill not overrided tags
                for cfg in lc_items:
                    for key, val in definable_tags.items():
                        if key not in cfg:
                            cfg[key] = str(val[2])
                        else:
                            cfg[key] = "%s*" % cfg[key]
            return myrunid, lc_items
        else:

            return None, None

    def getRunConfigsDict(self, runid, bFillFlag=True):
        ''' fillFlag = 0 => no fill, = 1 => fill with '-'; = 2 => fill with default_value '''
        mycursor = self.db_conn.cursor()
        definable_tags = self.getUserDefinableConfigTags(mycursor)

        myrunid, cfg_names = self.getRunConfigNames(runid, mycursor)
        if cfg_names:
            query = self.queries['User_Config_dict'] % ",".join( [ "\"%s\"" % x for x in cfg_names ] )
            lc_items = {}
            for row in mycursor.execute(query):
                lc_items[row[0]] = dict(eval(row[1]))     # user overrided tags
            
            if bFillFlag > 0:                # need to fill not overrided tags
                for cfg in lc_items.values():
                    for key, val in definable_tags.items():
                        if key not in cfg:
                            cfg[key] = str(val[2])
                        else:
                            cfg[key] = "%s*" % cfg[key]
            return myrunid, lc_items
        else:

            return None, None

    def getRunConfigNames(self, runid=None, cursor=None):
        ''' return the loader config list for a run, last run if runid is None  '''
        query = self.queries['Run_Config'] % runid if runid else self.queries['Last_Run_Config']
        mycursor = cursor if cursor else self.db_conn.cursor()
        ret = None
        for row in mycursor.execute(query):     # only one result
            ret = (row[1], [ lc for lc in row[0].split('-') ])
        if not cursor:
            mycursor.close()
        return ret
    
    def getAllResult(self, runid):
        assert runid
        mycursor =  self.db_conn.cursor()
        all_results = []
        for row in mycursor.execute(self.queries['Time_Results'] % int(runid)):   #TODO, enumerate?
            rowresult = dict()
            rowresult['rtime'] = dict(eval(row[0]))
            rowresult['gtime'] = eval(row[1])
            rowresult['pidstat'] = eval(row[2])
            rowresult['lcname'] = row[3]
            rowresult['n_parallel'] = row[4]

            all_results.append(rowresult)
        return all_results

    def __init__(self, db_name=None):
        self.db_name = db_name if db_name else self.DefaultDBName 
        self.db_conn = sqlite3.connect(self.db_name)

    def getExtraData(self, extra_data_key):
        return self.__extra_data[extra_data_key] if extra_data_key in  self.__extra_data else None

    def histogram (self, val, working_dir):
        self.__extra_data['histogram_file_path'] = val.replace("$WS_HOME", working_dir)

    def __proc_template(self, working_dir, templateFile, sub_key_val = None, extra_args = None) :
        f_path = templateFile.replace("$WS_HOME", working_dir)
        if os.path.isfile(f_path) :
            if f_path[-5:] == '.temp' :
                with open(f_path, 'r') as fd :
                    context = fd.read()
                jf_path = "%s.json" % f_path[:-5] 
                if sub_key_val:
                    for key, val in sub_key_val.items() :
                        context = context.replace(key, val)
                    with open(jf_path, 'w') as ofd:
                        ofd.write(context)
                f_path = jf_path
            if extra_args:
                for key, val in extra_args.items():
                    getattr(self, key)(val, working_dir) 
            return f_path
        else :
            print("WARN: template file %s not found" % f_path)
            return None

    def getDBName():
        return self.db_name if self.db_name else None

    def close(self):
        if self.db_conn:
            self.db_conn.close()

    def getHosts(self):
        mycursor = self.db_conn.cursor()
        mycursor.execute(self.queries["Host"])
        rows = list(mycursor.fetchall())
        hosts = []
        for r in rows:
            hosts.append(r[0])
        mycursor.close()
        return hosts
    
    def getTemplates(self, wdir):
        mycursor = self.db_conn.cursor()
        templates = {}
        for r in mycursor.execute(self.queries['Template']):
            temp_name = str(r[1]).replace("'", "\"")
            jstrParams = json.loads(str(r[2]).replace("'", "\"")) if r[2] else None
            jstrMore =   json.loads(str(r[3]).replace("'", "\"")) if r[3] else None
            input_file = self.__proc_template(wdir, temp_name, jstrParams, jstrMore) 
            templates[r[0]] = input_file if input_file else temp_name
        mycursor.close()
        return templates

    def getUserDefinableConfigTags(self, cursor):
        mycursor = cursor if cursor else self.db_conn.cursor()
        lc_overridable_tags={}
        for row in mycursor.execute(self.queries['LC_OverrideTag']):
            lc_overridable_tags[row[0]] = list(row)
        if not cursor:
            mycursor.close()
        return lc_overridable_tags

    def getConfigTags(self):
        lc_fixed_tags={}
        mycursor = self.db_conn.cursor()
        for row in mycursor.execute(self.queries['LC_Tag']):
            lc_fixed_tags[row[0]] = list(row)
        lc_overridable_tags=self.getUserDefinableConfigTags(mycursor)
        mycursor.close()
        return lc_fixed_tags, lc_overridable_tags         
    
    def getAllUserDefinedConfigItems(self):
        defined_loaders = {}
        mycursor = self.db_conn.cursor()
        for row in mycursor.execute(self.queries['AllUser_LC']):
            line = row[1].replace("u'", "\"").replace("'", "\"")
            cfg = eval( line )
            defined_loaders[row[0]] = (cfg, row[2])  
        mycursor.close()
        return defined_loaders
    
    def getAllRuns(self):
        defined_runs = {}
        mycursor = self.db_conn.cursor()
        for row in mycursor.execute(self.queries['Select_defined_run']) :
            defined_runs[row[0]] = row[1]
        mycursor.close()
        return defined_runs

    def addUserDefinedConfig(self, lcname, configStr) :
        mycursor = self.db_conn.cursor()
        stmt = self.queries['INSERT_LOADER'] % (lcname, configStr, int(time.time()) )
        mycursor.execute(stmt)
        loader_id = mycursor.lastrowid
        self.db_conn.commit()
        mycursor.close()
        return loader_id

    def addRunConfig(self, configs, cmd ):
        mycursor = self.db_conn.cursor()
        stmt = self.queries['INSERT_RUN_DEF'] % (configs, cmd, int(time.time()) )
        mycursor.execute(stmt)
        run_id = mycursor.lastrowid
        self.db_conn.commit()
        mycursor.close()
        return run_id

    def addRunLog(self, rundef_id, host, cmd, tiledb_ws, num_parallel=1):
        mycursor = self.db_conn.cursor()
        stmt = self.queries['INSERT_RUN_LOG'] % (rundef_id,num_parallel,cmd,tiledb_ws,host, int(time.time()) )
        mycursor.execute(stmt)
        run_id = mycursor.lastrowid
        self.db_conn.commit()
        mycursor.close()
        return run_id  

    def addQueryRun(self, run_id, cmd):
        mycursor = self.db_conn.cursor()
        stmt = self.queries['INSERT_QUERY_RUN_DEF'] % (run_id, cmd, int(time.time()) )
        mycursor.execute(stmt)
        run_id = mycursor.lastrowid
        self.db_conn.commit()
        mycursor.close()
        return run_id
