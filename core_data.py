import sqlite3
import os, os.path
import sys
import json
import time

# TODO: getAllHosta(), AllHost, setHosts(), SET_HOSTS
class RunVCFData(object):
    DefaultDBName = 'genomicsdb_loader.db'
    CreateDBScript = 'create_scheme.sql'
    PrefillDBScript = 'pre_fill.sql'

    __extra_data = {}

    queries = {
        "AllHost" : 'SELECT hostname FROM host;',
        "Host" : 'SELECT hostname FROM host WHERE avalability = 1;',
        "Template" : 'SELECT name, file_path, params, extra FROM template;',
        "LC_Tag" : 'SELECT name, type, default_value FROM loader_config_tag where user_definable=0;',
        "LC_OverrideTag" : 'SELECT name, type, default_value,tag_code FROM loader_config_tag where user_definable=1;',
        'AllUser_LC' : 'SELECT name, config, _id from loader_config_def;',
        'AllRuns' : 'SELECT _id, loader_configs from run_def;',

        'INSERT_LOADER' : "INSERT INTO loader_config_def (name, config) VALUES (\"%s\", \"%s\");",
        'INSERT_RUN_DEF' : 'INSERT INTO run_def (loader_configs, target_command) VALUES (\"%s\", \"%s\");',
        'INSERT_EXEC_DEF' : 'INSERT INTO exec_def (run_def_id, num_parallel, full_cmd, tiledb_ws, hostname, lcname, description) VALUES (%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\");',
        'INSERT_QUERY_RUN_DEF' : 'INSERT INTO run_def (run_loader_id, target_command) VALUES (%d, \"%s\");',

        'SET_HOSTS' : "UPDATE host set avalability=%d WHERE hostname in (%s);",  

        'Run_Config' : 'SELECT loader_configs, _id FROM run_def WHERE _id=%d;',
        'Last_Run_Config' : 'SELECT loader_configs, _id FROM run_def ORDER BY _id desc LIMIT 1;',
        'User_Config' : 'SELECT config FROM loader_config_def WHERE name in (%s);',
        'User_Config_dict' : "SELECT name, config FROM loader_config_def WHERE name in (%s);",
        'Time_Results' : 'SELECT tr.time_result, tr.genome_result, tr.pidstat_path, rl.lcname, rl.num_parallel,rl.description FROM time_result tr, exec_def rl where tr.run_id=rl._id and rl.run_def_id=%d order by rl._id desc;',
        'Runs_of_RunDef' : 'SELECT lcname, num_parallel, tiledb_ws, hostname, full_cmd FROM exec_def WHERE run_def_id=%d;',
        'Get_Command' : 'SELECT target_command from run_def WHERE _id = %d', 
        'Run_ConfigNames' : 'SELECT lcname from exec_def WHERE run_def_id = %d',
        'Last_Run_Def' : 'SELECT _id FROM run_def ORDER BY _id DESC LIMIT 1',
        'Get_LoaderRunId' : 'SELECT loader_configs from run_def WHERE _id = %d',
        }
    def __create_db(self):
        if os.path.isfile(self.CreateDBScript) and os.path.isfile(self.PrefillDBScript):
            with open(self.CreateDBScript) as fdsc:
                sqltext = fdsc.read()
            db_conn = sqlite3.connect(self.db_name)
            mycursor = db_conn.cursor()
            mycursor.executescript(sqltext)
            with open(self.PrefillDBScript) as fdsc:
                sqltext = fdsc.read()
            mycursor.executescript(sqltext)
            db_conn.commit()
            self.db_conn = db_conn
        else:
            raise RuntimeError("Miss DB creation scripts")

    def __init__(self, db_name=None):
        the_db_name = db_name if db_name else self.DefaultDBName 
        if os.path.isfile(the_db_name):
            self.db_name = the_db_name
            self.db_conn = sqlite3.connect(self.db_name)
        elif db_name:
            try:
                self.db_name = db_name
                self.__create_db()
            except:
                self.db_name = None
                print("Failed to create database, error: %s", sys.exc_info()[0])
                raise 
        else:
            raise ArgumentError("Invalid database name")

    def getRunsInfo(self, runid):
        assert(runid)
        mycursor = self.db_conn.cursor()
        query = self.queries["Runs_of_RunDef"] % runid
        run_info = []
        for row in mycursor.execute(query):
            run_info.append(dict({'lc_name': row[0], 'num_proc': row[1], 'tdb_ws':row[2],'host':row[3],'loader_config':row[4].split()[-1] }))
        return run_info

    def getTheseRunsInfo(self, runid, loader_list):
        assert(runid)
        mycursor = self.db_conn.cursor()
        query = self.queries["Runs_of_RunDef"] % runid
        run_info = []
        for row in mycursor.execute(query):
            if row[0] in loader_list:
                run_info.append(dict({'lc_name': row[0], 'num_proc': row[1], 'tdb_ws':row[2],'host':row[3],'loader_config':row[4].split()[-1] }))
        return run_info

    def getRunConfigs(self, runid, bFillFlag=True):
        ''' fillFlag = 0 => no fill, = 1 => fill with '-'; = 2 => fill with default_value '''
        mycursor = self.db_conn.cursor()
        definable_tags = self.getUserDefinableConfigTags(mycursor)
        myrunid = runid if runid else self.getLastRun();
        cfg_names = self.getRunConfigNames2(myrunid, mycursor)
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
    def getLastRun():
        mycursor = self.db_conn.cursor()
        last_run_def_id = mycursor.execute(self.queries['Last_Run_Def']).fetchone()
        return last_run_def_id[0]

    def getRunCommand(self, runid):
        mycursor = self.db_conn.cursor()
        cmd = mycursor.execute(self.queries['Get_Command'] % runid).fetchone()
        return cmd[0]

    def getLoadRunId(self, runid):
        mycursor = self.db_conn.cursor()
        runid = mycursor.execute(self.queries['Get_LoaderRunId'] % runid).fetchone()
        return list(eval(runid[0]))

    def getRunConfigsDict(self, myrunid, bFillFlag=True):
        ''' fillFlag = 0 => no fill, = 1 => fill with '-'; = 2 => fill with default_value '''
        assert(myrunid)
        mycursor = self.db_conn.cursor()
        definable_tags = self.getUserDefinableConfigTags(mycursor)

        cfg_names = self.getRunConfigNames2(myrunid, mycursor)
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
            return lc_items
        else:
            return None
    ''' TODO remove
    def getRunConfigNames(self, runid=None, cursor=None):
        return the loader config list for a run, last run if runid is None 
        query = self.queries['Run_Config'] % runid if runid else self.queries['Last_Run_Config']
        mycursor = cursor if cursor else self.db_conn.cursor()
        ret = None
        for row in mycursor.execute(query):     # only one result
            ret = (row[1], [ lc for lc in row[0].split('-') ])
        if not cursor:
            mycursor.close()
        return ret
    '''
    def getRunConfigNames2(self, runid, cursor=None):
        ''' return the loader config list for a run, last run if runid is None  '''
        mycursor = cursor if cursor else self.db_conn.cursor()
        ret = []
        stmt = self.queries['Run_ConfigNames'] % runid
        for row in mycursor.execute(stmt):
            ret.append(row[0]) 
        if not cursor:
            mycursor.close()
        return ret

    def getAllResult(self, runidstr):
        assert runidstr
        mycursor =  self.db_conn.cursor()
        all_results = []
        runid = int(runidstr)
        for row in mycursor.execute(self.queries['Time_Results'] % runid):   #TODO, enumerate?
            rowresult = dict()
            rowresult['rtime'] = dict(eval(row[0]))
            rowresult['gtime'] = eval(row[1])
            rowresult['pidstat'] = eval(row[2])
            rowresult['lcname'] = row[3]
            rowresult['n_parallel'] = row[4]
            try:
                rowresult['extra_info'] = dict(eval(row[5]))
            except:
                rowresult['extra_info'] = None

            all_results.append(rowresult)
        cmd = mycursor.execute(self.queries['Get_Command'] % runid).fetchone()
        return all_results, cmd[0]

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
        stmt = self.queries['INSERT_LOADER'] % (lcname, configStr )
        mycursor.execute(stmt)
        loader_id = mycursor.lastrowid
        self.db_conn.commit()
        mycursor.close()
        return loader_id

    def addRunConfig(self, configs, cmd ):
        mycursor = self.db_conn.cursor()
        stmt = self.queries['INSERT_RUN_DEF'] % (configs, cmd )
        mycursor.execute(stmt)
        run_id = mycursor.lastrowid
        self.db_conn.commit()
        mycursor.close()
        return run_id

    def addRunLog(self, rundef_id, host, cmd, tiledb_ws, loader_cfg, num_parallel=1, query_params=""):
        mycursor = self.db_conn.cursor()
        stmt = self.queries['INSERT_EXEC_DEF'] % (rundef_id, num_parallel, cmd, tiledb_ws, host, loader_cfg, query_params )
        mycursor.execute(stmt)
        run_id = mycursor.lastrowid
        self.db_conn.commit()
        mycursor.close()
        return run_id  