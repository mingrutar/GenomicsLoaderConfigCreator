#! /bin/python
import sqlite3
import sys
import os, re
import os.path
import json
import uuid
from datetime import datetime
import time
import shutil

one_MB = 1048576
TILE_WORKSPACE = "/mnt/app_hdd1/scratch/mingperf/tiledb-ws/"
TARGET_TEST_COMMAND = "vcf2tiledb"
MPIRUN = "/opt/openmpi/bin/mpirun"

db_queries = {"Host" : 'SELECT hostname FROM host WHERE avalability = 1',
    "LC_Tag" : 'SELECT name, type, default_value FROM loader_config_tag where user_definable=0',
    "LC_OverrideTag" : 'SELECT name, type, default_value,tag_code FROM loader_config_tag where user_definable=1',
    "Template" : 'SELECT name, file_path, params, extra FROM template',
    'Select_user_lc' : 'SELECT name, config, _id from loader_config_def',
    'Select_defined_run' : 'SELECT _id, loader_configs from run_def',
    'INSERT_LOADER' : "INSERT INTO loader_config_def (name, config, creation_ts) VALUES (\"%s\", \"%s\", %d)",
    'INSERT_RUN_DEF' : 'INSERT INTO run_def (loader_configs, creation_ts) VALUES (\"%s\", %d)'
}
# look up 
my_hostlist = []
my_templates = {}
loader_tags = {}
overridable_tags = {}
defined_loaders = {}        # ld_name, (config, dbid) 
defined_runs = {}           # dbid, 

# runtime
user_loader_conf_def = {}       # { uuid : { tag : val} }
run_config = {}                 # { uuid : [ (host, lcdef) ] }

histogram_fn = None
working_dir = os.environ.get('WS_HOME', os.getcwd())

def __proc_template( templateFile, sub_key_val = None, extra_args = None) :
    def histogram (val):
        global histogram_fn
        histogram_fn = val.replace("$WS_HOME", working_dir)
 
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
                locals()[key](val) 
        return f_path
    else :
        print("WARN: template file %s not found" % f_path)
        return None

def load_from_db() :
    db_conn = sqlite3.connect('genomicsdb_loader.db')
    mycursor = db_conn.cursor()
    mycursor.execute(db_queries["Host"])
    rows = list(mycursor.fetchall())
    for r in rows:
        my_hostlist.append(r[0])
    
 #  "Template" : 'SELECT name, file_path, params FROM template' }
    for r in mycursor.execute(db_queries['Template']):
        temp_name = str(r[1]).replace("'", "\"")
        jstrParams = json.loads(str(r[2]).replace("'", "\"")) if r[2] else None
        jstrMore =   json.loads(str(r[3]).replace("'", "\"")) if r[3] else None
        input_file = __proc_template(temp_name, jstrParams, jstrMore) 
        my_templates[r[0]] = input_file if input_file else temp_name
        
    for row in mycursor.execute(db_queries['LC_Tag']):
        loader_tags[row[0]] = list(row)
    for row in mycursor.execute(db_queries['LC_OverrideTag']):
        overridable_tags[row[0]] = list(row)

    for row in mycursor.execute(db_queries['Select_user_lc']) :
        cfg = json.loads(row[1].replace("'", "\"") )
        defined_loaders[row[0]] = (cfg, row[2])  
    for row in mycursor.execute(db_queries['Select_defined_run']) :
        defined_runs[row[0]] = row[1]
    db_conn.close()

# unique name for user def 
def loader_name(config) : 
    ''' list of tuple, make a short name '''
    name = []
    as_defaults = []
    for key in sorted(config.keys()):
        val = config[key]
        if key in overridable_tags and str(val) != overridable_tags[key][2]:
            if overridable_tags[key][1] == 'Boolean' :
                name.append("%s%s" % (overridable_tags[key][3], val[:1]))
            else :
                name.append("%s%s" % (overridable_tags[key][3], val) )
        else :
            as_defaults.append(key)
    for x in as_defaults:
        del config[x]
    return ''.join(name)

def addUserConfigs(user_defined) :
    if isinstance(user_defined, list) :
        ret_val = []
        db_conn = None
        for config in user_defined: #  config is dict,
            lcname = loader_name(config)
            if lcname not in defined_loaders:
                if not db_conn:
                    db_conn = sqlite3.connect('genomicsdb_loader.db')
                mycursor = db_conn.cursor()
                stmt = db_queries['INSERT_LOADER'] % (lcname, str(config), int(time.time()) )
                mycursor.execute(stmt)
                loader_id = mycursor.lastrowid
                defined_loaders[lcname] = (config, loader_id) 
            user_loader_conf_def[lcname] = defined_loaders[lcname][0]
            ret_val.append(lcname)
        if db_conn:
            db_conn.commit()
            db_conn.close()
        return ret_val
    else :
        print("WARN add requies a list object") 
        return None

def assign_host_run(lcdef_list) :
    host_num = len(my_hostlist)
    run_list = []
    for i in range(len(lcdef_list)) :
        run_list.append( (my_hostlist[i % host_num], lcdef_list[i])  )
    db_conn = sqlite3.connect('genomicsdb_loader.db')
    mycursor = db_conn.cursor()
    stmt = db_queries['INSERT_RUN_DEF'] % ("-".join(lcdef_list), int(time.time()) )
    mycursor.execute(stmt)
    run_id = mycursor.lastrowid
    db_conn.commit
    db_conn.close()
    run_config[run_id] = run_list
    return run_id

def __make_path(target_path) :
    if os.path.exists(target_path) :
        shutil.rmtree(target_path)
    else:
        os.makedirs(target_path)

def __str2num(x) :
    try:
        return int(x)
    except ValueError:
        try:
            return float(x)
        except ValueError:
            return None

def make_col_partition(bin_num):
    bin_num = int(bin_num)
    with open(histogram_fn, 'r') as rfd:
        context = rfd.readlines()
    lines = [ l.split(',') for l in context ]
    hgram = [ (x[0], x[1], float(x[2].rstrip()) ) for x in lines if len(x) == 3 ]
    bin_size = sum( [ x[2] for x in hgram] ) / bin_num
    partitions = []       
    subtotal = 0
    parnum = 0
    begin = 0
    for item in hgram:
        if subtotal == 0 :
            begin = item[0]
        subtotal += item[2]
        if (parnum < bin_num-1) and (subtotal > bin_size) :
            partitions.append({"array" :"TEST%d" % parnum,
                "begin" : begin, "workspace" : TILE_WORKSPACE })
            parnum += 1
            subtotal = 0
    if (subtotal > 0) :
        partitions.append({"array" :"TEST%d" % parnum,
            "begin" : begin, "workspace" : TILE_WORKSPACE })
    return partitions

transformer = {'String' : lambda x : x if isinstance(x, str) else None,
        'Number' : __str2num ,
        'Boolean' : lambda x: x.lower() == 'true' ,
        'Template' : lambda x: my_templates[x] , 
        'MB' : lambda x: int(x) * one_MB }

def __getValue(itemType, itemVal) :
    ''' String, Number, Boolean, Template, MB, func() '''
    if itemType in transformer:
        return transformer[itemType](itemVal)
    elif itemType[-2:] == '()':
        return globals()[itemType[:-2]](itemVal)
    else:
        return None

#SELECT name, type, default_value FROM loader_config_tag
# user only define the value
def __genLoadConfig( lc_items, use_mpirun ) :
    ''' return json load file '''
    load_conf = {}
    mpirun_num = 1
    for key, val in loader_tags.items() :
        load_conf[key] = __getValue(val[1], val[2])
    for key, val in overridable_tags.items() :
        if key in lc_items :
            val[2] = lc_items[key]
            if key == "column_partitions" and use_mpirun :
                mpirun_num = int(lc_items[key])
        load_conf [key] = __getValue( val[1], val[2])
    return load_conf, mpirun_num 
 
def __prepare_run (run_id, target_cmd, use_mpirun) :
    timestamp = datetime.now().strftime("%y%m%d%H%M")
    run_dir = os.path.join(working_dir, 'run_%s'%timestamp)
    __make_path(run_dir)
    ret = []
    for host, lc_id in run_config[run_id] :
        if lc_id in user_loader_conf_def:
            load_config, mpirun_num = __genLoadConfig(user_loader_conf_def[lc_id], use_mpirun) 
            jsonfn = os.path.join(run_dir, host+".json")
            with open(jsonfn, 'w') as ofd :
                json.dump(load_config, ofd)
            theCommand = "%s -np %d %s %s" % (MPIRUN, mpirun_num, target_cmd, jsonfn)  \
                if mpirun_num > 1 else "%s %s" % (target_cmd, jsonfn)
            ret.append((theCommand, host, jsonfn) )   
    return (run_id, ret)

def launch_run( run_id, use_mpirun=True, dryrun=False) :
    ''' assign host to loader_config. 
    1) not support run on multi hosts, 2) allow user assign host '''
    run_id, launch_info = __prepare_run(run_id, TARGET_TEST_COMMAND, use_mpirun)
    #TODO: check software readiness @ all hosts
    print("START run %s loaders @ %s" % (run_id, datetime.now()))
    ws_path = os.path.join(working_dir, 'run_ws')
    __make_path(ws_path)
    for runinfo in launch_info :
        exec_json = dict({ 'run_id' : run_id })
        exec_json['cmd'] = list( [ runinfo[0], runinfo[2] ] )
        jsonfl = os.path.join(ws_path, runinfo[1])
        with open(jsonfl, 'w') as ofd :
            json.dump(exec_json, ofd)
        if dryrun :
            shell_cmd = "ssh %s python run_exec.py %s &" % (runinfo[1], jsonfl )
            print('DRYRUN: os.system(%s)' % shell_cmd )
        else :
            print("launching test at %s" % (runinfo[0]))
            os.system("ssh %s python run_exec.py %s &" % (runinfo[1], jsonfl ))
    print("DONE launch... ")
 
if __name__ == '__main__' :
    load_from_db()

    loader_config = sys.argv[1] if(len(sys.argv) > 1) else "loader_def.json"
    ifl = os.path.join(os.getcwd(), loader_config)
    with open(loader_config, 'r') as ldf:
        loader_def_list = json.load(ldf)

    cfg_items = addUserConfigs(loader_def_list)

    if (cfg_items) :
        run_id = assign_host_run(cfg_items)
        launch_run(run_id, dryrun=True)
