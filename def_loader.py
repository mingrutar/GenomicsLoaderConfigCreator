#! /bin/python
import sys
import os, re
import os.path
import json
import uuid
from datetime import datetime
import time
import shutil
import core_data

one_KB = 1024
one_MB = 1048576
TILE_WORKSPACE = "/mnt/app_hdd1/scratch/mingperf/tiledb-ws/"
TARGET_TEST_COMMAND = "/home/mingrutar/cppProjects/GenomicsDB/bin/vcf2tiledb"
MPIRUN = "/opt/openmpi/bin/mpirun"
RUN_SCRIPT = os.path.join(os.getcwd(),"run_exec.py")

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

data_handler = core_data.RunVCFData()
histogram_fn = None
working_dir = os.environ.get('WS_HOME', os.getcwd())

def load_from_db() :
    global my_hostlist, my_templates, loader_tags, overridable_tags, defined_loaders, defined_runs
    my_hostlist = data_handler.getHosts()
    my_templates = data_handler.getTemplates(working_dir)
    loader_tags, overridable_tags = data_handler.getConfigTags() 

    defined_loaders = data_handler.getAllUserDefinedConfigItems()
    defined_runs = data_handler.getAllRuns

# unique name for user def 
def loader_name(config) : 
    ''' list of tuple, make a short name '''
    name = []
    as_defaults = []
    for key in sorted(config.keys()):
        val = config[key]
        
        if key in overridable_tags and str(val) != overridable_tags[key][2]:
            if overridable_tags[key][1] == 'Boolean' :
                name.append("%s%s" % (overridable_tags[key][3], 't' if val else 'f'))
            elif isinstance(val, list):
                name.append("%s%s%s" % (overridable_tags[key][3], val[0], val[1][:2])) 
            else:
                name.append("%s%s" % (overridable_tags[key][3], val))
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
                loader_id = data_handler.addUserDefinedConfig(lcname, str(config))
                defined_loaders[lcname] = (config, loader_id) 
            user_loader_conf_def[lcname] = defined_loaders[lcname][0]
            ret_val.append(lcname)
        if db_conn:
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

    run_id = data_handler.addRun( "-".join(lcdef_list), TARGET_TEST_COMMAND )
    run_config[run_id] = run_list
    return run_id

def __make_path(target_path) :
    if os.path.exists(target_path) :
        shutil.rmtree(target_path)
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
    partitions = []       

    histogram_fn = data_handler.getExtraData('histogram_file_path')
    if not histogram_fn:
        # TODO: run histogram utility to generate ?
        print("WARN: no histogram file. 1 partition only")      
        partitions.append({"array" :"TEST0", "begin" : 0, "workspace" : TILE_WORKSPACE })
    else:    
        with open(histogram_fn, 'r') as rfd:
            context = rfd.readlines()
        lines = [ l.split(',') for l in context ]
        hgram = [ (x[0], x[1], float(x[2].rstrip()) ) for x in lines if len(x) == 3 ]
        bin_size = sum( [ x[2] for x in hgram] ) / bin_num
        subtotal = 0
        parnum = 0
        begin = 0
        for item in hgram:
            if subtotal == 0 :
                begin = int(item[0])
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
        'Boolean' : lambda x: x if isinstance(x, bool) else x.lower() == 'true' ,
        'Template' : lambda x: my_templates[x] , 
        'MB' : lambda x: int(x) * one_MB,
        'KB' : lambda x : int(x) * one_KB }

def __getValue(itemType, itemVal) :
    ''' String, Number, Boolean, Template, MB, KB, func() '''
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
    # val from db tale
    for key, val in loader_tags.items() :
        load_conf[key] = __getValue(val[1], val[2])
    for key, val in overridable_tags.items() :
        if key in lc_items :
            uval = lc_items[key]
            if isinstance(uval, list) :             # user override the type 
                load_conf [key] = __getValue( uval[1], uval[0])
            else :
                load_conf [key] = __getValue( val[1], uval)
            if key == "column_partitions" and use_mpirun :
                mpirun_num = int(lc_items[key])
        else :                      # use default
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

def launch_run( run_id, use_mpirun=False, dryrun=False) :
    ''' assign host to loader_config. 
    1) not support run on multi hosts, 2) allow user assign host '''
    run_id, launch_info = __prepare_run(run_id, TARGET_TEST_COMMAND, use_mpirun)
    #TODO: check software readiness @ all hosts
    print("START run %s loaders @ %s" % (run_id, datetime.now()))
    ws_path = os.path.join(working_dir, 'run_ws')
    __make_path(ws_path)
    for runinfo in launch_info :
        exec_json = dict({ 'run_id' : run_id })
        exec_json['cmd'] = runinfo[0]
        jsonfl = os.path.join(ws_path, runinfo[1])
        with open(jsonfl, 'w') as ofd :
            json.dump(exec_json, ofd)
        if dryrun :
            shell_cmd = "ssh %s python %s %s &" % (runinfo[1], RUN_SCRIPT, jsonfl )
            print('DRYRUN: os.system(%s)' % shell_cmd )
        else :
            print("launching test at %s" % (runinfo[0]))
            os.system("ssh %s %s %s &" % (runinfo[1], RUN_SCRIPT, jsonfl ))
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
