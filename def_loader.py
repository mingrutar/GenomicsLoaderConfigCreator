#! /bin/python
import sys
import os, re
import os.path
import getopt
import platform
import json
import uuid
from datetime import datetime
import time
import shutil
from histogram import HistogramManager
from core_data import RunVCFData

one_KB = 1024
one_MB = 1048576
TILE_WORKSPACE_PREFIX = "/mnt/app_hdd1/scratch/mingquery/tiledb-ws"
# befor 3/92017: TILE_WORKSPACE_PREFIX = "/mnt/app_hdd1/scratch/mingperf/tiledb-ws"
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

data_handler = RunVCFData()
histogram_fn = None
working_dir = os.environ.get('WS_HOME', os.getcwd())
tile_workspace = ""

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
        for config in user_defined: #  config is dict,
            lcname = loader_name(config)
            if lcname not in defined_loaders:
                loader_id = data_handler.addUserDefinedConfig(lcname, str(config))
                defined_loaders[lcname] = (config, loader_id) 
            user_loader_conf_def[lcname] = defined_loaders[lcname][0]
            ret_val.append(lcname)
        return ret_val
    else :
        print("WARN add requies a list object") 
        return None

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
    global tile_workspace
    print("tile_workspace=%s" % tile_workspace)
    bin_num = int(bin_num)
    partitions = []       

    histogram_fn = data_handler.getExtraData('histogram_file_path')
    if not histogram_fn:
        # TODO: run histogram utility to generate ?
        print("WARN: no histogram file. 1 partition only")      
        partitions.append({"array" :"TEST0", "begin" : 0, "workspace" : tile_workspace })
    else:    
        hm = HistogramManager(histogram_fn)
        begin_list = hm.calc_bin_begin_pos( bin_num)
        for parnum, begin in enumerate(begin_list):
            partitions.append({"array" :"TEST%d" % parnum,
                "begin" : begin, "workspace" : tile_workspace })
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
def __genLoadConfig( lc_items ) :
    ''' return json load file '''
    load_conf = {}
    mpirun_num = 1
    # tile db ws is tiledb-ws_ts
    global tile_workspace
    timestamp = int(time.mktime(datetime.now().timetuple()))
    tile_workspace = "%s_%s/" % (TILE_WORKSPACE_PREFIX, timestamp)

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
            if key == "column_partitions" :
                mpirun_num = int(lc_items[key])
        else :                      # use default
            load_conf [key] = __getValue( val[1], val[2])
    return load_conf, mpirun_num, tile_workspace
 
def __prepare_run (run_id, target_cmd, user_mpirun) :
    timestamp = datetime.now().strftime("%y%m%d%H%M")
    run_dir = os.path.join(working_dir, 'run_%s'%timestamp)
    __make_path(run_dir)
    ret = {}
    commandList =[]                         # contains all (command, tile_ws, num_proc)
    for lc_id in run_config[run_id] :
        if lc_id in user_loader_conf_def:
            load_config, num_parallel, tile_ws = __genLoadConfig(user_loader_conf_def[lc_id]) 
            jsonfn = os.path.join(run_dir, "%s-%s.json" % (run_id, lc_id))
            with open(jsonfn, 'w') as ofd :
                json.dump(load_config, ofd)
            # multiple command per loader config according to num_paralle
            # commands use the same tiledb workspace
            if user_mpirun and lc_id in user_mpirun:
                for num_pr in user_mpirun[lc_id]:
                    theCommand = "%s -np %d %s %s" % (MPIRUN, num_pr, target_cmd, jsonfn)  \
                        if num_pr > 1 else "%s %s" % (target_cmd, jsonfn)
                    commandList.append((theCommand, tile_ws, num_pr))
            else:
                theCommand = "%s %s" % (target_cmd, jsonfn)
                commandList.append((theCommand, tile_ws, 1))
    return assign_host(commandList)

def assign_host( commands):
    num_host = len(my_hostlist)
    cmdlist_host = {}
    for i, cmd in enumerate(commands):
        if i < num_host:
            cmdlist_host[my_hostlist[i]] = [cmd]
        else:
            cmdlist_host[my_hostlist[i % num_host]].append(  cmd)
    return cmdlist_host

def launch_run( run_def_id, dryrun, user_mpirun=None) :
    ''' assign host to loader_config. 
    TODO: allow user assign host '''
    launch_info = __prepare_run(run_def_id, TARGET_TEST_COMMAND, user_mpirun)
    #TODO: check software readiness @ all hosts
    print("START run %s loaders @ %s" % (run_def_id, datetime.now()))
    ws_path = os.path.join(working_dir, 'run_ws')
    __make_path(ws_path)
    for host, runCmdList in launch_info.items():
        exec_list = []
        for runCmd in runCmdList:
            run_id = data_handler.addRunLog(run_def_id, host, runCmd[0], runCmd[1], runCmd[2])
        if dryrun :
            shell_cmd = "ssh %s %s %d &" % (host, RUN_SCRIPT, run_def_id )
            print('DRYRUN: os.system(%s)' % shell_cmd )
        else :
            print("launching test at %s" % (host))
            os.system("ssh %s %s %d &" % (host, RUN_SCRIPT, run_def_id ))
    print("DONE launch... ")

def assign_run_id(lcdef_list):
    global run_config 
    run_id = data_handler.addRunConfig( "-".join(lcdef_list), TARGET_TEST_COMMAND )
    run_config[run_id] = lcdef_list
    return run_id

def getRunSettings(args):
    myopts, parsed_args = getopt.getopt(args,"l:r:d")
    dryrun = platform.system() == 'Windows'
    loader_config =  "loader_def.json"
    parallel_config = None
    # check args
    for opt, input in myopts:
        if opt == '-l':
            loader_config = os.path.join(working_dir, input)
        if opt == '-r':
            parallel_config = os.path.join(working_dir, input)
        if opt == '--dryrun' or opt == '-d':
            dryrun = input.lower() in ['1', 'true'] 

    if not os.path.exists(loader_config):
        msg = "ERROR: cannot find load config file %s, exit..." % loader_config
        sys.exit(msg)
    if parallel_config and not os.path.exists(parallel_config):
        print("WARN: cannot find mpirun config file %s, will not use mpirun  ..." % parallel_config)
        parallel_config = None
    return loader_config, parallel_config, dryrun
    
if __name__ == '__main__' :
    loader_config, parallel_config, dryrun = getRunSettings(sys.argv[1:])
    load_from_db()
    ifl = os.path.join(os.getcwd(), loader_config)
    with open(loader_config, 'r') as ldf:
        loader_def_list = json.load(ldf)

    cfg_items = addUserConfigs(loader_def_list)

    if (cfg_items) :
        run_id = assign_run_id(cfg_items)
        if parallel_config:
            with open(parallel_config, 'r') as ldf:
                mpiruncfg = json.load(ldf)
            parallel_num = {}                   # loader config: list of parall
            for idx, val in mpiruncfg.items():
               parallel_num[cfg_items[int(idx)]] = val 
            launch_run(run_id, dryrun, parallel_num)
        else: 
            launch_run(run_id, dryrun)
    data_handler.close()