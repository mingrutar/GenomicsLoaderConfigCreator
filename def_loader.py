#! /bin/python
import sqlite3
import sys
import os, re
import os.path
import json
import uuid
from datetime import datetime
from pprint import pprint

one_MB = 1048576
TILE_WORKSPACE = "/mnt/app_hdd1/scratch/mingperf/tiledb-ws/"

db_queries = {"LoaderConfigTag" : 'SELECT name, type, default_value FROM loader_config_tag where user_definable={ud}',
    "Host" : 'SELECT hostname FROM host WHERE avalability = 1', 
    "Template" : 'SELECT name, file_path, params, extra FROM template' }

my_hostlist = []
my_templates = {}
loader_tags = {}
overridable_tags = {}

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
                    context.replace(key, val)
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
        
    for row in mycursor.execute(db_queries['LoaderConfigTag'].format(ud=0)):
        loader_tags[row[0]] = list(row)
    for row in mycursor.execute(db_queries['LoaderConfigTag'].format(ud=1)):
        overridable_tags[row[0]] = list(row)
    db_conn.close()

# input [ {, , }, { }], return [ ids]
def addUserConfigs(user_defined) :
    if isinstance(user_defined, list) :
        ret_val = []
        for config in user_defined:
            id = uuid.uuid4()
            user_loader_conf_def[id] = config
            # TODO: to db?
            ret_val.append(id)
        pprint(user_loader_conf_def)
        pprint(ret_val)    
        return ret_val
    else :
        print("WARN add requies a list object") 
        return None

def assign_host_run(lcdef_list) :
    host_num = len(my_hostlist)
    run_list = []
    print("#host =%d, #lc=%d" % (host_num, len(lcdef_list) ))
    for i in range(len(lcdef_list)) :
        run_list.append( (my_hostlist[i], lcdef_list[i])  )
    run_id = uuid.uuid4()
    #TODO persist to db?
    run_config[run_id] = run_list
    pprint(run_config)
    return run_id

def __make_path(target_path) :
    if os.path.exists(target_path) :
        for root, dirs, files in os.walk(target_path):
            for fn in files:
                print("WARN removing old loader file %s" % fn)
                os.remove(os.path.join(target_path, fn))
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

#
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
    print("__genLoadConfig: use_mpirun=%s" % use_mpirun)
    load_conf = {}
    mpirun_num = 1
    for key, val in loader_tags.items() :
#        print("key=%s, val[1]=%s, val[2]=%s" % (key, val[1], val[2]))
        load_conf[key] = __getValue(val[1], val[2])
    pprint(load_conf)
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
#            print("+++ load_config, mpirun_num = %d, jsonfn=%s" % (mpirun_num, jsonfn) )
#            pprint(load_config)
            with open(jsonfn, 'w') as ofd :
                json.dump(load_config, ofd)
            
            theCommand = "%s -n %d %s %s" % (MPIRUN, mpirun_num, target_cmd, jsonfn)  \
                if mpirun_num > 1 else "%s %s" % (target_cmd, jsonfn)
            ret.append((theCommand, host, jsonfn) )   
            print("INFO prepare_run made=%s" % ret)
    return (run_id, ret)

def launch_run( run_id, use_mpirun=True, dryrun=False) :
    ''' assign host to loader_config. 
    TODO 1) not support run on multi hosts, 2) allow user assign host '''
    command = "vcf2tiledb"
    run_id, launch_info = __prepare_run(run_id, command, use_mpirun)
    print("START run %s loaders: %s" % (run_id, launch_info))
    for runinfo in launch_info :
        exec_json = dict({ 'run_id' : run_id })
        exec_json['cmd'] = list( [ runinfo[1], runinfo[2] ] )
        jsonfl = runinfo[1]
        with open(jsonfl, 'w') as ofd :
            json.dump(exec_json, ofd)
        if dryrun :
            print('DRYRUN: os.system("ssh %s python run_exec.py %s &" % (runinfo[1], jsonfl ))')
        else :
            os.system("ssh %s python run_exec.py %s &" % (runinfo[1], jsonfl ))
        print("launched at %s" % (runinfo[0]))
 
if __name__ == '__main__' :
    load_from_db()
#    print("=== loaded tags ")
#    pprint(overridable_tags)

    loader_config = sys.argv[1] if(len(sys.argv) > 1) else "loader_def.json"
    ifl = os.path.join(os.getcwd(), loader_config)
    with open(loader_config, 'r') as ldf:
        loader_def_list = json.load(ldf)

    cfg_item_ids = addUserConfigs(loader_def_list)

    if (cfg_item_ids) :
        run_id = assign_host_run(cfg_item_ids)
        launch_run(run_id, dryrun=True)
