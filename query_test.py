import sys
import os, re
import os.path
import json
import time

'''
Test config 16 partitions, within a segment size  
   1) randomly pick 1000 positions and run N times
   2) pick sparse that distance > buf_size
   3) pick 20 positions in dense bin  
'''
'''
preparation:  From loader_config and mpirun json:
loader_config_def.json and mpirun.json: 
    bn16
    bn16nt100|{u'column_partitions': 16, u'num_cells_per_tile': 100}|1488407619
    14|bn16nt10000|{u'column_partitions': 16, u'num_cells_per_tile': 10000}|1488407619
    15|bn16nt100000|{u'column_partitions': 16, u'num_cells_per_tile': 100000}|1488407619
    16|bn16nt1000000
select * from run_def order by _id desc - find required runs
select * from run_log where run_def_id=21 - hostname, tiledb_ws, full loader_config.json
 => latest tiledbs ("workspace" : "/tmp/ws/", "array" : "t0_1_2")
check if all available  =>{run_log_id: workspace, astartPos, array_name, histgram_list } 
  from full_loader_config: "array": "TEST15", "begin": 2849200000,
            "workspace": "/mnt/app_hdd1/scratch/mingperf/tiledb-ws_1703011104/"

process: set segment_size   - we need to tune this value
    class PositionPicker: histogram, begin, end position()
            def pick_random(num_position)
            def pick_sparse(distance, upperlimit) return position with description
            def pick_dense()

db: experiement: _id, histogram
  query_config_def: experiement_id, loader_config_def_id, run_def_id=21, num_parallel, partition_density
  query_test: query_config_def_id, pick_type, num_position, path_to_positions
  use time_result, run_log
  query_config_tag: 
        "workspace" : "/tmp/ws/",
    "array" : "t0_1_2",
    "query_column_ranges" : [ [ [0, 100 ], 500 ] ],
    "query_row_ranges" : [ [ [0, 2 ] ] ],
    "vid_mapping_file" : "",
    "segment_size" : 
    "query_attributes" : [ "REF", "ALT", "BaseQRankSum", "MQ", "MQ0", "ClippingRankSum", "MQRankSum", "ReadPosRankSum", "DP", "GT", "GQ", "SB", "AD", "PL", "DP_FORMAT", "MIN_DP" ]

'''
my_hostlist = []
my_templates = {}
data_handler = None
working_dir = os.environ.get('WS_HOME', os.getcwd())

TARGET_TEST_COMMAND = "/home/mingrutar/cppProjects/GenomicsDB/bin/gt_mpi_gather"
RUN_SCRIPT = os.path.join(os.getcwd(),"run_exec.py")

def load_from_db() :

def __find_loader(runid=None):
    ''' for now find the last one '''
    loader_ws_path = os.path.join(working_dir, 'run_ws')
    host_loader_cfg = {}
    loader_runid = runid
    for host in my_hostlist:
        run_cfg = os.path.join(loader_ws_path, host)
        if os.path.exists(run_cfg):
            with open(run_cfg, 'r') as rfd:
                run_info = json.load(rfd)
            if not loader_runid:
                loader_runid = run_info['run_id']
            if loader_runid == run_info['run_id']:
                loader_cfg = run_info['cmd'].split()[1]
                host_loader_cfg[host] = loader_cfg
    query_run_id = data_handler.addQueryRun(loader_runid, TARGET_TEST_COMMAND)
    return host_loader_cfg, query_run_id

def launch_query(query_json, dryrun=False):
    query_ws_path = os.path.join(working_dir, 'run_query_ws')
    host_loader_cfg, run_id = __find_loader()
    for host, lc in host_loader_cfg.items():
        cmd = "{} -l {} -j {} --produce-Broad-GVCF".format(TARGET_TEST_COMMAND, lc, query_json)
        exec_json = dict({'run_id' : run_id, 'cmd': cmd })
        exec_json_fn = os.path.join(query_ws_path, host)
        with open(exec_json_fn, 'w') as ofd :
            json.dump(exec_json, ofd)
        if dryrun :
            shell_cmd = "ssh %s python %s %s &" % (host, RUN_SCRIPT, exec_json_fn )
            print('DRYRUN: os.system(%s)' % shell_cmd )
        else :
            print("launching test at %s" % (host))
            os.system("ssh %s %s %s &" % (host, RUN_SCRIPT, exec_json_fn ))
    print("DONE launch... ")

def fillPartialQuery(data_handler, lcName):
    
def parseTestDefinition(test_def):
    global data_handler, my_hostlist, my_templates
    data_handler = core_data.RunVCFData(test_def['source_db_path'])
    my_templates = data_handler.getTemplates(working_dir)

    hosts={}
    for batch in test_def.test_batch:
        run_info_list = core_data.getRunsInfo(batch['run_id'])
        for run in run_info_list:
            if run['host'] not in hosts:
                hosts[run['host']] = []
            pquery = fillPartialQuery(data_handler, run['lc_name'])
            
            hosts[run['host']].append( )
            
#    {'lc_name': row[0], 'num_proc': row[1], 'tdb_ws':row[2],'host':row[3],'loader_config':row[4].split()[-1] }))
 
if __name__ == '__main__' :
    load_from_db()

    query_config = sys.argv[1] if(len(sys.argv) > 1) else "test_query_def.json"
    query_cfg_fn = os.path.join(working_dir, query_config)

    if os.path.isfile(query_cfg_fn) :
        
        with open(query_cfg_fn, 'r') as ifd:
            test_def = json.load(ifd)
        launch_query(query_cfg_fn, dryrun=True )
