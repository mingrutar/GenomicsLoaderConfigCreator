import sys
import os, re
import os.path
import json
import time
from histogram import HistogramManager
from core_data import RunVCFData
from copy import deepcopy
import platform

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
PARTITION_NUM = 16    
TARGET_TEST_COMMAND = "/home/mingrutar/cppProjects/GenomicsDB/bin/gt_mpi_gather"
RUN_SCRIPT = os.path.join(os.getcwd(),"run_exec.py")

working_dir = os.environ.get('WS_HOME', os.getcwd())
query_ws_path = os.path.join(working_dir, 'run_query_ws')

PosSelection = { HistogramManager.DIST_RANDOM: 500,  
    HistogramManager.DIST_DENSE:50, HistogramManager.DIST_SPARSE: 60 }

# for our test
def prepareTest(test_def):
    data_handler = core_data.RunVCFData(test_def['source_db_path'])

    query_def_list = [ load_run_id['run_id'] for load_run_id in test_def.test_batch ]
    q_def_run_id = data_handler.addQueryRun( str(query_def_list), TARGET_TEST_COMMAND)

    my_templates = data_handler.getTemplates(working_dir)
    tq_master["vid_mapping_file"] = my_templates['vid']
    tq_master["reference_genome"] = my_templates["ref_genome"]  
    tq_master["callset_mapping_file"] = my_templates['callsets']
    tq_master["query_attributes"] = [ "REF", "ALT", "BaseQRankSum", "MQ", "MQ0", "ClippingRankSum", "MQRankSum", "ReadPosRankSum", "DP", "GT", "GQ", "SB", "AD", "PL", "DP_FORMAT", "MIN_DP" ]

    histogram_fn = data_handler.getExtraData('histogram_file_path')
    if not histogram_fn:
        histogram_fn = os.path.join(working_dir, 'templates', '1000_histogram')
    histogramManager = HistogramManager(histogram_fn)
    
    bin_start_list = histogramManager.calc_bin_idx_pos(PARTITION_NUM)
    hosts={}
    
    for batch in test_def.test_batch:
        run_info_list = data_handler.getRunsInfo(batch['run_id'])   # get loader info
        for run in run_info_list:
            host = run['host']
            # host_ws_dir = os.path.join(query_ws_path, host)
            # if not os.path.isdir(host_ws_dir):
            #     os.mkdir(host_ws_dir)
            if host not in hosts:
                hosts[host] = []
            tq_params = deepcopy (tq.master)
            tq_params["workspace"] = run['tdb_ws']
            if run['num_proc'] == 1:
                tq_params['array'] = 'TEST0'
            else:
                tq_params['array'] = [ 'TEST%d' % i for i in range(run['num_proc'])]
            for seg_size in test_def['segment_size']:
                npq_params = deepcopy(tq_params)
                npq_params['segment_size'] = seg_size
                for dist_name, num_pos in PosSelection.items():
                    from_bins = [ i in range(run['num_proc'])] 
                    npq_params['query_column_ranges'] = histogramManager.getPositions(dist_name, num_pos, from_bins)
                    query_fn = os.path.join(query_ws_path, 'query_%s-%d-%s.json' % (run['lc_name'], seg_size, pos_dist))
                    with open(query_fn, 'w') as wfd:
                        json.dump(npq_params)
                    cmd = "{} -j {} --produce-Broad-GVCF".format(TARGET_TEST_COMMAND, query_fn)
                    if run['num_proc'] > 1:
                        cmd = "mpirun -np %d %s" % (run['num_proc'], cmd)
                    data_handler.addRunLog(q_def_run_id, host, cmd, run['tdb_ws'],  run['num_proc'])
                    hosts[host].append(cmd)
    data_handler.close()
    return hosts, q_def_run_id

def launch_query(host_run_list, q_def_run_id):
    for host, cmd_list in host_run_list.items():
        for cmd in cmd_list:
            if dryrun :
                shell_cmd = "ssh %s %s %s &" % (host, RUN_SCRIPT, q_def_run_id)
                print('DRYRUN: os.system(%s)' % shell_cmd )
            else :
                print("launching test at %s, cmd= %s %s" % (host, RUN_SCRIPT, q_def_run_id))
                os.system("ssh %s %s %s &" % (host, RUN_SCRIPT, q_def_run_id ))
    print("DONE launch... ")

if __name__ == '__main__' :
    query_config = sys.argv[1] if(len(sys.argv) > 1) else "test_query_def.json"
    query_cfg_fn = os.path.join(working_dir, query_config)

    if os.path.isfile(query_cfg_fn) :
        
        with open(query_cfg_fn, 'r') as ifd:
            test_def = json.load(ifd)

        if os.path.isdir(query_ws_path):
            os.path.remove (query_ws_path)      #TODO: remove all
        os.makedir(query_ws_path)

        host_cfg_list, run_ids = prepareTest(test_def)
        if platform.system() != 'Windows':          # real run
            launch_query(host_cfg_list, run_ids )
    print("DONE ... ")
