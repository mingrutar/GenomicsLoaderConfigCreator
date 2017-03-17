import sys
import os, re
import os.path
import json
import time
from histogram import HistogramManager
from core_data import RunVCFData
from copy import deepcopy
import platform
from shutil import rmtree
from itertools import chain
'''
peformance test for genomics data retriever
'''
PARTITION_NUM = 16       # TODO hard coded for now
# define number of positions to pcik for random, dense and sparse
PosSelection = { HistogramManager.DIST_RANDOM: 500,  
    HistogramManager.DIST_DENSE: 50, HistogramManager.DIST_SPARSE: 60 }

TARGET_TEST_COMMAND = "/home/mingrutar/cppProjects/GenomicsDB/bin/gt_mpi_gather"
RUN_SCRIPT = os.path.join(os.getcwd(),"run_exec.py")

working_dir = os.environ.get('WS_HOME', os.getcwd())
query_ws_path = os.path.join(working_dir, 'ws_test_query')

# change as needed
def __make_string_of_args(npq_params):
    if 'query_column_ranges' in npq_params:
        npq_params['num_columns'] = len(npq_params['query_column_ranges'][0])
        npq_params.pop('query_column_ranges', None)
    npq_params.pop("vid_mapping_file", None)
    npq_params.pop("reference_genome", None)  
    npq_params.pop("callset_mapping_file", None)
    npq_params.pop('array', None)
    npq_params.pop('workspace', None)
    npq_params.pop('query_attributes', None)
    return str(npq_params)

# for our test
def prepareTest(test_def):
    db_path = os.path.join(working_dir, test_def['source_db_path'])
    data_handler = RunVCFData(db_path)

    query_def_list = [ load_run_id['run_id'] for load_run_id in test_def['test_batch'] ]
    q_def_run_id = data_handler.addRunConfig( str(query_def_list), TARGET_TEST_COMMAND)
    print("INFO: test query run id is %d" % q_def_run_id)
    
    my_templates = data_handler.getTemplates(working_dir)
    tq_master= {"vid_mapping_file" : my_templates['vid']}
    tq_master["reference_genome"] = my_templates["ref_genome"]  
    tq_master["callset_mapping_file"] = my_templates['callsets']
    tq_master["query_attributes"] = [ "REF", "ALT", "BaseQRankSum", "MQ", "MQ0", "ClippingRankSum", "MQRankSum", "ReadPosRankSum", "DP", "GT", "GQ", "SB", "AD", "PL", "DP_FORMAT", "MIN_DP" ]

    histogram_fn = data_handler.getExtraData('histogram_file_path')
    if not histogram_fn:
        histogram_fn = os.path.join(working_dir, 'templates', '1000_histogram')
    hm = HistogramManager(histogram_fn)
    
    bin_list = hm.calc_bin_idx_pos(PARTITION_NUM)
    bin_pos_list = [(bin_list[i]['pos'], bin_list[i+1]['pos']-1) for i in range(PARTITION_NUM-1)]
    bin_pos_list.append((bin_list[PARTITION_NUM-1]['pos'], None))
    hosts={}
    file_count = 0    
    for batch in test_def['test_batch']:
        if  'lcname' in test_def:
            run_info_list = data_handler.getTheseRunsInfo(batch['run_id'], test_def['lcname'])   # get loader info
        else:
            run_info_list = data_handler.getRunsInfo(batch['run_id'])   # get loader info
        for run in run_info_list:
            host = run['host']
            # host_ws_dir = os.path.join(query_ws_path, host)
            # if not os.path.isdir(host_ws_dir):
            #     os.mkdir(host_ws_dir)
            if host not in hosts:
                hosts[host] = []
            tq_params = deepcopy (tq_master)
            tq_params["workspace"] = run['tdb_ws']
            if run['num_proc'] == 1:
                tq_params['array'] = 'TEST0'
            else:
                tq_params['array'] = [ 'TEST%d' % i for i in range(run['num_proc'])]
            for seg_size in test_def['segment_size']:
                npq_params = deepcopy(tq_params)
                npq_params['segment_size'] = seg_size
                npq_params["scan_full"] = test_def["pick_mode"] == 'all' 
                if test_def["pick_mode"] != 'all':
                    for dist_name, num_pos in PosSelection.items():
                        selected_pos =[ hm.getPositions(dist_name, num_pos, bin_pos_list[ix]) for ix in range(run['num_proc']) ]
                        pos_list = list(chain.from_iterable(selected_pos))
                        npq_params['query_column_ranges'] = [ pos_list ]
                        query_fn = os.path.join(query_ws_path, 'query_%s-%s-%d-%s.json' % (run['lc_name'], run['num_proc'], seg_size, dist_name))
                        with open(query_fn, 'w') as wfd:
                            json.dump(npq_params, wfd)
                        file_count += 1
                        cmd = "{} -j {} --produce-Broad-GVCF".format(TARGET_TEST_COMMAND, query_fn)
                        if run['num_proc'] > 1:
                            cmd = "mpirun -np %d %s" % (run['num_proc'], cmd)
                        npq_params['pick_mode'] = dist_name
                        query_arg = __make_string_of_args(npq_params)
                        data_handler.addRunLog(q_def_run_id, host, cmd, run['tdb_ws'], run['lc_name'], run['num_proc'], query_arg)
                        hosts[host].append(cmd)
                else:
                    query_fn = os.path.join(query_ws_path, 'query_%s-%s-%d-%s.json' % (run['lc_name'], run['num_proc'], seg_size, 'all'))
                    with open(query_fn, 'w') as wfd:
                        json.dump(npq_params, wfd)
                    file_count += 1
                    cmd = "{} -j {} --produce-Broad-GVCF".format(TARGET_TEST_COMMAND, query_fn)
                    if run['num_proc'] > 1:
                        cmd = "mpirun -np %d %s" % (run['num_proc'], cmd)
                    query_arg = __make_string_of_args(npq_params)
                    data_handler.addRunLog(q_def_run_id, host, cmd, run['tdb_ws'], run['lc_name'], run['num_proc'], query_arg)
                    hosts[host].append(cmd)
                    
    data_handler.close()
    print("INFO: Generated %d query json @ %s" % (file_count, query_ws_path))
    return hosts, q_def_run_id

def launch_query(host_run_list, q_def_run_id):
    for host, cmd_list in host_run_list.items():
        for cmd in cmd_list:
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
            rmtree(query_ws_path) 
        os.mkdir(query_ws_path)

        host_cfg_list, run_ids = prepareTest(test_def)
        if platform.system() != 'Windows':          # real run
            launch_query(host_cfg_list, run_ids )
            print("DONE Launch... ")
        else:
            print("DRYRUN: done")
    else:
        print('cannot find query definition file %s' % query_config)
