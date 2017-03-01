import sys
import os, re
import os.path
import json
import time
import 

my_hostlist = []
my_templates = {}
data_handler = core_data.RunVCFData()
working_dir = os.environ.get('WS_HOME', os.getcwd())

TARGET_TEST_COMMAND = "/home/mingrutar/cppProjects/GenomicsDB/bin/gt_mpi_gather"
RUN_SCRIPT = os.path.join(os.getcwd(),"run_exec.py")

def load_from_db() :
    global my_hostlist, my_templates
    my_hostlist = data_handler.getHosts()
    my_templates = data_handler.getTemplates(working_dir)

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

if __name__ == '__main__' :
    load_from_db()

    query_config = sys.argv[1] if(len(sys.argv) > 1) else "query_def.json"
    query_cfg_fn = os.path.join(working_dir, query_config)

    if os.path.exists(query_cfg_fn) :
        launch_query(query_cfg_fn, dryrun=True )
