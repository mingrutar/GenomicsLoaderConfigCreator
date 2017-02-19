#! /usr/bin/python3

import sys
import os
from pprint import pprint
from collections import deque
from subprocess import Popen, PIPE, check_output
import json
import platform
import sqlite3
import time
import os.path
from datetime import datetime

PIDSTAT_INTERVAL = 1        #1 sec
TILE_WORKSPACE = "/mnt/app_hdd1/scratch/mingperf/tiledb-ws/"

DEVNULL = open(os.devnull, 'wb', 0)
time_format="-f cmd:%C,elapse_sec:%e,CPU_sec:%P,major_pf:%F,minor_pf:%R,v_cs:%w,fs_input:%I,fs_output:%O,iv_cs:%c,exit_sts:%x"
working_path = os.getcwd()

genome_profile_tags = {'Fetch from VCF' : 'fv', 
    'Combining cells' :'cc',
    'Flush output': 'fo',
     'Sections time' : 'st',
     'Time in single thread phase()' : 'ts',
     'Time in read_all()' : 'tr'}

def __proc_gen_result(geno_str) :
    ret = {}
    lines = geno_str.split(',')
    ret['op'] = genome_profile_tags[lines[1]]
    ret['wc'] = lines[3]
    ret['ct'] = lines[5]
    ret['cwc'] = lines[7]
    ret['cct'] = lines[9]
    ret['ncp'] = lines[11]
    return ret

def startPidStats(run_cmd ) :
  known_cmds = ['vcf2tiledb', 'gt_mpi_gather']
  exec_name = None
  for ge_exec in known_cmds:
    if ge_exec in run_cmd :
      exec_name = ge_exec
      break
  if exec_name :
    pidlist = check_output(['/usr/bin/pgrep', exec_name])
    pidstr = ','.join(pidlist)
    pstat = Popen(['/usr/bin/pidstat', '-dl', '1', '-p', '%s' % pidstr ], stdout=fdlog, stderr=fdlog) 

def measure_more( cmd, logfile ) :
  fdlog = open(logfile, 'w', 0)
  time_lines_count = 1     # how many lines /usr/bin/time produces
  theExecCmd = ['/usr/bin/time', time_format] + cmd
  pexec = Popen(theExecCmd, shell=False, stdout=DEVNULL, stderr=PIPE) 
  pstat = startPidStats(cmd)
  print("executing command %s, pexec-pid=%s, pstat.pid=%s " % (str(theExecCmd), pexec.pid, pstat.pid))

  with pexec.stderr:
    qexec = deque(iter(pexec.stderr.readline, b''))
#    qexec = deque(iter(pexec.stderr.readline, b''), maxlen=time_lines_count)
  rc = pexec.wait()
  fdlog.close()
  pstat.kill()

  # last is of time
  time_result = __proc_time_result(qexec.pop().decode().strip()) 
  genome_result = []
  for gl in qexec:
    #TODO: reformat
    line = gl.decode().strip()
    gr = __proc_gen_result(line)
    genome_result.append(gl)
  return time_result, genome_result 

INSERT_TIME ="INSERT INTO time_result (run_id, time_result, partition_1_size, db_size, pidstat_path, genome_result) \
 VALUES (%s, \"%s\", %d, %d, \"%s\",  \"%s\");"
def save_time_log(db_path, run_id, time_output, genome_output, pidstat_cvs) :
    stmt = INSERT_TIME % (run_id, str(time_output), str(genome_output), 0, 0, pidstat_cvs)
    print(stmt)
    db_conn = sqlite3.connect(db_path)
    crs = db_conn.cursor()
    crs.execute(stmt)
    print("inserted %d" % crs.lastrowid)
    db_conn.commit()
    db_conn.close()

def run_pre_test(working_dir, tiledb_root) :
  pre_test = os.path.join(working_dir, 'prelaunch_chec.bash')
  if os.path.isfile(pre_test) :
    pid = Popen([pre_test, tiledb_root], stdout=PIPE, stderr=PIPE)
    out, err = pid.communicate()
    print(out)
    return  pid.returncode == 0
  else:
    return None

def pidstat2cvs(ifile, of_prefix) :
    def __to_epoch(timestrs) :
        ''' timestrs = [ 'hh:mm:ss', 'A|PM' ] '''
        thetime = '%s %s %s' % (time.strftime('%d%m%y'), timestrs[0], timestrs[1])
        epochtime = time.mktime(time.strptime(thetime, '%d%m%y %I:%M:%S %p'))
        return epochtime
    with open(ifile, 'r') as fd:
        lines = fd.readlines()
    linelist = [ x.split()  for x in lines[3:] ]
    # find all unique (UID, PID)
    proc_set = set(map(tuple, [ x[2:4] for x in linelist] )) 
    pid_output = {}
    for pp in proc_set: 
        pid_output[pp] = [ (str(__to_epoch(x[0:2])), x[4:7][0], x[4:7][1], x[4:7][2] ) for x in linelist if tuple(x[2:4]) == pp ]
    
    cvs_pids = []    
    for key, val in pid_output.items() :
        ofile = "%s_%s.cvs" % (of_prefix, key[1])
        ofd = open(ofile, 'w')
        ofd.write("ts, rs, ws,ccws\n")
        [ ofd.write("%s\n" % ','.join(data)  ) for data in val ]
        ofd.close()
        cvs_pids.append(ofd)
    return cvs_pids 
    
if __name__ == '__main__' :
    jsonfl_path = sys.argv[1] 
    with open(jsonfl_path, 'r') as ldf:
        exec_json = json.load(ldf)
    working_dir = os.path.dirname( os.path.dirname(jsonfl_path)) 
    print("working_dir=%s" % working_dir)
    rc = run_pre_test( working_dir, TILE_WORKSPACE)
    if rc:  
      target_cmd = [ str(x.rstrip()) for x in  exec_json['cmd'].split(' ') ]
      # print('target_cmd=%s' % target_cmd)
      log_fn = "%d-%s_%s_pid.log" % (exec_json['run_id'], datetime.now().strftime("%y%m%d%H%M"), os.path.basename(jsonfl_path)[-4:])
      log2path = os.path.join(os.path.dirname(jsonfl_path), log_fn)
      print("pidstat.log=%s" % log2path)
      time_nval,genome_time = measure_more(target_cmd, log2path)

      stat_path = os.path.join(working_dir, 'stats')
      if not os.path.isdir(stat_path) :
        os.mkdir(stat_path)
      cvs_prefix = os.path.join(stat_path, log_fn[:-4])
      l = pidstat2cvs(log2path, cvs_prefix)
      cvsfile = [ os.path.basename(x) for x in l ]
      db_path = os.path.join(working_dir, 'genomicsdb_loader.db')
      if os.path.isfile(db_path) :
        save_time_log(db_path, exec_json['run_id'], time_nval, genome_time, cvsfile)
      else :
        print("not found %s" % db_path)
    