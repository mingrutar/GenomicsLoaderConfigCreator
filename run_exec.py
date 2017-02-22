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
hostname = platform.node().split('.')[0]
 
genome_profile_tags = {'fetch from vcf' : 'fv',
    'combining cells' :'cc',
    'flush output': 'fo',
    'sections time' : 'st',
    'time in single thread phase()' : 'ts',
    'time in read_all()' : 'tr'}

def __proc_gen_result(geno_str) :
#    print(" @{}: __proc_gen_result geno_str={}".format(hostname, geno_str))
    lines = geno_str.split(',')
    if lines[0].strip().upper() == 'GENOMICSDB_TIMER':
      ret = {}
      op_str = lines[1].strip().lower()
      if op_str in genome_profile_tags:
        ret['op'] = genome_profile_tags[op_str]
      else:
        print('WARN @%s: operation string %s not found' % (hostname, op_str))
        ret['op'] = op_str.replace(' ', '_')
      ret['wc'] = lines[3]
      ret['ct'] = lines[5]
      ret['cwc'] = lines[7]
      ret['cct'] = lines[9]
      ret['ncp'] = lines[11]
      return ret
    else:
      print("INFO @%s: not GENOMICSDB_TIMER, ignored: " % (hostname, geno_str))
      
def startPidStats(run_cmd, fdlog ) :
  known_cmds = ['vcf2tiledb', 'gt_mpi_gather']
  exec_name = None
  for ge_exec in known_cmds:
    if ge_exec in run_cmd :
      exec_name = ge_exec
      break
  pstat = None
  if exec_name :
    pidlist = check_output(['/usr/bin/pgrep', exec_name])
#    print("type(pidlist)={}, pidlist={}".format(type(pidlist), pidlist))
    pidstr = pidlist.decode('utf-8').replace('\n', ',')
#    print("INFO @{}: pid for {} are {}".format(hostname, exec_name, pidstr))
    pstat = Popen(['/usr/bin/pidstat', '-dl', '1', '-p', pidstr ], stdout=fdlog, stderr=fdlog)
  else:
    print('WARN @{}: did not find exec like {}'.format(hostname, ', '.join(known_cmds)))
  return pstat

def measure_more( cmd, logfile ) :
  time_lines_count = 6     # how many lines /usr/bin/time produces
  theExecCmd = ['/usr/bin/time', time_format] + cmd
  pexec = Popen(theExecCmd, shell=False, stdout=DEVNULL, stderr=PIPE)
  if pexec:
#    print("**1* INFO @%s: launched time 4 cmd=%s" % (hostname, cmd))
    time.sleep(1)
    fdlog = open(logfile, 'w')
    pstat = startPidStats(','.join(cmd), fdlog)
    print("**2* INFO @%s: executing command %s, pexec-pid=%s, pstat=%s" % (hostname, str(theExecCmd), pexec.pid, pstat.pid if pstat else 'no_stat'))
    with pexec.stderr:
      qexec = deque(iter(pexec.stderr.readline, b''))
    rc = pexec.wait()
    fdlog.close()
    if pstat:
        pstat.kill()
    print("**3* INFO @%s: #output=%d" % (hostname, len(qexec)) ) 
    # last is of time
    timeline = qexec.pop().decode('utf-8').strip()
    time_result = dict(x.split(':') for x in timeline.split(','))
    # output of vcf2tiledb
    genome_result=[]
    for i, gl in enumerate(qexec):
      line = gl.decode('utf-8').strip()
      if line:
        gr = __proc_gen_result(line)
        genome_result.append(gr)
      else:
        print("INFO @%s: empty line %i " % (hostname, i))
    return time_result, genome_result

  else:
    print("ERROR @{}: failed exec. cmd={}".format(hostname, theExecCmd) )

INSERT_TIME ="INSERT INTO time_result (run_id, time_result, genome_result, partition_1_size, db_size, pidstat_path) \
 VALUES (%s, \"%s\", \"%s\", %d, %d, \"%s\");"
def save_time_log(db_path, run_id, time_output, genome_output, pidstat_cvs) :
    stmt = INSERT_TIME % (run_id, str(time_output), str(genome_output), 0, 0, pidstat_cvs)
    print(stmt)
    db_conn = sqlite3.connect(db_path)
    crs = db_conn.cursor()
    crs.execute(stmt)
    print("INFO @%s: inserted %d" % (hostname, crs.lastrowid))
    db_conn.commit()
    db_conn.close()

def run_pre_test(working_dir, tiledb_root) :
  pre_test = os.path.join(working_dir, 'prelaunch_check.bash')
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
        cvs_pids.append(ofile)
    return cvs_pids

if __name__ == '__main__' :
    jsonfl_path = sys.argv[1]
    with open(jsonfl_path, 'r') as ldf:
        exec_json = json.load(ldf)
    working_dir = os.path.dirname( os.path.dirname(jsonfl_path))
    print("INFO %s: working_dir=%s" % (hostname, working_dir))
    rc = run_pre_test( working_dir, TILE_WORKSPACE)
    if rc:
      target_cmd = [ str(x.rstrip()) for x in  exec_json['cmd'].split(' ') ]
      # print('target_cmd=%s' % target_cmd)
      log_fn = "%d-%s_%s_pid.log" % (exec_json['run_id'], datetime.now().strftime("%y%m%d%H%M"), os.path.basename(jsonfl_path)[-4:])
      log2path = os.path.join(os.path.dirname(jsonfl_path), log_fn)
      print("INFO %s: pidstat.log=%s" % (hostname, log2path))
      time_nval,genome_time = measure_more(target_cmd, log2path)

      stat_path = os.path.join(working_dir, 'stats')
      if not os.path.isdir(stat_path) :
        os.mkdir(stat_path)
      cvs_prefix = os.path.join(stat_path, log_fn[:-4])
      cvsfiles = pidstat2cvs(log2path, cvs_prefix)
#      print("INFO %s: cvsfiles= %s" % (hostname, str(cvsfiles) ))
      cvsfile = [ os.path.basename(x) for x in cvsfiles ]
      db_path = os.path.join(working_dir, 'genomicsdb_loader.db')
      if os.path.isfile(db_path) :
        save_time_log(db_path, exec_json['run_id'], time_nval, genome_time, cvsfiles)
      else :
        print("INFO %s: not found %s" % (hostname, db_path))
