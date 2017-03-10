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

PIDSTAT_INTERVAL = 2        #in sec
# no longer in use TILE_WORKSPACE = "/mnt/app_hdd1/scratch/mingperf/tiledb-ws/"

DEVNULL = open(os.devnull, 'wb', 0)
working_path = os.getcwd()
g_hostname = platform.node().split('.')[0]
 
queries = { "SELECT_RUN_CMD" : "SELECT _id, full_cmd, tiledb_ws FROM run_log WHERE host_id like \"%s\" AND run_def_id=%d;",
   "INSERT_TIME" : "INSERT INTO time_result (run_id, target_comand, time_result, genome_result, partition_1_size, db_size, pidstat_path) \
 VALUES (%s, \"%s\", \"%s\", \"%s\", %s, \"%s\", \"%s\");" } 
genome_profile_tags = {'fetch from vcf' : 'fv',
    'combining cells' :'cc',
    'flush output': 'fo',
    'sections time' : 'st',
    'time in single thread phase()' : 'ts',
    'time in read_all()' : 'tr'}

def __proc_gen_result(geno_str) :
#    print(" @{}: __proc_gen_result geno_str={}".format(g_hostname, geno_str))
    lines = geno_str.split(',')
    if lines[0].strip().upper() == 'GENOMICSDB_TIMER':
      ret = {}
      op_str = lines[1].strip().lower()
      if op_str in genome_profile_tags:
        ret['op'] = genome_profile_tags[op_str]
      else:
        print('WARN @%s: operation string %s not found' % (g_hostname, op_str))
        ret['op'] = op_str.replace(' ', '_')
      ret['0'] = lines[3]
      ret['1'] = lines[5]
      ret['2'] = lines[7]
      ret['3'] = lines[9]
      ret['4'] = lines[11]
      return ret
    else:
      print("INFO @%s: not GENOMICSDB_TIMER, ignored: " % (g_hostname, geno_str))
      
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
#    print("INFO @{}: pid for {} are {}".format(g_hostname, exec_name, pidstr))
    pstat = Popen(['/usr/bin/pidstat', '-hdIlruw', '1', '-p', pidstr ], stdout=fdlog, stderr=fdlog)
  else:
    print('WARN @{}: did not find exec like {}'.format(g_hostname, ', '.join(known_cmds)))
  return pstat

def measure_more( cmd, logfile ) :
  time_lines_count = 6     # how many lines /usr/bin/time produces
  theExecCmd = ['/usr/bin/time', "-f", "0:%C,1:%e,2:%P,3:%F,4:%R,5:%w,6:%I,7:%O,8:%c,9:%x"] + cmd
  pexec = Popen(theExecCmd, shell=False, stdout=DEVNULL, stderr=PIPE)
  if pexec:
  #    print("**1* INFO @%s: launched time 4 cmd=%s" % (g_hostname, cmd))
    time.sleep(1)
    fdlog = open(logfile, 'w')
    pstat = startPidStats(','.join(cmd), fdlog)
    print("**2* INFO @%s: executing command %s, pexec-pid=%s, pstat=%s" % (g_hostname, str(theExecCmd), pexec.pid, pstat.pid if pstat else 'no_stat'))
    with pexec.stderr:
      qexec = deque(iter(pexec.stderr.readline, b''))
    rc = pexec.wait()
    fdlog.close()
    if pstat:
        pstat.kill()
    print("**3* INFO @%s: #output=%d" % (g_hostname, len(qexec)) ) 
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
        print("INFO @%s: empty line %i " % (g_hostname, i))
    return time_result, genome_result
  else:
    print("ERROR @{}: failed exec. cmd={}".format(g_hostname, theExecCmd) )

def save_time_log(db_path, run_id, cmd, time_output, genome_output, pidstat_cvs, tiledb_ws) :
    db_size = check_output(['du', '-sh', tiledb_ws]).decode('utf-8').split()[0]
    stmt = queries['INSERT_TIME'] % (run_id, cmd, str(time_output), str(genome_output), 0, db_size, pidstat_cvs)
    print(stmt)
    db_conn = sqlite3.connect(db_path)
    crs = db_conn.cursor()
    crs.execute(stmt)
    print("INFO @%s: inserted %d" % (g_hostname, crs.lastrowid))
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

def get_command(run_id, db_path):
    ret = []
    stmt = queries['SELECT_RUN_CMD'] % (g_hostname, run_id)
    print("INFO %s: run_pre_test stmt=%s" %(g_hostname, stmt))
    db_conn = sqlite3.connect(db_path)
    mycursor = db_conn.cursor()
    for row in mycursor.execute(stmt):
      ret.append((row[1], row[2], row[0]))
    db_conn.close()
    return ret

def pidstat2cvs(ifile, of_prefix) :
    extract_fields = lambda l: [ l[i] for i in [0, 6,7,12, 13,14,17 ] ]
    with open(ifile, 'r') as fd:
        lines = fd.readlines()
    header = extract_fields( lines[2][1:].split() )
    dataline = [ l for l in lines[3:] if l[0] != '#' and len(l) > 20 ]
    linelist = [ x.split()  for x in dataline ]
    # find all unique (UID, PID)
    proc_set = set(map(tuple, [ x[1:3] for x in linelist] ))
    pid_output = {}
    for pp in proc_set:
        pid_output[pp] = [ extract_fields(x) for x in linelist if tuple(x[1:3]) == pp ]

    cvs_pids = []
    for key, val in pid_output.items() :
        ofile = "%s_%s.cvs" % (of_prefix, key[1])
        ofd = open(ofile, 'w')
        ofd.write("%s\n" % ','.join(header) )
        [ ofd.write("%s\n" % ','.join(data)  ) for data in val ]
        ofd.close()
        cvs_pids.append(ofile)
    return cvs_pids

if __name__ == '__main__' :
    rundef_id =int(sys.argv[1])
    working_dir = os.path.dirname(sys.argv[0])
    db_path = os.path.join(working_dir, 'genomicsdb_loader.db')
    if not os.path.isfile(db_path) :
      print("INFO %s: not found %s, ...exit " % (g_hostname, db_path))
      exit()

    cmd_list = get_command(rundef_id, db_path)        # 
    for cmd, tiledb_ws, run_id in cmd_list:
      rc = run_pre_test( working_dir, tiledb_ws )
      if rc:
        target_cmd = [ str(x.rstrip()) for x in cmd.split(' ') ]
        # print('target_cmd=%s' % target_cmd)
        log_fn = "%d-%d-%s_pid.log" % (rundef_id, run_id, g_hostname)
        log2path = os.path.join(working_dir, "logs", log_fn)
        print("INFO %s: pidstat.log=%s" % (g_hostname, log2path))
        time_nval, genome_time = measure_more(target_cmd, log2path)

        stat_path = os.path.join(working_dir, 'stats')
        if not os.path.isdir(stat_path) :
          os.mkdir(stat_path)
        cvs_prefix = os.path.join(stat_path, log_fn[:-4])
        cvsfiles = pidstat2cvs(log2path, cvs_prefix)
  #      print("INFO %s: cvsfiles= %s" % (g_hostname, str(cvsfiles) ))
        cvsfile = [ os.path.basename(x) for x in cvsfiles ]
        save_time_log(db_path, run_id, os.path.basename(cmd), time_nval, genome_time, cvsfiles, tiledb_ws)
