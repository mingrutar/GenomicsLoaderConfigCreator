#! /usr/bin/python
import sys
import os
from pprint import pprint
from collections import deque
from subprocess import Popen, PIPE
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

def measure_more( cmd, log_path ) :
  logfile = os.path.join(log_path, "ps_%s.log" % platform.node())
  pidstatlog = open(logfile, 'w', 0)
  time_lines_count = 1     # how many lines /usr/bin/time produces
  theExecCmd = ['/usr/bin/time', time_format] + cmd
  print("executing command %s " % (str(theExecCmd)))
  pexec = Popen(theExecCmd, shell=False, stdout=DEVNULL, stderr=PIPE) 
  pstat = Popen(['/usr/bin/pidstat', '-dl', '1', '-p', '%s' % pexec.pid ], stdout=pidstatlog, stderr=pidstatlog) 
  print("pid_pexec=%s, pid_pstat=%s" % (pexec.pid, pstat.pid))

  with pexec.stderr:
    qexec = deque(iter(pexec.stderr.readline, b''), maxlen=time_lines_count)
    print("pexec exited .. pid_pexec=%s" % (pexec.pid))
  rc = pexec.wait()
 
  pidstatlog.close()
  pstat.kill()
  retStr = b''.join(qexec).decode().strip() 
  print( "retStr=%s" % retStr )
  name_val = dict(x.split(':') for x in retStr.split(','))
  return name_val, logfile

INSERT_TIME ="INSERT INTO time_result (run_id, log_text, partition_1_size, db_size, pidstat_path) \
 VALUES (%s, \"%s\", %d, %d, \"%s\");"
def save_time_log(db_path, run_id, time_output, pidstat_cvs) :
#    stmt = INSERT_TIME % (run_id, str(time_output), 0, 0, pidstat_cvs)
    stmt="INSERT INTO time_result (run_id, log_text, partition_1_size, db_size, pidstat_path)  VALUES (1, \"{u'minor_pf': u'198', u'cmd': u'sleep 5', u'major_pf': u'1', u'fs_output': u'0', u'v_cs': u'5', u'iv_cs': u'1', u'fs_input': u'72', u'exit_sts': u'0', u'CPU_sec': u'0%', u'elapse_sec': u'5.01'}\", 0, 0, 'pidstat_cvs');"
#    stmt = INSERT_TIME % (run_id, str(time_output), 0, 0, "pidstat_cvs")
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
        # pid : [( ts, rs, writePerSec, ccws) ]  
        pid_output[pp] = [ (str(__to_epoch(x[0:2])), x[4:7][0], x[4:7][1], x[4:7][2] ) for x in linelist if tuple(x[2:4]) == pp ]
        
    for key, val in pid_output.items() :
        ofile = "%s_%s.cvs" % (of_prefix, key[1])
        ofd = open(ofile, 'w')
        ofd.write("ts, rs, ws,ccws\n")
        [ ofd.write("%s\n" % ','.join(data)  ) for data in val ]
        ofd.close()
    os.remove(ifile)
    return ofile 
    
if __name__ == '__main__' :
    jsonfl_path = os.path.join(os.getcwd(), 'run_ws\compute-2-22')
    working_dir = os.path.dirname( os.path.dirname(jsonfl_path)) 
    print("working_dir=%s" % working_dir)
    rc = True
    if rc:  
      cvsfile = "c:\\Users\\mrutarx\\myprojects\\dev\\run_ws\\ps_compute-2-22.spark0.intel.com_175467.cvs" 
      exec_json = {'run_id' : 1}
      log_fn = "%d-%s_%s_pid.log" % (exec_json['run_id'], datetime.now().strftime("%y%m%d%H%M"), os.path.basename(jsonfl_path)[-4:])
      log2path = os.path.join(os.path.dirname(jsonfl_path), log_fn)
      print("pidstat.log=%s" % log2path)
      db_path = os.path.join(working_dir, 'genomicsdb_loader.db')
      if os.path.isfile(db_path) :
          save_time_log(db_path, exec_json['run_id'], None, cvsfile)
      else :
          print("not found %s" % db_path)
    