#! /usr/bin/python
import sys
import os
from pprint import pprint
from collections import deque
from subprocess import Popen, PIPE
import json
import platform
import sqlite3

PIDSTAT_INTERVAL = 1        #1 sec

DEVNULL = open(os.devnull, 'wb', 0)
time_format='-f "cmd:%C,elapse_sec:%e,CPU_sec:%P,major_pf:%F,minor_pf:%R,v_cs:%w,fs_input:%I,fs_output:%O,iv_cs:%c,exit_sts:%x"'
working_path = os.getcwd()

def measure_more( cmd, log_path ) :
  logfile = os.path.join(log_path, "ps_%s.log" % platform.node())
  pidstatlog = open(pidstatlog, 'w', 0)
  time_lines_count = 1     # how many lines /usr/bin/time produces
  pexec = Popen(['/usr/bin/time', time_format] + cmd, shell=False, stdout=DEVNULL, stderr=PIPE) 
  pstat = Popen(['/usr/bin/pidstat', '-dl', '1', '-p', '%s' % pexec.pid ], stdout=pidstatlog, stderr=pidstatlog) 
  print("pid_pexec=%s, pid_pstat=%s" % (pexec.pid, pstat.pid))

  with pexec.stderr:
    qexec = deque(iter(pexec.stderr.readline, b''), maxlen=time_lines_count)
  rc = pexec.wait()
  pstat.kill()
  retStr = b''.join(qexec).decode().strip() 
  print( retStr )
  name_val = dict(x.split(':') for x in retStr.split(','))
  return name_val, pidstatlog

INSERT_TIME ="INSERT INTO time_result (run_id, log_text, partition_1_size, db_size) \
 VALUES (%d, %s, %d, %d)"
def save_time_log(run_id, time_output) :
    db_conn = sqlite3.connect('genomicsdb_loader.db')
    mycursor = db_conn.cursor()
    #TODO check db size
    stmt = INSERT_TIME % (run_id, str(time_output, 0, 0))
    mycursor.execute(stmt)
    db_conn.commit()
    db_conn.close()

if __name__ == '__main__' :
    jsonfl_path = sys.argv[1]
    with open(jsonfl_path, 'r') as ldf:
        exec_json = json.load(ldf)
    pprint(exec_json)
    stat_path = os.path.join(os.path.abspath(jsonfl_path), 'stats')
    time_nval, pidstatlog = measure_more(exec_json['cmd'], stat_path)
    save_time_log(time_nval)
    #TODO make pidstatlog to csv file 
