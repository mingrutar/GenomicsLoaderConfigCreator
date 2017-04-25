#! /usr/bin/python3

import sys
import os
from pprint import pprint
from collections import deque, OrderedDict
from subprocess import Popen, PIPE, check_output, CalledProcessError
import json
import platform
import sqlite3
import time
import os.path
import stat
from datetime import datetime
from get_exec_info import GenomicsExecInfo

CURRENT_MPIRUN_PATH = "/usr/lib64/mpich/bin/mpirun"   #"/opt/openmpi/bin/mpirun" 

PIDSTAT_INTERVAL = 5        # in sec

DEVNULL = open(os.devnull, 'wb', 0)
working_path = os.getcwd()
g_hostname = platform.node().split('.')[0]
expected_lib_paths = [ '/home/mingrutar/opt/zlib/lib', '/usr/lib64/mpich/lib/', '/usr/lib64' ]
 
queries = { "SELECT_RUN_CMD" : "SELECT _id, full_cmd, tiledb_ws FROM exec_def WHERE hostname like \"%s\" AND run_def_id=%d;",
    "SELECT_RUN_CMD2" : "SELECT _id, full_cmd, tiledb_ws FROM exec_def WHERE hostname like \"%s\" AND _id=%d;",
    "INSERT_TIME" : "INSERT INTO time_result (run_id, cmd_version, time_result, genome_result, partition_1_size, db_size, pidstat_path) \
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
      print("INFO @%s: ignore no GENOMICSDB_TIMER, %s..." % (g_hostname, geno_str[:80]))

genome_queryprof_tags = { 'genomicsdb cell fill timer': 'cf',
   'bcf_t serialization' : 'bs',
   'operator time' : 'ot', 
   'sweep at query begin position' : 'sq', 
   'tiledb iterator' : 'ti', 
   'total scan_and_produce_broad_gvcf time for rank 0' : 'tt', 
   'tiledb to buffer cell' : 'tb', 
   'bcf_t creation time' : 'bc' }
def __proc_query_result(geno_str):
    lines = geno_str.split(',')
    if lines[0].strip().upper() == 'GENOMICSDB_TIMER':
      ret = {}
      op_str = lines[1].strip().lower()

      if op_str in genome_queryprof_tags:
        ret['op'] = genome_queryprof_tags[op_str]
      else:
        print('WARN @%s: operation string %s not found' % (g_hostname, op_str))
        ret['op'] = op_str.replace(' ', '_')
      
      for i in range(2, len(lines)):
          if 'Cpu time(s)' in lines[i]:
            i += 1
            ret['0'] = lines[i]    # CPU
          if 'Wall-clock time(s)' in lines[i]:
            i += 1
            ret['1'] = lines[i]    # wall clock
      return ret

def startPidStats(run_cmd, fdlog ) :
  known_cmds = ['vcf2tiledb', 'gt_mpi_gather', 'java']    #ProfileGenomicsDBCount
  exec_name = None
  for ge_exec in known_cmds:
    if ge_exec in run_cmd :
      exec_name = ge_exec
      break
  pstat = None
  if exec_name :
    try:
      #pidlist = check_output(['/usr/bin/pgrep', exec_name])
      #pidstr = pidlist.decode('utf-8').replace('\n', ',')
      pstat = Popen(['/usr/bin/pidstat', '-hdIruw', '-C', exec_name, str(PIDSTAT_INTERVAL) ], stdout=fdlog, stderr=fdlog)
    except CalledProcessError as cperr:
      print("CalledProcessError error: {}".format(cperr))
  else:
    print('WARN @{}: did not find exec like {}'.format(g_hostname, ', '.join(known_cmds)))
  return pstat

def measure_more( cmd, logfile, gen_result_func ) :
  time_lines_count = 6     # how many lines /usr/bin/time produces
  theExecCmd = ['/usr/bin/time', "-f", "0~%C,1~%e,2~%P,3~%F,4~%R,5~%w,6~%I,7~%O,8~%c,9~%x,10~%M,11~%t,12~%K"] + cmd
  pexec = Popen(theExecCmd, shell=False, stdout=DEVNULL, stderr=PIPE)
  if pexec:
  #    print("**1* INFO @%s: launched time 4 cmd=%s" % (g_hostname, cmd))
    time.sleep(1)
    fdlog = open(logfile, 'w')
    pstat = startPidStats(','.join(cmd), fdlog)
    print("**EXEC INFO @%s: command %s, pexec-pid=%s, pstat=%s" % (g_hostname, str(theExecCmd), pexec.pid, pstat.pid if pstat else 'no_stat'))
    with pexec.stderr:
      qexec = deque(iter(pexec.stderr.readline, b''))
    rc = pexec.wait()
    fdlog.close()
    if pstat:
        pstat.kill()
    print("**OUT_EXEC INFO @%s: #output=%d" % (g_hostname, len(qexec)) ) 
    # last is of time
    timeline = qexec.pop().decode('utf-8').strip()
    try:
      time_result = dict(x.split('~') for x in timeline.split(','))
    except ValueError:
      print("EXCEPTION: timeline=%s" % timeline)

    # output of vcf2tiledb
    genome_result=[]
    for i, gl in enumerate(qexec):
      line = gl.decode('utf-8').strip()
      if line:
        gr = gen_result_func(line)
        if gr:
          genome_result.append(gr)
      else:
        print("INFO @%s: empty line %i " % (g_hostname, i))
    return time_result, genome_result
  else:
    print("ERROR @{}: failed exec. cmd={}".format(g_hostname, theExecCmd) )

def save_time_log(db_path, run_id, exec_info, time_output, genome_output, pidstat_cvs, tiledb_ws) :
    db_size = check_output(['du', '-sh', tiledb_ws]).decode('utf-8').split()[0]
    stmt = queries['INSERT_TIME'] % (run_id, exec_info, str(time_output), str(genome_output), 0, db_size, pidstat_cvs)
    print(stmt)
    db_conn = sqlite3.connect(db_path)
    crs = db_conn.cursor()
    crs.execute(stmt)
    print("INFO @%s: inserted %d" % (g_hostname, crs.lastrowid))
    db_conn.commit()
    db_conn.close()

def run_pre_test(working_dir, tiledb_root, isLoading) :
  script = 'precheck_tdb_ws.bash' if isLoading else 'prelaunch_check.bash'
  pre_test = os.path.join(working_dir, script)
  st = os.stat(pre_test)
  os.chmod(pre_test, st.st_mode | stat.S_IEXEC)
  assert(os.path.isfile(pre_test))
  cmd = "%s %s" % (pre_test, tiledb_root)
  proc = Popen([cmd], shell=True)
  proc.wait()
  return  proc.returncode == 0

def get_command(run_def_id, db_path, run_id):
    ret = []
    stmt = queries['SELECT_RUN_CMD2'] % (g_hostname, run_id) if run_id else queries['SELECT_RUN_CMD'] % (g_hostname, run_def_id)
#    print("INFO %s: get_command stmt=%s" %(g_hostname, stmt))
    db_conn = sqlite3.connect(db_path)
    mycursor = db_conn.cursor()
    for row in mycursor.execute(stmt):
      ret.append((row[1], row[2], row[0]))    # full_cmd, tiledb_ws, _id
    db_conn.close()
    return ret

def pidstat2cvs(ifile, of_prefix) :
    extract_fields = lambda l: [ l[i] for i in [0, 6,7,8,9,10,11,12,13,14,17 ] ]
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

def check_lib_path(expected_paths):
    ''' list of full path of expected_paths '''
    env_str = 'LD_LIBRARY_PATH'
    epl = len(expected_paths)
    if env_str in os.environ:
        paths_dict = OrderedDict({p : i+epl for i, p in enumerate(os.environ[env_str].split(':')) if p } )
        for ep in reversed(expected_paths):
          if ep not in paths_dict:
            if os.paths.isdir(ep):
                epl -= 1
                paths_dict.add[ep] = epl
            else:
                raise RuntimeError('Needed library %s is not found' % ep)
        paths_dict = OrderedDict(sorted(paths_dict.items(), key=lambda x : x[1]))
        os.environ['LD_LIBRARY_PATH'] = ":".join([ p for p in paths_dict])
    else:
        os.environ['LD_LIBRARY_PATH'] = ":".join(expected_paths)
    print("INFO %s: LD_LIBRARY_PATH=%s" % (g_hostname, os.environ['LD_LIBRARY_PATH']))

def get_version_info(full_cmd):
    ''' full_cmd may include mpirun'''
    real_cmd = full_cmd[3] if os.path.basename(full_cmd[0]) == 'mpirun' else full_cmd[0]
    handler = GenomicsExecInfo()
    version = handler.get_version_info(real_cmd)
    print("INFO++ get_version_info:  real_cmd=%s, version=%s" % (real_cmd, version))
    return version
  
if __name__ == '__main__' :
    assert(len(sys.argv)>1)
    rundef_id =int(sys.argv[1])
    runid = int(sys.argv[2]) if len(sys.argv) > 2 else None
    working_dir = os.path.dirname(sys.argv[0])
    #TODO: $WS or os.getcwd()
    if not working_dir:
      working_dir = os.getcwd()

    db_path = os.path.join(working_dir, 'genomicsdb_loader.db')
    if not os.path.isfile(db_path) :
      print("INFO %s: not found %s, ...exit " % (g_hostname, db_path))
      exit()

    cmd_list = get_command(rundef_id, db_path, runid)        #
    if not cmd_list:
      print("ERROR %s: no command found for runid=%d" % (g_hostname, rundef_id))
      exit()
    # check enrinment
    try: 
      check_lib_path(expected_lib_paths)
    except RuntimeError as rterr:
      print("ERROR %s: check_lib_path error: %s" % (g_hostname, rterr) )
      exit()
    # 
    isLoading = 'vcf2tiledb' in cmd_list[0][0]
    gen_result_func = __proc_gen_result if  isLoading else __proc_query_result
    rcount = 1 
    rtotal = len(cmd_list)
    logspath = os.path.join(working_dir, "logs")
    if not os.path.isdir(logspath):
      os.makedirs(logspath)

    for cmd, tiledb_ws, run_id in cmd_list:
      rc = run_pre_test(working_dir, tiledb_ws, isLoading)
      if not rc:
        print("WARN %s: pretest failed with with runid=%s, cmd=%s, tiledb_ws=%s, continue to next" % (g_hostname,run_id, cmd, tiledb_ws))
        continue
# no longer needed     if not isLoading:    #TODO: temporary add loader file
#        cmd = "%s -l %s" % (cmd, os.path.join(working_dir, 'loader_config.json'))   
      print("++++START %s: %d/%d, rid=%s, tdb_ws=%s, cmd=%s" % (g_hostname,rcount,rtotal,run_id, tiledb_ws, cmd))
      target_cmd = [ str(x.rstrip()) for x in cmd.split(' ') ]
      version_str = get_version_info(target_cmd)

      # print('target_cmd=%s' % target_cmd)
      log_fn = "%d-%d-%s_pid.log" % (rundef_id, run_id, g_hostname)
      log2path = os.path.join(logspath, log_fn)
      print("INFO %s: pidstat.log=%s" % (g_hostname, log2path))
      time_nval, genome_time = measure_more(target_cmd, log2path, gen_result_func)
      if len(genome_time) > 0 :
        print("INFO %s: Corrected genomics executable output. The exit status=%s" % (g_hostname, time_nval['9']) )
        stat_path = os.path.join(working_dir, 'stats')
        if not os.path.isdir(stat_path) :
          os.mkdir(stat_path)
        cvs_prefix = os.path.join(stat_path, log_fn[:-4])
        cvsfiles = pidstat2cvs(log2path, cvs_prefix)
  #      print("INFO %s: cvsfiles= %s" % (g_hostname, str(cvsfiles) ))
        cvsfile = [ os.path.basename(x) for x in cvsfiles ]
        save_time_log(db_path, run_id, version_str, time_nval, genome_time, cvsfiles, tiledb_ws)
      else:
          print("WARN %s: No time measured for genomics executable, IGNORED. The exit status=%s" % (g_hostname, time_nval['9'] ))
      print("++++END %s: %d/%d, rid=%s" % (g_hostname, rcount, rtotal, run_id))
      rcount += 1
