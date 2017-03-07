#! /usr/bin/python
import time
import os, os.path
from collections import deque
from subprocess import Popen, PIPE, check_output

theExecCmd = ['python', os.path.join(os.getcwd(),'t_std_err.py') ]

DEVNULL = open(os.devnull, 'wb', 0)
pexec = Popen(theExecCmd, shell=False, stdout=DEVNULL, stderr=PIPE) 
#pstat = startPidStats(cmd)

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
  known_cmds = ['vcf2tiledb', 'python', 'gt_mpi_gather']
  exec_name = None
  for ge_exec in known_cmds:
    if ge_exec in run_cmd :
      exec_name = ge_exec
      break
  if exec_name :
    pidlist = check_output(['/usr/bin/pgrep', exec_name])
    pidstr = ','.join(pidlist)
    pstat = Popen(['/usr/bin/pidstat', '-dl', '1', '-p', '%s' % pidstr ], stdout=fdlog, stderr=fdlog) 
    return pstat
  else :
    return None

with pexec.stderr:
    qexec = deque(iter(pexec.stderr.readline, b''))
#    qexec = deque(iter(pexec.stderr.readline, b''), maxlen=time_lines_count)
rc = pexec.wait()
time_results = {}
ll = qexec.pop().decode().strip()
name_val = dict(x.split(':') for x in ll.split(','))
time_results['tr'] = name_val
genome_result = []
for i, ll in enumerate(qexec):
    line = ll.decode().strip()
    gr = __proc_gen_result(line)
    genome_result.append(gr)
time_results['gr'] = genome_result
print(str(time_results))
#retStr = b''.join(qexec).decode().strip() 
