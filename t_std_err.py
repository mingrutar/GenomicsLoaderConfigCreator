#! /usr/bin/python

from __future__ import print_function
import sys
import time

def eprint(*args, **kwargs):
    ''' print to stderr '''
    print(*args, file=sys.stderr, **kwargs)

time.sleep(1)

eprint("GENOMICSDB_TIMER,Fetch from VCF,Wall-clock time(s),269.713,Cpu time(s),262.142,Critical path wall-clock time(s),80.795,Cpu time(s),74.1936,#critical path,987")
eprint("GENOMICSDB_TIMER,Combining cells,Wall-clock time(s),464.615,Cpu time(s),462.818,Critical path wall-clock time(s),454.033,Cpu time(s),452.237,#critical path,198")
eprint("GENOMICSDB_TIMER,Flush output,Wall-clock time(s),0.000226,Cpu time(s),0.000235145,Critical path wall-clock time(s),0,Cpu time(s),0,#critical path,0")
eprint("GENOMICSDB_TIMER,Sections time,Wall-clock time(s),734.329,Cpu time(s),724.962,Critical path wall-clock time(s),0,Cpu time(s),0,#critical path,0")
eprint("GENOMICSDB_TIMER,Time in single thread phase(),Wall-clock time(s),0.001188,Cpu time(s),0.00119834,Critical path wall-clock time(s),0,Cpu time(s),0,#critical path,0")
eprint("GENOMICSDB_TIMER,Time in read_all(),Wall-clock time(s),734.331,Cpu time(s),724.964,Critical path wall-clock time(s),0,Cpu time(s),0,#critical path,0")
eprint("cmd:%C,elapse_sec:%e,CPU_sec:%P,major_pf:%F,minor_pf:%R,v_cs:%w,fs_input:%I,fs_output:%O,iv_cs:%c,exit_sts:%x")

time.sleep(1)
