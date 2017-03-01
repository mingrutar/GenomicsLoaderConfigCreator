#! /usr/bin/python
import sys
import getopt
import platform
import os, os.path

aa = sys.argv[1:]
print("type=%s, aa=%s" % (type(aa), aa))
myopts, args = getopt.getopt(aa,"l:r:d")
dryrun = platform.system() == 'Windows'
load_file =  "loader_def.json"
run_file = None

for opt, input in myopts:
    if opt == '-l':
        load_file = input
    if opt == '-r':
        run_file = input
    if opt == '--dryrun' or opt == '-d':
        dryrun = input.lower() in ['1', 'true'] 

if not os.path.exists(load_file):
    print("ERROR: cannot find load config file %s, exit..." % load_file)
#    exit(1)
if run_file and not os.path.exists(run_file):
    print("WARN: cannot find mpirun config file %s, will not use mpirun  ..." % run_file)
    run_file=None

print("dryrun=%s, loader=%s, run=%s" % (dryrun, load_file, run_file))    
print('DONE')