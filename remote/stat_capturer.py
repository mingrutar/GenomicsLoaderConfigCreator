#! /usr/bin/python3

import sys
import os
import os.path
import time
from subprocess import Popen
from utils.common import remake_path
from utils.constant_def import WAIT_TIME_FOR_LAUNCHING, PIDSTAT_INTERVAL

class StatCapturer():
    def __init__(self, log_root, csv_root):
        self.log_root = log_root
        self.csv_root = csv_root
        self.pid_logpath = None
        self.pstat = None
        self.fd_log = None

    @staticmethod
    def __write2file(cvs_prefix, pid, header, lines):
        ofile = "%s_%s.cvs" % (cvs_prefix, pid)
        with open(ofile, 'w') as ofd:
            ofd.write("%s\n" % ','.join(header))
            for fields in lines:
                ofd.write("%s\n" % ','.join(fields))
        return ofile

    @staticmethod
    def make_csv(from_log, to_csv_prefix):
        with open(from_log, 'r') as fd:
            lines = fd.readlines()
        f_idx = [0, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17]
        header = [f for i, f in enumerate(lines[2][1:].split()) if i in f_idx ]
        dataline = [l.split() for l in lines[3:] if l[0] != '#' and len(l) > 20]
        pid_d = {}
        for sl in dataline:
            k = (sl[1], sl[2])
            v = [ sl[i] for i in f_idx ]
            if k in pid_d:
                pid_d[k].append(v)
            else:
                pid_d[k] = [ v ]
        pid_csv_files = [StatCapturer.__write2file(to_csv_prefix, key[1], header, val) for  key, val in pid_d.items()]
        return pid_csv_files

    def start(self, fn_postfix, target_cmd):
        exec_name = os.path.basename(target_cmd)
        assert exec_name
        time.sleep(WAIT_TIME_FOR_LAUNCHING)
        fn = "pid_%s.log" % (fn_postfix)
        self.pid_logpath = os.path.join(self.log_root, fn)
        try:
            fd_log = open(self.pid_logpath, 'w')
            #pidlist = check_output(['/usr/bin/pgrep', exec_name])
            #pidstr = pidlist.decode('utf-8').replace('\n', ',')
            self.pstat = Popen(['/usr/bin/pidstat', '-hdIruw', '-C', exec_name, str(PIDSTAT_INTERVAL)], stdout=fd_log, stderr=fd_log)
            self.fd_log = fd_log
        except:
            print("caught exception %s: %s. No pidstat will be captured" % (sys.exc_info()[0], sys.exc_info()[1]))
            self.stop()

    def convert_pid_log(self, csv_root_path=None):
        csv_root = csv_root_path if csv_root_path else self.csv_root 
        cvs_prefix = os.path.join(csv_root, self.pid_logpath[:-4])
        return self.make_csv(self.pid_logpath, cvs_prefix)

    def stop(self):
        if self.fd_log: 
            self.fd_log.close()
            self.fd_log = None
        if self.pstat:
            self.pstat.kill()
            self.pstat = None
