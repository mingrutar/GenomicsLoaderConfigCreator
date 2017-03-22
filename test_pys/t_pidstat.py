import sys
import os, os.path

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

if __name__ == "__main__" :
    log2path = os.path.join(os.getcwd(), "test_pys", "18-616-compute-2-29_pid.log")
    cvs_prefix = "17"
    cvsfiles = pidstat2cvs(log2path, cvs_prefix)
    print("create csv file: %s" % cvsfiles)