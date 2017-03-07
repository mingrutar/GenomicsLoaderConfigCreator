#! /usr/bin/python3

import result_handler as rh


def pidstat2cvs(ifile, of_prefix) :
    def __to_epoch(timestrs) :
        ''' timestrs = [ 'hh:mm:ss', 'A|PM' ] '''
        thetime = '%s %s %s' % (time.strftime('%d%m%y'), timestrs[0], timestrs[1])
        epochtime = time.mktime(time.strptime(thetime, '%d%m%y %I:%M:%S %p'))
        return epochtime
    extract_fields = lambda l: [ l[i] for i in [0, 1, 7, 8, 13, 14,15,19 ] ]
    with open(ifile, 'r') as fd:
        lines = fd.readlines()
    ll = lines[2][1:].split()
    header = extract_fields( ll )
    dataline = [ l for l in lines[3:] if l[0] != '#' and len(l) > 20 ]
    linelist = [ x.split()  for x in dataline ]
    # find all unique (UID, PID)
    proc_set = set(map(tuple, [ x[2:4] for x in linelist] ))
    pid_output = {}
    for pp in proc_set:
        pid_output[pp] = [ extract_fields(x) for x in linelist if tuple(x[2:4]) == pp ]

    cvs_pids = []
    for key, val in pid_output.items() :
        ofile = "%s_%s.cvs" % (of_prefix, key[1])
        ofd = open(ofile, 'w')
        ofd.write("%s\n" % ','.join(header))
        [ ofd.write("%s\n" % ','.join(data)  ) for data in val ]
        ofd.close()
        cvs_pids.append(ofile)
    return cvs_pids



if __name__ == '__main__':
#    pidstat2cvs('14-1702231437_2-25_pid.log', 'pidcsv2_')

    resultData = rh.TimeResultHandler()
    confgiList = resultData.getRunSetting()

    runid, data, col = resultData.getRunSetting4Pandas()
    # test time
    time_data, col_header = resultData.get_time_result(17)
    #test genomics data
    gen_data, col_header = resultData.get_genome_results(17, 'RUN_1')

    pidfiles = resultData.get_pidstats(17)
    for pids in pidfiles:                       # each machine
        print("pifiles=%s" % ",".join(pids))

    csv_file = resultData.export2csv(17)
    print("csv file @ %s" % csv_file)

    resultData.close()
    print("DONE")
