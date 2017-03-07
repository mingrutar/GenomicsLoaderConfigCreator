#! /usr/bin/python3
# plot pidstats
# generates cvs
#TODOs:
# get time results - table row= a test, 
# get genome results - table row = a stage
# duration view: bin-view x - stages, y - duration 
import sqlite3
import numpy as np
import pandas as pd
import os, os.path
import sys

class TimeResultHandler(object):
    def __init__(self, wkspace=None ):
        self.__wspace = wkspace if wkspace else os.getcwd()
        self.__runid = None
        self.setResultPath(".")
    
    def setResultPath(self, result_path):
        self.__source = os.path.join(self.__wspace, result_path)
        sys.path.append(os.path.dirname(self.__wspace))
        import core_data
        dbpath = os.path.join(self.__source, core_data.RunVCFData.DefaultDBName)
        if os.path.isfile(dbpath):
            if hasattr(self, 'data_handler') and self.data_handler:
                self.data_handler.close() 
            self.data_handler = core_data.RunVCFData(dbpath) 
            print("INFO found genomicd loader db at %s" % dbpath )

    def __transform4Pandas(self, inputRows) :
        ''' inputRows = [ ]
         [{k:v, k2:v2}] => [k, k2,..], [ [v, ...], [v2,...],  ]  arrya of (label, num_row)'''
        row_labels = [ x for x in inputRows[0].keys() ]
        data = []
        for i in range( len(row_labels)):
            data.append(['-'] * len(inputRows))
        for ri, row in enumerate(inputRows):
            for i, tag in enumerate(row_labels):
                data[i][ri] = row[tag]
        return row_labels, data
    
    def getRunSetting(self, runid=None):
        return self.data_handler.getRunConfigs(runid)
        
    def getRunSetting4Pandas(self, runid=None):
        ''' runid = -1 means the last run, output LOAD_x as column'''
        # confgiList [{,}] config_tag with user value or -
        my_runid, confgiList = self.data_handler.getRunConfigs(runid) 
        if confgiList:
            row_labels, data = self.__transform4Pandas(confgiList)
            col_header = [ "LOAD_%d" % (i+1) for i in range(len(confgiList))]
            pddata = [ (row_labels[i], dr) for i, dr in enumerate( data) ]
            return my_runid, pddata, col_header
        else:
            print("WARN cannot find run_id %d " % runid)

    def __get_all_result(self, runid):
        assert runid
        if runid != self.__runid or not self.__runid:
            results = self.data_handler.getAllResult(runid)
            if results:
                self.__all_results = results
                self.__runid = runid

    time_row_labels =['Command', 'Wall Clock (sec)', 'CPU %','Major Page Fault', 'Minor Page Fault', 
         'File System Input', 'File System Output', 'Involunteer Context Switch', 'Volunteer Context Switch','Exit Code']
    
    def shorten_command(self, line):
        llst = [  os.path.basename(cl) for cl in line.split() ]
        return " ".join(llst)

    def get_time_result(self, runid):
        self.__get_all_result(runid)
        col_header = []
        rtimelist = []
        for row in self.__all_results:
            row['rtime']['0'] = self.shorten_command(row['rtime']['0'])
            rtimelist.append(row['rtime'])
        row_idx, data = self.__transform4Pandas(rtimelist)
        row_labels = [ self.time_row_labels[int(i)] for  i in row_idx ]
        col_header = [ "LOAD_%d" % (i+1) for i in range(len(rtimelist))] 
        pddata = [ (row_labels[i], pd ) for i, pd in enumerate(data)]
        return pddata, col_header

    genome_db_tags = {'fv' : 'Fetch from VCF',
    'cc' : 'Combining Cells', 'fo' : 'Flush Output',
    'st' : 'sections time', 'ts' : 'time in single thread phase()',
    'tr' : 'time in read_all()'}
    gtime_col_header = ['Wall-clock time(s)', 'Cpu time(s)', 'Critical path wall-clock time(s)', 
            'Critical path Cpu time(s)', '#critical path']
    def __get_genome_result4run(self, gendata4run):
        row_list = [0.0] * len(self.gtime_col_header)
        for key, val in gendata4run.items():
            if key != 'op':
                row_list[int(key)] = val
        return self.genome_db_tags[gendata4run['op']], row_list
                
    def get_genome_results(self, runid, subidStr):
        ''' subidStr = LOAD_1,  LOAD_n '''
        self.__get_all_result(runid)
        subid = int(subidStr.split("_")[1]) - 1
        assert(subid < len(self.__all_results))
        rows = []
        for gtimes in self.__all_results[subid]['gtime'] :      # list
            rows.append(self.__get_genome_result4run(gtimes))
        return rows, self.gtime_col_header

    def get_pidstats(self, runid):
        self.__get_all_result(runid)
        pidstas = []
        for pidrow in self.__all_results:
            pidstas.append([ os.path.join(self.__source, 'stats', os.path.basename(fp)) for fp in pidrow['pidstat']])
        # pidstas = [ map(lambda fp: os.path.join(self.__source, 'stats', os.path.basename(fp)), pidrow['pidstat']) for pidrow in self.__all_results ]
        return pidstas
    
    def close(self):
        self.data_handler.close()

    def write_csv_labels(self, csv_fd, confgiList):
        ldname =  next (iter (confgiList.values()))
        lc_labels = [ k for k in ldname.keys() ]
        rtime = self.__all_results[0]['rtime']      # name:val
        rtime_labels = [ self.time_row_labels[int(x)] for x in rtime.keys() ]
        
        gtime_labels = []
        gtimes = self.__all_results[0]['gtime']      #  30 labels num_ops x num(gtime_col_header)
        num_gtime_labels = len(self.genome_db_tags) * len(self.gtime_col_header)
        gtimes = gtimes[: num_gtime_labels]
        for gt_item in gtimes:
            opname, gdata = self.__get_genome_result4run(gt_item)
            gt_op_labels = [ "%s_%s" % (opname, hname) for hname in self.gtime_col_header ]
            gtime_labels.extend(gt_op_labels)
        labels = lc_labels +  rtime_labels + gtime_labels
        csv_fd.write("%s\n" % ",".join(labels))
        csv_fd.flush()

    def export2csv(self, run_dir, runid=None):
        my_runid, configDict = self.data_handler.getRunConfigsDict(runid) 
        assert(my_runid != None and len(configDict) > 1)
        print("INFO export test run %d to csv..." % my_runid)

        self.__get_all_result(my_runid)
        filename = os.path.join(self.__wspace, "csvfiles", "%s_%s.csv" % (run_dir, my_runid))
        csv_fd = open(filename, 'w')
        self.write_csv_labels(csv_fd, configDict)

        for row in self.__all_results:
            lc_data = [ v for v in configDict[row['lcname']].values() ]
            rtime = row['rtime']      # name:val
            rtime_data = [ v for v in rtime.values() ]
            perproc_count = 0
            num_op = len(self.genome_db_tags)
            gtime_data = []
            for gtime in row['gtime']:      # list
                opname, gdata = self.__get_genome_result4run(gtime)
                gtime_data.extend(gdata)
                perproc_count += 1
                if perproc_count % num_op == 0:
                    aRow = lc_data + rtime_data + gtime_data
                    csv_fd.write("%s\n" % ",".join(aRow) )
                    csv_fd.flush()
        csv_fd.close()
        return filename

def testTimeResultHandler(resultData):
    runid, confgiList = resultData.getRunSetting()

    runid, data, col = resultData.getRunSetting4Pandas()
    # test time
    time_data, col_header = resultData.get_time_result(runid)
    #test genomics data
    gen_data, col_header = resultData.get_genome_results(runid, 'LOAD_1')

    pidfiles = resultData.get_pidstats(runid)
    for pids in pidfiles:                       # each machine
        print("pifiles=%s" % ",".join(pids))

if __name__ == '__main__':
    mypath = os.path.dirname(sys.argv[0])
    print("mypath=%s" % mypath)
    resultData = TimeResultHandler(mypath)

    run_dir = "run_mpi" 
    resultData.setResultPath(run_dir)
    runid = 1
    csv_file = resultData.export2csv(run_dir, runid)
    print("csv file @ %s" % csv_file)

    resultData.close()
    print("DONE")
    