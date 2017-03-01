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
#        pidstas = [ map(lambda fp: os.path.join(self.__source, 'stats', os.path.basename(fp)), pidrow['pidstat']) for pidrow in self.__all_results ]
        return pidstas
    
    def close(self):
        self.data_handler.close()

    def export2csv(self, runid=None):
        my_runid, confgiList = self.data_handler.getRunConfigs(runid) 
        print("INFO export test run %d to csv..." % my_runid)
        self.__get_all_result(my_runid)
        labels = []
        rowlist = []
        filename = os.path.join(self.__wspace, "csvfiles", "run_%s.csv" % my_runid)
        csv_fd = open(filename, 'w')
        for i, row in enumerate(confgiList):
            aRow = []
            if i == 0:
                for k, v in confgiList[i].items():
                    labels.append(k)
                    aRow.append(v)
            else:
                aRow = [ v for v in confgiList[i].values() ]
            
            rtime = self.__all_results[i]['rtime']      # name:val
            if i == 0:
                time_labels = [ self.time_row_labels[int(x)] for x in rtime.keys() ]
                labels.extend(time_labels)
            tData = [ v for v in rtime.values() ]
            aRow.extend(tData)

            for gtime in self.__all_results[i]['gtime']:      # list
                opname, gdata = self.__get_genome_result4run(gtime)
                if i == 0:
                    gen_labels = [ "%s_%s" % (opname, hname) for hname in self.gtime_col_header ]
                    labels.extend(gen_labels)
                aRow.extend(gdata)
            if i == 0:
                csv_fd.write("%s\n" % ",".join(labels) )
            csv_fd.write("%s\n" % ",".join(aRow))
            csv_fd.flush()
        csv_fd.close()
        return filename

if __name__ == '__main__':
    mypath = os.path.dirname(sys.argv[0])
    print("mypath=%s" % mypath)

    resultData = TimeResultHandler(mypath)
    
    resultData.setResultPath("run_6")

    runid, confgiList = resultData.getRunSetting()

    runid, data, col = resultData.getRunSetting4Pandas()
    # test time
    time_data, col_header = resultData.get_time_result(runid)
    #test genomics data
    gen_data, col_header = resultData.get_genome_results(runid, 'LOAD_1')

    pidfiles = resultData.get_pidstats(runid)
    for pids in pidfiles:                       # each machine
        print("pifiles=%s" % ",".join(pids))

    csv_file = resultData.export2csv(runid)
    print("csv file @ %s" % csv_file)

    resultData.close()
    print("DONE")
    