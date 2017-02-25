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
import core_data

class TimeResultHandler(object):
    def __init__(self):
        self.data_handler = core_data.RunVCFData()

    def __transform4Pandas(self, inputRows) :
        ''' inputRows = [ ]
         [{k:v, k2:v2}] => [k, k2,..], [ [v, ...], [v2,...],  ]  arrya of (label, num_row)'''
        row_labels = inputRows[0].keys
        ret = [[]] * len(row_labels)
        row_idx = 0
        for run_cfg in confgiList:
            for i, tag in enumerate(row_labels):
                ret[i].append( run_cfg[tag])
        return row_labels, ret
    
    def getRunSetting(self, runid=None):
        my_runid, confgiList = self.data_handler.getRunConfigs(runid)
        return confgiList
        
    def getRunSetting4Pandas(self, runid=None):
        ''' runid = -1 means the last run, output RUN_x as column'''
        # confgiList [{,}] config_tag with user value or -
        my_runid, confgiList = self.data_handler.getRunConfigs(runid) 
        if confgiList:
            row_labels, data = self.__transform4Pandas(confgiList)
            col_header = [ "RUN_%d" % (i+1) for i in range(len(confgiList))] 
            return my_runid, zip(row_label, data), col_header
        else:
            print("WARN cannot find run_id %d " % runid)

    def __get_all_result(self, runid):
        assert runid
        if runid != self.__runid:
            results = self.data_handler.getAllResult(runid)
            if results:
                self.__all_results - results
                self.__runid = runid

    time_labels = {'Command' : 'cmd', 'Wall Clock (sec)' : 'elapse_sec', 'CPU %' : 'CPU_sec',
      'Major Page Fault' : 'major_pf', 'Minor Page Fault' : 'minor_pf',
      'File System Input' : 'fs_input', 'File System Output' : 'fs_output',
      'Involunteer Context Switch' : 'iv_cs', 'Volunteer Context Switch' : 'v_cs',
      'Exit Code' : 'exit_sts'}
    def get_time_result_0(self, runid):
        self.__get_all_result(runid)
        time4plot = list()
        for dspname, strname in self.time_labels.items():
            if strname != 'cmd':
                time4plot.append( (dspname, [ r['rtime'][strname] for r in self.__all_results]) )
        col_header = [ "RUN_%d" % (x+1) for x in range(len(self.__all_results)) ]
        return time4plot, col_header

    row_labels =['Command', 'Wall Clock (sec)', 'CPU %','Major Page Fault', 'Minor Page Fault', 
         'File System Input', 'File System Output', 'Involunteer Context Switch', 'Volunteer Context Switch','Exit Code']
    
    def get_time_result(self, runid):
        self.__get_all_result(runid)
        col_arrays = [ [] ] * len(row_labels) 
        col_header = []
        for i, run in enumerate(self.__all_results['rtime']):
            col_header.append("RUN_%d" % (i+1))
            for key, val in run.items():
                col_arrays[int(key)].append(val)
        data = zip(row_labels, col_arrays)
        return data, col_header

    genome_db_tags = {'fv' : 'Fetch from VCF',
    'cc' : 'Combining Cells', 'fo' : 'Flush Output',
    'st' : 'sections time', 'ts' : 'time in single thread phase()',
    'tr' : 'time in read_all()'}
    gtime_col_header = ['Wall-clock time(s)', 'Cpu time(s)', 'Critical path wall-clock time(s)', 
            'Critical path Cpu time(s)', '#critical path']
    def get_genome_results(self, runid, subidStr):
        self.__get_all_result(runid)
        subid = int(subidStr.split("_")[0])
        rows = []
        if subid < len(self.__all_results):
            for gtimes in self.__all_results[subid]['gtime'] :      # list
                row_list = [0.0] * len(col_header)
                for key, val in gtimes.items():
                    if key != 'op':
                        row_list[int(key)-1] = val
                rows.append((genome_db_tags[gtimes['op']], row_list))
            return rows, self.gtime_col_header
        else:
            print("Run with %s not found. ")
            return None

    def get_pidstats(self, runid):
        self.__get_all_result(runid)
        #TODO? read all cvs files in?
        return [ r['pidstat'] for r in self.__all_results ]
    
    def close(self):
        self.data_handler.close()

    def export2csv(runid=None):
        my_runid, confgiList = self.data_handler.getRunConfigs(runid) 
        self.__get_all_result(my_runid)

        #TODO: piece them together