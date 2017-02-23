#! /usr/bin/python3
# plot pidstats
# generates cvs
#TODOs:
# get time results - table row= a test, 
# get genome results - table row = a stage
# duration view: bin-view x - stages, y - duration 
import sqlite3

class TimeResultHandler(object):
    queries = {
        'definable_tags' : 'SELECT name FROM loader_config_tag WHERE user_definable=1;',
        'definable_tags' : 'SELECT name FROM loader_config_tag WHERE user_definable=1;',
        'user_config' : 'SELECT config FROM loader_config_def where name in (%s);',
        'run_config' : 'SELECT loader_configs, _id FROM run_def where _id=%d;',
        'last_run_config' : 'SELECT loader_configs, _id FROM run_def ORDER BY _id desc LIMIT 1;',
        'time_results' : 'SELECT time_result, genome_result, pidstat_path FROM time_result where run_id=%d;'}

    def __init__(self):
        self.db_conn = sqlite3.connect('genomicsdb_loader.db')
        self.mycursor = self.db_conn.cursor()
        self.__runid = None

    def __get_run_config(self, runid):
        query = self.queries['last_run_config'] if runid == -1 else self.queries['run_config'] % runid
        for row in self.mycursor.execute(query):
            return int(row[1]), row[0].split('-')
        return None

    #TODO: make last run as default
    def get_run_setting(self, runid=-1):
        ''' runid = -1 means the last run '''
        col_header = []
        for row in self.mycursor.execute(self.queries['definable_tags']):
           col_header.append(row[0])
         
        # ['bn20sg10KB', 'bn20sg100KB', 'bn20sg1','bn20pbf']
        my_runid, cfg_names = self.__get_run_config(runid) 
        if cfg_names:
            query = self.queries['user_config'] % ",".join( [ "\"%s\"" % x for x in cfg_names ] )
            data = []                                   # (RUN_#, [])
            i = 0
            for row in self.mycursor.execute(query):   #TODO, enumerate?
                cfg = dict(eval(row[0]))
                rcfg = [ str(cfg[x]) if x in  cfg else '-' for x in col_header ]
                i += 1
                data.append( ("RUN_%d" % i, rcfg ))
            return my_runid, data, col_header
        else:
            print("WARN cannot find run_id %d " % runid)

    def get_run_setting2(self, runid=-1):
        ''' runid = -1 means the last run, output RUN_x as column'''
        data_dict = {}
        for row in self.mycursor.execute(self.queries['definable_tags']):
           data_dict[row[0]] = list()          #initiate with definable column names
         
        # ['bn20sg10KB', 'bn20sg100KB', 'bn20sg1','bn20pbf']
        my_runid, cfg_names = self.__get_run_config(runid) 
        if cfg_names:
            col_header = []
            i = 1
            query = self.queries['user_config'] % ",".join( [ "\"%s\"" % x for x in cfg_names ] )
            for row in self.mycursor.execute(query):
                col_header.append("RUN_%d" % i)
                i += 1
                cfg = dict(eval(row[0]))
                for key, vlist in data_dict.items():
                    vlist.append(cfg[key] if key in cfg else '-')
            data = [ (k, v) for k, v in data_dict.items() ]
            return my_runid, data, col_header
        else:
            print("WARN cannot find run_id %d " % runid)


    def __get_all_result(self, runid):
        assert runid
        if runid != self.__runid:
            self.__all_results = []
            for row in self.mycursor.execute(self.queries['time_results'] % int(runid)):   #TODO, enumerate?
                rowresult = dict()
                rowresult['rtime'] = dict(eval(row[0]))
                rowresult['gtime'] = eval(row[1])
                rowresult['pidstat'] = eval(row[2])
                self.__all_results.append(rowresult)
            if self.__all_results:
                self.__runid = runid

    time_labels = {'Command' : 'cmd', 'Wall Clock (sec)' : 'elapse_sec', 'CPU %' : 'CPU_sec',
      'Major Page Fault' : 'major_pf', 'Minor Page Fault' : 'minor_pf',
      'File System Input' : 'fs_input', 'File System Output' : 'fs_output',
      'Involunteer Context Switch' : 'iv_cs', 'Volunteer Context Switch' : 'v_cs',
      'Exit Code' : 'exit_sts'}

    def get_time_result(self, runid):
        self.__get_all_result(runid)
        time4plot = list()
        for dspname, strname in self.time_labels.items():
            if strname != 'cmd':
                time4plot.append( (dspname, [ r['rtime'][strname] for r in self.__all_results]) )
        col_header = [ "RUN_%d" % (x+1) for x in range(len(self.__all_results)) ]
        return time4plot, col_header

    def get_pidstats(self, runid):
        self.__get_all_result(runid)
        #TODO? read all cvs files in?
        return [ r['pidstat'] for r in self.__all_results ]
    
    def close(self):
        self.db_conn.close()

