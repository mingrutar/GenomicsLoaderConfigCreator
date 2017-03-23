#! /usr/bin/python3
import sqlite3
import sys
import os
import os.path
import datetime
import hashlib
import platform

class GenomicsExecInfo(object):
    # TODO: remove when version is available
    TEMP_VERSION = 'not available' 
    DefaultDBName = 'genomicsdb_loader.db'
    queries = { 
        'CREATE_VERSION_TABLE' : 'CREATE TABLE IF NOT EXISTS exec_info (_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
            'buid_tag TEXT NOT NULL,name TEXT NOT NULL,version TEXT NOT NULL,full_path TEXT NOT NULL,' \
            'build_time INTEGER NULL,hash TEXT,additional TEXT );',
        'SELECT_LAST_TAG' : 'SELECT buid_tag FROM exec_info ORDER BY _id desc limit 1;',
        'SELECT_EXEC_INFO' : 'SELECT name, version, full_path, hash from exec_info where buid_tag=\"%s\";',
        'INSERT_EXEC_INFO' : 'INSERT INTO exec_info (buid_tag, name, version, full_path, build_time,hash,additional) ' \
            ' VALUES (\"%s\",\"%s\",\"%s\",\"%s\",%d,\"%s\",\"%s\");'        
        }
    def __init__(self, db_name=None):
        self.db_name = db_name if db_name else self.DefaultDBName 
        self.db_conn = sqlite3.connect(self.db_name)
        mycursor = self.db_conn.cursor()
        mycursor.execute(self.queries["CREATE_VERSION_TABLE"])
        mycursor.close()

    def __del__(self):
        self.db_conn.close()
    
    def close(self):
        self.db_conn.close()
        
    def __insert_info(self, mycursor, full_fn):
        hashcode = hashlib.sha256(open(full_fn, 'rb').read()).hexdigest()
        stmt = self.queries["INSERT_EXEC_INFO"] % (self.tag, os.path.basename(full_fn),self.TEMP_VERSION, full_fn,
             os.path.getctime(full_fn), hashcode, "")
        mycursor.execute(stmt)

    def updateBuild(self, exec_path, tag):
        assert(os.path.isdir(exec_path))     
        self.tag = tag
        mycursor = self.db_conn.cursor()
        for fn in next(os.walk(exec_path))[2]: 
            self.__insert_info(mycursor, os.path.join(exec_path, fn))
        self.db_conn.commit()
        print("insert version info for exec @ %s" % exec_path)

    def checkExecs(self):
        mycursor = self.db_conn.cursor()
        my_tag = mycursor.execute(self.queries['SELECT_LAST_TAG']).fetchone()
        stmt = self.queries['SELECT_EXEC_INFO'] % my_tag
        for row in mycursor.execute(stmt):
            full_path = row[2]
            if os.path.exists(full_path):
                thehash = row[3]
                checkhash = hashlib.sha256(open(full_path, 'rb').read()).hexdigest()
                if thehash != checkhash:
                    print("WARN: The file %s seems changed" % full_path)
                else:
                    print("INFO: %s is valid, version='%s'" % (full_path, row[1]) )
            else:
                print("WARN: cannot find file %s" % full_path)
        
if __name__ == "__main__":
    if platform.system() != 'Windows':          # real run
        exec_path = os.path.join("/", "home", "mingrutar", "cppProjects", "GenomicsDB", "bin")
    else:
        exec_path = os.path.join("\\", "opt")
    tag = datetime.date.today().isoformat()
    exec_info_handler = GenomicsExecInfo()
    exec_info_handler.updateBuild(exec_path, tag)
    exec_info_handler.checkExecs()
    exec_info_handler.close()
