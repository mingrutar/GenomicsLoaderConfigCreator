#! /usr/bin/python3
import sqlite3
import sys
import os
import os.path
import datetime
import hashlib
import platform
from subprocess import check_output 

class GenomicsExecInfo(object):
    # TODO: remove when version is available
    NO_VERSION = 'not available' 
    DefaultDBName = 'genomicsdb_loader.db'
    #TODO: change after refactory
    check_version_list = ["vcf2tiledb", "gt_mpi_gather"]

    queries = { 
        'CREATE_VERSION_TABLE' : 'CREATE TABLE IF NOT EXISTS exec_info (_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
            'buid_tag TEXT NOT NULL,name TEXT NOT NULL,version TEXT NOT NULL,full_path TEXT NOT NULL,' \
            'build_time INTEGER NULL,hash TEXT,additional TEXT );',
        'SELECT_LAST_TAG' : 'SELECT buid_tag FROM exec_info ORDER BY _id desc limit 1;',
        'SELECT_EXEC_INFO' : 'SELECT name, version, full_path, hash from exec_info where buid_tag=\"%s\";',
        "GET_CMD_VERSION" : "SELECT version, hash FROM exec_info WHERE full_path=\"%s\" ORDER BY _id desc limit 1;",
        "GET_COUNT_BY_HASH" : "SELECT count(*) FROM exec_info WHERE full_path=\"%s\" AND hash=\"%s\";",
        'INSERT_EXEC_INFO' : 'INSERT INTO exec_info (buid_tag, name, version, full_path, build_time,hash,other) ' \
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
        
    def __insert_info(self, mycursor, full_fn, need_check_version):
        hashcode = hashlib.sha256(open(full_fn, 'rb').read()).hexdigest()
        stmt = self.queries['GET_COUNT_BY_HASH'] % (full_fn, hashcode)
        if not mycursor.execute(stmt).fetchone()[0]:      
            # we do not have it
            version = check_output([full_fn, "--version"]).decode('utf-8').strip() if need_check_version else self.NO_VERSION
            stmt = self.queries["INSERT_EXEC_INFO"] % (self.tag, os.path.basename(full_fn), version, full_fn,
                os.path.getctime(full_fn), hashcode, "")
            mycursor.execute(stmt)
            return True
        else:
            return False

    def updateBuild(self, exec_path, tag=None):
        assert(os.path.isdir(exec_path))     
        self.tag = tag if tag else datetime.date.today().isoformat()
        mycursor = self.mycursor if hasattr(self, 'mycursor') else self.db_conn.cursor()
        num_inserted = 0;
        for fn in next(os.walk(exec_path))[2]: 
            if self.__insert_info(mycursor, os.path.join(exec_path, fn), fn in self.check_version_list ):
                num_inserted += 1
        self.db_conn.commit()
        if not hasattr(self, 'mycursor'):
            self.db_conn.close()
        print("insert %d version info for exec @ %s" % (num_inserted, exec_path))

    def __check_exec_hash(self, full_path, hashcode):
        if os.path.exists(full_path):
            checkhash = hashlib.sha256(open(full_path, 'rb').read()).hexdigest()
            return hashcode == checkhash
        else:
            raise RuntimeError("WARN: cannot find file %s" % full_path)
        
    def checkExecs(self):
        mycursor = self.db_conn.cursor()
        my_tag = mycursor.execute(self.queries['SELECT_LAST_TAG']).fetchone()
        stmt = self.queries['SELECT_EXEC_INFO'] % my_tag
        for row in mycursor.execute(stmt):
            try:
                if self.__check_exec_hash(row[2], row[3]):
                    print("INFO: %s is valid, version='%s'" % (row[2], row[1]) )
                else:
                    print("WARN: The file %s seems changed" % row[2])
            except RuntimeError as rte:
                print("FATAL: cannot find file %s: %s" % (row[2], rte))
        self.db_conn.close()
        
    def get_version_info(self, full_path):
        try:
            mycursor = self.db_conn.cursor()
            stmt = self.queries['GET_CMD_VERSION'] % full_path
            mycursor.execute(stmt)
            if mycursor.rowcount > 0:
                version_str, hash_code = mycursor.fetchone()
                if self.__check_exec_hash(full_path, hash_code):
                    return version_str
            self.mycursor = mycursor
            self.updateBuild(os.path.dirname(full_path))
            version_str, hash_code = self.mycursor.execute(stmt).fetchone()
            return version_str
        finally:
            self.db_conn.close()

if __name__ == "__main__":
    if platform.system() != 'Windows':          # real run
        exec_path = os.path.join("/", "home", "mingrutar", "cppProjects", "GenomicsDB", "bin")
    else:
        exec_path = os.path.join("\\", "opt")
    tag = datetime.date.today().isoformat()
    exec_info_handler = GenomicsExecInfo()
    print('==== get_version_info =====')
    exec_info_handler3 = GenomicsExecInfo()
    version=exec_info_handler3.get_version_info('/home/mingrutar/cppProjects/GenomicsDB/bin/vcf2tiledb')
    print("version=", version)

    print('==== updateBuild =====')
    exec_info_handler.updateBuild(exec_path, tag)
    # print('==== checkExecs =====')
    # exec_info_handler2 = GenomicsExecInfo()
    # exec_info_handler2.checkExecs()
