#! /usr/bin/python3
import sys
import os
import os.path
import json
import platform
from test_query import check_env, prepareTest, launch_query

TEST_PATH = os.path.join("/", "home","mingrutar","GenomicsDBPerfTest","dev")
JAVA_CLASS = "ProfileGenomicsDBCount"

def __prepareJavaCommand(root_path, java_class):
    java_root = os.path.join(root_path, "java")
    class_path_list =[java_root]
    class_path_list.append(os.path.join(java_root, "fromJar"))
    return "java -cp %s %s -query %%s" % (":".join(class_path_list), java_class)

def __java_test_cmd(query_fn):
    return query_cmd_tmpl % query_fn

if __name__ == '__main__' :
    query_config = sys.argv[1] if(len(sys.argv) > 1) else "test_query_def.json"
    test_def = check_env(query_config)
    
    query_cmd_tmpl = __prepareJavaCommand(TEST_PATH, JAVA_CLASS)
    host_cfg_list, run_ids = prepareTest(test_def, __java_test_cmd)
    if platform.system() != 'Windows':          # real run
        launch_query(host_cfg_list, run_ids )
        print("DONE Launch... ")
    else:
        print("DRYRUN: done")
