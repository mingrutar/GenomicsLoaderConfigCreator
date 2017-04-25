#! /usr/bin/python3
import sys
import os
import os.path
import traceback
from interface.loader import LoaderInterface

def test_more(host_list=None):
    print("=+= test more =+=")
    config = os.path.join(mycwd, 'input_jsons', 'loader_def.json')
    iloader.run(config)

def test_test(host_list=None):
    print("=-= test test =-=")
    config = os.path.join(mycwd, 'input_jsons', 'loader_def_test.json')
    iloader.run(config, host_list)
    
def test_generate_ld():
    print("=== test generate_batch_loader_config ===")
    out2dir = os.path.join(mycwd, 'tmp_cfg')
    remake_path(out2dir)
    config = os.path.join(mycwd, 'input_jsons', 'loader_def.json')
    iloader.generate_batch_loader_config(config, out2dir)

if __name__ == "__main__":
    try:
        mycwd = os.getcwd()
        if not mycwd in sys.path:
            sys.path.insert(0, mycwd)
        
        host_list = ["compute-2-22", "compute-2-29", "compute-2-23"]
        iloader = LoaderInterface(mycwd)
        test_test()
        #test_more(host_list)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=10, file=sys.stdout)
