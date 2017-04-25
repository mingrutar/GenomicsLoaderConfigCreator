#! /usr/bin/python3
import os
import os.path
import sys
import traceback
import platform
from utils.common import get_stats_path
from remote.exec_test import RunTest

if __name__ == '__main__':
    assert len(sys.argv) > 2, "Invalid arguments"
    working_dir = os.path.abspath(os.path.dirname(__file__))
    sys.path.append(working_dir)

    task_id = int(sys.argv[1])
    db_path = sys.argv[2]
    assert task_id and db_path, 'invalid argument'
    if not os.path.isfile(db_path):
        db_path = os.path.join(working_dir, db_path)
        assert os.path.isfile(db_path), 'cannot find database'

    # remake_path(logspath)
    hostname = platform.node().split('.')[0]
    print("hostname=%s, working_dir=%s, task_id=%s, db_path=%s" % (hostname, working_dir, task_id, db_path))
    try:
        runner = RunTest(db_path)
        log_spath, csv_path = get_stats_path(working_dir)
        runner.run_test(task_id, log_spath, csv_path)
    except:
        print("caught exception @", platform.node().split('.')[0])
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=20, file=sys.stdout)
    finally:
        exit()
