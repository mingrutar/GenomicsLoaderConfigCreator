#! /usr/bin/python3

import logging
import shutil
import os
import os.path
import time
import platform
import stat

g_root_log_name = None
   
def remake_path(target_path):
    if os.path.exists(target_path):
        shutil.rmtree(target_path)
    os.makedirs(target_path)

def get_stats_path(root_path):
    return os.path.join(root_path, 'stats_row'), os.path.join(root_path, 'stats')

def str2num(x):
    ''' bery forgiven '''
    try:
        return int(x)
    except Exception:
        try:
            return float(x)
        except Exception:
            return None
            
def gen_timestamp():
    return int(time.time())

def is_windows():
    return platform.system() == 'Windows'

def check_x_mode(file_path):
    if not is_windows():
        assert os.path.isfile(file_path), ' file %s not found' % file_path
        st = os.stat(file_path)
        os.chmod(file_path, st.st_mode | stat.S_IEXEC)

def __get_root_logger(log_name):
    global g_root_log_name

    g_root_log_name = log_name
    format_str = '%(asctime)s %(name)s::%(levelname)s: %(message)s'
    logging.basicConfig(format=format_str, datefmt='%m%d %H:%M:%S', level=logging.DEBUG)
    logger = logging.getLogger(log_name)
    # create file handler which logs even debug messages
    fhandler = logging.FileHandler("%s.log" % log_name, mode='w')
    formatter = logging.Formatter(fmt=format_str, datefmt='%m%d%Y %I:%M %p')
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)

    # ch = logging.StreamHandler()   this one goes to console, dup with logger
    # ch.setLevel(logging.INFO)
    # ch.setFormatter(formatter)
    # logger.addHandler(ch)
    return logger

def __get_sub_logger(log_name):
    return logging.getLogger("%s.%s" % (g_root_log_name, log_name))

# @public got NameError
def get_my_logger(log_name):
    return __get_sub_logger(log_name) if g_root_log_name else __get_root_logger(log_name)
