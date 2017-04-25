#! /usr/bin/python3

MPIRUN_PATH = "/usr/lib64/mpich/bin/mpirun"
DEFAULT_DB_NAME = "genomicsdb_loader.db"
NO_VERSION = 'not available' 

GDB_COMMANDS = ['vcf2tiledb', 'gt_mpi_gather', 'java']    
LOADER_EXEC = GDB_COMMANDS[0]
CHECK_VERSION_LIST = GDB_COMMANDS[:2]

CONFIG_DIRNAME = 'config_files'
RUN_SCRIPT = "main_remote.py"

ENV_TILEDB_ROOT = 'TILEDB_ROOT'
DEFAULT_TDB_PREFIX = "/mnt/app_hdd1/scratch/mingperf/tiledb-ws"

WAIT_TIME_FOR_LAUNCHING = 0.5           # 0.5 sec
GENOMICSDB_TIMER = 'GENOMICSDB_TIMER'

ENV_LIB_PATH = 'LD_LIBRARY_PATH'
DEFAULT_LIB_PATHS = ['/home/mingrutar/opt/protobuf/lib','/usr/lib64/mpich/lib/', '/usr/lib64']

PIDSTAT_INTERVAL = 5                    # in sec
CLEAN_CACHE_SCRIPT_PATH = "/tools/utilities/bin/clear_caches.sh"

TIME_FORMATTER = "0~%C,1~%e,2~%P,3~%F,4~%R,5~%w,6~%I,7~%O,8~%c,9~%x,10~%M,11~%t,12~%K"

genome_profile_tags = {'fetch from vcf': 'fv', 'combining cells':'cc', 'flush output': 'fo',\
    'sections time': 'st', 'time in single thread phase()': 'ts', 'time in read_all()': 'tr'}
genome_queryprof_tags = {'genomicsdb cell fill timer': 'cf', 'bcf_t serialization': 'bs', \
   'operator time': 'ot', 'sweep at query begin position': 'sq', 'tiledb iterator': 'ti', \
   'total scan_and_produce_broad_gvcf time for rank 0': 'tt', 'tiledb to buffer cell': 'tb', \
   'bcf_t creation time': 'bc'}
   