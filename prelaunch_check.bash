#! /bin/bash
# run:
# for r in $(find $HOME/GenomicsDBPerfTest/loaders -name '*.json' -exec basename {} \; | cut -d'_' -f1); do ssh $r $HOME/GenomicsDBPerfTest/prelaunch.bash;done
TEST_WS_DIR="/mnt/app_hdd1/scratch/mingperf/tiledb-ws"
#clean_caches util path
CLEAN_CACHES="/tools/utilities/bin/clear_caches.sh"
#executable
EXEC_NAME="$HOME/cppProjects/GenomicsDB/bin/vcf2tiledb"
EXEC_NAME_GT="$HOME/cppProjects/GenomicsDB/bin/gt_mpi_gather"

INIT_TILEDB="$HOME/cppProjects/GenomicsDB/bin/create_tiledb_workspace"
#MPIRUN="/usr/lib64/mpich/bin/mpirun"
MPIRUN="/opt/openmpi/bin/mpirun"

check_app() {
  #check if vcf2tiledb exists
  if ! [  -x "$(command -v $1)" ]; then
    echo "$(hostname) :  $(basename $1) not found. Build it first.. exit";
    return 1;
  fi
  
  mypid=$(pgrep $(basename $1))
  if ! [ -z $mypid ] ; then
    echo "$0 found $(basename $1E) running: $mypid"
    kill $mypid
    while ! [ -z $mypid ] ; do 
      sleep 1
      mypid=$(pgrep $(basename $1))
    done
    echo "$0 killed all $(basename $1)"
  fi   
  return 0 
}
check4run() {

  \time sleep 1 >/dev/null 2>&1
  if [ $? -ne 0 ] ; then
    echo "$(hostname) : could not find time .. exit";
    return 1;
  fi

  if [ ! -x $CLEAN_CACHES ]; then
    echo "$(hostname) : Could not fid $CLEAN_CACHES.. exit";
    return 1;
  fi

  #check if vcf2tiledb exists
  check_app $EXEC_NAME
  if [$? -ne 0 ]; then
    echo "Error in checking $EXEC_NAME ... exit";
    return 1;
  fi

  check_app $EXEC_NAME_GT
  if [$? -ne 0 ]; then
    echo "Error in checking $EXEC_NAME_GT ... exit";
    return 1;
  fi

  sudo $CLEAN_CACHES
  if [ $? -ne 0 ] ; then
     echo "$(hostname) : sudo $CLEAN_CACHES not work ? "
 #    return 1;
  else
     echo "$(hostname) sudo $CLEAN_CACHES worked ok"
  fi

  echo "clean test disk $TEST_WS_DIR, current df -h:" 
  df -h $TEST_WS_DIR
  if [ -d $TEST_WS_DIR ] ; then
     [[ -f $TEST_WS_DIR/__tiledb_workspace.tdb ]] ||  touch $TEST_WS_DIR/__tiledb_workspace.tdb
     [[ -f $TEST_WS_DIR/__tiledb_group.tdb ]] ||  touch $TEST_WS_DIR/__tiledb_group.tdb
     rm -rf $TEST_WS_DIR/TEST*
  else
    mkdir -p $TEST_WS_DIR
    $INIT_TILEDB $TEST_WS_DIR
    $(dirname $EXEC_NAME)/create_tiledb_workspace $TEST_WS_DIR
  fi
  echo "after cleaning: df -h:"
  df -h $TEST_WS_DIR

  #check mpirun
  if ! [  -x "$(command -v $MPIRUN)" ]; then
    echo "$(hostname) :  $(basename $MPIRUN) not found. Install it first.. exit";
    return 1;
  fi

  return 0
}

[[ $# -gt 1 ]] && TEST_WS_DIR=$1
echo "$(hostname) TEST_WS_DIR=$TEST_WS_DIR" 
check4run
[[ $? -eq 0 ]] && echo "$(hostname) : all good "
