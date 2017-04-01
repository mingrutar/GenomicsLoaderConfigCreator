#! /bin/bash

check_tiledb_ws() { 
  TEST_WS_DIR=$1
  echo "$(hostname) TEST_WS_DIR=$TEST_WS_DIR" 
    
  if [ -d $TEST_WS_DIR ] ; then
     [[ -f $TEST_WS_DIR/__tiledb_workspace.tdb ]] ||  touch $TEST_WS_DIR/__tiledb_workspace.tdb
     [[ -f $TEST_WS_DIR/__tiledb_group.tdb ]] ||  touch $TEST_WS_DIR/__tiledb_group.tdb
     rm -rf $TEST_WS_DIR/TEST*
     echo "removed old TEST* from tiledb workspace $TEST_WS_DIR" 
  else
    mkdir -p $TEST_WS_DIR
    $INIT_TILEDB $TEST_WS_DIR
    $(dirname $EXEC_NAME)/create_tiledb_workspace $TEST_WS_DIR
    echo "created tiledb workspace $TEST_WS_DIR" 
  fi
  echo "Output of df -h $TEST_WS_DIR :"
  df -h $TEST_WS_DIR
}

echo 
script=$(dirname $0)/prelaunch_check.bash
echo "===== precheck for loader $script"
source $script 
check_tiledb_ws $@
#[[ $? -eq 0 ]] && echo "$(hostname) : all good for loading"
#echo 
