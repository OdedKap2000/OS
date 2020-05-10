#!/bin/bash

function read_yn() {
  while true; do
    read -p "$1" yn
    case $yn in
        [Yy]* ) return 0; exit;;
        [Nn]* ) return 255; exit;;
        * ) echo "Please answer yes or no.";;
    esac
  done
}

TEST_DIR="${0%/*}"

TESTS=`ls ${TEST_DIR} | grep .cpp`
FAIL=0
SUCCESS=0
for TEST_FILENAME in $TESTS
do
  TEST_FILENAME_LEN=$(expr length "${TEST_FILENAME}")
  TEST_NAME="${TEST_FILENAME%.*}"
  v=$(printf "%-$((TEST_FILENAME_LEN + 4))s")
  echo "~ ${TEST_FILENAME} ~"
  echo "${v// /*}"

  (g++ -std=c++11 -Wall -o ${TEST_DIR}/${TEST_NAME} ${TEST_DIR}/../*.cpp ${TEST_DIR}/${TEST_FILENAME})
  if [ $? -eq 0 ]
  then
    (${TEST_DIR}/${TEST_NAME})
  else
    echo "g++ ${TEST_FILENAME} faild"
  fi
  read_yn "continue to next test? [Y/n]: "
  if [ $? -eq 255 ]
  then
    exit
  fi
  echo ""
done
