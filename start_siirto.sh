#!/bin/bash
clear
EXITSTATUS=0

command="siirto"
eval $command

if [ $? != 0 ]; then
  EXITSTATUS=1
fi
exit $EXITSTATUS
