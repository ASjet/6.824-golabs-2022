#!/bin/bash
rm -f mr-out* wc.so
go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrcoordinator.go pg-*.txt

if [[ $? == 0 ]]
then
  cat mr-out* | sort > mr-out-merge
  echo ======Check output======
  # Check output
  diff out mr-out-merge > /dev/null
  if [[ $? == 0 ]]
  then
    echo PASS
  else
    echo WRONG
  fi
  echo ========================
fi
