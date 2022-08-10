#!/bin/bash
seq 1 $1 | xargs -I{} -n 1 -P $1 go run -race mrworker.go wc.so

