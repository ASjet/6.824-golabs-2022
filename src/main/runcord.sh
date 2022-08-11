#!/bin/bash
rm -f mr-out* wc.so
go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrcoordinator.go pg-*.txt
