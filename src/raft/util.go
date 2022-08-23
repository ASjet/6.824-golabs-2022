package raft

import (
	"log"
	"os"
)

// Debugging
const (
	DEBUG     = true
	LOG_LEVEL = 1
)

var debugLogger *log.Logger
var infoLogger *log.Logger
var warningLogger *log.Logger

func init() {
	if LOG_LEVEL < 1 {
		debugLogger = log.New(os.Stdout, "DEBUG ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	} else {
		odebug, err := os.Create("debug.log")
		if err != nil {
			log.Fatalf("init: %v", err)
		}
		debugLogger = log.New(odebug, "DEBUG ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	}
	if LOG_LEVEL < 2 {
		infoLogger = log.New(os.Stdout, "INFO  ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	} else {
		oinfo, err := os.Create("info.log")
		if err != nil {
			log.Fatalf("init: %v", err)
		}
		infoLogger = log.New(oinfo, "INFO  ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	}
	if LOG_LEVEL < 3 {
		warningLogger = log.New(os.Stderr, "WARN  ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	} else {
		owarn, err := os.Create("warning.log")
		if err != nil {
			log.Fatalf("init: %v", err)
		}
		warningLogger = log.New(owarn, "WARN  ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	}
}

func debug(fmts string, args ...any) {
	if DEBUG {
		debugLogger.Printf(fmts, args...)
	}
}

func info(fmts string, args ...any) {
	if DEBUG {
		infoLogger.Printf(fmts, args...)
	}
}

func warning(fmts string, args ...any) {
	if DEBUG {
		warningLogger.Printf(fmts, args...)
	}
}
