package raft

import (
	"log"
	"os"
)

// Debugging
const (
	DEBUG     = true
	LOG_LEVEL = 4
)

var debugLogger *log.Logger
var infoLogger *log.Logger
var warningLogger *log.Logger

func init() {
	odebug, err := os.Create("debug.log")
	if LOG_LEVEL < 1 {
		debugLogger = log.New(os.Stderr, "DEBUG ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	} else {
		if err != nil {
			log.Fatalf("init: %v", err)
		}
		debugLogger = log.New(odebug, "DEBUG ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	}
	if LOG_LEVEL < 2 {
		infoLogger = log.New(os.Stderr, "INFO  ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	} else {
		if err != nil {
			log.Fatalf("init: %v", err)
		}
		infoLogger = log.New(odebug, "INFO  ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	}
	if LOG_LEVEL < 3 {
		warningLogger = log.New(os.Stderr, "WARN  ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	} else {
		if err != nil {
			log.Fatalf("init: %v", err)
		}
		warningLogger = log.New(odebug, "WARN  ", log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
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
