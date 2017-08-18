package gomsg

import (
	"log"

	l "github.com/quintans/toolkit/log"
)

type Log struct{}

func (lg Log) IsActive(level l.LogLevel) bool {
	return true
}

func (lg Log) Tracef(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (lg Log) Debugf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (lg Log) Infof(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (lg Log) Warnf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (lg Log) Errorf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (lg Log) Fatalf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (lg Log) CallerAt(depth int) l.ILogger {
	return lg
}
