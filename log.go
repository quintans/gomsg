package gomsg

import (
	"log"

	l "github.com/quintans/toolkit/log"
)

type Log struct{}

func (l Log) IsActive(level l.LogLevel) bool {
	return true
}

func (l Log) Tracef(format string, args ...interface{}) {
	log.Printf(format, args...)
}
func (l Log) Debugf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
func (l Log) Infof(format string, args ...interface{}) {
	log.Printf(format, args...)
}
func (l Log) Warnf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
func (l Log) Errorf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
func (l Log) Fatalf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
