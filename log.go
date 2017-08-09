package gomsg

import (
	"bytes"
	"fmt"
	"runtime"
	"time"
)

type Logger interface {
	Tracef(string, ...interface{})
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
}

type Log struct {
	Level     int
	Calldepth int
}

const (
	ALL int = iota
	TRACE
	DEBUG
	INFO
	WARN
	ERROR
	FATAL
	NONE
)

var logLevels = [...]string{"ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "NONE"}

func (this Log) Tracef(format string, what ...interface{}) {
	this.logf(TRACE, format, what...)
}

func (this Log) Debugf(format string, what ...interface{}) {
	this.logf(DEBUG, format, what...)
}

func (this Log) Infof(format string, what ...interface{}) {
	this.logf(INFO, format, what...)
}

func (this Log) Warnf(format string, what ...interface{}) {
	this.logf(WARN, format, what...)
}

func (this Log) Errorf(format string, what ...interface{}) {
	this.logf(ERROR, format, what...)
}

func (this Log) Fatalf(format string, what ...interface{}) {
	this.logf(FATAL, format, what...)
}

func (this Log) logf(level int, format string, a ...interface{}) {
	if level >= this.Level {
		var t = time.Now()

		// Caller
		_, file, line, ok := runtime.Caller(this.Calldepth + 3)
		if ok {
			short := file
			for i := len(file) - 1; i > 0; i-- {
				if file[i] == '/' {
					short = file[i+1:]
					break
				}
			}
			file = short
		} else {
			file = "???"
			line = 0
		}

		var form bytes.Buffer
		// time + level + caller + line
		var arr = []interface{}{t.Format("2006-01-02 15:04:05.999"), logLevels[level], file, line}
		form.WriteString("%s %5s [%s:%d] ")

		arr = append(arr, a...)
		form.WriteString(format)
		form.WriteString("\n")

		fmt.Printf(form.String(), arr...)
	}
}

var logger Logger = Log{Level: 2}

func SetLogger(lgr Logger) {
	logger = lgr
}
