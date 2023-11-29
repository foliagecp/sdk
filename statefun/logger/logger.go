package logger

import (
	"fmt"
	"io"
	"runtime"

	logrus "github.com/sirupsen/logrus"
)

type LogLevel = logrus.Level

const (
	PanicLevel LogLevel = iota
	// FatalLevel level. Logs and then calls `logger.Exit(1)`. It will exit even if the
	// logging level is set to Panic.
	FatalLevel
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel
	// TraceLevel level. Designates finer-grained informational events than the Debug.
	TraceLevel
)

var (
	reportCaller bool = false
)

func SetOutput(out io.Writer) {
	logrus.SetOutput(out)
}

func SetOutputLevel(ll LogLevel) {
	logrus.SetLevel(ll)
}

func SetReportCaller(include bool) {
	//logrus.SetReportCaller(include)
	reportCaller = include
}

func GetCustomLogEntry(pc uintptr, file string, line int, ok bool) LogEntry {
	var le *logrus.Entry
	if reportCaller {
		le = logrus.WithField("caller", fmt.Sprintf("%s:%d", file, line))
	} else {
		le = logrus.NewEntry(logrus.StandardLogger())
	}
	return LogEntry{le}
}

type LogEntry struct {
	logrusLogEntry *logrus.Entry
}

func (le *LogEntry) Logln(ll LogLevel, args ...interface{}) {
	switch ll {
	case PanicLevel:
		le.logrusLogEntry.Panicln(args...)
	case FatalLevel:
		le.logrusLogEntry.Fatalln(args...)
	case ErrorLevel:
		le.logrusLogEntry.Errorln(args...)
	case WarnLevel:
		le.logrusLogEntry.Warnln(args...)
	case InfoLevel:
		le.logrusLogEntry.Infoln(args...)
	case DebugLevel:
		le.logrusLogEntry.Debugln(args...)
	case TraceLevel:
		le.logrusLogEntry.Traceln(args...)
	}
}

func Logln(ll LogLevel, args ...interface{}) {
	le := GetCustomLogEntry(runtime.Caller(1))
	le.Logln(ll, args...)
}

func (le *LogEntry) Logf(ll LogLevel, format string, args ...interface{}) {
	switch ll {
	case PanicLevel:
		le.logrusLogEntry.Panicf(format, args...)
	case FatalLevel:
		le.logrusLogEntry.Fatalf(format, args...)
	case ErrorLevel:
		le.logrusLogEntry.Errorf(format, args...)
	case WarnLevel:
		le.logrusLogEntry.Warnf(format, args...)
	case InfoLevel:
		le.logrusLogEntry.Infof(format, args...)
	case DebugLevel:
		le.logrusLogEntry.Debugf(format, args...)
	case TraceLevel:
		le.logrusLogEntry.Tracef(format, args...)
	}
}

func Logf(ll LogLevel, format string, args ...interface{}) {
	le := GetCustomLogEntry(runtime.Caller(1))
	le.Logf(ll, format, args...)
}
