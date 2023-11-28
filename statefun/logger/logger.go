package logger

import (
	"fmt"
	"io"
	"runtime"

	logrus "github.com/sirupsen/logrus"
)

type LogLevel = logrus.Level
type LogEntry = logrus.Entry

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

func GetCustomLogEntry(pc uintptr, file string, line int, ok bool) *LogEntry {
	var le *logrus.Entry
	if reportCaller {
		le = logrus.WithField("caller", fmt.Sprintf("%s:%d", file, line))
	} else {
		le = logrus.NewEntry(logrus.StandardLogger())
	}
	return le
}

func LoglnEntry(ll LogLevel, le *LogEntry, args ...interface{}) {
	switch ll {
	case PanicLevel:
		le.Panicln(args...)
	case FatalLevel:
		le.Fatalln(args...)
	case ErrorLevel:
		le.Errorln(args...)
	case WarnLevel:
		le.Warnln(args...)
	case InfoLevel:
		le.Infoln(args...)
	case DebugLevel:
		le.Debugln(args...)
	case TraceLevel:
		le.Traceln(args...)
	}
}

func Logln(ll LogLevel, args ...interface{}) {
	le := GetCustomLogEntry(runtime.Caller(1))
	LoglnEntry(ll, le, args...)
}

func LogfEntry(ll LogLevel, le *LogEntry, format string, args ...interface{}) {
	switch ll {
	case PanicLevel:
		le.Panicf(format, args...)
	case FatalLevel:
		le.Fatalf(format, args...)
	case ErrorLevel:
		le.Errorf(format, args...)
	case WarnLevel:
		le.Warnf(format, args...)
	case InfoLevel:
		le.Infof(format, args...)
	case DebugLevel:
		le.Debugf(format, args...)
	case TraceLevel:
		le.Tracef(format, args...)
	}
}

func Logf(ll LogLevel, format string, args ...interface{}) {
	le := GetCustomLogEntry(runtime.Caller(1))
	LogfEntry(ll, le, format, args...)
}
