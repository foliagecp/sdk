package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sync"
)

// TODO: Test for quotes in docker logs

// LogLevel represents the severity of a log message
type LogLevel = slog.Level

const (
	// skipStackFrames number of goroutine's stack frames to ascend.
	// We are never calling func log() directly, so 2
	skipStackFrames = 2

	// Not all Logging levels exist in slog, so we repeat some existing ones and add our own.
	// These are: DebugLevel, InfoLevel, WarnLevel, ErrorLevel.

	TraceLevel = LogLevel(-8)
	DebugLevel = LogLevel(-4)
	InfoLevel  = LogLevel(0)
	WarnLevel  = LogLevel(4)
	ErrorLevel = LogLevel(8)
	FatalLevel = LogLevel(12)
	PanicLevel = LogLevel(16)
)

var (
	//
	defaultOptions = Options{
		Output:       os.Stdout,
		Level:        InfoLevel,
		ReportCaller: true,
	}
	optionsMu sync.Mutex

	defaultOutput = os.Stdout

	globalLogger *Logger
	globalMu     sync.RWMutex
	initialized  bool
)

// Logger wraps slog.Logger with additional functionality
type Logger struct {
	slogger      *slog.Logger
	levelVar     *slog.LevelVar
	reportCaller bool
	fields       map[string]interface{}
	mu           sync.RWMutex
}

// Options for configuring the logger
type Options struct {
	Output        io.Writer
	Level         LogLevel
	ReportCaller  bool
	JSONFormat    bool
	InitialFields map[string]interface{}
}

// SetDefaultOptions sets global logger's options
//
// Deprecated.
func SetDefaultOptions(
	output io.Writer,
	level LogLevel,
	reportCaller bool,
) {
	optionsMu.Lock()
	defaultOptions = Options{
		Output:       output,
		Level:        level,
		ReportCaller: reportCaller,
	}
	optionsMu.Unlock()
}

// GetLogger returns the global logger instance
func GetLogger() *Logger {
	globalMu.RLock()
	if initialized {
		defer globalMu.RUnlock()
		return globalLogger
	}
	globalMu.RUnlock()

	globalMu.Lock()
	defer globalMu.Unlock()

	if !initialized {
		globalLogger = NewLogger(defaultOptions)
		initialized = true
	}

	return globalLogger
}

// NewLogger creates a new Logger instance
func NewLogger(opts Options) *Logger {
	if opts.Output == nil {
		opts.Output = defaultOutput
	}

	levelVar := &slog.LevelVar{}
	levelVar.Set(opts.Level)

	var handler slog.Handler
	if opts.JSONFormat {
		handler = slog.NewJSONHandler(opts.Output, &slog.HandlerOptions{Level: levelVar})
	} else {
		handler = slog.NewTextHandler(opts.Output, &slog.HandlerOptions{Level: levelVar})
	}

	return &Logger{
		slogger:      slog.New(handler),
		levelVar:     levelVar,
		reportCaller: opts.ReportCaller,
		fields:       opts.InitialFields,
	}
}

// SetOptions sets new options for Logger
func (l *Logger) SetOptions(
	output io.Writer,
	level LogLevel,
	reportCaller bool,
	jsonFormat bool,
) {
	optionsMu.Lock()
	defer optionsMu.Unlock()

	l.levelVar.Set(level)
	l.reportCaller = reportCaller

	handlerOpts := &slog.HandlerOptions{
		AddSource: reportCaller,
		Level:     l.levelVar,
	}

	var handler slog.Handler
	if jsonFormat {
		handler = slog.NewJSONHandler(output, handlerOpts)
	} else {
		handler = slog.NewTextHandler(output, handlerOpts)
	}

	l.slogger = slog.New(handler)
}

func (l *Logger) SetLevel(level LogLevel) {
	l.levelVar.Set(level)
}

// With returns a new Logger with the given fields added to its context
// Allows for `nested` logger creation
func (l *Logger) With(fields map[string]interface{}) *Logger {
	newLogger := &Logger{
		slogger:      l.slogger,
		levelVar:     l.levelVar,
		reportCaller: l.reportCaller,
		fields:       make(map[string]interface{}),
	}

	l.mu.RLock()
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	l.mu.RUnlock()

	for k, v := range fields {
		newLogger.fields[k] = v
	}

	return newLogger
}

func (l *Logger) logf(ctx context.Context, level LogLevel, msg string, args ...interface{}) {
	l.log(ctx, level, fmt.Sprintf(msg, args...))
}

// log logs a message at the specified level
func (l *Logger) log(ctx context.Context, level LogLevel, msg string, args ...interface{}) {
	if level < l.levelVar.Level() {
		return
	}

	attrs := make([]slog.Attr, 0, len(l.fields)+len(args)/2+1) // +1 for potential caller

	l.mu.RLock()
	for k, v := range l.fields {
		attrs = append(attrs, slog.Any(k, v))
	}
	l.mu.RUnlock()

	// Add args to attrs
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			attrs = append(attrs, slog.Any(fmt.Sprint(args[i]), args[i+1]))
		}
	}

	if l.reportCaller {
		_, file, line, ok := runtime.Caller(skipStackFrames)
		if ok {
			attrs = append(attrs, slog.String("caller", fmt.Sprintf("%s:%d", file, line)))
		}
	}

	l.slogger.LogAttrs(ctx, level, msg, attrs...)

	if level == PanicLevel {
		panic(msg)
	} else if level == FatalLevel {
		os.Exit(1)
	}
}

// Helper methods for each log level

func (l *Logger) Trace(ctx context.Context, msg string, args ...interface{}) {
	l.log(ctx, TraceLevel, msg, args...)
}

func (l *Logger) Debug(ctx context.Context, msg string, args ...interface{}) {
	l.log(ctx, DebugLevel, msg, args...)
}

func (l *Logger) Info(ctx context.Context, msg string, args ...interface{}) {
	l.log(ctx, InfoLevel, msg, args...)
}

func (l *Logger) Warn(ctx context.Context, msg string, args ...interface{}) {
	l.log(ctx, WarnLevel, msg, args...)
}

func (l *Logger) Error(ctx context.Context, msg string, args ...interface{}) {
	l.log(ctx, ErrorLevel, msg, args...)
}

func (l *Logger) Fatal(ctx context.Context, msg string, args ...interface{}) {
	l.log(ctx, FatalLevel, msg, args...)
}

func (l *Logger) Panic(ctx context.Context, msg string, args ...interface{}) {
	l.log(ctx, PanicLevel, msg, args...)
}

func (l *Logger) Tracef(ctx context.Context, msg string, args ...interface{}) {
	l.logf(ctx, TraceLevel, msg, args...)
}

func (l *Logger) Debugf(ctx context.Context, msg string, args ...interface{}) {
	l.logf(ctx, DebugLevel, msg, args...)
}

func (l *Logger) Infof(ctx context.Context, msg string, args ...interface{}) {
	l.logf(ctx, InfoLevel, msg, args...)
}

func (l *Logger) Warnf(ctx context.Context, msg string, args ...interface{}) {
	l.logf(ctx, WarnLevel, msg, args...)
}

func (l *Logger) Errorf(ctx context.Context, msg string, args ...interface{}) {
	l.logf(ctx, ErrorLevel, msg, args...)
}

func (l *Logger) Fatalf(ctx context.Context, msg string, args ...interface{}) {
	l.logf(ctx, FatalLevel, msg, args...)
}

func (l *Logger) Panicf(ctx context.Context, msg string, args ...interface{}) {
	l.logf(ctx, PanicLevel, msg, args...)
}

// Logf formats log messages, uses global logger
//
// Deprecated. Use l.Trace(), l.Debug(), l.Info(), l.Warn(), l.Error(), l.Fatal(), l.Panic() instead
func Logf(ll LogLevel, format string, args ...interface{}) {
	l := GetLogger()
	switch ll {
	case PanicLevel:
		l.Panicf(context.TODO(), format, args...)
	case FatalLevel:
		l.Fatalf(context.TODO(), format, args...)
	case ErrorLevel:
		l.Errorf(context.TODO(), format, args...)
	case WarnLevel:
		l.Warnf(context.TODO(), format, args...)
	case InfoLevel:
		l.Infof(context.TODO(), format, args...)
	case DebugLevel:
		l.Debugf(context.TODO(), format, args...)
	case TraceLevel:
		l.Tracef(context.TODO(), format, args...)
	}
}

// Logln formats log messages and adds a newline separator.
//
// Deprecated. Use l.Trace(), l.Debug(), l.Info(), l.Warn(), l.Error(), l.Fatal(), l.Panic() instead
func Logln(ll LogLevel, format string, args ...interface{}) {
	Logf(ll, format, args...)
}
