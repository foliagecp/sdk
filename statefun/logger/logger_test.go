package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

func TestNewLogger(t *testing.T) {
	logger := NewLogger(Options{
		Level:        InfoLevel,
		ReportCaller: true,
		JSONFormat:   true,
	})

	if logger == nil {
		t.Fatal("NewLogger returned nil")
	}

	if logger.levelVar.Level() != InfoLevel {
		t.Errorf("Expected level %v, got %v", InfoLevel, logger.levelVar.Level())
	}

	if !logger.reportCaller {
		t.Error("Expected reportCaller to be true")
	}
}

func TestLogLevels(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(Options{
		Output:     buf,
		Level:      TraceLevel,
		JSONFormat: true,
	})

	ctx := context.Background()

	testCases := []struct {
		level   LogLevel
		logFunc func(context.Context, string, ...interface{})
	}{
		{TraceLevel, logger.Trace},
		{DebugLevel, logger.Debug},
		{InfoLevel, logger.Info},
		{WarnLevel, logger.Warn},
		{ErrorLevel, logger.Error},
	}

	for _, tc := range testCases {
		buf.Reset()
		tc.logFunc(ctx, "test message", "key", "value")

		var logEntry map[string]interface{}
		if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
			t.Fatalf("Failed to unmarshal log entry: %v", err)
		}

		if logEntry["level"] != levelToString(tc.level) {
			t.Errorf("Expected level %s, got %s", tc.level, logEntry["level"])
		}

		if logEntry["msg"] != "test message" {
			t.Errorf("Expected message 'test message', got %s", logEntry["msg"])
		}

		if logEntry["key"] != "value" {
			t.Errorf("Expected key 'value', got %s", logEntry["key"])
		}
	}
}

func TestWith(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(Options{
		Output:     buf,
		Level:      InfoLevel,
		JSONFormat: true,
	})

	childLogger := logger.With(map[string]interface{}{"component": "test"})

	ctx := context.Background()
	childLogger.Info(ctx, "test message")

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to unmarshal log entry: %v", err)
	}

	if logEntry["component"] != "test" {
		t.Errorf("Expected component 'test', got %s", logEntry["component"])
	}
}

func TestReportCaller(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(Options{
		Output:       buf,
		Level:        InfoLevel,
		ReportCaller: true,
		JSONFormat:   true,
	})

	ctx := context.Background()
	logger.Info(ctx, "test message")

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to unmarshal log entry: %v", err)
	}

	if _, ok := logEntry["caller"]; !ok {
		t.Error("Expected caller information, but it was not present")
	}
}

// TestLevelIdentity to avoid logging level changing by mistakes
func TestLevelIdentity(t *testing.T) {
	if LogLevel(-8) != TraceLevel {
		t.Errorf("Logging levels aren't equal")
	}
	if LogLevel(-4) != DebugLevel {
		t.Errorf("Logging levels aren't equal")
	}
	if LogLevel(0) != InfoLevel {
		t.Errorf("Logging levels aren't equal")
	}
	if LogLevel(4) != WarnLevel {
		t.Errorf("Logging levels aren't equal")
	}
	if LogLevel(8) != ErrorLevel {
		t.Errorf("Logging levels aren't equal")
	}
	if LogLevel(12) != FatalLevel {
		t.Errorf("Logging levels aren't equal")
	}
	if LogLevel(16) != PanicLevel {
		t.Errorf("Logging levels aren't equal")
	}
}

func TestSetLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(Options{
		Output:     buf,
		Level:      InfoLevel,
		JSONFormat: true,
	})

	ctx := context.Background()

	logger.Debug(ctx, "debug message")
	if buf.Len() > 0 {
		t.Error("Debug message was logged with InfoLevel")
	}

	logger.SetLevel(DebugLevel)

	buf.Reset()
	logger.Debug(ctx, "debug message")
	if buf.Len() == 0 {
		t.Error("Debug message was not logged after SetLevel to DebugLevel")
	}
}

func TestSetOptions(t *testing.T) {
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	logger := NewLogger(Options{
		Output:       buf1,
		Level:        InfoLevel,
		ReportCaller: false,
		JSONFormat:   false,
	})

	ctx := context.Background()
	logger.Info(ctx, "test message")

	if buf1.Len() == 0 {
		t.Error("Message was not logged to initial buffer")
	}

	logger.SetOptions(buf2, DebugLevel, true, true)

	buf1.Reset()
	buf2.Reset()

	logger.Debug(ctx, "debug message")

	if buf1.Len() > 0 {
		t.Error("Message was logged to old buffer after SetOptions")
	}
	if buf2.Len() == 0 {
		t.Error("Message was not logged to new buffer after SetOptions")
	}

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf2.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to unmarshal JSON log entry: %v", err)
	}
}

func TestGetLoggerSingleton(t *testing.T) {
	logger1 := GetLogger()
	logger2 := GetLogger()
	logger3 := GetLogger()

	if logger1 != logger2 || logger2 != logger3 {
		t.Error("GetLogger should return the same instance")
	}
}

func TestConcurrentSetLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(Options{
		Output:     buf,
		Level:      InfoLevel,
		JSONFormat: true,
	})

	const numGoroutines = 50
	levels := []LogLevel{TraceLevel, DebugLevel, InfoLevel, WarnLevel, ErrorLevel}

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			level := levels[i%len(levels)]
			logger.SetLevel(level)
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			logger.Info(context.Background(), fmt.Sprintf("message %d", i))
		}(i)
	}

	wg.Wait()

	if t.Failed() {
		t.Error("Race condition detected in SetLevel")
	}
}

func TestConcurrentSetOptions(t *testing.T) {
	logger := NewLogger(Options{
		Level:      InfoLevel,
		JSONFormat: true,
	})

	const numGoroutines = 100
	var wg sync.WaitGroup

	buffers := make([]*bytes.Buffer, numGoroutines)
	for i := range buffers {
		buffers[i] = &bytes.Buffer{}
	}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			logger.SetOptions(
				buffers[i%len(buffers)],
				LogLevel(i%5*4),
				i%2 == 0,
				i%2 == 1,
			)
		}(i)
	}

	for i := 0; i < numGoroutines*2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			logger.Info(context.Background(), fmt.Sprintf("concurrent message %d", i))
		}(i)
	}

	wg.Wait()

	if t.Failed() {
		t.Error("Race condition detected in SetOptions")
	}
}
