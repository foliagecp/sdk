package logger

import (
	"bytes"
	"context"
	"encoding/json"
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

	if logger.level != InfoLevel {
		t.Errorf("Expected level %v, got %v", InfoLevel, logger.level)
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

		if logEntry["level"] != tc.level.String() {
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
