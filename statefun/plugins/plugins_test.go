package plugins

// GetForID must degrade to a nil executor — never panic on the bare interface
// type assertion — for the two states a live system actually produces: an id
// that is absent from the map (never added, or already collected by the
// function-type gc between AddForID and a later lookup), and the nil
// placeholder AddForID stores when no constructor function is configured.

import "testing"

func Test_GetForID_MissingAndNilConstructorDoNotPanic(t *testing.T) {
	tnex := NewTypenameExecutor("alias", "source", nil)

	if got := tnex.GetForID("never-added"); got != nil {
		t.Fatalf("missing id must yield a nil executor, got %v", got)
	}

	tnex.AddForID("present-but-nil") // no constructor -> nil placeholder stored
	if got := tnex.GetForID("present-but-nil"); got != nil {
		t.Fatalf("nil-constructor placeholder must yield a nil executor, got %v", got)
	}

	tnex.RemoveForID("present-but-nil")
	if got := tnex.GetForID("present-but-nil"); got != nil {
		t.Fatalf("removed id must yield a nil executor, got %v", got)
	}
}
