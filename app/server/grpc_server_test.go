package server

import "testing"

func TestNormalizeRoomID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "colon format", input: "dm:alice:bob", expected: "dm:alice:bob"},
		{name: "hyphen format", input: "dm-alice-bob", expected: "dm:alice:bob"},
		{name: "reversed hyphen format", input: "dm-bob-alice", expected: "dm:alice:bob"},
		{name: "reversed colon format", input: "dm:bob:alice", expected: "dm:alice:bob"},
		{name: "group room unchanged", input: "group:xyz", expected: "group:xyz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeRoomID(tt.input); got != tt.expected {
				t.Fatalf("normalizeRoomID(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
