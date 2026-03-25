package proxy

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestReadSSEStream_MergesMultilineData(t *testing.T) {
	input := strings.NewReader("data: {\"type\":\"response.output_text.delta\",\n" +
		"data: \"delta\":\"hello\"}\n\n" +
		"data: [DONE]\n\n")

	var events []string
	err := ReadSSEStream(input, func(data []byte) bool {
		events = append(events, string(data))
		return true
	})
	if err != nil {
		t.Fatalf("ReadSSEStream returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	want := "{\"type\":\"response.output_text.delta\",\n\"delta\":\"hello\"}"
	if events[0] != want {
		t.Fatalf("unexpected merged event: got %q want %q", events[0], want)
	}
}

func TestClassifyStreamOutcome(t *testing.T) {
	tests := []struct {
		name         string
		ctxErr       error
		readErr      error
		writeErr     error
		gotTerminal  bool
		wantStatus   int
		wantKind     string
		wantPenalize bool
	}{
		{
			name:        "terminal success",
			gotTerminal: true,
			wantStatus:  200,
		},
		{
			name:         "client canceled",
			ctxErr:       context.Canceled,
			wantStatus:   logStatusClientClosed,
			wantPenalize: false,
		},
		{
			name:         "upstream timeout",
			readErr:      errors.New("read timeout"),
			wantStatus:   logStatusUpstreamStreamBreak,
			wantKind:     "timeout",
			wantPenalize: true,
		},
		{
			name:         "upstream early eof",
			wantStatus:   logStatusUpstreamStreamBreak,
			wantKind:     "transport",
			wantPenalize: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outcome := classifyStreamOutcome(tc.ctxErr, tc.readErr, tc.writeErr, tc.gotTerminal)
			if outcome.logStatusCode != tc.wantStatus {
				t.Fatalf("status mismatch: got %d want %d", outcome.logStatusCode, tc.wantStatus)
			}
			if outcome.failureKind != tc.wantKind {
				t.Fatalf("failure kind mismatch: got %q want %q", outcome.failureKind, tc.wantKind)
			}
			if outcome.penalize != tc.wantPenalize {
				t.Fatalf("penalize mismatch: got %v want %v", outcome.penalize, tc.wantPenalize)
			}
		})
	}
}
