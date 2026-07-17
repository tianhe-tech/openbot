package opencode

import (
	"errors"
	"testing"

	sdk "github.com/sst/opencode-sdk-go"
)

func TestExtractTextFromAssistantMessageJSONDeduplicatesParts(t *testing.T) {
	raw := []byte(`{
		"parts": [
			{"text": "Name\nIntel"},
			{"content": "Name\nIntel"}
		]
	}`)

	got := extractTextFromAssistantMessageJSON(raw)
	want := "Name\nIntel"
	if got != want {
		t.Fatalf("extractTextFromAssistantMessageJSON() = %q, want %q", got, want)
	}
}

func TestExtractTextFromSessionPartsDeduplicatesSameText(t *testing.T) {
	parts := []sdk.Part{
		{Type: sdk.PartTypeText, Text: "Name\nIntel"},
		{Type: sdk.PartTypeText, Text: "Name\nIntel"},
	}

	got := extractTextFromSessionParts(parts)
	want := "Name\nIntel"
	if got != want {
		t.Fatalf("extractTextFromSessionParts() = %q, want %q", got, want)
	}
}

func TestIsSessionBusyError(t *testing.T) {
	busy := errors.New(`POST "http://127.0.0.1:42671/session/ses_09b6/shell?directory=%2Froot%2Fopenbot": 409 Conflict {"_tag":"SessionBusyError","sessionID":"ses_09b6","message":"Session is busy: ses_09b6"}`)
	if !isSessionBusyError(busy) {
		t.Fatal("expected 409 SessionBusyError to be detected as busy")
	}
	if isSessionBusyError(nil) {
		t.Fatal("nil error must not be busy")
	}
	if isSessionBusyError(errors.New("some other 500 error")) {
		t.Fatal("unrelated error must not be busy")
	}
}
