package opencode

import (
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
