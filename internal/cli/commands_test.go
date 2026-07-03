package cli

import (
	"testing"
	"time"
)

func TestParseSince(t *testing.T) {
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)

	if got, err := parseSince("", now); err != nil || got != nil {
		t.Errorf("empty: %v, %v", got, err)
	}

	got, err := parseSince("24h", now)
	if err != nil || !got.Equal(now.Add(-24*time.Hour)) {
		t.Errorf("duration: %v, %v", got, err)
	}

	got, err = parseSince("2026-07-01", now)
	if err != nil || !got.Equal(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("date: %v, %v", got, err)
	}

	got, err = parseSince("2026-07-01T06:30:00Z", now)
	if err != nil || !got.Equal(time.Date(2026, 7, 1, 6, 30, 0, 0, time.UTC)) {
		t.Errorf("RFC3339: %v, %v", got, err)
	}

	if _, err := parseSince("-24h", now); err == nil {
		t.Error("negative duration must error")
	}
	if _, err := parseSince("yesterday", now); err == nil {
		t.Error("garbage must error")
	}
}
