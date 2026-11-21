package test

import (
	"testing"
	"time"
)

func bucketByKey(t *testing.T, r uiReply, key string) map[string]any {
	t.Helper()
	for _, b := range buckets(r) {
		if b["key"] == key {
			return b
		}
	}
	t.Fatalf("no bucket %s in %v", key, r.Body["buckets"])
	return nil
}

func atOf(t *testing.T, b map[string]any) *time.Time {
	t.Helper()
	if b["at"] == nil {
		return nil
	}
	s, _ := b["at"].(string)
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("bucket at %q is not RFC 3339: %v", s, err)
	}
	return &parsed
}

// Requirement: journal resolves operator-zone calendar reads.
func TestV0048JournalResolvesOperatorZoneCalendarReads(t *testing.T) {
	t.Run("Moscow day excludes neighboring UTC hours", func(t *testing.T) {
		h := newHarness(t)
		h.queuedAt(3, 1010, time.Date(2026, 10, 17, 21, 30, 0, 0, time.UTC))
		h.queuedAt(3, 1011, time.Date(2026, 10, 17, 20, 59, 0, 0, time.UTC))
		r := h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2026-W42", "time_zone": "Europe/Moscow"})
		if r.Body["time_zone"] != "Europe/Moscow" {
			t.Fatalf("time_zone %v, want Europe/Moscow", r.Body["time_zone"])
		}
		sunday := bucketByKey(t, r, "2026-10-18")
		if at := atOf(t, sunday); at == nil || !at.Equal(time.Date(2026, 10, 17, 21, 0, 0, 0, time.UTC)) {
			t.Fatalf("2026-10-18 at %v, want 2026-10-17T21:00:00Z", sunday["at"])
		}
		if num(sunday["total"]) != 1 || num(bucketByKey(t, r, "2026-10-17")["total"]) != 1 {
			t.Fatalf("Moscow membership wrong: Sat %v Sun %v", bucketByKey(t, r, "2026-10-17"), sunday)
		}
		utc := h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2026-W42"})
		if utc.Body["time_zone"] != "UTC" || num(bucketByKey(t, utc, "2026-10-17")["total"]) != 2 {
			t.Fatalf("omitted zone must keep UTC: %v", utc.Body["time_zone"])
		}
	})

	t.Run("DST, non-hour offsets and skipped dates", func(t *testing.T) {
		h := newHarness(t)
		// New York spring forward on 2025-03-09: a 23-hour day.
		h.queuedAt(3, 1, time.Date(2025, 3, 10, 3, 59, 0, 0, time.UTC)) // 23:59 EDT on 03-09
		spring := h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2025-W10", "time_zone": "America/New_York"})
		b := bucketByKey(t, spring, "2025-03-09")
		if at := atOf(t, b); at == nil || !at.Equal(time.Date(2025, 3, 9, 5, 0, 0, 0, time.UTC)) || num(b["total"]) != 1 {
			t.Fatalf("spring bucket %v, want at 05:00Z with the 23:59 EDT task", b)
		}
		// Autumn fall back on 2025-11-02: a 25-hour day keeps its last hour.
		h.queuedAt(3, 2, time.Date(2025, 11, 3, 4, 30, 0, 0, time.UTC)) // 23:30 EST on 11-02
		autumn := h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2025-W44", "time_zone": "America/New_York"})
		b = bucketByKey(t, autumn, "2025-11-02")
		if at := atOf(t, b); at == nil || !at.Equal(time.Date(2025, 11, 2, 4, 0, 0, 0, time.UTC)) || num(b["total"]) != 1 {
			t.Fatalf("autumn bucket %v, want at 04:00Z with the 23:30 EST task", b)
		}
		// Asia/Kolkata is UTC+05:30.
		h.queuedAt(3, 3, time.Date(2026, 10, 12, 18, 29, 0, 0, time.UTC))
		h.queuedAt(3, 4, time.Date(2026, 10, 12, 18, 30, 0, 0, time.UTC))
		kolkata := h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2026-W42", "time_zone": "Asia/Kolkata"})
		mon, tue := bucketByKey(t, kolkata, "2026-10-12"), bucketByKey(t, kolkata, "2026-10-13")
		if num(mon["total"]) != 1 || num(tue["total"]) != 1 {
			t.Fatalf("Kolkata half-hour boundary: Mon %v Tue %v", mon, tue)
		}
		if at := atOf(t, tue); at == nil || !at.Equal(time.Date(2026, 10, 12, 18, 30, 0, 0, time.UTC)) {
			t.Fatalf("Kolkata 2026-10-13 at %v, want 2026-10-12T18:30:00Z", tue["at"])
		}
		// Pacific/Apia skipped 2011-12-30.
		h.queuedAt(3, 5, time.Date(2011, 12, 31, 10, 0, 0, 0, time.UTC)) // 2012-01-01 00:00 local
		apia := h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2011-W52", "time_zone": "Pacific/Apia"})
		if n := len(buckets(apia)); n != 7 {
			t.Fatalf("Apia week buckets = %d, want 7", n)
		}
		skipped := bucketByKey(t, apia, "2011-12-30")
		if skipped["at"] != nil || num(skipped["total"]) != 0 {
			t.Fatalf("skipped Apia date %v, want a zero bucket with null at", skipped)
		}
		if num(bucketByKey(t, apia, "2012-01-01")["total"]) != 1 {
			t.Fatalf("Apia task not in its local date 2012-01-01")
		}
	})

	t.Run("Invalid time zone", func(t *testing.T) {
		h := newHarness(t)
		h.queuedAt(3, 1010, time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC))
		h.store.FailReads = true // any data read would answer 503
		for _, zone := range []any{"+03:00", "", "Local", "../../etc/passwd", "Mars/Olympus", []any{"UTC", "UTC"}} {
			r := h.ui("dashboard", 3, map[string]any{"period": "week", "time_zone": zone})
			if r.Status != 400 || r.code() != "invalid_time_zone" {
				t.Fatalf("time_zone %v: %d %s, want 400 invalid_time_zone before any read", zone, r.Status, r.Raw)
			}
		}
	})
}
