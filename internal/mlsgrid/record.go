// Package mlsgrid implements a client for the MLS Grid API (RESO Web API /
// OData v4): request/paging mechanics, quirk-tolerant record decoding, and
// retry orchestration against the rate limiter.
//
// Records are kept as raw JSON rather than decoded into a fixed struct: the
// feed's field set varies by MLS and by record, and the storage layer maps
// fields to columns through a table-driven map. The accessors on Record absorb
// the feed's known type quirks, each of which has a fixture in testdata/odata.
package mlsgrid

import (
	"encoding/json"
	"time"
)

// Record is one feed record (a Property, OpenHouse, or an expanded child),
// preserving the original JSON for lossless storage.
type Record struct {
	raw    json.RawMessage
	fields map[string]json.RawMessage
}

// ParseRecord decodes one JSON object from a feed page.
func ParseRecord(raw json.RawMessage) (Record, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Record{}, err
	}
	return Record{raw: raw, fields: fields}, nil
}

// Raw returns the record exactly as received, for JSONB storage.
func (r Record) Raw() json.RawMessage { return r.raw }

// Has reports whether the field was present in the record. Absence is normal:
// each record carries only the fields its source MLS populated.
func (r Record) Has(key string) bool {
	v, ok := r.fields[key]
	return ok && string(v) != "null"
}

// String returns the field as a string, or "" when absent or not a string.
func (r Record) String(key string) string {
	var s string
	if v, ok := r.fields[key]; ok {
		_ = json.Unmarshal(v, &s)
	}
	return s
}

// Int returns an integer field. The feed sometimes serializes integers as
// JSON floats (e.g. 12.0), which are accepted and truncated.
func (r Record) Int(key string) (int, bool) {
	v, ok := r.fields[key]
	if !ok {
		return 0, false
	}
	var i int
	if err := json.Unmarshal(v, &i); err == nil {
		return i, true
	}
	var f float64
	if err := json.Unmarshal(v, &f); err == nil {
		return int(f), true
	}
	return 0, false
}

// Float returns a numeric field.
func (r Record) Float(key string) (float64, bool) {
	v, ok := r.fields[key]
	if !ok {
		return 0, false
	}
	var f float64
	if err := json.Unmarshal(v, &f); err != nil {
		return 0, false
	}
	return f, true
}

// Bool returns a boolean field.
func (r Record) Bool(key string) (bool, bool) {
	v, ok := r.fields[key]
	if !ok {
		return false, false
	}
	var b bool
	if err := json.Unmarshal(v, &b); err != nil {
		return false, false
	}
	return b, true
}

// timeLayouts: Data Dictionary timestamps are RFC3339 UTC, but some arrive
// timezone-naive; naive values are interpreted as UTC.
var timeLayouts = []string{time.RFC3339, "2006-01-02T15:04:05"}

// Time returns a timestamp field, always in UTC.
func (r Record) Time(key string) (time.Time, bool) {
	s := r.String(key)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// dateLayouts: some date fields (e.g. CloseDate on MRED) arrive as free text.
var dateLayouts = []string{"2006-01-02", time.RFC3339, "2006-01-02T15:04:05", "01/02/2006"}

// Date returns a date field, tolerating the feed's free-text date formats.
// Unparseable values return false; the original text stays in Raw.
func (r Record) Date(key string) (time.Time, bool) {
	s := r.String(key)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range dateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// StringList returns a multi-value field. Some fields (Contingency,
// Possession) arrive as either a single string or an array of strings.
func (r Record) StringList(key string) []string {
	v, ok := r.fields[key]
	if !ok {
		return nil
	}
	var arr []string
	if err := json.Unmarshal(v, &arr); err == nil {
		return arr
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil && s != "" {
		return []string{s}
	}
	return nil
}

// Children returns an expanded child collection (Media, Rooms, UnitTypes).
func (r Record) Children(key string) []Record {
	v, ok := r.fields[key]
	if !ok {
		return nil
	}
	var raws []json.RawMessage
	if err := json.Unmarshal(v, &raws); err != nil {
		return nil
	}
	out := make([]Record, 0, len(raws))
	for _, raw := range raws {
		child, err := ParseRecord(raw)
		if err != nil {
			continue
		}
		out = append(out, child)
	}
	return out
}

// ListingKey returns the cross-MLS unique key (primary key upstream).
func (r Record) ListingKey() string { return r.String("ListingKey") }

// ModificationTimestamp returns the Grid-set modification time that drives
// replication cursors.
func (r Record) ModificationTimestamp() (time.Time, bool) {
	return r.Time("ModificationTimestamp")
}

// CanView reports MlgCanView. A missing field counts as viewable: the feed
// only marks records false to revoke them, and treating absence as false
// would delete the entire local dataset.
func (r Record) CanView() bool {
	v, ok := r.Bool("MlgCanView")
	return !ok || v
}
