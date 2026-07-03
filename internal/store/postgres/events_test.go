package postgres

import (
	"testing"
)

func strPtr(s string) *string { return &s }

func TestClassifyNewListing(t *testing.T) {
	events := classifyEvents(nil, strPtr("425000"), strPtr("Active"))
	if len(events) != 1 || events[0].typ != "new_listing" {
		t.Fatalf("events = %+v, want single new_listing", events)
	}
	if events[0].oldValue != nil || *events[0].newValue != "425000" {
		t.Errorf("new_listing values: old=%v new=%v", events[0].oldValue, events[0].newValue)
	}
}

func TestClassifyNoChange(t *testing.T) {
	prior := &priorListing{listPrice: strPtr("425000"), status: strPtr("Active")}
	if events := classifyEvents(prior, strPtr("425000"), strPtr("Active")); len(events) != 0 {
		t.Errorf("identical record must produce no events (ge-cursor re-processing must be silent), got %+v", events)
	}
}

func TestClassifyPriceChange(t *testing.T) {
	prior := &priorListing{listPrice: strPtr("425000"), status: strPtr("Active")}
	events := classifyEvents(prior, strPtr("399000"), strPtr("Active"))
	if len(events) != 1 || events[0].typ != "price_change" {
		t.Fatalf("events = %+v", events)
	}
	if *events[0].oldValue != "425000" || *events[0].newValue != "399000" {
		t.Errorf("price_change values: %+v", events[0])
	}
}

func TestClassifyPriceAppears(t *testing.T) {
	prior := &priorListing{status: strPtr("Active")}
	events := classifyEvents(prior, strPtr("399000"), strPtr("Active"))
	if len(events) != 1 || events[0].typ != "price_change" || events[0].oldValue != nil {
		t.Errorf("price appearing on a stored row: %+v", events)
	}
}

func TestClassifyStatusChange(t *testing.T) {
	prior := &priorListing{listPrice: strPtr("425000"), status: strPtr("Active")}
	events := classifyEvents(prior, strPtr("425000"), strPtr("Pending"))
	if len(events) != 1 || events[0].typ != "status_change" {
		t.Fatalf("events = %+v", events)
	}
}

func TestClassifyBackOnMarket(t *testing.T) {
	for _, from := range []string{"Pending", "Active Under Contract", "Cancelled", "Canceled", "Expired", "Withdrawn", "Hold"} {
		prior := &priorListing{status: strPtr(from)}
		events := classifyEvents(prior, nil, strPtr("Active"))
		if len(events) != 1 || events[0].typ != "back_on_market" {
			t.Errorf("%s -> Active: got %+v, want back_on_market", from, events)
		}
	}
	// Coming Soon -> Active is a normal progression, not back-on-market.
	prior := &priorListing{status: strPtr("Coming Soon")}
	events := classifyEvents(prior, nil, strPtr("Active"))
	if len(events) != 1 || events[0].typ != "status_change" {
		t.Errorf("Coming Soon -> Active: got %+v, want status_change", events)
	}
}

func TestClassifyPriceAndStatusTogether(t *testing.T) {
	prior := &priorListing{listPrice: strPtr("425000"), status: strPtr("Active")}
	events := classifyEvents(prior, strPtr("399000"), strPtr("Pending"))
	if len(events) != 2 {
		t.Fatalf("want both events, got %+v", events)
	}
}

func TestFeedPriceRendering(t *testing.T) {
	rec := mustRecord(t, `{"ListPrice": 425000}`)
	if p := feedPrice(rec); p == nil || *p != "425000" {
		t.Errorf("whole-dollar price: %v", p)
	}
	rec = mustRecord(t, `{"ListPrice": 449999.5}`)
	if p := feedPrice(rec); p == nil || *p != "449999.5" {
		t.Errorf("fractional price: %v", p)
	}
	rec = mustRecord(t, `{}`)
	if p := feedPrice(rec); p != nil {
		t.Errorf("absent price: %v", p)
	}
}
