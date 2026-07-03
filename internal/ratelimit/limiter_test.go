package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/testclock"
)

// mid-hour start so window-boundary math is visible.
var t0 = time.Date(2026, 6, 1, 12, 30, 0, 0, time.UTC)

func TestHourlyCapSleepsToWallClockBoundary(t *testing.T) {
	clock := testclock.At(t0)
	l := New(Config{Hourly: 2}, clock)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := l.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Third request: the hour budget is spent at 12:30; the limiter must
	// sleep exactly to 13:00 UTC — not one hour from process start.
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	slept := clock.Slept()
	if len(slept) == 0 || slept[0] != 30*time.Minute {
		t.Errorf("slept %v, want first sleep of 30m (to the 13:00 UTC boundary)", slept)
	}
}

func TestDailyCapSleepsToUTCMidnight(t *testing.T) {
	clock := testclock.At(t0)
	l := New(Config{Daily: 1}, clock)
	ctx := context.Background()

	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	slept := clock.Slept()
	want := 11*time.Hour + 30*time.Minute // 12:30 → 00:00 UTC
	if len(slept) == 0 || slept[0] != want {
		t.Errorf("slept %v, want first sleep of %v (to UTC midnight)", slept, want)
	}
}

func TestByteBudgetBlocksUntilNextHour(t *testing.T) {
	clock := testclock.At(t0)
	l := New(Config{BytesHourly: 1000}, clock)
	ctx := context.Background()

	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	l.RecordResponse(1500, 200) // blow the hourly download budget
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	slept := clock.Slept()
	if len(slept) == 0 || slept[0] != 30*time.Minute {
		t.Errorf("slept %v, want 30m to the next hour window", slept)
	}
	// New window: budget is fresh, no further sleeping.
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(clock.Slept()) != len(slept) {
		t.Error("fresh window should not sleep")
	}
}

func TestWindowRollsResetCounters(t *testing.T) {
	clock := testclock.At(t0)
	l := New(Config{Hourly: 1}, clock)
	ctx := context.Background()

	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour) // cross the boundary externally
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(clock.Slept()) != 0 {
		t.Errorf("after window roll no sleep needed, slept %v", clock.Slept())
	}
}

func TestRPSReservationDelays(t *testing.T) {
	clock := testclock.At(t0)
	l := New(Config{RPS: 2, Burst: 1}, clock)
	ctx := context.Background()

	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	slept := clock.Slept()
	if len(slept) != 1 || slept[0] != 500*time.Millisecond {
		t.Errorf("2 rps burst 1: second request should wait 500ms, slept %v", slept)
	}
}

func TestCircuitBreaker(t *testing.T) {
	clock := testclock.At(t0)
	l := New(Config{CircuitThreshold: 3}, clock)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		l.RecordResponse(0, 429)
	}
	if l.CircuitOpen() {
		t.Fatal("2 consecutive 429s must not trip a threshold of 3")
	}
	l.RecordResponse(0, 200) // success resets the streak
	l.RecordResponse(0, 429)
	l.RecordResponse(0, 429)
	if l.CircuitOpen() {
		t.Fatal("streak was reset by the 2xx")
	}
	l.RecordResponse(0, 429)
	if !l.CircuitOpen() {
		t.Fatal("3 consecutive 429s must open the circuit")
	}
	if err := l.Wait(ctx); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Wait with open circuit = %v, want ErrCircuitOpen", err)
	}
}

func TestSnapshotRestoreSameWindow(t *testing.T) {
	clock := testclock.At(t0)
	l := New(Config{Hourly: 10}, clock)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := l.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	l.RecordResponse(1234, 200)
	snap := l.Snapshot()
	if snap.HourRequests != 4 || snap.HourBytes != 1234 {
		t.Fatalf("snapshot = %+v", snap)
	}

	// Simulated crash-restart within the same hour: budget must survive.
	l2 := New(Config{Hourly: 5}, clock)
	l2.Restore(snap)
	if got := l2.Snapshot(); got.HourRequests != 4 {
		t.Errorf("restored requests = %d, want 4 — restarts must not launder the budget", got.HourRequests)
	}
	if err := l2.Wait(ctx); err != nil { // 5th of 5 allowed
		t.Fatal(err)
	}
	if err := l2.Wait(ctx); err != nil { // 6th must sleep to boundary
		t.Fatal(err)
	}
	if len(clock.Slept()) == 0 {
		t.Error("6th request in a 5-cap hour should have slept to the boundary")
	}
}

func TestRestoreIgnoresExpiredWindow(t *testing.T) {
	clock := testclock.At(t0)
	stale := Usage{
		HourStart:    t0.Truncate(time.Hour).Add(-2 * time.Hour),
		HourRequests: 9999,
		DayStart:     t0.Truncate(24 * time.Hour).Add(-24 * time.Hour),
		DayRequests:  9999,
	}
	l := New(Config{Hourly: 5, Daily: 10}, clock)
	l.Restore(stale)
	if got := l.Snapshot(); got.HourRequests != 0 || got.DayRequests != 0 {
		t.Errorf("expired windows must not be restored: %+v", got)
	}
}

func TestWaitContextCancellation(t *testing.T) {
	clock := testclock.At(t0)
	l := New(Config{Hourly: 1}, clock)
	if err := l.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled ctx = %v, want context.Canceled", err)
	}
}
