package store

import (
	"net"
	"testing"
	"time"
)

func d(name string, v6 bool, ip string, tsNanos int64, del bool) Delta {
	return Delta{Name: name, V6: v6, IP: ip, TSNanos: tsNanos, Deleted: del}
}

func TestApplyDeltaLWW(t *testing.T) {
	s := New()
	if !s.ApplyDelta(d("h", false, "10.0.0.1", 100, false)) {
		t.Fatal("first apply should change")
	}
	if !s.ApplyDelta(d("h", false, "10.0.0.2", 200, false)) {
		t.Fatal("newer apply should change")
	}
	if s.ApplyDelta(d("h", false, "10.0.0.9", 150, false)) {
		t.Fatal("older apply must be ignored")
	}
	if ip, _ := s.LookupA("h"); !ip.Equal(net.ParseIP("10.0.0.2")) {
		t.Fatalf("LWW kept wrong ip: %v", ip)
	}
}

func TestApplyDeltaTombstoneHidesAndWins(t *testing.T) {
	s := New()
	s.ApplyDelta(d("h", false, "10.0.0.1", 100, false))
	if !s.ApplyDelta(d("h", false, "", 200, true)) {
		t.Fatal("tombstone should change")
	}
	if _, ok := s.LookupA("h"); ok {
		t.Fatal("tombstoned record must not resolve")
	}
	if _, ok := s.ReverseLookup(net.ParseIP("10.0.0.1")); ok {
		t.Fatal("tombstoned reverse must be gone")
	}
	if s.ApplyDelta(d("h", false, "10.0.0.1", 150, false)) {
		t.Fatal("older register must not beat tombstone")
	}
	if _, ok := s.LookupA("h"); ok {
		t.Fatal("record resurrected - LWW broken")
	}
}

func TestEqualTSLiveConflictConverges(t *testing.T) {
	ts := time.Unix(1000, 0).UnixNano()
	a := New()
	b := New()
	a.ApplyDelta(Delta{Name: "h", IP: "10.0.0.1", TSNanos: ts})
	b.ApplyDelta(Delta{Name: "h", IP: "10.0.0.2", TSNanos: ts})
	// each node applies the other's record (push-pull / broadcast); both must
	// converge to the SAME ip despite the equal timestamp.
	a.ApplyDelta(Delta{Name: "h", IP: "10.0.0.2", TSNanos: ts})
	b.ApplyDelta(Delta{Name: "h", IP: "10.0.0.1", TSNanos: ts})
	ipA, _ := a.LookupA("h")
	ipB, _ := b.LookupA("h")
	if ipA.String() != ipB.String() {
		t.Fatalf("nodes diverged on equal-ts conflict: A=%v B=%v", ipA, ipB)
	}
	if ipA.String() != "10.0.0.1" {
		t.Fatalf("expected smaller IP to win the tie, got %v", ipA)
	}
}

func TestNoResurrectExpiredLiveDelta(t *testing.T) {
	s := New()
	now := time.Unix(10000, 0)
	s.SetExpiry(100*time.Second, func() time.Time { return now })

	// a stale peer re-gossips an old live record (age 10000s >> maxttl 100s)
	old := Delta{Name: "h", IP: "10.0.0.1", TSNanos: time.Unix(0, 0).UnixNano()}
	if s.ApplyDelta(old) {
		t.Fatal("expired live delta must be rejected (would resurrect after tombstone GC)")
	}
	if _, ok := s.LookupA("h"); ok {
		t.Fatal("expired live delta must not be applied")
	}
	// a fresh live delta within maxttl is still accepted
	fresh := Delta{Name: "h2", IP: "10.0.0.2", TSNanos: now.Add(-10 * time.Second).UnixNano()}
	if !s.ApplyDelta(fresh) {
		t.Fatal("fresh live delta within maxttl must be accepted")
	}
}

func TestNoResurrectExpiredTombstoneFromPeer(t *testing.T) {
	now := time.Unix(100000, 0)
	maxttl := 100 * time.Second

	// Node A deleted help.vm long ago; its tombstone has since been hard-GC'd by
	// Purge, so A has no record of the name at all.
	a := New()
	a.SetExpiry(maxttl, func() time.Time { return now })

	var events []Event
	a.SetEvents(func(e Event) { events = append(events, e) })

	// push/pull anti-entropy: a peer that has NOT yet purged still ships the
	// long-dead tombstone in its snapshot (age 100000s >> maxttl 100s).
	oldTomb := Delta{Name: "help.vm", TSNanos: time.Unix(0, 0).UnixNano(), Deleted: true}
	a.Merge([]Delta{oldTomb})

	// The stale tombstone must NOT re-create the record. If it does, A logs a
	// replicate-delete, the next Purge GCs it again, and the next push/pull
	// resurrects it - an unbounded thrash that never converges.
	for _, e := range events {
		if e.Kind == "replicate-delete" {
			t.Fatalf("expired tombstone resurrected on merge -> replicate-delete thrash: %+v", e)
		}
	}
	if e := a.fwd["help.vm"]; e != nil {
		t.Fatalf("expired tombstone must not be re-installed, got %+v", e)
	}
}

func TestFreshTombstonePropagatesToUnknownName(t *testing.T) {
	now := time.Unix(100000, 0)
	maxttl := 100 * time.Second

	a := New()
	a.SetExpiry(maxttl, func() time.Time { return now })

	// A recently-deleted name (age 10s < maxttl 100s) that A has never seen must
	// still propagate, so a genuine delete reaches every node. The age guard must
	// reject only tombstones already past maxttl.
	fresh := Delta{Name: "gone.vm", TSNanos: now.Add(-10 * time.Second).UnixNano(), Deleted: true}
	if !a.ApplyDelta(fresh) {
		t.Fatal("fresh tombstone within maxttl must propagate to an unknown name")
	}
}

func TestPurgeRemovesTombstones(t *testing.T) {
	s := New()
	s.ApplyDelta(d("h", false, "", time.Unix(1000, 0).UnixNano(), true))
	n := s.Purge(100*time.Second, time.Unix(2000, 0))
	if n != 1 {
		t.Fatalf("purged %d, want 1 (tombstone GC)", n)
	}
}
