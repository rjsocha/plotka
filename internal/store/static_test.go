package store

import (
	"net"
	"testing"
	"time"
)

func TestPurgeKeepsStatic(t *testing.T) {
	s := New()
	s.RegisterStatic("reg.vm", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	s.Register("dyn", net.ParseIP("10.0.0.2"), time.Unix(1000, 0))
	// far in the future, tiny maxttl: dynamic must go, static must stay
	n := s.Purge(time.Second, time.Unix(1000000, 0))
	if n != 1 {
		t.Fatalf("purged %d, want 1 (only the dynamic)", n)
	}
	if _, ok := s.LookupA("reg.vm"); !ok {
		t.Fatal("static record must survive purge")
	}
	if _, ok := s.LookupA("dyn"); ok {
		t.Fatal("dynamic record should have been purged")
	}
}

func TestPurgeBroadcastsTombstone(t *testing.T) {
	// Expiry must propagate: a node purging a record emits a tombstone so a peer
	// with a longer ttl drops it too.
	a := New()
	var bcast []Delta
	a.SetOnChange(func(d Delta) { bcast = append(bcast, d) })
	a.Register("h", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	bcast = nil // discard the register broadcast

	a.Purge(time.Second, time.Unix(1000000, 0)) // tiny ttl -> h expires

	var tomb *Delta
	for i := range bcast {
		if bcast[i].Name == "h" && bcast[i].Deleted {
			tomb = &bcast[i]
		}
	}
	if tomb == nil {
		t.Fatalf("purge must broadcast a tombstone, got %+v", bcast)
	}
	// applying it to a peer (with the record still live) removes it there
	b := New()
	b.Register("h", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	b.ApplyDelta(*tomb)
	if _, ok := b.LookupA("h"); ok {
		t.Fatal("tombstone from purge must remove the record on the peer")
	}
}

func TestStaticBeatsDynamic(t *testing.T) {
	s := New()
	s.RegisterStatic("reg.vm", net.ParseIP("10.53.53.53"), time.Unix(1000, 0))
	s.Register("reg.vm", net.ParseIP("10.9.9.9"), time.Unix(2000, 0))
	if ip, _ := s.LookupA("reg.vm"); !ip.Equal(net.ParseIP("10.53.53.53")) {
		t.Fatalf("dynamic overrode static: %v", ip)
	}
	s.Delete("reg.vm", time.Unix(3000, 0))
	if _, ok := s.LookupA("reg.vm"); !ok {
		t.Fatal("dynamic delete removed a static record")
	}
}

func TestStaticUpdatesStaticLWW(t *testing.T) {
	s := New()
	s.RegisterStatic("reg.vm", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	s.RegisterStatic("reg.vm", net.ParseIP("10.0.0.2"), time.Unix(2000, 0))
	if ip, _ := s.LookupA("reg.vm"); !ip.Equal(net.ParseIP("10.0.0.2")) {
		t.Fatalf("static-vs-static should be LWW: %v", ip)
	}
}

func TestStaticTakesOverDynamic(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.1"), time.Unix(2000, 0))
	s.RegisterStatic("h", net.ParseIP("10.0.0.2"), time.Unix(1000, 0))
	if ip, _ := s.LookupA("h"); !ip.Equal(net.ParseIP("10.0.0.2")) {
		t.Fatalf("static should take over dynamic: %v", ip)
	}
}

func TestListMarksStatic(t *testing.T) {
	s := New()
	s.RegisterStatic("reg.vm", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	s.Register("dyn", net.ParseIP("10.0.0.2"), time.Unix(1000, 0))
	var staticSeen, dynSeen bool
	for _, it := range s.List() {
		if it.Name == "reg.vm" && it.Static {
			staticSeen = true
		}
		if it.Name == "dyn" && !it.Static {
			dynSeen = true
		}
	}
	if !staticSeen || !dynSeen {
		t.Fatalf("List must mark static vs dynamic: %+v", s.List())
	}
}
