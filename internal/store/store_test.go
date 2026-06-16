package store

import (
	"net"
	"testing"
	"time"
)

func TestNameCaseInsensitive(t *testing.T) {
	s := New()
	s.Register("Web.Host", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	if _, ok := s.LookupA("web.host"); !ok {
		t.Fatal("lookup should be case-insensitive vs registration")
	}
	s.Delete("WEB.HOST", time.Unix(2000, 0))
	if _, ok := s.LookupA("web.host"); ok {
		t.Fatal("delete should be case-insensitive")
	}
}

func TestRegisterLookupA(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	s.Register("host.a", net.ParseIP("10.0.0.5"), now)

	ip, ok := s.LookupA("host.a")
	if !ok || !ip.Equal(net.ParseIP("10.0.0.5")) {
		t.Fatalf("LookupA = %v,%v", ip, ok)
	}
	if _, ok := s.LookupAAAA("host.a"); ok {
		t.Fatal("expected no AAAA")
	}
}

func TestRegisterBothFamilies(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	s.Register("dual", net.ParseIP("10.0.0.5"), now)
	s.Register("dual", net.ParseIP("2001:db8::1"), now)

	if ip, ok := s.LookupA("dual"); !ok || !ip.Equal(net.ParseIP("10.0.0.5")) {
		t.Fatalf("A = %v,%v", ip, ok)
	}
	if ip, ok := s.LookupAAAA("dual"); !ok || !ip.Equal(net.ParseIP("2001:db8::1")) {
		t.Fatalf("AAAA = %v,%v", ip, ok)
	}
}

func TestReRegisterUpdatesIP(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	s.Register("h", net.ParseIP("10.0.0.6"), time.Unix(1001, 0))
	if ip, _ := s.LookupA("h"); !ip.Equal(net.ParseIP("10.0.0.6")) {
		t.Fatalf("expected updated IP, got %v", ip)
	}
}

func TestReverseLookup(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	if name, ok := s.ReverseLookup(net.ParseIP("10.0.0.5")); !ok || name != "h" {
		t.Fatalf("ReverseLookup = %q,%v", name, ok)
	}
	if _, ok := s.ReverseLookup(net.ParseIP("10.0.0.9")); ok {
		t.Fatal("expected miss")
	}
}

func TestDeleteRemovesForwardAndReverse(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	s.Delete("h", time.Unix(2000, 0))
	if _, ok := s.LookupA("h"); ok {
		t.Fatal("expected A gone")
	}
	if _, ok := s.ReverseLookup(net.ParseIP("10.0.0.5")); ok {
		t.Fatal("expected reverse gone")
	}
}

func TestPurgeExpiresOldRecords(t *testing.T) {
	s := New()
	s.Register("old", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	s.Register("fresh", net.ParseIP("10.0.0.6"), time.Unix(2000, 0))
	n := s.Purge(100*time.Second, time.Unix(2050, 0))
	if n != 1 {
		t.Fatalf("purged %d, want 1", n)
	}
	if _, ok := s.LookupA("old"); ok {
		t.Fatal("old should be purged")
	}
	if _, ok := s.LookupA("fresh"); !ok {
		t.Fatal("fresh should remain")
	}
	if _, ok := s.ReverseLookup(net.ParseIP("10.0.0.5")); ok {
		t.Fatal("old reverse should be purged")
	}
}

func TestListReturnsAllRecords(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	s.Register("h", net.ParseIP("2001:db8::1"), time.Unix(1000, 0))
	items := s.List()
	if len(items) != 2 {
		t.Fatalf("List len = %d, want 2", len(items))
	}
}
