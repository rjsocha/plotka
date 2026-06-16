package server

import (
	"net"
	"testing"
	"time"

	"plotka/internal/store"
)

func TestReassertStatics(t *testing.T) {
	st := store.New()
	clock := time.Unix(1000, 0)
	statics := []Static{{Name: "registry.vm", IP: net.ParseIP("10.53.53.53")}}
	ReassertStatics(st, statics, clock)
	if ip, ok := st.LookupA("registry.vm"); !ok || !ip.Equal(net.ParseIP("10.53.53.53")) {
		t.Fatalf("static not registered: %v,%v", ip, ok)
	}
	clock2 := time.Unix(5000, 0)
	ReassertStatics(st, statics, clock2)
	if n := st.Purge(time.Second, clock2.Add(500*time.Millisecond)); n != 0 {
		t.Fatalf("purged %d, expected static kept", n)
	}
}

func TestParseStatic(t *testing.T) {
	s, err := ParseStatic("10.53.53.53:registry.vm")
	if err != nil || s.Name != "registry.vm" || !s.IP.Equal(net.ParseIP("10.53.53.53")) {
		t.Fatalf("got %+v err %v", s, err)
	}
	if _, err := ParseStatic("garbage"); err == nil {
		t.Fatal("expected error")
	}
}
