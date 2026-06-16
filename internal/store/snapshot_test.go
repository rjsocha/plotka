package store

import (
	"net"
	"testing"
	"time"
)

func TestDeleteTombstonesWithTS(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	s.Delete("h", time.Unix(1001, 0))
	if _, ok := s.LookupA("h"); ok {
		t.Fatal("delete should hide record")
	}
	snap := s.Snapshot()
	var found bool
	for _, dl := range snap {
		if dl.Name == "h" && dl.Deleted && dl.TSNanos == time.Unix(1001, 0).UnixNano() {
			found = true
		}
	}
	if !found {
		t.Fatalf("snapshot missing tombstone: %+v", snap)
	}
}

func TestMergeAppliesLWW(t *testing.T) {
	a := New()
	a.Register("h", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	b := New()
	b.Register("h", net.ParseIP("10.0.0.2"), time.Unix(2000, 0))
	b.Register("only.b", net.ParseIP("10.0.0.3"), time.Unix(1500, 0))

	a.Merge(b.Snapshot())
	if ip, _ := a.LookupA("h"); !ip.Equal(net.ParseIP("10.0.0.2")) {
		t.Fatalf("merge LWW failed: %v", ip)
	}
	if _, ok := a.LookupA("only.b"); !ok {
		t.Fatal("merge should add only.b")
	}
}

func TestOnChangeFiresOnLocalNotRemote(t *testing.T) {
	s := New()
	var got []Delta
	s.SetOnChange(func(dl Delta) { got = append(got, dl) })
	s.Register("local", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	s.ApplyDelta(Delta{Name: "remote", IP: "10.0.0.2", TSNanos: time.Unix(1000, 0).UnixNano()})
	if len(got) != 1 || got[0].Name != "local" {
		t.Fatalf("onChange should fire only for local: %+v", got)
	}
}
