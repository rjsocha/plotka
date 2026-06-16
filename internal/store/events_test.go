package store

import (
	"net"
	"testing"
	"time"
)

func TestEventsHook(t *testing.T) {
	s := New()
	var got []Event
	s.SetEvents(func(e Event) { got = append(got, e) })

	s.Register("a", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))                               // register
	s.ApplyDelta(Delta{Name: "b", IP: "10.0.0.2", TSNanos: time.Unix(1000, 0).UnixNano()})     // replicate
	s.Register("c", net.ParseIP("10.0.0.3"), time.Unix(1000, 0))                               // register
	s.Delete("a", time.Unix(2000, 0))                                                          // deregister
	s.Purge(time.Second, time.Unix(1000000, 0))                                                // expire (b, c, a-tombstone)

	kinds := map[string]int{}
	for _, e := range got {
		kinds[e.Kind]++
	}
	if kinds["register"] != 2 {
		t.Errorf("register events = %d, want 2 (%+v)", kinds["register"], got)
	}
	if kinds["replicate"] != 1 {
		t.Errorf("replicate events = %d, want 1 (%+v)", kinds["replicate"], got)
	}
	if kinds["deregister"] != 1 {
		t.Errorf("deregister events = %d, want 1 (%+v)", kinds["deregister"], got)
	}
	if kinds["expire"] < 1 {
		t.Errorf("expire events = %d, want >=1 (%+v)", kinds["expire"], got)
	}

	// register event must carry the IP; deregister must not
	for _, e := range got {
		if e.Kind == "register" && e.IP == "" {
			t.Errorf("register event missing IP: %+v", e)
		}
		if e.Kind == "deregister" && e.IP != "" {
			t.Errorf("deregister event should have no IP: %+v", e)
		}
	}
}
