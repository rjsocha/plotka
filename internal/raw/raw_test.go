package raw

import (
	"net"
	"testing"
	"time"

	"plotka/internal/store"
)

func TestHandleRegister(t *testing.T) {
	st := store.New()
	src := net.ParseIP("10.0.0.5")
	now := func() time.Time { return time.Unix(1000, 0) }
	Handle(st, []byte(":+host.a\n"), src, now)
	if ip, ok := st.LookupA("host.a"); !ok || !ip.Equal(src) {
		t.Fatalf("got %v,%v", ip, ok)
	}
}

func TestHandleExplicitAddr(t *testing.T) {
	st := store.New()
	now := func() time.Time { return time.Unix(1000, 0) }
	Handle(st, []byte(":+[192.168.1.2].host.a"), net.ParseIP("203.0.113.9"), now)
	if ip, _ := st.LookupA("host.a"); !ip.Equal(net.ParseIP("192.168.1.2")) {
		t.Fatalf("got %v", ip)
	}
}

func TestHandleIgnoresMissingMarker(t *testing.T) {
	st := store.New()
	now := func() time.Time { return time.Unix(1000, 0) }
	Handle(st, []byte("+host.a"), net.ParseIP("10.0.0.5"), now)
	if _, ok := st.LookupA("host.a"); ok {
		t.Fatal("should not register without ':' marker")
	}
}
