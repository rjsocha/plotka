package server

import (
	"net"
	"testing"
	"time"

	"plotka/internal/protocol"
	"plotka/internal/store"
)

func TestApplyRegisterSourceIP(t *testing.T) {
	st := store.New()
	cmd := protocol.Command{Op: protocol.OpRegister, Name: "h"}
	src := net.ParseIP("10.0.0.5")
	if err := Apply(st, cmd, src, time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}
	if ip, ok := st.LookupA("h"); !ok || !ip.Equal(src) {
		t.Fatalf("got %v,%v", ip, ok)
	}
}

func TestApplyRegisterExplicitAddr(t *testing.T) {
	st := store.New()
	cmd := protocol.Command{Op: protocol.OpRegister, Addr: "192.168.1.2", Name: "h"}
	if err := Apply(st, cmd, net.ParseIP("203.0.113.9"), time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}
	if ip, _ := st.LookupA("h"); !ip.Equal(net.ParseIP("192.168.1.2")) {
		t.Fatalf("got %v", ip)
	}
}

func TestApplyDeregister(t *testing.T) {
	st := store.New()
	st.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	cmd := protocol.Command{Op: protocol.OpDeregister, Name: "h"}
	if err := Apply(st, cmd, net.ParseIP("10.0.0.5"), time.Unix(1001, 0)); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.LookupA("h"); ok {
		t.Fatal("expected deleted")
	}
}

func TestApplyBadExplicitAddr(t *testing.T) {
	st := store.New()
	cmd := protocol.Command{Op: protocol.OpRegister, Addr: "not-an-ip", Name: "h"}
	if err := Apply(st, cmd, net.ParseIP("10.0.0.5"), time.Unix(1000, 0)); err == nil {
		t.Fatal("expected error for bad addr")
	}
}
