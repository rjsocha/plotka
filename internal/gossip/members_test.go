package gossip

import (
	"fmt"
	"testing"

	"plotka/internal/store"
)

func TestMembersList(t *testing.T) {
	p := freeUDPTCP(t)
	g, err := Create(Config{Name: "solo", BindAddr: "127.0.0.1", BindPort: p, AdvertiseAddr: "127.0.0.1", Store: store.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ms := g.MemberList()
	if len(ms) != 1 || ms[0].Name != "solo" {
		t.Fatalf("members = %+v", ms)
	}
	if ms[0].Addr != fmt.Sprintf("127.0.0.1:%d", p) {
		t.Fatalf("addr = %q", ms[0].Addr)
	}
	if ms[0].State != "alive" {
		t.Fatalf("state = %q", ms[0].State)
	}
}
