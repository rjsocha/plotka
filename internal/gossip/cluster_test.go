package gossip

import (
	"fmt"
	"net"
	"testing"
	"time"

	"plotka/internal/store"
)

func freeUDPTCP(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestTwoNodeConvergence(t *testing.T) {
	stA := store.New()
	stB := store.New()
	portA := freeUDPTCP(t)
	portB := freeUDPTCP(t)

	gA, err := Create(Config{Name: "A", BindAddr: "127.0.0.1", BindPort: portA, AdvertiseAddr: "127.0.0.1", Store: stA})
	if err != nil {
		t.Fatal(err)
	}
	defer gA.Close()
	gB, err := Create(Config{Name: "B", BindAddr: "127.0.0.1", BindPort: portB, AdvertiseAddr: "127.0.0.1", Store: stB})
	if err != nil {
		t.Fatal(err)
	}
	defer gB.Close()

	if err := gB.Join([]string{fmt.Sprintf("127.0.0.1:%d", portA)}); err != nil {
		t.Fatalf("join: %v", err)
	}

	stA.Register("from.a", net.ParseIP("10.0.0.1"), time.Now())
	waitFor(t, 3*time.Second, func() bool {
		_, ok := stB.LookupA("from.a")
		return ok
	})

	stA.Delete("from.a", time.Now())
	waitFor(t, 3*time.Second, func() bool {
		_, ok := stB.LookupA("from.a")
		return !ok
	})
}
