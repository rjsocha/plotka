package gossip

import (
	"fmt"
	"net"
	"testing"
	"time"

	"plotka/internal/store"
)

func newNode(t *testing.T, name string) (*Gossip, *store.Store, string) {
	t.Helper()
	st := store.New()
	p := freeUDPTCP(t)
	g, err := Create(Config{Name: name, BindAddr: "127.0.0.1", BindPort: p, AdvertiseAddr: "127.0.0.1", Store: st})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g, st, fmt.Sprintf("127.0.0.1:%d", p)
}

// Two islands of 2 nodes that share one seed list: one Rejoin on any node
// merges them into a single 4-node cluster and syncs records across.
func TestRejoinHealsTwoPlusTwoSplit(t *testing.T) {
	gA, stA, aA := newNode(t, "A")
	gB, _, aB := newNode(t, "B")
	gC, stC, aC := newNode(t, "C")
	gD, _, aD := newNode(t, "D")
	seeds := []string{aA, aB, aC, aD}

	if err := gB.Join([]string{aA}); err != nil {
		t.Fatal(err)
	}
	if err := gD.Join([]string{aC}); err != nil {
		t.Fatal(err)
	}
	all := []*Gossip{gA, gB, gC, gD}
	waitFor(t, 3*time.Second, func() bool {
		for _, g := range all {
			if g.Members() != 2 {
				return false
			}
		}
		return true
	})

	stC.Register("from.c", net.ParseIP("10.0.0.3"), time.Now())
	stA.Register("from.a", net.ParseIP("10.0.0.1"), time.Now())

	n, err := gA.Rejoin(seeds)
	if err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	if n != 2 {
		t.Fatalf("rejoin attempted %d seeds, want 2 (C, D)", n)
	}

	waitFor(t, 5*time.Second, func() bool {
		for _, g := range all {
			if g.Members() != 4 {
				return false
			}
		}
		return true
	})
	waitFor(t, 5*time.Second, func() bool {
		_, okA := stA.LookupA("from.c")
		_, okC := stC.LookupA("from.a")
		return okA && okC
	})
}

// A healthy cluster (every seed an alive member, self included) needs no join.
func TestRejoinNoopWhenAllSeedsAlive(t *testing.T) {
	gA, _, aA := newNode(t, "A")
	gB, _, aB := newNode(t, "B")
	if err := gB.Join([]string{aA}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool { return gA.Members() == 2 && gB.Members() == 2 })

	n, err := gA.Rejoin([]string{aA, aB})
	if err != nil || n != 0 {
		t.Fatalf("rejoin = %d, %v; want 0, nil", n, err)
	}
}

// A seed without a port uses the node's own cluster port, like memberlist.
func TestRejoinBareIPSeedUsesBindPort(t *testing.T) {
	gA, _, _ := newNode(t, "A")
	n, err := gA.Rejoin([]string{"127.0.0.1"})
	if err != nil || n != 0 {
		t.Fatalf("rejoin = %d, %v; want 0, nil (self)", n, err)
	}
}
