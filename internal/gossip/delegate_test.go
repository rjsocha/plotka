package gossip

import (
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"plotka/internal/store"
)

func TestDelegateNotifyMsgAppliesDelta(t *testing.T) {
	st := store.New()
	q := &memberlist.TransmitLimitedQueue{NumNodes: func() int { return 1 }, RetransmitMult: 1}
	d := &delegate{store: st, q: q}

	b := encodeDelta(store.Delta{Name: "h", IP: "10.0.0.5", TSNanos: time.Unix(1000, 0).UnixNano()})
	d.NotifyMsg(b)

	if ip, ok := st.LookupA("h"); !ok || !ip.Equal(net.ParseIP("10.0.0.5")) {
		t.Fatalf("NotifyMsg did not apply: %v,%v", ip, ok)
	}
}

func TestDelegateStateRoundTrip(t *testing.T) {
	src := store.New()
	src.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	q := &memberlist.TransmitLimitedQueue{NumNodes: func() int { return 1 }, RetransmitMult: 1}
	dsrc := &delegate{store: src, q: q}

	buf := dsrc.LocalState(false)

	dst := store.New()
	ddst := &delegate{store: dst, q: q}
	ddst.MergeRemoteState(buf, false)

	if _, ok := dst.LookupA("h"); !ok {
		t.Fatal("state did not round-trip via LocalState/MergeRemoteState")
	}
}
