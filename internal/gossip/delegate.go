// Package gossip replicates the store across nodes via hashicorp/memberlist:
// real changes are broadcast; full state reconciles via push-pull.
package gossip

import (
	"encoding/json"

	"github.com/hashicorp/memberlist"
	"plotka/internal/store"
)

func encodeDelta(dl store.Delta) []byte {
	b, _ := json.Marshal(dl)
	return b
}

// broadcast is a memberlist.Broadcast carrying one encoded Delta. Invalidates
// is false: correctness comes from LWW, not from queue de-duplication.
type broadcast struct{ msg []byte }

func (b *broadcast) Invalidates(memberlist.Broadcast) bool { return false }
func (b *broadcast) Message() []byte                       { return b.msg }
func (b *broadcast) Finished()                             {}

// delegate implements memberlist.Delegate.
type delegate struct {
	store *store.Store
	q     *memberlist.TransmitLimitedQueue
}

func (d *delegate) NodeMeta(int) []byte { return nil }

// NotifyMsg receives a broadcast Delta, applies it under LWW, and if it
// changed local state, re-queues it so it keeps spreading.
func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}
	var dl store.Delta
	if err := json.Unmarshal(b, &dl); err != nil {
		return
	}
	if d.store.ApplyDelta(dl) {
		cp := make([]byte, len(b))
		copy(cp, b)
		d.q.QueueBroadcast(&broadcast{msg: cp})
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.q.GetBroadcasts(overhead, limit)
}

// LocalState returns the full store snapshot for push-pull anti-entropy.
func (d *delegate) LocalState(join bool) []byte {
	b, _ := json.Marshal(d.store.Snapshot())
	return b
}

// MergeRemoteState merges a peer's full snapshot under LWW.
func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	var snap []store.Delta
	if err := json.Unmarshal(buf, &snap); err != nil {
		return
	}
	d.store.Merge(snap)
}
