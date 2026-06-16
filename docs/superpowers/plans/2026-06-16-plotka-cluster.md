# plotka cluster (gossip HA) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the single-node `plotka` into a multi-master, eventually-consistent cluster: a write to any node replicates to every node via gossip, behind an (operator-managed) anycast VIP.

**Architecture:** Add a replication layer using `hashicorp/memberlist` (SWIM gossip + push-pull anti-entropy). The store gains a per-record last-seen timestamp with **last-writer-wins** merge and **tombstones** for deletes (so deletions propagate and self-GC after `--maxttl`). A `gossip` package wraps memberlist with a Delegate: real changes are broadcast immediately; full state is reconciled periodically via push-pull. The DNS/registry/HTTP service path is unchanged - it still binds the VIP; gossip uses each host's own unicast IP (auto-derived as "a host IP that is not the registry VIP").

**Tech Stack:** Go, `github.com/hashicorp/memberlist`, stdlib `encoding/json` for delta wire format. Builds on the merged single-node core.

Design source: `docs/superpowers/specs/2026-06-15-plotka-design.md` (Replication, Timing & traffic, Static records & deletion).

---

## Background: what changes and why

The single-node store hard-deletes and has no notion of "newer wins". Replication needs three things the core lacks:

1. **A timestamp per record + LWW merge** - so a registration/deletion arriving from another node is accepted only if newer.
2. **Tombstones** - a delete becomes a `deleted=true` record carrying the deletion timestamp, so the deletion can win an LWW race and propagate; the tombstone is GC'd by `Purge` after `--maxttl`.
3. **Snapshot / Merge / change-notification** - to feed memberlist's broadcast queue (on change) and push-pull anti-entropy (full state).

The unit of replication is a **Delta**: one record for one (name, family).

```go
type Delta struct {
    Name    string `json:"n"`
    V6      bool   `json:"6,omitempty"` // family: false=A, true=AAAA
    IP      string `json:"i,omitempty"` // empty when Deleted
    TSNanos int64  `json:"t"`           // unix nanoseconds, the LWW clock
    Deleted bool   `json:"d,omitempty"` // tombstone
}
```

LWW rule everywhere: an incoming Delta wins iff `TSNanos > existing.ts`. Equal or older is ignored.

---

## File Structure

```
internal/store/store.go        # MODIFY: add `deleted` to record; Delta; LWW core; Snapshot/Merge/ApplyDelta; OnChange
internal/store/delta.go        # NEW: Delta type + conversions
internal/server/apply.go       # MODIFY: Delete now needs a timestamp
internal/httpapi/httpapi.go    # MODIFY: DELETE passes now()
internal/admin/admin.go        # MODIFY: DELETE passes now()
internal/netid/netid.go        # NEW: advertise-address auto-derivation
internal/gossip/gossip.go      # NEW: memberlist wrapper + lifecycle (Join, Close)
internal/gossip/delegate.go    # NEW: memberlist.Delegate + Broadcast impl
cmd/plotka/server.go           # MODIFY: gossip flags (--bind/--port/--advertise/--join/--gossip-key[-file]) + wiring
```

Note on `Delete` signature change: the core's `Delete(name)` becomes `Delete(name string, ts time.Time)`. Callers: `internal/server/apply.go` (already has `now`), `internal/httpapi` and `internal/admin` (both have a `now func() time.Time`). All three are updated in Tasks 2-3.

---

## Task 1: Store - timestamps already exist; add tombstones + Delta + LWW core

**Files:**
- Create: `internal/store/delta.go`
- Modify: `internal/store/store.go`
- Test: `internal/store/lww_test.go`

- [ ] **Step 1: Write the failing test**

`internal/store/lww_test.go`:
```go
package store

import (
	"net"
	"testing"
	"time"
)

func d(name string, v6 bool, ip string, tsNanos int64, del bool) Delta {
	return Delta{Name: name, V6: v6, IP: ip, TSNanos: tsNanos, Deleted: del}
}

func TestApplyDeltaLWW(t *testing.T) {
	s := New()
	// older then newer wins; then an older one is ignored
	if !s.ApplyDelta(d("h", false, "10.0.0.1", 100, false)) {
		t.Fatal("first apply should change")
	}
	if !s.ApplyDelta(d("h", false, "10.0.0.2", 200, false)) {
		t.Fatal("newer apply should change")
	}
	if s.ApplyDelta(d("h", false, "10.0.0.9", 150, false)) {
		t.Fatal("older apply must be ignored")
	}
	if ip, _ := s.LookupA("h"); !ip.Equal(net.ParseIP("10.0.0.2")) {
		t.Fatalf("LWW kept wrong ip: %v", ip)
	}
}

func TestApplyDeltaTombstoneHidesAndWins(t *testing.T) {
	s := New()
	s.ApplyDelta(d("h", false, "10.0.0.1", 100, false))
	// tombstone with newer ts hides the record
	if !s.ApplyDelta(d("h", false, "", 200, true)) {
		t.Fatal("tombstone should change")
	}
	if _, ok := s.LookupA("h"); ok {
		t.Fatal("tombstoned record must not resolve")
	}
	if _, ok := s.ReverseLookup(net.ParseIP("10.0.0.1")); ok {
		t.Fatal("tombstoned reverse must be gone")
	}
	// an older registration must NOT resurrect it
	if s.ApplyDelta(d("h", false, "10.0.0.1", 150, false)) {
		t.Fatal("older register must not beat tombstone")
	}
	if _, ok := s.LookupA("h"); ok {
		t.Fatal("record resurrected - LWW broken")
	}
}

func TestPurgeRemovesTombstones(t *testing.T) {
	s := New()
	s.ApplyDelta(d("h", false, "", time.Unix(1000, 0).UnixNano(), true))
	n := s.Purge(100*time.Second, time.Unix(2000, 0))
	if n != 1 {
		t.Fatalf("purged %d, want 1 (tombstone GC)", n)
	}
}
```

- [ ] **Step 2: Run `go test ./internal/store/ -run 'ApplyDelta|Purge'` - expect FAIL (undefined: Delta/ApplyDelta).**

- [ ] **Step 3: Create `internal/store/delta.go`:**
```go
package store

// Delta is the unit of replication: one record for one (name, family).
type Delta struct {
	Name    string `json:"n"`
	V6      bool   `json:"6,omitempty"` // false=A, true=AAAA
	IP      string `json:"i,omitempty"` // empty when Deleted
	TSNanos int64  `json:"t"`           // unix nanoseconds (LWW clock)
	Deleted bool   `json:"d,omitempty"` // tombstone
}
```

- [ ] **Step 4: Modify `internal/store/store.go`.**

(a) Add `deleted bool` to `record`:
```go
type record struct {
	ip      net.IP
	ts      time.Time
	deleted bool
}
```

(b) Add an `onChange` callback field and a setter. Change the struct and `New`:
```go
type Store struct {
	mu       sync.RWMutex
	fwd      map[string]*entry
	rev      map[string]string
	onChange func(Delta) // notified on LOCAL changes only; may be nil
}

func New() *Store {
	return &Store{fwd: map[string]*entry{}, rev: map[string]string{}}
}

// SetOnChange registers a callback invoked (outside the lock) whenever a LOCAL
// mutation actually changes state. Used by the gossip layer to broadcast.
func (s *Store) SetOnChange(f func(Delta)) {
	s.mu.Lock()
	s.onChange = f
	s.mu.Unlock()
}
```

(c) Add the LWW core and the remote entry point. The core operates under the lock and returns whether state changed:
```go
// applyLocked applies one delta under LWW. Returns true if state changed.
func (s *Store) applyLocked(dl Delta) bool {
	name := normName(dl.Name)
	e := s.fwd[name]
	var cur *record
	if e != nil {
		if dl.V6 {
			cur = e.aaaa
		} else {
			cur = e.a
		}
	}
	if cur != nil && dl.TSNanos <= cur.ts.UnixNano() {
		return false // older-or-equal: LWW ignores
	}
	if e == nil {
		e = &entry{}
		s.fwd[name] = e
	}
	// drop any previous reverse mapping for the old ip of this family
	if cur != nil && cur.ip != nil {
		delete(s.rev, cur.ip.String())
	}
	nr := &record{ts: time.Unix(0, dl.TSNanos), deleted: dl.Deleted}
	if !dl.Deleted {
		nr.ip = net.ParseIP(dl.IP)
		if nr.ip != nil {
			s.rev[nr.ip.String()] = name
		}
	}
	if dl.V6 {
		e.aaaa = nr
	} else {
		e.a = nr
	}
	return true
}

// ApplyDelta applies a REMOTE delta (from gossip) under LWW. Returns whether
// state changed (so the caller can spread it further). Does NOT fire onChange.
func (s *Store) ApplyDelta(dl Delta) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyLocked(dl)
}
```

(d) Rewrite `Register` to go through the core and fire onChange on local changes:
```go
// Register sets the A or AAAA record for name (family from ip) to ip@ts.
func (s *Store) Register(name string, ip net.IP, ts time.Time) {
	dl := Delta{Name: name, V6: !isV4(ip), IP: ip.String(), TSNanos: ts.UnixNano()}
	s.mu.Lock()
	changed := s.applyLocked(dl)
	cb := s.onChange
	s.mu.Unlock()
	if changed && cb != nil {
		cb(dl)
	}
}
```

(e) Update `LookupA`/`LookupAAAA` to skip tombstones:
```go
func (s *Store) LookupA(name string) (net.IP, bool) {
	name = normName(name)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.fwd[name]; e != nil && e.a != nil && !e.a.deleted {
		return e.a.ip, true
	}
	return nil, false
}

func (s *Store) LookupAAAA(name string) (net.IP, bool) {
	name = normName(name)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.fwd[name]; e != nil && e.aaaa != nil && !e.aaaa.deleted {
		return e.aaaa.ip, true
	}
	return nil, false
}
```
(`ReverseLookup` needs no change: tombstoning removes the reverse key in `applyLocked`.)

(f) Replace `Purge` so it removes records (including tombstones) older than maxttl:
```go
func (s *Store) Purge(maxttl time.Duration, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	purged := 0
	for name, e := range s.fwd {
		if e.a != nil && now.Sub(e.a.ts) > maxttl {
			if e.a.ip != nil {
				delete(s.rev, e.a.ip.String())
			}
			e.a = nil
			purged++
		}
		if e.aaaa != nil && now.Sub(e.aaaa.ts) > maxttl {
			if e.aaaa.ip != nil {
				delete(s.rev, e.aaaa.ip.String())
			}
			e.aaaa = nil
			purged++
		}
		if e.a == nil && e.aaaa == nil {
			delete(s.fwd, name)
		}
	}
	return purged
}
```

(g) Update `List` to skip tombstones:
```go
func (s *Store) List() []ListItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ListItem
	for name, e := range s.fwd {
		if e.a != nil && !e.a.deleted {
			out = append(out, ListItem{name, e.a.ip.String(), e.a.ts})
		}
		if e.aaaa != nil && !e.aaaa.deleted {
			out = append(out, ListItem{name, e.aaaa.ip.String(), e.aaaa.ts})
		}
	}
	return out
}
```

- [ ] **Step 5: Run `go test ./internal/store/` - expect PASS (old tests + new LWW/tombstone/purge tests).** The existing `TestRegisterLookupA` etc. still pass because `Register` now routes through `applyLocked` with strictly-increasing real timestamps in tests.

NOTE: existing `TestPurgeExpiresOldRecords` uses live (non-tombstone) records and still holds. The existing `TestReRegisterUpdatesIP` re-registers with a newer ts (1001 > 1000) so LWW accepts it.

- [ ] **Step 6: Commit**
```bash
git add internal/store/
git commit -m "feat(store): tombstones + LWW core + ApplyDelta + onChange"
```

---

## Task 2: Store - Delete as tombstone (signature change) + Snapshot/Merge

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/snapshot_test.go`

- [ ] **Step 1: Write the failing test**

`internal/store/snapshot_test.go`:
```go
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
	// snapshot must still carry the tombstone for propagation
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
	b.Register("h", net.ParseIP("10.0.0.2"), time.Unix(2000, 0)) // newer
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
	s.Register("local", net.ParseIP("10.0.0.1"), time.Unix(1000, 0)) // fires
	s.ApplyDelta(Delta{Name: "remote", IP: "10.0.0.2", TSNanos: time.Unix(1000, 0).UnixNano()}) // must NOT fire
	if len(got) != 1 || got[0].Name != "local" {
		t.Fatalf("onChange should fire only for local: %+v", got)
	}
}
```

- [ ] **Step 2: Run `go test ./internal/store/ -run 'Tombstone|Merge|OnChange'` - expect FAIL (Delete signature / Snapshot undefined).**

- [ ] **Step 3: Modify `internal/store/store.go`.**

(a) Replace `Delete` and `deleteLocked` with a tombstoning delete that takes a timestamp and fires onChange per affected family:
```go
// Delete tombstones a name's records at ts (so the deletion replicates via
// LWW and self-GCs after maxttl). Fires onChange for each family changed.
func (s *Store) Delete(name string, ts time.Time) {
	name = normName(name)
	var fired []Delta
	s.mu.Lock()
	if e := s.fwd[name]; e != nil {
		for _, v6 := range []bool{false, true} {
			cur := e.a
			if v6 {
				cur = e.aaaa
			}
			if cur == nil || cur.deleted {
				continue
			}
			dl := Delta{Name: name, V6: v6, TSNanos: ts.UnixNano(), Deleted: true}
			if s.applyLocked(dl) {
				fired = append(fired, dl)
			}
		}
	}
	cb := s.onChange
	s.mu.Unlock()
	for _, dl := range fired {
		if cb != nil {
			cb(dl)
		}
	}
}
```
(Remove the old `deleteLocked` method - it is no longer used.)

(b) Add `Snapshot` and `Merge`:
```go
// Snapshot returns every record (including tombstones) as Deltas, for
// push-pull anti-entropy.
func (s *Store) Snapshot() []Delta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Delta
	add := func(name string, v6 bool, r *record) {
		if r == nil {
			return
		}
		dl := Delta{Name: name, V6: v6, TSNanos: r.ts.UnixNano(), Deleted: r.deleted}
		if !r.deleted && r.ip != nil {
			dl.IP = r.ip.String()
		}
		out = append(out, dl)
	}
	for name, e := range s.fwd {
		add(name, false, e.a)
		add(name, true, e.aaaa)
	}
	return out
}

// Merge applies a batch of remote deltas under LWW (no onChange; push-pull
// convergence does not need re-broadcast).
func (s *Store) Merge(deltas []Delta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, dl := range deltas {
		s.applyLocked(dl)
	}
}
```

- [ ] **Step 4: Run `go test ./internal/store/` - expect FAIL elsewhere:** the package compiles but callers of `Delete(name)` in OTHER packages now break the build. That is expected; they are fixed in Task 3. For THIS task, verify the store package's own tests pass: `go test ./internal/store/`.

- [ ] **Step 5: Commit**
```bash
git add internal/store/
git commit -m "feat(store): tombstoning Delete(ts) + Snapshot/Merge"
```

---

## Task 3: Update Delete callers (apply, http, admin)

The build is currently broken because `Delete` takes a timestamp now. Fix the three callers.

**Files:**
- Modify: `internal/server/apply.go`
- Modify: `internal/httpapi/httpapi.go`
- Modify: `internal/admin/admin.go`
- Test: existing tests in those packages (adjust where they assert deletion)

- [ ] **Step 1: Run `go build ./...` to see the breakage** (expect errors: `not enough arguments in call to st.Delete`).

- [ ] **Step 2: Fix `internal/server/apply.go`** - in the `OpDeregister` case, pass the timestamp:
```go
	case protocol.OpDeregister:
		st.Delete(cmd.Name, now)
		return nil
```
(`Apply` already receives `now time.Time`.)

- [ ] **Step 3: Fix `internal/httpapi/httpapi.go`** - the DELETE path goes through `h.apply(...)` which calls `server.Apply` with `h.now()`, so deletion already carries a timestamp and needs NO change. Verify by reading: `apply` builds a `protocol.Command{Op: OpDeregister}` and calls `server.Apply(h.st, base, src, h.now())`. No direct `st.Delete` call exists here - confirm and move on.

- [ ] **Step 4: Fix `internal/admin/admin.go`** - the `DELETE` case calls `st.Delete(fields[1])`; change to:
```go
	case "DELETE":
		if len(fields) != 2 {
			fmt.Fprint(c, "ERR usage: DELETE <name>\n")
			return
		}
		st.Delete(fields[1], now())
		fmt.Fprint(c, "OK\n")
```

- [ ] **Step 5: Run `go build ./... && go test ./...` - expect PASS.** The existing `admin` and `server` deletion tests still hold (deletion hides the record from `LookupA`).

- [ ] **Step 6: Commit**
```bash
git add internal/server/ internal/httpapi/ internal/admin/
git commit -m "fix: pass timestamp to tombstoning Delete in all callers"
```

---

## Task 4: netid - advertise-address auto-derivation

**Files:**
- Create: `internal/netid/netid.go`
- Test: `internal/netid/netid_test.go`

- [ ] **Step 1: Write the failing test**

`internal/netid/netid_test.go`:
```go
package netid

import "testing"

func TestPickAdvertise(t *testing.T) {
	// candidates are the host's IPs; exclude the registry VIP, expect the rest.
	got, err := pick([]string{"10.0.0.2", "10.53.53.53"}, "10.53.53.53")
	if err != nil || got != "10.0.0.2" {
		t.Fatalf("got %q err %v", got, err)
	}
}

func TestPickAmbiguous(t *testing.T) {
	if _, err := pick([]string{"10.0.0.2", "10.0.0.3"}, "10.53.53.53"); err == nil {
		t.Fatal("expected ambiguity error for >1 non-VIP candidate")
	}
}

func TestPickNone(t *testing.T) {
	if _, err := pick([]string{"10.53.53.53"}, "10.53.53.53"); err == nil {
		t.Fatal("expected error when only the VIP is available")
	}
}
```

- [ ] **Step 2: Run `go test ./internal/netid/` - expect FAIL (undefined: pick).**

- [ ] **Step 3: Write implementation**

`internal/netid/netid.go`:
```go
// Package netid derives the gossip advertise address: a host unicast IP that
// is not the service VIP. Gossip must reach a specific node, so it cannot use
// the anycast VIP.
package netid

import (
	"fmt"
	"net"
)

// pick returns the single candidate that is not vip, or an error if zero or
// more than one remain (caller must then set --advertise explicitly).
func pick(candidates []string, vip string) (string, error) {
	var keep []string
	for _, c := range candidates {
		if c != vip {
			keep = append(keep, c)
		}
	}
	switch len(keep) {
	case 1:
		return keep[0], nil
	case 0:
		return "", fmt.Errorf("no host IP other than the registry VIP %q; set --advertise", vip)
	default:
		return "", fmt.Errorf("multiple host IPs %v; set --advertise explicitly", keep)
	}
}

// Advertise enumerates the host's global-unicast IPs and picks the one that is
// not vip. Loopback and link-local addresses are skipped.
func Advertise(vip string) (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	var cands []string
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			continue
		}
		if ip.IsGlobalUnicast() {
			cands = append(cands, ip.String())
		}
	}
	return pick(cands, vip)
}
```

- [ ] **Step 4: Run `go test ./internal/netid/` - expect PASS.**

- [ ] **Step 5: Commit**
```bash
git add internal/netid/
git commit -m "feat(netid): auto-derive gossip advertise address (host IP minus VIP)"
```

---

## Task 5: gossip delegate + Broadcast (memberlist plumbing, unit-tested without a network)

**Files:**
- Create: `internal/gossip/delegate.go`
- Test: `internal/gossip/delegate_test.go`

- [ ] **Step 1: Add the dependency**
```bash
go get github.com/hashicorp/memberlist@latest
```

- [ ] **Step 2: Write the failing test** (the delegate's encode/decode + store wiring is testable without sockets)

`internal/gossip/delegate_test.go`:
```go
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
```

- [ ] **Step 3: Run `go test ./internal/gossip/` - expect FAIL (undefined: delegate/encodeDelta).**

- [ ] **Step 4: Write implementation**

`internal/gossip/delegate.go`:
```go
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
```

- [ ] **Step 5: Run `go test ./internal/gossip/` - expect PASS.**

- [ ] **Step 6: Commit**
```bash
git add internal/gossip/ go.mod go.sum
git commit -m "feat(gossip): memberlist Delegate - broadcast + push-pull merge"
```

---

## Task 6: gossip lifecycle - Create / Join / Close, wired to the store

**Files:**
- Create: `internal/gossip/gossip.go`
- Test: `internal/gossip/cluster_test.go` (two in-process nodes)

- [ ] **Step 1: Write the failing test** (real two-node convergence on loopback)

`internal/gossip/cluster_test.go`:
```go
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

	// register on A -> should appear on B (broadcast)
	stA.Register("from.a", net.ParseIP("10.0.0.1"), time.Now())
	waitFor(t, 3*time.Second, func() bool {
		_, ok := stB.LookupA("from.a")
		return ok
	})

	// delete on A -> tombstone should hide it on B
	stA.Delete("from.a", time.Now())
	waitFor(t, 3*time.Second, func() bool {
		_, ok := stB.LookupA("from.a")
		return !ok
	})
}
```

- [ ] **Step 2: Run `go test ./internal/gossip/ -run TestTwoNode` - expect FAIL (undefined: Create/Config).**

- [ ] **Step 3: Write implementation**

`internal/gossip/gossip.go`:
```go
package gossip

import (
	"time"

	"github.com/hashicorp/memberlist"
	"plotka/internal/store"
)

// Config holds the gossip parameters. PushPull defaults to 1h when zero.
type Config struct {
	Name          string // unique node name
	BindAddr      string // host unicast IP to listen on ("" = all)
	BindPort      int    // default 7946 set by caller
	AdvertiseAddr string // host unicast IP peers dial (never the VIP)
	SecretKey     []byte // 16/24/32 bytes, or nil
	PushPull      time.Duration
	Store         *store.Store
}

type Gossip struct {
	ml *memberlist.Memberlist
}

// Create builds a memberlist node, wires the store's onChange to the broadcast
// queue, and starts gossiping. It does not join any peers (call Join).
func Create(c Config) (*Gossip, error) {
	q := &memberlist.TransmitLimitedQueue{RetransmitMult: 3}
	d := &delegate{store: c.Store, q: q}

	cfg := memberlist.DefaultLANConfig()
	cfg.Name = c.Name
	cfg.BindAddr = c.BindAddr
	cfg.BindPort = c.BindPort
	cfg.AdvertiseAddr = c.AdvertiseAddr
	cfg.AdvertisePort = c.BindPort
	cfg.Delegate = d
	cfg.LogOutput = logDiscard{}
	if len(c.SecretKey) > 0 {
		cfg.SecretKey = c.SecretKey
	}
	if c.PushPull > 0 {
		cfg.PushPullInterval = c.PushPull
	}

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}
	q.NumNodes = func() int { return ml.NumMembers() }

	// local changes -> broadcast queue
	c.Store.SetOnChange(func(dl store.Delta) {
		q.QueueBroadcast(&broadcast{msg: encodeDelta(dl)})
	})

	return &Gossip{ml: ml}, nil
}

// Join contacts seed peers (host IPs, never the VIP). Safe with an empty list
// (first node).
func (g *Gossip) Join(seeds []string) error {
	if len(seeds) == 0 {
		return nil
	}
	_, err := g.ml.Join(seeds)
	return err
}

func (g *Gossip) Members() int { return g.ml.NumMembers() }

func (g *Gossip) Close() error {
	_ = g.ml.Leave(time.Second)
	return g.ml.Shutdown()
}

// logDiscard silences memberlist's internal logger.
type logDiscard struct{}

func (logDiscard) Write(p []byte) (int, error) { return len(p), nil }
```

- [ ] **Step 4: Run `go test ./internal/gossip/` - expect PASS.** If the two-node test is flaky, raise the `waitFor` budget; do not weaken the assertions. Memberlist on loopback converges well under a second.

- [ ] **Step 5: Commit**
```bash
git add internal/gossip/
git commit -m "feat(gossip): node lifecycle (Create/Join/Close) + store wiring"
```

---

## Task 7: Server flags + wiring gossip into `plotka server`

**Files:**
- Modify: `cmd/plotka/server.go`
- Test: manual (covered by the gossip cluster test + a build/vet gate)

- [ ] **Step 1: Add the gossip flags and wiring to `runServer`.** Insert the new flags alongside the existing ones, and start gossip after the store is created and before blocking on the signal.

Add these flags (after the existing `--register` var):
```go
	gBind := fs.String("bind", "", "gossip listen IP (\"\" = all interfaces)")
	gPort := fs.Int("port", 7946, "gossip port (TCP+UDP)")
	gAdvertise := fs.String("advertise", "", "gossip advertise IP (default: a host IP that is not --registry-bind)")
	gJoin := fs.String("join", "", "comma-separated seed peer host IPs (not the VIP)")
	gKey := fs.String("gossip-key", "", "shared gossip secret (16/24/32 bytes base64); also PLOTKA_GOSSIP_KEY or --gossip-key-file")
	gKeyFile := fs.String("gossip-key-file", "", "file containing the gossip secret")
	nodeName := fs.String("node-name", "", "unique node name (default: hostname)")
```

After `st := store.New()` and the admin/listener setup, add the gossip bring-up (before installing the signal handler):
```go
	advertise := *gAdvertise
	if advertise == "" {
		a, err := netid.Advertise(*regBind)
		if err != nil {
			return fmt.Errorf("advertise: %w", err)
		}
		advertise = a
	}
	key, err := loadGossipKey(*gKey, *gKeyFile)
	if err != nil {
		return err
	}
	name := *nodeName
	if name == "" {
		if hn, _ := os.Hostname(); hn != "" {
			name = hn
		} else {
			name = advertise
		}
	}
	g, err := gossip.Create(gossip.Config{
		Name:          name,
		BindAddr:      *gBind,
		BindPort:      *gPort,
		AdvertiseAddr: advertise,
		SecretKey:     key,
		PushPull:      time.Hour,
		Store:         st,
	})
	if err != nil {
		return fmt.Errorf("gossip: %w", err)
	}
	defer g.Close()
	if j := splitNonEmpty(*gJoin, ","); len(j) > 0 {
		if err := g.Join(j); err != nil {
			fmt.Fprintln(os.Stderr, "gossip join:", err) // non-fatal: retries via push-pull as peers appear
		}
	}
```

Add the helpers at file scope:
```go
// loadGossipKey resolves the secret from flag, file, or PLOTKA_GOSSIP_KEY env
// (in that order). Empty => unencrypted gossip. The value is base64-std.
func loadGossipKey(flagVal, file string) ([]byte, error) {
	raw := flagVal
	if raw == "" && file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("gossip-key-file: %w", err)
		}
		raw = strings.TrimSpace(string(b))
	}
	if raw == "" {
		raw = os.Getenv("PLOTKA_GOSSIP_KEY")
	}
	if raw == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("gossip key: not valid base64: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	default:
		return nil, fmt.Errorf("gossip key must decode to 16, 24, or 32 bytes, got %d", len(key))
	}
}

func splitNonEmpty(s, sep string) []string {
	var out []string
	for _, p := range strings.Split(s, sep) {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
```

Add imports to `cmd/plotka/server.go`: `"encoding/base64"`, `"strings"`, `"plotka/internal/gossip"`, `"plotka/internal/netid"`. (`os`, `fmt`, `time` are already imported.)

- [ ] **Step 2: Run `go build ./... && go vet ./...` - expect clean.**

- [ ] **Step 3: Commit**
```bash
git add cmd/plotka/server.go
git commit -m "feat: gossip flags and bring-up in plotka server"
```

---

## Task 8: Two-node end-to-end smoke test (manual) + docs

**Files:**
- Modify: `README.md` (cluster section)
- Modify: `packaging/plotka.service` (add gossip flags as a comment/example)

- [ ] **Step 1: Build and run two nodes on loopback (different ports), no root**

Terminal A:
```bash
go build -o /tmp/plotka ./cmd/plotka
/tmp/plotka server --registry-bind 127.0.0.1 --registry-port 5354 \
  --bind 127.0.0.1 --port 7946 --advertise 127.0.0.1 \
  --admin-socket /tmp/a.sock --node-name A
```
Terminal B:
```bash
/tmp/plotka server --registry-bind 127.0.0.2 --registry-port 5354 \
  --bind 127.0.0.2 --port 7946 --advertise 127.0.0.2 \
  --join 127.0.0.1:7946 --admin-socket /tmp/b.sock --node-name B
```
Terminal C - register on A, resolve on B:
```bash
printf ':+[10.7.7.7].cluster.host' > /dev/tcp/127.0.0.1/5354
sleep 1
dig @127.0.0.2 -p 5354 cluster.host +short   # expect 10.7.7.7 (replicated A->B)
printf ':-cluster.host' > /dev/tcp/127.0.0.1/5354
sleep 1
dig @127.0.0.2 -p 5354 cluster.host +short   # expect empty (tombstone replicated)
```
Confirm both expectations. (`127.0.0.2` is a loopback alias available by default on Linux.)

- [ ] **Step 2: Add a "Clustering" section to `README.md`** documenting: the gossip flags (`--bind/--port/--advertise/--join/--gossip-key[-file]`/`PLOTKA_GOSSIP_KEY`), that every node runs the same flag set (advertise auto-derived), that `--join` takes host IPs not the VIP, and how to generate a key: `head -c 32 /dev/urandom | base64`.

- [ ] **Step 3: Add the gossip flags to `packaging/plotka.service`** ExecStart (e.g. `--port 7946 --join 10.0.0.11,10.0.0.12,10.0.0.13` and a note that `--bind`/`--advertise` are auto unless the host has >1 non-VIP IP, and the key comes from `PLOTKA_GOSSIP_KEY` via an `EnvironmentFile`).

- [ ] **Step 4: Final gate**
```bash
go build ./... && go vet ./... && go test -race ./...
```
Expect all green.

- [ ] **Step 5: Commit**
```bash
git add README.md packaging/
git commit -m "docs: clustering setup (gossip flags, key, join)"
```

---

## Self-Review notes (for the implementer)

- **Spec coverage:** LWW + tombstones (Tasks 1-2), multi-master write-any/replicate-all (Tasks 5-6), broadcast-on-change + push-pull `ts` propagation (delegate + Create), bootstrap from one/few seeds (`Join`), advertise = host-IP-not-VIP (Task 4 + Task 7), shared-key gossip with env/file to keep the secret out of `ps` (Task 7), `--maxttl` tombstone GC (Task 1 Purge). Anycast/routing stays external (not built).
- **Type consistency:** `Delta{Name, V6, IP, TSNanos, Deleted}` is the single wire/merge type across store + gossip. `Delete(name, ts)` is the changed signature; the only non-test caller that passed a bare name was `admin` (fixed in Task 3); `server.Apply` and `httpapi` route through `server.Apply(..., now)`.
- **Concurrency:** `onChange` is captured under the lock and invoked after unlock to avoid re-entrancy/deadlock (store mutation inside the callback would deadlock otherwise). `applyLocked` is the single LWW core used by Register/Delete/ApplyDelta/Merge.
- **Known follow-ups (not in this plan):** tuning gossip intervals via flags (currently only PushPull is set, to 1h); `Invalidates` queue de-dup (left false - LWW is correct without it); not broadcasting pure `ts`-refreshes is achieved naturally because `applyLocked` returns false when the incoming ts is not newer, so a same-ip re-register with a newer ts DOES broadcast - acceptable at this scale; if broadcast volume ever matters, add an "ip unchanged => skip broadcast" check in `Register`.
