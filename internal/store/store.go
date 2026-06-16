// Package store is plotka's in-memory record store: per name an optional A and
// AAAA record (each with a last-seen timestamp), plus a reverse index for PTR.
package store

import (
	"net"
	"strings"
	"sync"
	"time"
)

type record struct {
	ip      net.IP
	ts      time.Time
	deleted bool
	static  bool
}

type entry struct {
	a    *record
	aaaa *record
}

type Store struct {
	mu       sync.RWMutex
	fwd      map[string]*entry // name -> entry
	rev      map[string]string // ip.String() -> name
	onChange func(Delta)
	events   func(Event)
	maxttl   time.Duration   // 0 = no apply-time age check
	clock    func() time.Time // wall clock for the age check (default time.Now)
}

// Event describes a store mutation, for verbose logging. Kind is one of
// "register", "deregister", "replicate", "replicate-delete", "expire". IP is
// empty for deletes/tombstones.
type Event struct {
	Kind string
	Name string
	IP   string
}

// SetOnChange registers a callback invoked (outside the lock) whenever a LOCAL
// mutation actually changes state. Used by the gossip layer to broadcast.
func (s *Store) SetOnChange(f func(Delta)) {
	s.mu.Lock()
	s.onChange = f
	s.mu.Unlock()
}

// SetEvents registers a callback invoked (outside the lock) for every applied
// mutation - local, replicated, or expiry - for verbose logging. nil = silent.
func (s *Store) SetEvents(f func(Event)) {
	s.mu.Lock()
	s.events = f
	s.mu.Unlock()
}

// SetExpiry enables the apply-time age check: an incoming LIVE delta older than
// maxttl is rejected, so a record cannot resurrect from a stale peer's snapshot
// after its tombstone has been purged. maxttl<=0 disables the check. clock may
// be nil (defaults to time.Now); it exists for tests.
func (s *Store) SetExpiry(maxttl time.Duration, clock func() time.Time) {
	s.mu.Lock()
	s.maxttl = maxttl
	s.clock = clock
	s.mu.Unlock()
}

func (s *Store) nowTime() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

func New() *Store {
	return &Store{fwd: map[string]*entry{}, rev: map[string]string{}}
}

func isV4(ip net.IP) bool { return ip.To4() != nil }

// normName lowercases names so all channels and DNS lookups agree (DNS is
// case-insensitive per RFC 4343).
func normName(name string) string { return strings.ToLower(name) }

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

	// Static precedence (immutability): a static record is authoritative.
	if cur != nil && cur.static && !dl.Static {
		return false // dynamic register/delete cannot touch a static record
	}
	if cur != nil && !cur.static && dl.Static {
		return s.writeLocked(e, name, dl) // static takes over a dynamic record
	}

	// BUG-2 guard: never accept a non-static live delta older than maxttl
	// (would resurrect after a tombstone is purged). Static is exempt.
	if !dl.Static && !dl.Deleted && s.maxttl > 0 && s.nowTime().Sub(time.Unix(0, dl.TSNanos)) > s.maxttl {
		return false
	}

	// LWW with a deterministic, node-independent tie-break so every node
	// converges to the same record:
	//   - strictly newer ts wins;
	//   - on equal ts: an existing tombstone wins; an incoming tombstone beats a
	//     live record (delete bias); two live records break the tie by smaller
	//     IP string (a total order), so cross-node equal-ts conflicts reconcile.
	if cur != nil {
		if dl.TSNanos < cur.ts.UnixNano() {
			return false
		}
		if dl.TSNanos == cur.ts.UnixNano() {
			switch {
			case cur.deleted:
				return false // existing tombstone wins and stays
			case dl.Deleted:
				// incoming tombstone over live record: accept (delete bias)
			default:
				// both live: smaller IP wins; equal IP is a no-op
				if dl.IP >= cur.ip.String() {
					return false
				}
			}
		}
	}
	if e == nil {
		e = &entry{}
		s.fwd[name] = e
	}
	return s.writeLocked(e, name, dl)
}

// writeLocked installs dl's record into entry e (maintaining the reverse index).
func (s *Store) writeLocked(e *entry, name string, dl Delta) bool {
	var cur *record
	if dl.V6 {
		cur = e.aaaa
	} else {
		cur = e.a
	}
	if cur != nil && cur.ip != nil {
		delete(s.rev, cur.ip.String())
	}
	nr := &record{ts: time.Unix(0, dl.TSNanos), deleted: dl.Deleted, static: dl.Static}
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

// ApplyDelta applies a REMOTE delta under LWW. Returns whether state changed.
// Does NOT fire onChange (but does fire the verbose events hook).
func (s *Store) ApplyDelta(dl Delta) bool {
	s.mu.Lock()
	changed := s.applyLocked(dl)
	ev := s.events
	s.mu.Unlock()
	if changed && ev != nil {
		kind := "replicate"
		if dl.Deleted {
			kind = "replicate-delete"
		}
		ev(Event{kind, normName(dl.Name), dl.IP})
	}
	return changed
}

// Register sets the A or AAAA record for name (family from ip) to ip@ts.
func (s *Store) Register(name string, ip net.IP, ts time.Time) {
	dl := Delta{Name: name, V6: !isV4(ip), IP: ip.String(), TSNanos: ts.UnixNano()}
	s.mu.Lock()
	changed := s.applyLocked(dl)
	cb := s.onChange
	ev := s.events
	s.mu.Unlock()
	if changed {
		if cb != nil {
			cb(dl)
		}
		if ev != nil {
			ev(Event{"register", normName(name), ip.String()})
		}
	}
}

// RegisterStatic sets an immutable static record (from --register / config).
func (s *Store) RegisterStatic(name string, ip net.IP, ts time.Time) {
	dl := Delta{Name: name, V6: !isV4(ip), IP: ip.String(), TSNanos: ts.UnixNano(), Static: true}
	s.mu.Lock()
	changed := s.applyLocked(dl)
	cb := s.onChange
	ev := s.events
	s.mu.Unlock()
	if changed {
		if cb != nil {
			cb(dl)
		}
		if ev != nil {
			ev(Event{"register", normName(name), ip.String()})
		}
	}
}

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

// ReverseLookup returns the name registered for ip, for PTR answers.
func (s *Store) ReverseLookup(ip net.IP) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	name, ok := s.rev[ip.String()]
	return name, ok
}

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
	ev := s.events
	s.mu.Unlock()
	for _, dl := range fired {
		if cb != nil {
			cb(dl)
		}
	}
	if ev != nil && len(fired) > 0 {
		ev(Event{"deregister", name, ""})
	}
}

// Purge acts on every record older than maxttl. A live record is converted to a
// tombstone (ts=now) and broadcast via onChange, so expiry propagates across the
// cluster (a peer with a longer ttl still drops it); an already-dead tombstone is
// GC'd locally with no broadcast. Static records are exempt. Returns the count of
// records acted on. A live host whose record was expired re-appears on its next
// heartbeat (newer ts beats the tombstone) - keep maxttl > heartbeat interval.
func (s *Store) Purge(maxttl time.Duration, now time.Time) int {
	s.mu.Lock()
	var expired []Event  // verbose events (live -> tombstone)
	var tombs []Delta    // tombstones to broadcast (live -> tombstone)
	purged := 0
	for name, e := range s.fwd {
		for _, v6 := range []bool{false, true} {
			r := e.a
			if v6 {
				r = e.aaaa
			}
			if r == nil || r.static || now.Sub(r.ts) <= maxttl {
				continue
			}
			ip := ""
			if r.ip != nil {
				ip = r.ip.String()
				delete(s.rev, ip)
			}
			purged++
			if r.deleted {
				// GC an old tombstone: local hard-remove, deletion already propagated.
				if v6 {
					e.aaaa = nil
				} else {
					e.a = nil
				}
				continue
			}
			// Live record expired: replace with a fresh tombstone and broadcast it.
			tomb := &record{ts: now, deleted: true}
			dl := Delta{Name: name, V6: v6, TSNanos: now.UnixNano(), Deleted: true}
			if v6 {
				e.aaaa = tomb
			} else {
				e.a = tomb
			}
			tombs = append(tombs, dl)
			expired = append(expired, Event{"expire", name, ip})
		}
		if e.a == nil && e.aaaa == nil {
			delete(s.fwd, name)
		}
	}
	cb := s.onChange
	ev := s.events
	s.mu.Unlock()
	for _, dl := range tombs {
		if cb != nil {
			cb(dl)
		}
	}
	if ev != nil {
		for _, e := range expired {
			ev(e)
		}
	}
	return purged
}

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
		dl := Delta{Name: name, V6: v6, TSNanos: r.ts.UnixNano(), Deleted: r.deleted, Static: r.static}
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

// Merge applies a batch of remote deltas under LWW (no onChange). Fires the
// verbose events hook for each delta that changed state.
func (s *Store) Merge(deltas []Delta) {
	s.mu.Lock()
	var changed []Delta
	for _, dl := range deltas {
		if s.applyLocked(dl) {
			changed = append(changed, dl)
		}
	}
	ev := s.events
	s.mu.Unlock()
	if ev != nil {
		for _, dl := range changed {
			kind := "replicate"
			if dl.Deleted {
				kind = "replicate-delete"
			}
			ev(Event{kind, normName(dl.Name), dl.IP})
		}
	}
}

// ListItem is one record (one family) for `client list`.
type ListItem struct {
	Name   string
	IP     string
	TS     time.Time
	Static bool
}

func (s *Store) List() []ListItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ListItem
	for name, e := range s.fwd {
		if e.a != nil && !e.a.deleted {
			out = append(out, ListItem{name, e.a.ip.String(), e.a.ts, e.a.static})
		}
		if e.aaaa != nil && !e.aaaa.deleted {
			out = append(out, ListItem{name, e.aaaa.ip.String(), e.aaaa.ts, e.aaaa.static})
		}
	}
	return out
}
