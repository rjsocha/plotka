# plotka core (single node) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-node `plotka` daemon: a small DNS server that answers from an in-memory store which clients populate via a minimal register/deregister protocol, plus a CLI for admin.

**Architecture:** One Go binary. A shared in-memory store (`name -> A?/AAAA?`, each with a last-seen timestamp, plus a reverse index for PTR). Registrations arrive over three front-ends muxed on one `IP:port` - raw line, DNS query, HTTP - all parsed by a single grammar. A non-recursive DNS responder serves A/AAAA/PTR with TTL 0. A background sweep purges records older than `--maxttl`. A `plotka client` talks to the server over a unix socket. Gossip replication is explicitly out of scope for this plan (see plotka-cluster).

**Tech Stack:** Go, `github.com/miekg/dns` (DNS wire), `github.com/soheilhy/cmux` (TCP first-byte mux), Go stdlib for everything else (`net`, `flag`, `net/http`, unix sockets).

Design source: `docs/superpowers/specs/2026-06-15-plotka-design.md`.

---

## File Structure

```
go.mod
cmd/plotka/main.go          # subcommand dispatch: server | client | help | version
cmd/plotka/server.go        # `server` flag parsing + wiring
cmd/plotka/client.go        # `client` flag parsing + unix-socket calls
internal/protocol/protocol.go   # parse "<op>[addr].name" grammar (shared by all channels)
internal/store/store.go         # in-memory store + reverse index + purge
internal/dnssrv/dnssrv.go       # DNS responder (A/AAAA/PTR, TTL 0) + `:`-qname registration
internal/raw/raw.go             # raw line channel handler
internal/httpapi/httpapi.go     # HTTP channel handlers
internal/listener/listener.go   # TCP cmux + UDP dispatch
internal/admin/admin.go         # unix-socket admin server + client helpers
internal/server/server.go       # assembles store + listeners + purge timer + --register re-assert
packaging/plotka.service        # systemd unit
```

Each `internal/*` package has one responsibility and is unit-tested in isolation. `internal/server` is the only package that wires them together.

---

## Task 1: Module skeleton + version/help

**Files:**
- Create: `go.mod`
- Create: `cmd/plotka/main.go`
- Test: `cmd/plotka/main_test.go`

- [ ] **Step 1: Create the module**

Run:
```bash
cd /home/socha/git/github.com/super-cool-and-all-dns-registry
go mod init plotka
```
(The module path `plotka` is fine for an unpublished binary; change it later if you publish.)

- [ ] **Step 2: Write the failing test**

`cmd/plotka/main_test.go`:
```go
package main

import "testing"

func TestDispatchUnknownReturnsError(t *testing.T) {
	if err := dispatch([]string{"bogus"}); err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestDispatchVersion(t *testing.T) {
	if err := dispatch([]string{"version"}); err != nil {
		t.Fatalf("version should succeed, got %v", err)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./cmd/plotka/`
Expected: FAIL - `undefined: dispatch`.

- [ ] **Step 4: Write minimal implementation**

`cmd/plotka/main.go`:
```go
package main

import (
	"fmt"
	"os"
)

const version = "0.1.0"

func dispatch(args []string) error {
	if len(args) == 0 {
		return runHelp()
	}
	switch args[0] {
	case "server":
		return runServer(args[1:])
	case "client":
		return runClient(args[1:])
	case "version":
		fmt.Println("plotka", version)
		return nil
	case "help", "-h", "--help":
		return runHelp()
	default:
		return fmt.Errorf("unknown subcommand %q (try: server, client, help, version)", args[0])
	}
}

func runHelp() error {
	fmt.Println(`plotka - small dynamic DNS registry

usage:
  plotka server [flags]   run the daemon
  plotka client <op> ...  admin against the local server (list|set|delete|purge)
  plotka version
  plotka help`)
	return nil
}

func main() {
	if err := dispatch(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "plotka:", err)
		os.Exit(1)
	}
}
```

Add temporary stubs so it compiles (replaced in later tasks). `cmd/plotka/server.go`:
```go
package main

func runServer(args []string) error { return nil }
```
`cmd/plotka/client.go`:
```go
package main

func runClient(args []string) error { return nil }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./cmd/plotka/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod cmd/plotka/
git commit -m "feat: module skeleton with subcommand dispatch"
```

---

## Task 2: Protocol grammar parser

The grammar (from the spec), with the channel marker `:` already stripped by the caller:
```
<op>[<addr>].<name>
  op   = +  register | -  deregister
  addr = optional [IPv4] or [IPv6] (brackets literal); absent -> use source IP
  name = hostname (may contain dots, e.g. host.lab)
```

**Files:**
- Create: `internal/protocol/protocol.go`
- Test: `internal/protocol/protocol_test.go`

- [ ] **Step 1: Write the failing test**

`internal/protocol/protocol_test.go`:
```go
package protocol

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		in       string
		wantOp   Op
		wantAddr string
		wantName string
		wantErr  bool
	}{
		{"+abc.pl", OpRegister, "", "abc.pl", false},
		{"-abc.pl", OpDeregister, "", "abc.pl", false},
		{"+host.lab", OpRegister, "", "host.lab", false},
		{"+[192.168.1.2].abc.pl", OpRegister, "192.168.1.2", "abc.pl", false},
		{"-[192.168.1.2].abc.pl", OpDeregister, "192.168.1.2", "abc.pl", false},
		{"+[2001:db8::1].v6.host", OpRegister, "2001:db8::1", "v6.host", false},
		{"", 0, "", "", true},
		{"abc.pl", 0, "", "", true},      // no op
		{"+", 0, "", "", true},           // no name
		{"+[10.0.0.1]", 0, "", "", true}, // addr but no name
		{"+[bad.name", 0, "", "", true},  // unterminated bracket
		{"+[10.0.0.1]abc", 0, "", "", true}, // missing dot after bracket
	}
	for _, c := range cases {
		got, err := Parse(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("Parse(%q): expected error, got %+v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q): unexpected error %v", c.in, err)
			continue
		}
		if got.Op != c.wantOp || got.Addr != c.wantAddr || got.Name != c.wantName {
			t.Errorf("Parse(%q) = %+v, want op=%v addr=%q name=%q", c.in, got, c.wantOp, c.wantAddr, c.wantName)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/protocol/`
Expected: FAIL - `undefined: Parse`.

- [ ] **Step 3: Write minimal implementation**

`internal/protocol/protocol.go`:
```go
// Package protocol parses the plotka registration grammar shared by the raw,
// DNS, and HTTP channels. The leading ':' channel marker (raw/DNS) must be
// stripped by the caller before calling Parse.
package protocol

import (
	"fmt"
	"strings"
)

type Op int

const (
	OpRegister Op = iota
	OpDeregister
)

// Command is a parsed register/deregister request.
type Command struct {
	Op   Op
	Addr string // explicit address, "" => use connection source IP
	Name string
}

// Parse parses "<op>[addr].name". Op is the first byte (+/-). An optional
// [addr] follows, then a dot, then the name. Without [addr] the rest is name.
func Parse(s string) (Command, error) {
	if s == "" {
		return Command{}, fmt.Errorf("empty token")
	}
	var cmd Command
	switch s[0] {
	case '+':
		cmd.Op = OpRegister
	case '-':
		cmd.Op = OpDeregister
	default:
		return Command{}, fmt.Errorf("token %q: missing +/- op", s)
	}
	rest := s[1:]
	if strings.HasPrefix(rest, "[") {
		end := strings.IndexByte(rest, ']')
		if end < 0 {
			return Command{}, fmt.Errorf("token %q: unterminated [addr]", s)
		}
		cmd.Addr = rest[1:end]
		after := rest[end+1:]
		if !strings.HasPrefix(after, ".") {
			return Command{}, fmt.Errorf("token %q: expected '.' after [addr]", s)
		}
		cmd.Name = after[1:]
	} else {
		cmd.Name = rest
	}
	if cmd.Name == "" {
		return Command{}, fmt.Errorf("token %q: empty name", s)
	}
	return cmd, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/protocol/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/protocol/
git commit -m "feat: registration grammar parser"
```

---

## Task 3: Store - register and lookup (A/AAAA)

**Files:**
- Create: `internal/store/store.go`
- Test: `internal/store/store_test.go`

- [ ] **Step 1: Write the failing test**

`internal/store/store_test.go`:
```go
package store

import (
	"net"
	"testing"
	"time"
)

func TestRegisterLookupA(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	s.Register("host.a", net.ParseIP("10.0.0.5"), now)

	ip, ok := s.LookupA("host.a")
	if !ok || !ip.Equal(net.ParseIP("10.0.0.5")) {
		t.Fatalf("LookupA = %v,%v", ip, ok)
	}
	if _, ok := s.LookupAAAA("host.a"); ok {
		t.Fatal("expected no AAAA")
	}
}

func TestRegisterBothFamilies(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	s.Register("dual", net.ParseIP("10.0.0.5"), now)
	s.Register("dual", net.ParseIP("2001:db8::1"), now)

	if ip, ok := s.LookupA("dual"); !ok || !ip.Equal(net.ParseIP("10.0.0.5")) {
		t.Fatalf("A = %v,%v", ip, ok)
	}
	if ip, ok := s.LookupAAAA("dual"); !ok || !ip.Equal(net.ParseIP("2001:db8::1")) {
		t.Fatalf("AAAA = %v,%v", ip, ok)
	}
}

func TestReRegisterUpdatesIP(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	s.Register("h", net.ParseIP("10.0.0.6"), time.Unix(1001, 0))
	if ip, _ := s.LookupA("h"); !ip.Equal(net.ParseIP("10.0.0.6")) {
		t.Fatalf("expected updated IP, got %v", ip)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/`
Expected: FAIL - `undefined: New`.

- [ ] **Step 3: Write minimal implementation**

`internal/store/store.go`:
```go
// Package store is plotka's in-memory record store: per name an optional A and
// AAAA record (each with a last-seen timestamp), plus a reverse index for PTR.
package store

import (
	"net"
	"sync"
	"time"
)

type record struct {
	ip net.IP
	ts time.Time
}

type entry struct {
	a    *record
	aaaa *record
}

type Store struct {
	mu  sync.RWMutex
	fwd map[string]*entry // name -> entry
	rev map[string]string // ip.String() -> name
}

func New() *Store {
	return &Store{fwd: map[string]*entry{}, rev: map[string]string{}}
}

func isV4(ip net.IP) bool { return ip.To4() != nil }

// Register sets the A or AAAA record for name (family chosen by ip) to ip@ts.
func (s *Store) Register(name string, ip net.IP, ts time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.fwd[name]
	if e == nil {
		e = &entry{}
		s.fwd[name] = e
	}
	r := &record{ip: ip, ts: ts}
	if isV4(ip) {
		if e.a != nil {
			delete(s.rev, e.a.ip.String())
		}
		e.a = r
	} else {
		if e.aaaa != nil {
			delete(s.rev, e.aaaa.ip.String())
		}
		e.aaaa = r
	}
	s.rev[ip.String()] = name
}

func (s *Store) LookupA(name string) (net.IP, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.fwd[name]; e != nil && e.a != nil {
		return e.a.ip, true
	}
	return nil, false
}

func (s *Store) LookupAAAA(name string) (net.IP, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.fwd[name]; e != nil && e.aaaa != nil {
		return e.aaaa.ip, true
	}
	return nil, false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/store/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/
git commit -m "feat: store register + A/AAAA lookup"
```

---

## Task 4: Store - reverse lookup (PTR), delete, purge, list

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go` (add cases)

- [ ] **Step 1: Write the failing test**

Append to `internal/store/store_test.go`:
```go
func TestReverseLookup(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	if name, ok := s.ReverseLookup(net.ParseIP("10.0.0.5")); !ok || name != "h" {
		t.Fatalf("ReverseLookup = %q,%v", name, ok)
	}
	if _, ok := s.ReverseLookup(net.ParseIP("10.0.0.9")); ok {
		t.Fatal("expected miss")
	}
}

func TestDeleteRemovesForwardAndReverse(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	s.Delete("h")
	if _, ok := s.LookupA("h"); ok {
		t.Fatal("expected A gone")
	}
	if _, ok := s.ReverseLookup(net.ParseIP("10.0.0.5")); ok {
		t.Fatal("expected reverse gone")
	}
}

func TestPurgeExpiresOldRecords(t *testing.T) {
	s := New()
	s.Register("old", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	s.Register("fresh", net.ParseIP("10.0.0.6"), time.Unix(2000, 0))
	// maxttl 100s, now = 2050 => old (age 1050) purged, fresh (age 50) kept.
	n := s.Purge(100*time.Second, time.Unix(2050, 0))
	if n != 1 {
		t.Fatalf("purged %d, want 1", n)
	}
	if _, ok := s.LookupA("old"); ok {
		t.Fatal("old should be purged")
	}
	if _, ok := s.LookupA("fresh"); !ok {
		t.Fatal("fresh should remain")
	}
	if _, ok := s.ReverseLookup(net.ParseIP("10.0.0.5")); ok {
		t.Fatal("old reverse should be purged")
	}
}

func TestListReturnsAllRecords(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.5"), time.Unix(1000, 0))
	s.Register("h", net.ParseIP("2001:db8::1"), time.Unix(1000, 0))
	items := s.List()
	if len(items) != 2 {
		t.Fatalf("List len = %d, want 2", len(items))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/`
Expected: FAIL - `undefined: ReverseLookup` (and others).

- [ ] **Step 3: Write minimal implementation**

Append to `internal/store/store.go`:
```go
// ReverseLookup returns the name registered for ip, for PTR answers.
func (s *Store) ReverseLookup(ip net.IP) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	name, ok := s.rev[ip.String()]
	return name, ok
}

// Delete removes a name and both its records (and their reverse entries).
func (s *Store) Delete(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteLocked(name)
}

func (s *Store) deleteLocked(name string) {
	e := s.fwd[name]
	if e == nil {
		return
	}
	if e.a != nil {
		delete(s.rev, e.a.ip.String())
	}
	if e.aaaa != nil {
		delete(s.rev, e.aaaa.ip.String())
	}
	delete(s.fwd, name)
}

// Purge drops any record whose age (now-ts) exceeds maxttl. A name whose
// records are all purged is removed entirely. Returns the count of records
// (A/AAAA, counted separately) purged.
func (s *Store) Purge(maxttl time.Duration, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	purged := 0
	for name, e := range s.fwd {
		if e.a != nil && now.Sub(e.a.ts) > maxttl {
			delete(s.rev, e.a.ip.String())
			e.a = nil
			purged++
		}
		if e.aaaa != nil && now.Sub(e.aaaa.ts) > maxttl {
			delete(s.rev, e.aaaa.ip.String())
			e.aaaa = nil
			purged++
		}
		if e.a == nil && e.aaaa == nil {
			delete(s.fwd, name)
		}
	}
	return purged
}

// ListItem is one record (one family) for `client list`.
type ListItem struct {
	Name string
	IP   string
	TS   time.Time
}

func (s *Store) List() []ListItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ListItem
	for name, e := range s.fwd {
		if e.a != nil {
			out = append(out, ListItem{name, e.a.ip.String(), e.a.ts})
		}
		if e.aaaa != nil {
			out = append(out, ListItem{name, e.aaaa.ip.String(), e.aaaa.ts})
		}
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/store/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/
git commit -m "feat: store reverse lookup, delete, purge, list"
```

---

## Task 5: Apply helper - turn a protocol.Command into a store mutation

This centralizes the "source IP vs explicit [addr]" decision so every channel reuses it.

**Files:**
- Create: `internal/server/apply.go`
- Test: `internal/server/apply_test.go`

- [ ] **Step 1: Write the failing test**

`internal/server/apply_test.go`:
```go
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
	// source IP differs (NAT gateway) and must be ignored.
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/server/`
Expected: FAIL - `undefined: Apply`.

- [ ] **Step 3: Write minimal implementation**

`internal/server/apply.go`:
```go
// Package server wires the store, listeners, and timers together.
package server

import (
	"fmt"
	"net"
	"time"

	"plotka/internal/protocol"
	"plotka/internal/store"
)

// Apply executes a parsed command against the store. For registration, the IP
// is the explicit cmd.Addr if present, otherwise the connection source IP.
func Apply(st *store.Store, cmd protocol.Command, src net.IP, now time.Time) error {
	switch cmd.Op {
	case protocol.OpDeregister:
		st.Delete(cmd.Name)
		return nil
	case protocol.OpRegister:
		ip := src
		if cmd.Addr != "" {
			ip = net.ParseIP(cmd.Addr)
			if ip == nil {
				return fmt.Errorf("invalid address %q", cmd.Addr)
			}
		}
		if ip == nil {
			return fmt.Errorf("no source IP and no explicit address")
		}
		st.Register(cmd.Name, ip, now)
		return nil
	default:
		return fmt.Errorf("unknown op %v", cmd.Op)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/server/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/server/
git commit -m "feat: apply parsed command to store (source-IP vs explicit addr)"
```

---

## Task 6: Raw line channel

Parses one line (`:+name`, `:-name`, ...), strips the `:` marker, applies, responds with nothing (fire-and-forget) - reads at most one line then returns.

**Files:**
- Create: `internal/raw/raw.go`
- Test: `internal/raw/raw_test.go`

- [ ] **Step 1: Write the failing test**

`internal/raw/raw_test.go`:
```go
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
	// no leading ':' => not a raw command, ignored, no panic
	Handle(st, []byte("+host.a"), net.ParseIP("10.0.0.5"), now)
	if _, ok := st.LookupA("host.a"); ok {
		t.Fatal("should not register without ':' marker")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/raw/`
Expected: FAIL - `undefined: Handle`.

- [ ] **Step 3: Write minimal implementation**

`internal/raw/raw.go`:
```go
// Package raw implements the raw line registration channel: a ':'-prefixed
// token ("+name" / "-name" / "+[addr].name"), fire-and-forget, no response.
package raw

import (
	"net"
	"strings"
	"time"

	"plotka/internal/protocol"
	"plotka/internal/server"
	"plotka/internal/store"
)

// Handle parses one raw token and applies it. Malformed input is silently
// dropped (fire-and-forget, untrusted-but-closed network).
func Handle(st *store.Store, line []byte, src net.IP, now func() time.Time) {
	s := strings.TrimSpace(string(line))
	if !strings.HasPrefix(s, ":") {
		return
	}
	cmd, err := protocol.Parse(s[1:])
	if err != nil {
		return
	}
	_ = server.Apply(st, cmd, src, now())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/raw/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/raw/
git commit -m "feat: raw line registration channel"
```

---

## Task 7: DNS responder - resolve A/AAAA/PTR with TTL 0

**Files:**
- Create: `internal/dnssrv/dnssrv.go`
- Test: `internal/dnssrv/dnssrv_test.go`

- [ ] **Step 1: Add the dependency**

Run:
```bash
go get github.com/miekg/dns@latest
```

- [ ] **Step 2: Write the failing test**

`internal/dnssrv/dnssrv_test.go`:
```go
package dnssrv

import (
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	"plotka/internal/store"
)

func now() time.Time { return time.Unix(1000, 0) }

func query(h *Handler, qname string, qtype uint16) *dns.Msg {
	req := new(dns.Msg)
	req.SetQuestion(dns.Fqdn(qname), qtype)
	w := &testWriter{}
	h.ServeDNS(w, req)
	return w.msg
}

type testWriter struct {
	dns.ResponseWriter
	msg *dns.Msg
}

func (w *testWriter) WriteMsg(m *dns.Msg) error { w.msg = m; return nil }
func (w *testWriter) RemoteAddr() net.Addr      { return &net.UDPAddr{IP: net.ParseIP("10.0.0.5")} }

func TestResolveA(t *testing.T) {
	st := store.New()
	st.Register("host.a", net.ParseIP("10.0.0.7"), now())
	h := New(st, now)

	m := query(h, "host.a", dns.TypeA)
	if len(m.Answer) != 1 {
		t.Fatalf("answers = %d", len(m.Answer))
	}
	a := m.Answer[0].(*dns.A)
	if a.A.String() != "10.0.0.7" {
		t.Fatalf("A = %v", a.A)
	}
	if a.Hdr.Ttl != 0 {
		t.Fatalf("ttl = %d, want 0", a.Hdr.Ttl)
	}
}

func TestResolveUnknownNXDOMAIN(t *testing.T) {
	h := New(store.New(), now)
	m := query(h, "nope.nope", dns.TypeA)
	if m.Rcode != dns.RcodeNameError {
		t.Fatalf("rcode = %d, want NXDOMAIN", m.Rcode)
	}
}

func TestResolvePTR(t *testing.T) {
	st := store.New()
	st.Register("host.a", net.ParseIP("10.0.0.7"), now())
	h := New(st, now)
	ptrName, _ := dns.ReverseAddr("10.0.0.7")
	m := query(h, ptrName, dns.TypePTR)
	if len(m.Answer) != 1 {
		t.Fatalf("ptr answers = %d", len(m.Answer))
	}
	if got := m.Answer[0].(*dns.PTR).Ptr; got != dns.Fqdn("host.a") {
		t.Fatalf("ptr = %q", got)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/dnssrv/`
Expected: FAIL - `undefined: New`.

- [ ] **Step 4: Write minimal implementation**

`internal/dnssrv/dnssrv.go`:
```go
// Package dnssrv is plotka's authoritative, non-recursive DNS responder.
// It answers A/AAAA/PTR from the store with TTL 0 (local server, no caching)
// and NXDOMAIN for anything unknown. Registration via ':'-prefixed qnames is
// added in a later task.
package dnssrv

import (
	"net"
	"time"

	"github.com/miekg/dns"
	"plotka/internal/store"
)

type Handler struct {
	st  *store.Store
	now func() time.Time
}

func New(st *store.Store, now func() time.Time) *Handler {
	return &Handler{st: st, now: now}
}

func (h *Handler) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(req)
	m.Authoritative = true

	if len(req.Question) == 1 {
		q := req.Question[0]
		name := q.Name
		switch q.Qtype {
		case dns.TypeA:
			if ip, ok := h.st.LookupA(unfqdn(name)); ok {
				m.Answer = append(m.Answer, &dns.A{
					Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 0},
					A:   ip,
				})
			}
		case dns.TypeAAAA:
			if ip, ok := h.st.LookupAAAA(unfqdn(name)); ok {
				m.Answer = append(m.Answer, &dns.AAAA{
					Hdr:  dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 0},
					AAAA: ip,
				})
			}
		case dns.TypePTR:
			if ip := ptrToIP(name); ip != nil {
				if host, ok := h.st.ReverseLookup(ip); ok {
					m.Answer = append(m.Answer, &dns.PTR{
						Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypePTR, Class: dns.ClassINET, Ttl: 0},
						Ptr: dns.Fqdn(host),
					})
				}
			}
		}
	}

	if len(m.Answer) == 0 {
		m.Rcode = dns.RcodeNameError // NXDOMAIN
	}
	_ = w.WriteMsg(m)
}

func unfqdn(s string) string { return dns.CanonicalName(s)[:max(0, len(dns.CanonicalName(s))-1)] }

func ptrToIP(name string) net.IP {
	// dns has no public reverse parser; use the helper.
	ip, err := reverseToIP(name)
	if err != nil {
		return nil
	}
	return ip
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
```

Add `internal/dnssrv/reverse.go` (parses `*.in-addr.arpa` / `*.ip6.arpa` back to an IP):
```go
package dnssrv

import (
	"fmt"
	"strings"

	"github.com/miekg/dns"
	"net"
)

// reverseToIP converts an in-addr.arpa / ip6.arpa name back to a net.IP.
func reverseToIP(name string) (net.IP, error) {
	n := strings.ToLower(dns.CanonicalName(name))
	switch {
	case strings.HasSuffix(n, ".in-addr.arpa."):
		labels := strings.Split(strings.TrimSuffix(n, ".in-addr.arpa."), ".")
		if len(labels) != 4 {
			return nil, fmt.Errorf("bad in-addr.arpa")
		}
		// labels are reversed octets
		ip := fmt.Sprintf("%s.%s.%s.%s", labels[3], labels[2], labels[1], labels[0])
		if p := net.ParseIP(ip); p != nil {
			return p, nil
		}
		return nil, fmt.Errorf("bad v4 octets")
	case strings.HasSuffix(n, ".ip6.arpa."):
		nib := strings.Split(strings.TrimSuffix(n, ".ip6.arpa."), ".")
		if len(nib) != 32 {
			return nil, fmt.Errorf("bad ip6.arpa")
		}
		var hex [32]byte
		for i := 0; i < 32; i++ {
			hex[31-i] = nib[i][0]
		}
		var b strings.Builder
		for i := 0; i < 32; i++ {
			b.WriteByte(hex[i])
			if i%4 == 3 && i != 31 {
				b.WriteByte(':')
			}
		}
		if p := net.ParseIP(b.String()); p != nil {
			return p, nil
		}
		return nil, fmt.Errorf("bad v6 nibbles")
	}
	return nil, fmt.Errorf("not a reverse name")
}
```

Note: the `unfqdn` helper above is convoluted; replace its body with the simpler `strings.TrimSuffix(s, ".")` after lowercasing - adjust if the test reveals a mismatch. Verify against `TestResolveA`.

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/dnssrv/`
Expected: PASS. If `unfqdn` misbehaves, simplify to:
```go
func unfqdn(s string) string { return strings.TrimSuffix(strings.ToLower(s), ".") }
```
(and add `"strings"` to imports), then re-run.

- [ ] **Step 6: Commit**

```bash
git add internal/dnssrv/ go.mod go.sum
git commit -m "feat: DNS responder for A/AAAA/PTR with TTL 0"
```

---

## Task 8: DNS-channel registration (`:`-prefixed qname)

A DNS query whose qname starts with `:` is a registration command, not a lookup. Answer with an immediate empty NOERROR so `dig` returns at once.

**Files:**
- Modify: `internal/dnssrv/dnssrv.go`
- Test: `internal/dnssrv/dnssrv_test.go` (add)

- [ ] **Step 1: Write the failing test**

Append to `internal/dnssrv/dnssrv_test.go`:
```go
func TestRegisterViaDNSQName(t *testing.T) {
	st := store.New()
	h := New(st, now)
	// qname ":+host.a" - registers the source IP (10.0.0.5 from testWriter)
	m := query(h, ":+host.a", dns.TypeA)
	if m.Rcode != dns.RcodeSuccess {
		t.Fatalf("rcode = %d, want NOERROR", m.Rcode)
	}
	if ip, ok := st.LookupA("host.a"); !ok || ip.String() != "10.0.0.5" {
		t.Fatalf("registration via DNS failed: %v,%v", ip, ok)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/dnssrv/`
Expected: FAIL - registration not happening, NXDOMAIN returned.

- [ ] **Step 3: Write minimal implementation**

In `ServeDNS`, immediately after `m.Authoritative = true` and before the `if len(req.Question) == 1` block, insert:
```go
	if len(req.Question) == 1 && strings.HasPrefix(req.Question[0].Name, ":") {
		h.handleRegister(w, req, m)
		return
	}
```
Add the method (and ensure `"strings"`, plus imports for protocol/server, are present):
```go
func (h *Handler) handleRegister(w dns.ResponseWriter, req *dns.Msg, m *dns.Msg) {
	token := unfqdn(req.Question[0].Name) // ":+name" or ":-name"
	src := remoteIP(w)
	if cmd, err := protocol.Parse(token[1:]); err == nil {
		_ = server.Apply(h.st, cmd, src, h.now())
	}
	// minimal empty NOERROR so dig returns immediately
	_ = w.WriteMsg(m)
}

func remoteIP(w dns.ResponseWriter) net.IP {
	switch a := w.RemoteAddr().(type) {
	case *net.UDPAddr:
		return a.IP
	case *net.TCPAddr:
		return a.IP
	}
	return nil
}
```
Add imports: `"plotka/internal/protocol"` and `"plotka/internal/server"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/dnssrv/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/dnssrv/
git commit -m "feat: DNS-channel registration via ':'-prefixed qname"
```

---

## Task 9: HTTP channel

**Files:**
- Create: `internal/httpapi/httpapi.go`
- Test: `internal/httpapi/httpapi_test.go`

- [ ] **Step 1: Write the failing test**

`internal/httpapi/httpapi_test.go`:
```go
package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"plotka/internal/store"
)

func now() time.Time { return time.Unix(1000, 0) }

func TestPostRegisters(t *testing.T) {
	st := store.New()
	h := New(st, now)
	req := httptest.NewRequest(http.MethodPost, "/host.a", nil)
	req.RemoteAddr = "10.0.0.5:9999"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code %d", rr.Code)
	}
	if ip, ok := st.LookupA("host.a"); !ok || ip.String() != "10.0.0.5" {
		t.Fatalf("got %v,%v", ip, ok)
	}
}

func TestDeleteDeregisters(t *testing.T) {
	st := store.New()
	st.Register("host.a", parse("10.0.0.5"), now())
	h := New(st, now)
	req := httptest.NewRequest(http.MethodDelete, "/host.a", nil)
	req.RemoteAddr = "10.0.0.5:9999"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if _, ok := st.LookupA("host.a"); ok {
		t.Fatal("expected deregistered")
	}
}

func TestLegacyGetPrefixRegisters(t *testing.T) {
	st := store.New()
	h := New(st, now)
	req := httptest.NewRequest(http.MethodGet, "/+host.a", nil)
	req.RemoteAddr = "10.0.0.5:9999"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if _, ok := st.LookupA("host.a"); !ok {
		t.Fatal("legacy GET /+name should register")
	}
}

func TestGetResolves(t *testing.T) {
	st := store.New()
	st.Register("host.a", parse("10.0.0.7"), now())
	h := New(st, now)
	req := httptest.NewRequest(http.MethodGet, "/host.a", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Body.String() != "10.0.0.7\n" {
		t.Fatalf("body = %q", rr.Body.String())
	}
}
```
Add a small helper in the test file:
```go
import "net"

func parse(s string) net.IP { return net.ParseIP(s) }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/httpapi/`
Expected: FAIL - `undefined: New`.

- [ ] **Step 3: Write minimal implementation**

`internal/httpapi/httpapi.go`:
```go
// Package httpapi implements the HTTP registration channel.
//   POST|PUT /[addr.]name  -> register
//   DELETE   /name         -> deregister
//   GET /+[addr.]name | /-name -> legacy back-compat (op in path)
//   GET /name              -> resolve (debug)
package httpapi

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"plotka/internal/protocol"
	"plotka/internal/server"
	"plotka/internal/store"
)

type Handler struct {
	st  *store.Store
	now func() time.Time
}

func New(st *store.Store, now func() time.Time) *Handler {
	return &Handler{st: st, now: now}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	src := hostIP(r.RemoteAddr)

	switch r.Method {
	case http.MethodPost, http.MethodPut:
		h.apply(w, protocol.Command{Op: protocol.OpRegister}, path, src)
	case http.MethodDelete:
		h.apply(w, protocol.Command{Op: protocol.OpDeregister}, path, src)
	case http.MethodGet:
		if strings.HasPrefix(path, "+") || strings.HasPrefix(path, "-") {
			cmd, err := protocol.Parse(path)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := server.Apply(h.st, cmd, src, h.now()); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		// resolve
		if ip, ok := h.st.LookupA(path); ok {
			fmt.Fprintf(w, "%s\n", ip)
			return
		}
		if ip, ok := h.st.LookupAAAA(path); ok {
			fmt.Fprintf(w, "%s\n", ip)
			return
		}
		http.NotFound(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// apply parses a path of the form "[addr.]name" for POST/PUT/DELETE, where the
// op comes from the HTTP method, and applies it.
func (h *Handler) apply(w http.ResponseWriter, base protocol.Command, path string, src net.IP) {
	addr, name := splitAddrPath(path)
	base.Addr = addr
	base.Name = name
	if name == "" {
		http.Error(w, "empty name", http.StatusBadRequest)
		return
	}
	if err := server.Apply(h.st, base, src, h.now()); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// splitAddrPath parses "[10.0.0.1].name" -> ("10.0.0.1","name"); "name" -> ("","name").
func splitAddrPath(p string) (addr, name string) {
	if strings.HasPrefix(p, "[") {
		end := strings.IndexByte(p, ']')
		if end >= 0 && strings.HasPrefix(p[end+1:], ".") {
			return p[1:end], p[end+2:]
		}
		return "", ""
	}
	return "", p
}

func hostIP(remoteAddr string) net.IP {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	return net.ParseIP(host)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/httpapi/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/httpapi/
git commit -m "feat: HTTP registration channel (REST verbs + legacy GET)"
```

---

## Task 10: Single-port listener - TCP cmux + UDP dispatch

**Files:**
- Create: `internal/listener/listener.go`
- Test: `internal/listener/listener_test.go`

- [ ] **Step 1: Add the dependency**

Run:
```bash
go get github.com/soheilhy/cmux@latest
```

- [ ] **Step 2: Write the failing test (UDP dispatch classifier)**

The classifier is the testable core (full socket wiring is integration-tested in Task 13). `internal/listener/listener_test.go`:
```go
package listener

import "testing"

func TestClassifyUDP(t *testing.T) {
	cases := []struct {
		in   []byte
		want kind
	}{
		{[]byte(":+host\n"), kindRaw},
		{[]byte(":-host"), kindRaw},
		{mustDNSQuery("host.a"), kindDNS},
	}
	for _, c := range cases {
		if got := classifyUDP(c.in); got != c.want {
			t.Errorf("classifyUDP(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
```
Add the DNS-packet test helper in the same file:
```go
import "github.com/miekg/dns"

func mustDNSQuery(name string) []byte {
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(name), dns.TypeA)
	b, err := m.Pack()
	if err != nil {
		panic(err)
	}
	return b
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/listener/`
Expected: FAIL - `undefined: classifyUDP`.

- [ ] **Step 4: Write minimal implementation**

`internal/listener/listener.go`:
```go
// Package listener serves DNS, raw, and HTTP on one IP:port. UDP carries DNS +
// raw; TCP carries DNS + raw + HTTP, split by first byte via cmux.
package listener

import (
	"github.com/miekg/dns"
)

type kind int

const (
	kindDNS kind = iota
	kindRaw
)

// classifyUDP decides whether a UDP datagram is a raw command or a DNS message.
// Rule: if it parses as a DNS message, it's DNS; else if it starts with ':',
// it's raw. (A ':'-prefixed raw payload never parses as valid DNS.)
func classifyUDP(p []byte) kind {
	var m dns.Msg
	if err := m.Unpack(p); err == nil {
		return kindDNS
	}
	if len(p) > 0 && p[0] == ':' {
		return kindRaw
	}
	return kindDNS // let the DNS server reject malformed input
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/listener/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/listener/ go.mod go.sum
git commit -m "feat: UDP protocol classifier (DNS vs raw)"
```

---

## Task 11: Listener wiring - bind UDP + TCP, route to handlers

**Files:**
- Modify: `internal/listener/listener.go`
- Test: `internal/listener/serve_test.go`

- [ ] **Step 1: Write the failing test (end-to-end over real sockets)**

`internal/listener/serve_test.go`:
```go
package listener

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/miekg/dns"
	"plotka/internal/dnssrv"
	"plotka/internal/httpapi"
	"plotka/internal/store"
)

func now() time.Time { return time.Unix(1000, 0) }

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestServeRawUDPThenDNSResolve(t *testing.T) {
	st := store.New()
	port := freePort(t)
	srv := &Server{
		Addr:    "127.0.0.1",
		Port:    port,
		Store:   st,
		DNS:     dnssrv.New(st, now),
		HTTP:    httpapi.New(st, now),
		Now:     now,
	}
	go srv.Serve()
	defer srv.Close()
	waitListening(t, port)

	// register host.x = 10.1.2.3 via raw UDP, explicit addr
	c, _ := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	fmt.Fprint(c, ":+[10.1.2.3].host.x")
	c.Close()
	time.Sleep(50 * time.Millisecond)

	// resolve via DNS UDP
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn("host.x"), dns.TypeA)
	resp, err := dns.Exchange(m, fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Answer) != 1 || resp.Answer[0].(*dns.A).A.String() != "10.1.2.3" {
		t.Fatalf("resolve failed: %+v", resp.Answer)
	}
}

func TestServeHTTPRegister(t *testing.T) {
	st := store.New()
	port := freePort(t)
	srv := &Server{Addr: "127.0.0.1", Port: port, Store: st, DNS: dnssrv.New(st, now), HTTP: httpapi.New(st, now), Now: now}
	go srv.Serve()
	defer srv.Close()
	waitListening(t, port)

	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/web.host", port), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if _, ok := st.LookupA("web.host"); !ok {
		t.Fatal("HTTP register failed")
	}
}

func waitListening(t *testing.T, port int) {
	for i := 0; i < 50; i++ {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not start listening")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/listener/ -run TestServe`
Expected: FAIL - `undefined: Server`.

- [ ] **Step 3: Write minimal implementation**

Append to `internal/listener/listener.go`:
```go
import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/soheilhy/cmux"
	"plotka/internal/dnssrv"
	"plotka/internal/httpapi"
	"plotka/internal/raw"
	"plotka/internal/store"
)

type Server struct {
	Addr string
	Port int

	Store *store.Store
	DNS   *dnssrv.Handler
	HTTP  *httpapi.Handler
	Now   func() time.Time

	udp     *net.UDPConn
	tcp     net.Listener
	httpSrv *http.Server
	dnsTCP  *dns.Server
}

func (s *Server) Serve() error {
	hostport := fmt.Sprintf("%s:%d", s.Addr, s.Port)

	// --- UDP ---
	uaddr, err := net.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return err
	}
	s.udp, err = net.ListenUDP("udp", uaddr)
	if err != nil {
		return err
	}
	go s.serveUDP()

	// --- TCP (cmux) ---
	s.tcp, err = net.Listen("tcp", hostport)
	if err != nil {
		return err
	}
	mux := cmux.New(s.tcp)
	// raw: first byte ':'
	rawL := mux.Match(cmux.PrefixMatcher(":"))
	// HTTP: methods
	httpL := mux.Match(cmux.HTTP1Fast())
	// DNS over TCP: everything else
	dnsL := mux.Match(cmux.Any())

	s.httpSrv = &http.Server{Handler: s.HTTP}
	go s.httpSrv.Serve(httpL)

	s.dnsTCP = &dns.Server{Listener: dnsL, Handler: s.DNS}
	go s.dnsTCP.ActivateAndServe()

	go s.serveRawTCP(rawL)

	return mux.Serve()
}

func (s *Server) serveUDP() {
	buf := make([]byte, 4096)
	for {
		n, addr, err := s.udp.ReadFromUDP(buf)
		if err != nil {
			return
		}
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		switch classifyUDP(pkt) {
		case kindRaw:
			raw.Handle(s.Store, pkt, addr.IP, s.Now)
		default:
			go s.answerDNSUDP(pkt, addr)
		}
	}
}

func (s *Server) answerDNSUDP(pkt []byte, addr *net.UDPAddr) {
	var req dns.Msg
	if err := req.Unpack(pkt); err != nil {
		return
	}
	w := &udpWriter{conn: s.udp, addr: addr}
	s.DNS.ServeDNS(w, &req)
}

func (s *Server) serveRawTCP(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			buf := make([]byte, 512)
			n, _ := c.Read(buf)
			if n > 0 {
				ip := connIP(c.RemoteAddr())
				raw.Handle(s.Store, buf[:n], ip, s.Now)
			}
		}(conn)
	}
}

func (s *Server) Close() error {
	if s.udp != nil {
		s.udp.Close()
	}
	if s.httpSrv != nil {
		s.httpSrv.Shutdown(context.Background())
	}
	if s.dnsTCP != nil {
		s.dnsTCP.Shutdown()
	}
	if s.tcp != nil {
		s.tcp.Close()
	}
	return nil
}

func connIP(a net.Addr) net.IP {
	if t, ok := a.(*net.TCPAddr); ok {
		return t.IP
	}
	return nil
}
```
Add `internal/listener/udpwriter.go` (a `dns.ResponseWriter` over the shared UDP socket):
```go
package listener

import (
	"net"

	"github.com/miekg/dns"
)

type udpWriter struct {
	conn *net.UDPConn
	addr *net.UDPAddr
}

func (w *udpWriter) WriteMsg(m *dns.Msg) error {
	b, err := m.Pack()
	if err != nil {
		return err
	}
	_, err = w.conn.WriteToUDP(b, w.addr)
	return err
}

func (w *udpWriter) Write(b []byte) (int, error) { return w.conn.WriteToUDP(b, w.addr) }
func (w *udpWriter) LocalAddr() net.Addr         { return w.conn.LocalAddr() }
func (w *udpWriter) RemoteAddr() net.Addr        { return w.addr }
func (w *udpWriter) Close() error                { return nil }
func (w *udpWriter) TsigStatus() error           { return nil }
func (w *udpWriter) TsigTimersOnly(bool)          {}
func (w *udpWriter) Hijack()                      {}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/listener/`
Expected: PASS. (If cmux's `PrefixMatcher` needs a reader that consumes the byte, confirm the raw handler still sees the full line; cmux replays matched bytes to the accepted connection, so `c.Read` returns the original payload including the leading `:`.)

- [ ] **Step 5: Commit**

```bash
git add internal/listener/
git commit -m "feat: single-port listener (UDP DNS+raw, TCP cmux DNS+raw+HTTP)"
```

---

## Task 12: Admin unix socket - server side + client calls

A line protocol over the unix socket: `LIST`, `SET <name> <ip>`, `DELETE <name>`, `PURGE`. Responses are text.

**Files:**
- Create: `internal/admin/admin.go`
- Test: `internal/admin/admin_test.go`

- [ ] **Step 1: Write the failing test**

`internal/admin/admin_test.go`:
```go
package admin

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"plotka/internal/store"
)

func now() time.Time { return time.Unix(1000, 0) }

func TestSetListDeletePurge(t *testing.T) {
	st := store.New()
	sock := filepath.Join(t.TempDir(), "plotka.sock")
	srv, err := Listen(sock, st, now)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	if out := call(t, sock, "SET host.a 10.0.0.5"); out != "OK\n" {
		t.Fatalf("SET => %q", out)
	}
	if out := call(t, sock, "LIST"); out == "" {
		t.Fatal("LIST empty")
	}
	if _, ok := st.LookupA("host.a"); !ok {
		t.Fatal("SET did not register")
	}
	call(t, sock, "DELETE host.a")
	if _, ok := st.LookupA("host.a"); ok {
		t.Fatal("DELETE did not remove")
	}
	if out := call(t, sock, "PURGE"); out == "" {
		t.Fatal("PURGE no response")
	}
}

func call(t *testing.T, sock, cmd string) string {
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Write([]byte(cmd + "\n"))
	buf := make([]byte, 4096)
	c.SetReadDeadline(time.Now().Add(time.Second))
	n, _ := c.Read(buf)
	return string(buf[:n])
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/admin/`
Expected: FAIL - `undefined: Listen`.

- [ ] **Step 3: Write minimal implementation**

`internal/admin/admin.go`:
```go
// Package admin is the local unix-socket control plane for `plotka client`.
// Line protocol: LIST | SET <name> <ip> | DELETE <name> | PURGE.
package admin

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"plotka/internal/store"
)

type Server struct {
	l    net.Listener
	sock string
}

// MaxTTL is consulted by PURGE; set by the server wiring. Default large.
var MaxTTL = 7 * 24 * time.Hour

func Listen(sock string, st *store.Store, now func() time.Time) (*Server, error) {
	_ = os.Remove(sock) // stale socket from a previous run
	l, err := net.Listen("unix", sock)
	if err != nil {
		return nil, err
	}
	s := &Server{l: l, sock: sock}
	go s.accept(st, now)
	return s, nil
}

func (s *Server) accept(st *store.Store, now func() time.Time) {
	for {
		c, err := s.l.Accept()
		if err != nil {
			return
		}
		go handle(c, st, now)
	}
}

func handle(c net.Conn, st *store.Store, now func() time.Time) {
	defer c.Close()
	line, _ := bufio.NewReader(c).ReadString('\n')
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return
	}
	switch strings.ToUpper(fields[0]) {
	case "LIST":
		for _, it := range st.List() {
			fmt.Fprintf(c, "%s\t%s\t%s\n", it.Name, it.IP, it.TS.Format(time.RFC3339))
		}
	case "SET":
		if len(fields) != 3 {
			fmt.Fprint(c, "ERR usage: SET <name> <ip>\n")
			return
		}
		ip := net.ParseIP(fields[2])
		if ip == nil {
			fmt.Fprint(c, "ERR invalid ip\n")
			return
		}
		st.Register(fields[1], ip, now())
		fmt.Fprint(c, "OK\n")
	case "DELETE":
		if len(fields) != 2 {
			fmt.Fprint(c, "ERR usage: DELETE <name>\n")
			return
		}
		st.Delete(fields[1])
		fmt.Fprint(c, "OK\n")
	case "PURGE":
		n := st.Purge(MaxTTL, now())
		fmt.Fprintf(c, "OK purged %d\n", n)
	default:
		fmt.Fprintf(c, "ERR unknown command %q\n", fields[0])
	}
}

func (s *Server) Close() error {
	err := s.l.Close()
	_ = os.Remove(s.sock)
	return err
}

// Call connects to the socket, sends one command line, and returns the reply.
func Call(sock, cmd string) (string, error) {
	c, err := net.Dial("unix", sock)
	if err != nil {
		return "", err
	}
	defer c.Close()
	fmt.Fprintln(c, cmd)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return b.String(), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/admin/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/admin/
git commit -m "feat: unix-socket admin control plane (LIST/SET/DELETE/PURGE)"
```

---

## Task 13: Server wiring - flags, purge timer, --register re-assert

**Files:**
- Modify: `cmd/plotka/server.go`
- Create: `internal/server/run.go`
- Test: `internal/server/run_test.go`

- [ ] **Step 1: Write the failing test (purge loop + register re-assert tick)**

`internal/server/run_test.go`:
```go
package server

import (
	"net"
	"testing"
	"time"

	"plotka/internal/store"
)

func TestReassertStatics(t *testing.T) {
	st := store.New()
	clock := time.Unix(1000, 0)
	statics := []Static{{Name: "registry.vm", IP: net.ParseIP("10.53.53.53")}}
	ReassertStatics(st, statics, clock)
	if ip, ok := st.LookupA("registry.vm"); !ok || !ip.Equal(net.ParseIP("10.53.53.53")) {
		t.Fatalf("static not registered: %v,%v", ip, ok)
	}
	// later tick refreshes ts
	clock2 := time.Unix(5000, 0)
	ReassertStatics(st, statics, clock2)
	// purge with small ttl at clock2 must keep it (ts just refreshed)
	if n := st.Purge(time.Second, clock2.Add(500*time.Millisecond)); n != 0 {
		t.Fatalf("purged %d, expected static kept", n)
	}
}

func TestParseStatic(t *testing.T) {
	s, err := ParseStatic("10.53.53.53:registry.vm")
	if err != nil || s.Name != "registry.vm" || !s.IP.Equal(net.ParseIP("10.53.53.53")) {
		t.Fatalf("got %+v err %v", s, err)
	}
	if _, err := ParseStatic("garbage"); err == nil {
		t.Fatal("expected error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/server/ -run 'TestReassert|TestParseStatic'`
Expected: FAIL - `undefined: Static`.

- [ ] **Step 3: Write minimal implementation**

`internal/server/run.go`:
```go
package server

import (
	"fmt"
	"net"
	"strings"
	"time"

	"plotka/internal/store"
)

// Static is a name/IP pair from a --register flag, re-asserted on a timer.
type Static struct {
	Name string
	IP   net.IP
}

// ParseStatic parses "ip:name" (e.g. "10.53.53.53:registry.vm"). For IPv6 use
// brackets: "[2001:db8::1]:name".
func ParseStatic(s string) (Static, error) {
	host := s
	var name string
	if strings.HasPrefix(s, "[") {
		end := strings.IndexByte(s, ']')
		if end < 0 || !strings.HasPrefix(s[end+1:], ":") {
			return Static{}, fmt.Errorf("bad --register %q", s)
		}
		host = s[1:end]
		name = s[end+2:]
	} else {
		i := strings.IndexByte(s, ':')
		if i < 0 {
			return Static{}, fmt.Errorf("bad --register %q (want ip:name)", s)
		}
		host = s[:i]
		name = s[i+1:]
	}
	ip := net.ParseIP(host)
	if ip == nil || name == "" {
		return Static{}, fmt.Errorf("bad --register %q", s)
	}
	return Static{Name: name, IP: ip}, nil
}

// ReassertStatics registers every static at time now (refreshing ts).
func ReassertStatics(st *store.Store, statics []Static, now time.Time) {
	for _, s := range statics {
		st.Register(s.Name, s.IP, now)
	}
}

// RunLoops starts the purge and re-assert tickers; it blocks until stop closes.
func RunLoops(st *store.Store, statics []Static, maxttl, purgeEvery, reassertEvery time.Duration, now func() time.Time, stop <-chan struct{}) {
	ReassertStatics(st, statics, now())
	purgeT := time.NewTicker(purgeEvery)
	reassertT := time.NewTicker(reassertEvery)
	defer purgeT.Stop()
	defer reassertT.Stop()
	for {
		select {
		case <-stop:
			return
		case <-purgeT.C:
			st.Purge(maxttl, now())
		case <-reassertT.C:
			ReassertStatics(st, statics, now())
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/server/`
Expected: PASS.

- [ ] **Step 5: Wire `cmd/plotka/server.go` (no new test; covered by manual smoke in Task 15)**

Replace `cmd/plotka/server.go`:
```go
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"plotka/internal/admin"
	"plotka/internal/dnssrv"
	"plotka/internal/httpapi"
	"plotka/internal/listener"
	"plotka/internal/server"
	"plotka/internal/store"
)

type staticList []server.Static

func (s *staticList) String() string { return fmt.Sprintf("%v", *s) }
func (s *staticList) Set(v string) error {
	st, err := server.ParseStatic(v)
	if err != nil {
		return err
	}
	*s = append(*s, st)
	return nil
}

func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	regBind := fs.String("registry-bind", "10.53.53.53", "service (DNS+registry+HTTP) bind IP")
	regPort := fs.Int("registry-port", 53, "service port")
	sock := fs.String("admin-socket", "/run/plotka/admin.sock", "unix admin socket path")
	maxttl := fs.Duration("maxttl", 7*24*time.Hour, "purge records older than this")
	purgeEvery := fs.Duration("purge-interval", time.Hour, "how often to run purge")
	reassertEvery := fs.Duration("reassert-interval", time.Hour, "how often to refresh --register statics")
	var statics staticList
	fs.Var(&statics, "register", "static ip:name, repeatable")
	if err := fs.Parse(args); err != nil {
		return err
	}

	st := store.New()
	now := time.Now
	admin.MaxTTL = *maxttl

	adm, err := admin.Listen(*sock, st, now)
	if err != nil {
		return fmt.Errorf("admin socket: %w", err)
	}
	defer adm.Close()

	lsrv := &listener.Server{
		Addr:  *regBind,
		Port:  *regPort,
		Store: st,
		DNS:   dnssrv.New(st, now),
		HTTP:  httpapi.New(st, now),
		Now:   now,
	}
	go func() {
		if err := lsrv.Serve(); err != nil {
			fmt.Fprintln(os.Stderr, "listener:", err)
		}
	}()
	defer lsrv.Close()

	stop := make(chan struct{})
	go server.RunLoops(st, statics, *maxttl, *purgeEvery, *reassertEvery, now, stop)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stop)
	return nil
}
```

Run: `go build ./...`
Expected: builds clean.

- [ ] **Step 6: Commit**

```bash
git add cmd/plotka/server.go internal/server/run.go
git commit -m "feat: server wiring - flags, purge + re-assert loops"
```

---

## Task 14: Client subcommand over the admin socket

**Files:**
- Modify: `cmd/plotka/client.go`
- Test: `cmd/plotka/client_test.go`

- [ ] **Step 1: Write the failing test**

`cmd/plotka/client_test.go`:
```go
package main

import (
	"path/filepath"
	"testing"
	"time"

	"plotka/internal/admin"
	"plotka/internal/store"
)

func TestClientSetViaSocket(t *testing.T) {
	st := store.New()
	sock := filepath.Join(t.TempDir(), "p.sock")
	srv, err := admin.Listen(sock, st, func() time.Time { return time.Unix(1000, 0) })
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	if err := clientCmd(sock, []string{"set", "host.a", "10.0.0.5"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.LookupA("host.a"); !ok {
		t.Fatal("client set did not reach store")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/plotka/ -run TestClientSet`
Expected: FAIL - `undefined: clientCmd`.

- [ ] **Step 3: Write minimal implementation**

Replace `cmd/plotka/client.go`:
```go
package main

import (
	"flag"
	"fmt"
	"strings"

	"plotka/internal/admin"
)

func runClient(args []string) error {
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	sock := fs.String("admin-socket", "/run/plotka/admin.sock", "unix admin socket path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return clientCmd(*sock, fs.Args())
}

// clientCmd maps a client subcommand to an admin line and prints the reply.
func clientCmd(sock string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: plotka client <list|set|delete|purge> ...")
	}
	var line string
	switch args[0] {
	case "list":
		line = "LIST"
	case "set":
		if len(args) != 3 {
			return fmt.Errorf("usage: plotka client set <name> <ip>")
		}
		line = fmt.Sprintf("SET %s %s", args[1], args[2])
	case "delete":
		if len(args) != 2 {
			return fmt.Errorf("usage: plotka client delete <name>")
		}
		line = "DELETE " + args[1]
	case "purge":
		line = "PURGE"
	default:
		return fmt.Errorf("unknown client op %q", args[0])
	}
	out, err := admin.Call(sock, line)
	if err != nil {
		return err
	}
	fmt.Print(strings.TrimRight(out, "\n") + "\n")
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/plotka/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/plotka/client.go cmd/plotka/client_test.go
git commit -m "feat: client subcommand over admin socket"
```

---

## Task 15: Full build, vet, and end-to-end smoke test

**Files:**
- Create: `packaging/plotka.service`
- Create: `README.md`

- [ ] **Step 1: Build and vet**

Run:
```bash
go build ./... && go vet ./... && go test ./...
```
Expected: all pass.

- [ ] **Step 2: Manual smoke test on a high port (no root needed)**

Run (terminal A):
```bash
go run ./cmd/plotka server --registry-bind 127.0.0.1 --registry-port 5354 \
  --admin-socket /tmp/plotka.sock --register 127.0.0.1:registry.vm
```
Run (terminal B):
```bash
# raw register over TCP
printf ':+[10.9.9.9].box.lab' | nc -w1 127.0.0.1 5354
# resolve
dig @127.0.0.1 -p 5354 box.lab +short          # expect 10.9.9.9
dig @127.0.0.1 -p 5354 registry.vm +short       # expect 127.0.0.1
# register over DNS channel (note the ':' marker dodges dig's flag parsing)
dig @127.0.0.1 -p 5354 :+dns.box +short
dig @127.0.0.1 -p 5354 dns.box +short            # expect 127.0.0.1 (source IP)
# HTTP register + resolve
curl -s -X POST http://127.0.0.1:5354/web.box
curl -s http://127.0.0.1:5354/web.box            # expect 127.0.0.1
# admin
go run ./cmd/plotka client --admin-socket /tmp/plotka.sock list
```
Expected: each resolve returns the IP shown in the comment.

- [ ] **Step 3: Write the systemd unit**

`packaging/plotka.service`:
```ini
[Unit]
Description=plotka dynamic DNS registry
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/plotka server \
  --registry-bind 10.53.53.53 --registry-port 53 \
  --admin-socket /run/plotka/admin.sock \
  --maxttl 168h --register 10.53.53.53:registry.vm
RuntimeDirectory=plotka
User=plotka
DynamicUser=yes
AmbientCapabilities=CAP_NET_BIND_SERVICE
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 4: Write a short README**

`README.md` - capture: what it is (one paragraph), the registration grammar (`:+name`, `:-name`, `:+[addr].name`), the three channels with one example each (from Task 15 smoke test), the server flags, and a pointer to `docs/superpowers/specs/2026-06-15-plotka-design.md`. Note clearly: **clustering/gossip is a follow-up (plotka-cluster); this build is single-node.**

- [ ] **Step 5: Commit**

```bash
git add packaging/ README.md
git commit -m "chore: systemd unit, README, smoke-test checklist"
```

---

## Self-Review notes (for the implementer)

- **Spec coverage:** grammar (Task 2), source-IP vs explicit/NAT addr (Task 5), A+AAAA+PTR+TTL0+NXDOMAIN (Tasks 7-8), three channels (raw 6/11, DNS 7-8, HTTP 9), single-port mux (10-11), purge/maxttl + heartbeat-as-register + --register re-assert (4,13), unix-socket client (12,14), systemd AmbientCapabilities (15). No persistence (nothing to build). Gossip deliberately deferred to plotka-cluster.
- **Deferred to plotka-cluster:** `--bind/--port` (gossip), `--join`, `--advertise` auto-derivation, `--gossip-key`/env/file, tombstones for replicated deletes, push-pull timing. In this single-node build, `delete` is a hard delete (no tombstone needed without gossip).
- **Known wrinkle:** the `unfqdn` helper in Task 7 - if the miekg/dns canonicalization path is awkward, use the simpler `strings.TrimSuffix(strings.ToLower(s), ".")` shown in Task 7 Step 5.
