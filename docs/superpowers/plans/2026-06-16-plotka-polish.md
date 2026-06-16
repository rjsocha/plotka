# plotka polish (feedback round 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the first round of post-testing feedback: immutable `--register` records, per-protocol listener ports with a relocate model, an HTTP scheme redesign, cluster visibility (join/leave logs + `client cluster status`), and packaging/naming fixes.

**Architecture:** Build on the merged single-node + gossip cluster. Five themes: (1) store gains an immutable "static" record class; (2) the listener moves from a single muxed port to a relocate model where each protocol (dns/http/tcp) lives on a dedicated port or the opt-in mux; (3) HTTP is redesigned to path-segment registration + GET-as-query; (4) gossip exposes member events and a member list; (5) CLI/admin gain `cluster status` and a clearer `list`.

**Tech Stack:** Go, existing deps (miekg/dns, soheilhy/cmux, hashicorp/memberlist).

Design context: `docs/superpowers/specs/2026-06-15-plotka-design.md` and the two prior plans.

## Locked decisions (from the feedback conversation)

- **New default listeners:** mux OFF; `--registry-dns-port 53`, `--registry-http-port 80`, `--registry-tcp-port 2000`. Mux is opt-in via `--registry-port` (default `0`).
- **Relocate model:** a protocol with a dedicated `--registry-<p>-port` leaves the mux; the mux (`--registry-port`, if >0) carries only the protocols without a dedicated port. Ports must be distinct; at least one protocol must be enabled, else startup error.
- **Channel naming:** user-facing name is **"tcp"**, not "raw" (the internal package stays `raw`; only flags/help/docs say tcp).
- **tcp channel is TCP-only** (decision to confirm in review): UDP serves only DNS. `classifyUDP` is removed. UDP registration remains via `dig :+name`.
- **HTTP redesign (variant B):**
  - `POST|PUT /name` -> register, source IP; `POST|PUT /ip/name` -> register, explicit IP
  - `DELETE /name` -> deregister
  - `GET /name` -> forward query: 200 + IP(s) one per line (A and/or AAAA), 404 if none
  - `GET /ip` -> reverse query: 200 + name, 404 if none (segment parses as IP -> reverse)
  - legacy `GET /+name` removed
- **`--register` records are immutable (static):** a dynamic registration/deletion cannot change or remove a static record; static-vs-static is LWW. `list` marks static entries.
- **env rename:** `PLOTKA_GOSSIP_KEY` -> `PLOTKA_CLUSTER_KEY`. Remove the keyless WARNING line.
- **admin socket default:** `/run/plotka/plotka.sock`.
- **cluster visibility:** log node join/leave; `plotka client cluster status` lists members (name, advertised addr:port, state).
- **systemd:** `DynamicUser=yes`, `RuntimeDirectory=plotka`, `RuntimeDirectoryMode=0700`, `AmbientCapabilities=CAP_NET_BIND_SERVICE`, default-flag ExecStart; `plotka client` runs as root.

---

## Task 1: Store - immutable (static) records

**Files:**
- Modify: `internal/store/delta.go`, `internal/store/store.go`
- Test: `internal/store/static_test.go`

- [ ] **Step 1: Write the failing test**

`internal/store/static_test.go`:
```go
package store

import (
	"net"
	"testing"
	"time"
)

func TestStaticBeatsDynamic(t *testing.T) {
	s := New()
	s.RegisterStatic("reg.vm", net.ParseIP("10.53.53.53"), time.Unix(1000, 0))
	// a NEWER dynamic registration must NOT override a static record
	s.Register("reg.vm", net.ParseIP("10.9.9.9"), time.Unix(2000, 0))
	if ip, _ := s.LookupA("reg.vm"); !ip.Equal(net.ParseIP("10.53.53.53")) {
		t.Fatalf("dynamic overrode static: %v", ip)
	}
	// a dynamic delete must NOT remove a static record
	s.Delete("reg.vm", time.Unix(3000, 0))
	if _, ok := s.LookupA("reg.vm"); !ok {
		t.Fatal("dynamic delete removed a static record")
	}
}

func TestStaticUpdatesStaticLWW(t *testing.T) {
	s := New()
	s.RegisterStatic("reg.vm", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	s.RegisterStatic("reg.vm", net.ParseIP("10.0.0.2"), time.Unix(2000, 0)) // newer static
	if ip, _ := s.LookupA("reg.vm"); !ip.Equal(net.ParseIP("10.0.0.2")) {
		t.Fatalf("static-vs-static should be LWW: %v", ip)
	}
}

func TestStaticTakesOverDynamic(t *testing.T) {
	s := New()
	s.Register("h", net.ParseIP("10.0.0.1"), time.Unix(2000, 0))
	// static asserts authority even with an older ts
	s.RegisterStatic("h", net.ParseIP("10.0.0.2"), time.Unix(1000, 0))
	if ip, _ := s.LookupA("h"); !ip.Equal(net.ParseIP("10.0.0.2")) {
		t.Fatalf("static should take over dynamic: %v", ip)
	}
}

func TestListMarksStatic(t *testing.T) {
	s := New()
	s.RegisterStatic("reg.vm", net.ParseIP("10.0.0.1"), time.Unix(1000, 0))
	s.Register("dyn", net.ParseIP("10.0.0.2"), time.Unix(1000, 0))
	var staticSeen, dynSeen bool
	for _, it := range s.List() {
		if it.Name == "reg.vm" && it.Static {
			staticSeen = true
		}
		if it.Name == "dyn" && !it.Static {
			dynSeen = true
		}
	}
	if !staticSeen || !dynSeen {
		t.Fatalf("List must mark static vs dynamic: %+v", s.List())
	}
}
```

- [ ] **Step 2: Run `go test ./internal/store/ -run 'Static|ListMarks'` - expect FAIL.**

- [ ] **Step 3: Modify `internal/store/delta.go`** - add a `Static` field:
```go
type Delta struct {
	Name    string `json:"n"`
	V6      bool   `json:"6,omitempty"`
	IP      string `json:"i,omitempty"`
	TSNanos int64  `json:"t"`
	Deleted bool   `json:"d,omitempty"`
	Static  bool   `json:"s,omitempty"`
}
```

- [ ] **Step 4: Modify `internal/store/store.go`.**

(a) add `static bool` to `record`:
```go
type record struct {
	ip      net.IP
	ts      time.Time
	deleted bool
	static  bool
}
```

(b) In `applyLocked`, the static precedence is checked BEFORE the timestamp LWW, and static records are exempt from the BUG-2 age check. Replace the body's guard section. The full new `applyLocked` is:
```go
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
		// static takes over a dynamic record regardless of ts (operator config wins)
		return s.writeLocked(e, name, dl)
	}

	// BUG-2 guard: never accept a non-static live delta older than maxttl
	// (would resurrect after a tombstone is purged). Static is exempt - it is
	// re-asserted by its owner and must not silently expire here.
	if !dl.Static && !dl.Deleted && s.maxttl > 0 && s.nowTime().Sub(time.Unix(0, dl.TSNanos)) > s.maxttl {
		return false
	}

	// LWW with a deterministic, node-independent tie-break.
	if cur != nil {
		if dl.TSNanos < cur.ts.UnixNano() {
			return false
		}
		if dl.TSNanos == cur.ts.UnixNano() {
			switch {
			case cur.deleted:
				return false
			case dl.Deleted:
				// incoming tombstone over live: accept
			default:
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

// writeLocked installs the delta's record into entry e (rev index maintained).
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
```
NOTE: `writeLocked` assumes `e` is already in `s.fwd` for the static-takeover path. In that path `cur != nil` so `e` exists. Keep the `if e == nil` insert before the normal write path as shown.

(c) `Register` stays as-is (builds a non-static Delta). Add `RegisterStatic`:
```go
// RegisterStatic sets an immutable static record (from --register / config).
func (s *Store) RegisterStatic(name string, ip net.IP, ts time.Time) {
	dl := Delta{Name: name, V6: !isV4(ip), IP: ip.String(), TSNanos: ts.UnixNano(), Static: true}
	s.mu.Lock()
	changed := s.applyLocked(dl)
	cb := s.onChange
	s.mu.Unlock()
	if changed && cb != nil {
		cb(dl)
	}
}
```

(d) Snapshot must carry `Static`:
```go
		dl := Delta{Name: name, V6: v6, TSNanos: r.ts.UnixNano(), Deleted: r.deleted, Static: r.static}
```

(e) `ListItem` gains `Static`, and `List` populates it:
```go
type ListItem struct {
	Name   string
	IP     string
	TS     time.Time
	Static bool
}
```
```go
		if e.a != nil && !e.a.deleted {
			out = append(out, ListItem{name, e.a.ip.String(), e.a.ts, e.a.static})
		}
		if e.aaaa != nil && !e.aaaa.deleted {
			out = append(out, ListItem{name, e.aaaa.ip.String(), e.aaaa.ts, e.aaaa.static})
		}
```

- [ ] **Step 5: Run `go test ./internal/store/` - expect PASS** (existing + new). The existing `internal/server` apply tests and admin tests still pass (they use dynamic Register/Delete).

- [ ] **Step 6: Commit**
```bash
git add internal/store/
git commit -m "feat(store): immutable static records (--register authority)"
```

---

## Task 2: HTTP redesign - path-segment registration + GET-as-query

**Files:**
- Rewrite: `internal/httpapi/httpapi.go`
- Test: `internal/httpapi/httpapi_test.go` (replace)

- [ ] **Step 1: Replace the test file**

`internal/httpapi/httpapi_test.go`:
```go
package httpapi

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"plotka/internal/store"
)

func now() time.Time { return time.Unix(1000, 0) }

func do(h *Handler, method, target, remote string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	if remote != "" {
		req.RemoteAddr = remote
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestPostNameSourceIP(t *testing.T) {
	st := store.New()
	h := New(st, now)
	if rr := do(h, http.MethodPost, "/host.a", "10.0.0.5:1"); rr.Code != 200 {
		t.Fatalf("code %d", rr.Code)
	}
	if ip, ok := st.LookupA("host.a"); !ok || ip.String() != "10.0.0.5" {
		t.Fatalf("got %v,%v", ip, ok)
	}
}

func TestPostIPName(t *testing.T) {
	st := store.New()
	h := New(st, now)
	do(h, http.MethodPut, "/10.1.2.3/host.a", "203.0.113.9:1")
	if ip, _ := st.LookupA("host.a"); !ip.Equal(net.ParseIP("10.1.2.3")) {
		t.Fatalf("got %v", ip)
	}
}

func TestDeleteName(t *testing.T) {
	st := store.New()
	st.Register("host.a", net.ParseIP("10.0.0.5"), now())
	h := New(st, now)
	do(h, http.MethodDelete, "/host.a", "10.0.0.5:1")
	if _, ok := st.LookupA("host.a"); ok {
		t.Fatal("expected deregistered")
	}
}

func TestGetNameForward(t *testing.T) {
	st := store.New()
	st.Register("host.a", net.ParseIP("10.0.0.7"), now())
	h := New(st, now)
	rr := do(h, http.MethodGet, "/host.a", "")
	if rr.Code != 200 || rr.Body.String() != "10.0.0.7\n" {
		t.Fatalf("code %d body %q", rr.Code, rr.Body.String())
	}
}

func TestGetNameMissing404(t *testing.T) {
	h := New(store.New(), now)
	if rr := do(h, http.MethodGet, "/nope", ""); rr.Code != 404 {
		t.Fatalf("code %d, want 404", rr.Code)
	}
}

func TestGetIPReverse(t *testing.T) {
	st := store.New()
	st.Register("host.a", net.ParseIP("10.0.0.7"), now())
	h := New(st, now)
	rr := do(h, http.MethodGet, "/10.0.0.7", "")
	if rr.Code != 200 || rr.Body.String() != "host.a\n" {
		t.Fatalf("code %d body %q", rr.Code, rr.Body.String())
	}
}

func TestGetDualStackBothLines(t *testing.T) {
	st := store.New()
	st.Register("dual", net.ParseIP("10.0.0.7"), now())
	st.Register("dual", net.ParseIP("2001:db8::1"), now())
	h := New(st, now)
	rr := do(h, http.MethodGet, "/dual", "")
	if rr.Body.String() != "10.0.0.7\n2001:db8::1\n" {
		t.Fatalf("body %q", rr.Body.String())
	}
}
```

- [ ] **Step 2: Run `go test ./internal/httpapi/` - expect FAIL (old behavior).**

- [ ] **Step 3: Replace `internal/httpapi/httpapi.go`:**
```go
// Package httpapi implements the HTTP channel.
//   POST|PUT /name        -> register (source IP)
//   POST|PUT /ip/name     -> register (explicit IP)
//   DELETE   /name        -> deregister
//   GET /name             -> forward query: IP(s), 404 if none
//   GET /ip               -> reverse query: name, 404 if none
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

func New(st *store.Store, now func() time.Time) *Handler { return &Handler{st: st, now: now} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	segs := splitPath(r.URL.Path)
	src := hostIP(r.RemoteAddr)

	switch r.Method {
	case http.MethodPost, http.MethodPut:
		h.register(w, segs, src)
	case http.MethodDelete:
		if len(segs) != 1 {
			http.Error(w, "usage: DELETE /name", http.StatusBadRequest)
			return
		}
		if err := server.Apply(h.st, protocol.Command{Op: protocol.OpDeregister, Name: segs[0]}, src, h.now()); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		h.query(w, segs)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// register: /name (source IP) or /ip/name (explicit).
func (h *Handler) register(w http.ResponseWriter, segs []string, src net.IP) {
	var cmd protocol.Command
	cmd.Op = protocol.OpRegister
	switch len(segs) {
	case 1:
		cmd.Name = segs[0]
	case 2:
		if net.ParseIP(segs[0]) == nil {
			http.Error(w, "first segment must be an IP", http.StatusBadRequest)
			return
		}
		cmd.Addr = segs[0]
		cmd.Name = segs[1]
	default:
		http.Error(w, "usage: POST /name or /ip/name", http.StatusBadRequest)
		return
	}
	if err := server.Apply(h.st, cmd, src, h.now()); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// query: GET /<seg> - seg parses as IP -> reverse; else -> forward.
func (h *Handler) query(w http.ResponseWriter, segs []string) {
	if len(segs) != 1 || segs[0] == "" {
		http.Error(w, "usage: GET /name or /ip", http.StatusBadRequest)
		return
	}
	seg := segs[0]
	if ip := net.ParseIP(seg); ip != nil {
		if name, ok := h.st.ReverseLookup(ip); ok {
			fmt.Fprintf(w, "%s\n", name)
			return
		}
		http.NotFound(w, &http.Request{})
		return
	}
	var wrote bool
	if ip, ok := h.st.LookupA(seg); ok {
		fmt.Fprintf(w, "%s\n", ip)
		wrote = true
	}
	if ip, ok := h.st.LookupAAAA(seg); ok {
		fmt.Fprintf(w, "%s\n", ip)
		wrote = true
	}
	if !wrote {
		http.NotFound(w, &http.Request{})
	}
}

// splitPath splits "/a/b" -> ["a","b"]; "/" -> []. Empty segments dropped.
func splitPath(p string) []string {
	var out []string
	for _, s := range strings.Split(p, "/") {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func hostIP(remoteAddr string) net.IP {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	return net.ParseIP(host)
}
```
NOTE: `http.NotFound(w, &http.Request{})` writes a 404; passing a zero Request is fine (NotFound only writes status+body). If `go vet` dislikes it, use `w.WriteHeader(http.StatusNotFound)` instead.

- [ ] **Step 4: Run `go test ./internal/httpapi/` - expect PASS.**

- [ ] **Step 5: Commit**
```bash
git add internal/httpapi/
git commit -m "feat(httpapi): path-segment registration + GET-as-query, drop legacy GET"
```

---

## Task 3: Listener refactor - relocate-model ports (dns/http/tcp dedicated or muxed)

This replaces the single-port `Server` with a `Config`-driven set of listeners. UDP serves DNS only (tcp channel is TCP-only); `classifyUDP` is removed.

**Files:**
- Rewrite: `internal/listener/listener.go`
- Keep: `internal/listener/udpwriter.go` (unchanged)
- Replace: `internal/listener/listener_test.go`, `internal/listener/serve_test.go`

- [ ] **Step 1: Replace the tests**

Delete `internal/listener/listener_test.go` (the `classifyUDP` test - that function is gone). Replace `internal/listener/serve_test.go`:
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
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitListening(t *testing.T, port int) {
	for i := 0; i < 100; i++ {
		if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond); err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("port %d not listening", port)
}

func newServers(t *testing.T, cfg Config, st *store.Store) *Servers {
	cfg.Bind = "127.0.0.1"
	cfg.Store = st
	cfg.DNS = dnssrv.New(st, now)
	cfg.HTTP = httpapi.New(st, now)
	cfg.Now = now
	s, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// dedicated ports: dns + http + tcp, mux off
func TestDedicatedPorts(t *testing.T) {
	st := store.New()
	dnsP, httpP, tcpP := freePort(t), freePort(t), freePort(t)
	s := newServers(t, Config{DNSPort: dnsP, HTTPPort: httpP, TCPPort: tcpP}, st)
	defer s.Close()
	waitListening(t, tcpP)

	// tcp line register
	c, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", tcpP))
	fmt.Fprint(c, ":+[10.1.1.1].t.host")
	c.Close()
	time.Sleep(80 * time.Millisecond)
	if ip, ok := st.LookupA("t.host"); !ok || ip.String() != "10.1.1.1" {
		t.Fatalf("tcp register: %v,%v", ip, ok)
	}

	// dns resolve
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn("t.host"), dns.TypeA)
	resp, err := dns.Exchange(m, fmt.Sprintf("127.0.0.1:%d", dnsP))
	if err != nil || len(resp.Answer) != 1 {
		t.Fatalf("dns resolve: %v %+v", err, resp)
	}

	// http register
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/web.host", httpP), nil)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if _, ok := st.LookupA("web.host"); !ok {
		t.Fatal("http register failed")
	}
}

// mux: all three on one port
func TestMuxPort(t *testing.T) {
	st := store.New()
	p := freePort(t)
	s := newServers(t, Config{MuxPort: p}, st)
	defer s.Close()
	waitListening(t, p)

	c, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", p))
	fmt.Fprint(c, ":+[10.2.2.2].m.host")
	c.Close()
	time.Sleep(80 * time.Millisecond)
	if _, ok := st.LookupA("m.host"); !ok {
		t.Fatal("mux tcp register failed")
	}
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn("m.host"), dns.TypeA)
	resp, _ := dns.Exchange(m, fmt.Sprintf("127.0.0.1:%d", p))
	if len(resp.Answer) != 1 {
		t.Fatalf("mux dns resolve: %+v", resp.Answer)
	}
}

func TestValidationErrors(t *testing.T) {
	// nothing enabled
	if _, err := Start(Config{Bind: "127.0.0.1"}); err == nil {
		t.Fatal("expected error when no listener enabled")
	}
	// port collision (dedicated == mux)
	if _, err := Start(Config{Bind: "127.0.0.1", MuxPort: 5300, DNSPort: 5300}); err == nil {
		t.Fatal("expected error on port collision")
	}
}
```

- [ ] **Step 2: Run `go test ./internal/listener/` - expect FAIL (Config/Start/Servers undefined).**

- [ ] **Step 3: Rewrite `internal/listener/listener.go`:**
```go
// Package listener serves the DNS, HTTP, and tcp-line registration channels.
// Each protocol runs on a dedicated port, or shares the opt-in mux port. UDP
// serves DNS only; the tcp channel is TCP-only.
package listener

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/soheilhy/cmux"
	"plotka/internal/dnssrv"
	"plotka/internal/httpapi"
	"plotka/internal/raw"
	"plotka/internal/store"
)

type proto int

const (
	pDNS proto = iota
	pHTTP
	pTCP
)

// Config describes the listener layout. A protocol with a dedicated port (>0)
// runs there; otherwise it runs on MuxPort (if >0); otherwise it is disabled.
type Config struct {
	Bind     string
	MuxPort  int // 0 = no mux
	DNSPort  int // 0 = on mux or disabled
	HTTPPort int
	TCPPort  int

	Store *store.Store
	DNS   *dnssrv.Handler
	HTTP  *httpapi.Handler
	Now   func() time.Time
}

// Servers is the running set, closeable.
type Servers struct {
	mu      sync.Mutex
	closers []func()
}

// Start binds all listeners per cfg and starts serving. Returns an error on
// misconfiguration (no protocol enabled, or a port collision).
func Start(cfg Config) (*Servers, error) {
	// port -> set of protocols
	ports := map[int]map[proto]bool{}
	assign := func(p proto, dedicated int) {
		port := dedicated
		if port == 0 {
			port = cfg.MuxPort
		}
		if port == 0 {
			return // disabled
		}
		if ports[port] == nil {
			ports[port] = map[proto]bool{}
		}
		ports[port][p] = true
	}
	assign(pDNS, cfg.DNSPort)
	assign(pHTTP, cfg.HTTPPort)
	assign(pTCP, cfg.TCPPort)

	if len(ports) == 0 {
		return nil, fmt.Errorf("no listener enabled: set --registry-port or a --registry-<proto>-port")
	}
	// collision: a dedicated port must not equal another assigned port unless
	// both arrived via the mux. Detect: a dedicated port that also hosts a
	// different protocol whose dedicated port differs => only legal if it IS
	// the mux port. We approximate by rejecting any dedicated port equal to the
	// mux port, or two dedicated ports being equal.
	dedicated := []int{cfg.DNSPort, cfg.HTTPPort, cfg.TCPPort}
	for i, di := range dedicated {
		if di == 0 {
			continue
		}
		if di == cfg.MuxPort {
			return nil, fmt.Errorf("port %d used by both a dedicated protocol and the mux", di)
		}
		for j := i + 1; j < len(dedicated); j++ {
			if dedicated[j] == di {
				return nil, fmt.Errorf("port %d assigned to two protocols; use the mux for shared ports", di)
			}
		}
	}

	s := &Servers{}
	for port, set := range ports {
		if err := s.startPort(cfg, port, set); err != nil {
			s.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *Servers) add(f func()) { s.mu.Lock(); s.closers = append(s.closers, f); s.mu.Unlock() }

// startPort binds one port. A single protocol => dedicated listener; multiple
// => cmux on TCP. UDP (for DNS) is bound when DNS is present.
func (s *Servers) startPort(cfg Config, port int, set map[proto]bool) error {
	hostport := fmt.Sprintf("%s:%d", cfg.Bind, port)

	// UDP: only DNS uses it.
	if set[pDNS] {
		uaddr, err := net.ResolveUDPAddr("udp", hostport)
		if err != nil {
			return err
		}
		udp, err := net.ListenUDP("udp", uaddr)
		if err != nil {
			return err
		}
		s.add(func() { udp.Close() })
		go serveDNSUDP(udp, cfg.DNS)
	}

	tcpL, err := net.Listen("tcp", hostport)
	if err != nil {
		return err
	}
	s.add(func() { tcpL.Close() })

	if len(set) == 1 {
		switch {
		case set[pDNS]:
			ds := &dns.Server{Listener: tcpL, Handler: cfg.DNS}
			s.add(func() { ds.Shutdown() })
			go ds.ActivateAndServe()
		case set[pHTTP]:
			hs := &http.Server{Handler: cfg.HTTP}
			s.add(func() { hs.Shutdown(context.Background()) })
			go hs.Serve(tcpL)
		case set[pTCP]:
			go serveTCPLine(tcpL, cfg.Store, cfg.Now)
		}
		return nil
	}

	// shared port: cmux split by first byte.
	m := cmux.New(tcpL)
	if set[pTCP] {
		go serveTCPLine(m.Match(cmux.PrefixMatcher(":")), cfg.Store, cfg.Now)
	}
	if set[pHTTP] {
		hs := &http.Server{Handler: cfg.HTTP}
		s.add(func() { hs.Shutdown(context.Background()) })
		go hs.Serve(m.Match(cmux.HTTP1Fast()))
	}
	if set[pDNS] {
		ds := &dns.Server{Listener: m.Match(cmux.Any()), Handler: cfg.DNS}
		s.add(func() { ds.Shutdown() })
		go ds.ActivateAndServe()
	}
	go m.Serve()
	return nil
}

func (s *Servers) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.closers) - 1; i >= 0; i-- {
		s.closers[i]()
	}
	s.closers = nil
	return nil
}

func serveDNSUDP(udp *net.UDPConn, h *dnssrv.Handler) {
	buf := make([]byte, 4096)
	for {
		n, addr, err := udp.ReadFromUDP(buf)
		if err != nil {
			return
		}
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		go func() {
			var req dns.Msg
			if req.Unpack(pkt) != nil {
				return
			}
			h.ServeDNS(&udpWriter{conn: udp, addr: addr}, &req)
		}()
	}
}

func serveTCPLine(l net.Listener, st *store.Store, now func() time.Time) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			line, _ := bufio.NewReader(c).ReadString('\n')
			if line != "" {
				raw.Handle(st, []byte(line), connIP(c.RemoteAddr()), now)
			}
		}(conn)
	}
}

func connIP(a net.Addr) net.IP {
	if t, ok := a.(*net.TCPAddr); ok {
		return t.IP
	}
	return nil
}
```

- [ ] **Step 4: Run `go test ./internal/listener/` - expect PASS.** Note the build will fail in `cmd/plotka` (old `listener.Server{...}` usage) until Task 6; that is expected. Confirm the listener package's own tests pass: `go test ./internal/listener/`.

- [ ] **Step 5: Commit**
```bash
git add internal/listener/
git commit -m "feat(listener): relocate-model ports (dedicated dns/http/tcp + opt-in mux), UDP=DNS-only"
```

---

## Task 4: Gossip - join/leave events + member list

**Files:**
- Create: `internal/gossip/events.go`
- Modify: `internal/gossip/gossip.go`
- Test: `internal/gossip/members_test.go`

- [ ] **Step 1: Write the failing test**

`internal/gossip/members_test.go`:
```go
package gossip

import (
	"fmt"
	"net"
	"testing"
	"time"

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
	_ = net.ParseIP // keep import if unused otherwise
	_ = time.Second
}
```

- [ ] **Step 2: Run `go test ./internal/gossip/ -run TestMembersList` - expect FAIL (MemberList undefined).**

- [ ] **Step 3: Create `internal/gossip/events.go`:**
```go
package gossip

import (
	"fmt"
	"io"

	"github.com/hashicorp/memberlist"
)

// Member is a snapshot of a cluster node for `client cluster status`.
type Member struct {
	Name  string `json:"name"`
	Addr  string `json:"addr"` // advertised ip:port
	State string `json:"state"`
}

// eventLogger logs joins/leaves to w.
type eventLogger struct{ w io.Writer }

func (e eventLogger) NotifyJoin(n *memberlist.Node) {
	fmt.Fprintf(e.w, "plotka: node joined %q (%s:%d)\n", n.Name, n.Addr, n.Port)
}
func (e eventLogger) NotifyLeave(n *memberlist.Node) {
	fmt.Fprintf(e.w, "plotka: node left %q (%s:%d)\n", n.Name, n.Addr, n.Port)
}
func (e eventLogger) NotifyUpdate(*memberlist.Node) {}

func stateString(s memberlist.NodeStateType) string {
	switch s {
	case memberlist.StateAlive:
		return "alive"
	case memberlist.StateSuspect:
		return "suspect"
	case memberlist.StateDead:
		return "dead"
	case memberlist.StateLeft:
		return "left"
	default:
		return "unknown"
	}
}
```

- [ ] **Step 4: Modify `internal/gossip/gossip.go`.**

(a) add `EventLog io.Writer` to `Config` (after `Store`):
```go
	Store    *store.Store
	EventLog io.Writer // join/leave log sink; nil = silent
```
and add `"io"` to imports.

(b) in `Create`, wire the event delegate (after `cfg.Delegate = d`):
```go
	if c.EventLog != nil {
		cfg.Events = eventLogger{w: c.EventLog}
	}
```

(c) add `MemberList`:
```go
// MemberList returns a snapshot of known cluster nodes.
func (g *Gossip) MemberList() []Member {
	var out []Member
	for _, n := range g.ml.Members() {
		out = append(out, Member{
			Name:  n.Name,
			Addr:  fmt.Sprintf("%s:%d", n.Addr, n.Port),
			State: stateString(n.State),
		})
	}
	return out
}
```
and add `"fmt"` to the gossip.go imports if not present.

- [ ] **Step 5: Run `go test ./internal/gossip/` - expect PASS.**

- [ ] **Step 6: Commit**
```bash
git add internal/gossip/
git commit -m "feat(gossip): join/leave event logging + MemberList"
```

---

## Task 5: Admin + client - `cluster status` and clearer `list`

**Files:**
- Modify: `internal/admin/admin.go`
- Modify: `cmd/plotka/client.go`
- Test: `internal/admin/cluster_test.go`

- [ ] **Step 1: Write the failing test**

`internal/admin/cluster_test.go`:
```go
package admin

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"plotka/internal/store"
)

func TestClusterCommand(t *testing.T) {
	st := store.New()
	sock := filepath.Join(t.TempDir(), "p.sock")
	srv, err := Listen(sock, st, func() time.Time { return time.Unix(1000, 0) })
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	SetMembers(func() string { return "node-a\t10.0.0.1:7946\talive\n" })

	out, _ := Call(sock, "CLUSTER")
	if !strings.Contains(out, "node-a") {
		t.Fatalf("CLUSTER output = %q", out)
	}
}

func TestListShowsStatic(t *testing.T) {
	st := store.New()
	st.RegisterStatic("reg.vm", parseIP("10.0.0.1"), time.Unix(1000, 0))
	sock := filepath.Join(t.TempDir(), "p.sock")
	srv, _ := Listen(sock, st, func() time.Time { return time.Unix(1000, 0) })
	defer srv.Close()
	out, _ := Call(sock, "LIST")
	if !strings.Contains(out, "static") {
		t.Fatalf("LIST should mark static: %q", out)
	}
}
```
Add a tiny helper in the same file:
```go
import "net"

func parseIP(s string) net.IP { return net.ParseIP(s) }
```

- [ ] **Step 2: Run `go test ./internal/admin/ -run 'Cluster|ListShowsStatic'` - expect FAIL.**

- [ ] **Step 3: Modify `internal/admin/admin.go`.**

(a) add a package-level members provider (set by the server wiring):
```go
// membersFn returns the cluster member table (already formatted lines); set by
// the server. nil => cluster info unavailable.
var membersFn func() string

// SetMembers registers the cluster member provider for the CLUSTER command.
func SetMembers(f func() string) { membersFn = f }
```

(b) in `handle`, update the `LIST` case to mark static and add a `CLUSTER` case:
```go
	case "LIST":
		for _, it := range st.List() {
			kind := "dynamic"
			if it.Static {
				kind = "static"
			}
			fmt.Fprintf(c, "%s\t%s\t%s\t%s\n", it.Name, it.IP, it.TS.Format(time.RFC3339), kind)
		}
	case "CLUSTER":
		if membersFn == nil {
			fmt.Fprint(c, "ERR cluster info unavailable\n")
			return
		}
		fmt.Fprint(c, membersFn())
```

- [ ] **Step 4: Update `cmd/plotka/client.go`** - add the `cluster` subcommand mapping. In `clientCmd`'s switch, add:
```go
	case "cluster":
		if len(args) != 2 || args[1] != "status" {
			return fmt.Errorf("usage: plotka client cluster status")
		}
		line = "CLUSTER"
```

- [ ] **Step 5: Run `go test ./internal/admin/ ./cmd/plotka/` - expect PASS** (cmd/plotka build still red until Task 6; if so, run only `go test ./internal/admin/`).

- [ ] **Step 6: Commit**
```bash
git add internal/admin/ cmd/plotka/client.go
git commit -m "feat: admin CLUSTER command + static marker in list; client cluster status"
```

---

## Task 6: Server wiring - new flags, defaults, env rename, listener + cluster wiring

**Files:**
- Modify: `cmd/plotka/server.go`
- Modify: `internal/server/run.go` (RunLoops uses RegisterStatic now)

- [ ] **Step 1: `internal/server/run.go` - statics must register as immutable.** In `ReassertStatics`, change `st.Register` to `st.RegisterStatic`:
```go
func ReassertStatics(st *store.Store, statics []Static, now time.Time) {
	for _, s := range statics {
		st.RegisterStatic(s.Name, s.IP, now)
	}
}
```
Run `go test ./internal/server/` - the existing `TestReassertStatics` still passes (the static still resolves; purge with tiny ttl keeps it because re-assert refreshed ts - and static is exempt from the age guard but the test uses Purge directly which is unaffected).

- [ ] **Step 2: Rewrite the flag block and listener/gossip wiring in `cmd/plotka/server.go`.**

Replace the listener-related flags. Remove `regPort` mux-only assumption; add the port set:
```go
	regBind := fs.String("registry-bind", "10.53.53.53", "service bind IP (shared by all listeners)")
	muxPort := fs.Int("registry-port", 0, "mux port (DNS+HTTP+tcp on one port); 0 = mux off")
	dnsPort := fs.Int("registry-dns-port", 53, "dedicated DNS port (0 = on mux)")
	httpPort := fs.Int("registry-http-port", 80, "dedicated HTTP port (0 = on mux)")
	tcpPort := fs.Int("registry-tcp-port", 2000, "dedicated tcp-line registration port (0 = on mux)")
	sock := fs.String("admin-socket", "/run/plotka/plotka.sock", "unix admin socket path")
```
(Keep `maxttl`, `purgeEvery`, `reassertEvery`, `statics`, and the gossip flags as-is.)

Replace the `lsrv := &listener.Server{...}` block and its goroutine with:
```go
	lsrv, err := listener.Start(listener.Config{
		Bind:     *regBind,
		MuxPort:  *muxPort,
		DNSPort:  *dnsPort,
		HTTPPort: *httpPort,
		TCPPort:  *tcpPort,
		Store:    st,
		DNS:      dnssrv.New(st, now),
		HTTP:     httpapi.New(st, now),
		Now:      now,
	})
	if err != nil {
		return fmt.Errorf("listener: %w", err)
	}
	defer lsrv.Close()
```
(`err` is declared later by `admin.Listen`; move the `adm, err := admin.Listen(...)` BEFORE this block so `err` exists, then use `=`; or declare `var err error` near the top. Make it compile.)

- [ ] **Step 3: Wire gossip event log + cluster members + env rename + drop warning.**

In `gossip.Create(...)` config add `EventLog: os.Stderr`. After gossip is up, register the members provider for the admin CLUSTER command:
```go
	admin.SetMembers(func() string {
		var b strings.Builder
		for _, m := range g.MemberList() {
			fmt.Fprintf(&b, "%s\t%s\t%s\n", m.Name, m.Addr, m.State)
		}
		return b.String()
	})
```
Remove the keyless WARNING block (the `if len(key) == 0 { ... }`).

In `loadGossipKey`, rename the env var: `os.Getenv("PLOTKA_GOSSIP_KEY")` -> `os.Getenv("PLOTKA_CLUSTER_KEY")`. Update the `--gossip-key` flag help text to say `PLOTKA_CLUSTER_KEY`.

- [ ] **Step 4: Run `go build ./... && go vet ./... && go test ./...` - expect all green.**

- [ ] **Step 5: Manual smoke (new defaults need ports 53/80 -> use a non-default high-port run):**
```bash
go build -o /tmp/plotka ./cmd/plotka
/tmp/plotka server --registry-dns-port 5353 --registry-http-port 5380 --registry-tcp-port 5390 \
  --admin-socket /tmp/p.sock --register 127.0.0.1:registry.vm &
sleep 1
printf ':+[10.1.1.1].t.host' > /dev/tcp/127.0.0.1/5390
dig @127.0.0.1 -p 5353 t.host +short                 # 10.1.1.1
curl -s -X POST http://127.0.0.1:5380/w.host          # register
curl -s http://127.0.0.1:5380/w.host                  # resolve
curl -s http://127.0.0.1:5380/10.1.1.1                # reverse -> t.host
/tmp/plotka client --admin-socket /tmp/p.sock list    # registry.vm marked static
kill %1; rm -f /tmp/p.sock /tmp/plotka
```

- [ ] **Step 6: Commit**
```bash
git add cmd/plotka/ internal/server/run.go
git commit -m "feat: relocate-model port flags, new defaults, PLOTKA_CLUSTER_KEY, cluster wiring"
```

---

## Task 7: Packaging + docs + two-node smoke

**Files:**
- Rewrite: `packaging/plotka.service`
- Modify: `README.md`

- [ ] **Step 1: Rewrite `packaging/plotka.service`:**
```ini
[Unit]
Description=plotka dynamic DNS registry
After=network-online.target
Wants=network-online.target

[Service]
# Shared cluster secret: PLOTKA_CLUSTER_KEY=... (kept out of `ps`).
# Generate once: head -c 32 /dev/urandom | base64
EnvironmentFile=-/etc/plotka/cluster.env
# Default ports: DNS 53, HTTP 80, tcp-line 2000 (mux off). Admin socket
# defaults to /run/plotka/plotka.sock (RuntimeDirectory below).
ExecStart=/usr/local/bin/plotka server \
  --register 10.53.53.53:registry.vm \
  --port 7946 --join 10.0.0.11,10.0.0.12,10.0.0.13
RuntimeDirectory=plotka
RuntimeDirectoryMode=0700
DynamicUser=yes
AmbientCapabilities=CAP_NET_BIND_SERVICE
Restart=on-failure

[Install]
WantedBy=multi-user.target
```
(Note in a comment: `plotka client` must run as root because RuntimeDirectoryMode=0700 + DynamicUser; or switch to a static `User=plotka` if non-root client access is needed.)

- [ ] **Step 2: Update `README.md`** to reflect: new default ports (DNS 53 / HTTP 80 / tcp 2000, mux opt-in via `--registry-port`); the relocate model; the new HTTP scheme (`/name`, `/ip/name`, methods, GET-as-query); channel called "tcp"; `--register` immutable; `plotka client cluster status`; `PLOTKA_CLUSTER_KEY`; admin socket default. Update the channel examples (tcp-line uses `/dev/tcp`; note UDP registration is via `dig :+name`). Keep it accurate to the implemented behavior.

- [ ] **Step 3: Two-node smoke (loopback, high ports)** - run two servers with dedicated ports on `127.0.0.1`/`127.0.0.2`, gossip join, register on A, resolve on B; run `plotka client cluster status` against A and confirm both nodes are listed alive. Confirm join/leave log lines appear on stderr when B starts/stops.

- [ ] **Step 4: Final gate**
```bash
go build ./... && go vet ./... && go test -race ./...
```
Expect all green.

- [ ] **Step 5: Commit**
```bash
git add packaging/ README.md
git commit -m "docs: relocate-model ports, new HTTP scheme, cluster status, systemd 0700"
```

---

## Self-Review notes (for the implementer)

- **Decision to surface:** the tcp channel is TCP-only (UDP=DNS-only, `classifyUDP` removed). If the UDP line protocol must return, re-add a UDP raw path on the tcp/mux port.
- **Static precedence:** static beats dynamic unconditionally; static-vs-static and dynamic-vs-dynamic are LWW. Static is exempt from the BUG-2 age guard (re-asserted by its owner). A dynamic delete (`!Static`) cannot tombstone a static record.
- **Listener collision rule:** dedicated ports must be distinct and must not equal the mux port; >=1 protocol must be enabled. Two protocols intentionally sharing a port = use the mux.
- **Port 53/80 privilege:** real runs need `CAP_NET_BIND_SERVICE` (systemd) or high ports (tests/smoke use high ports).
- **Naming:** user-facing "tcp"; the internal `raw` package name is unchanged (invisible to users).
