package httpapi

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"plotka/internal/admin"
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
	h := New(st, now, time.Hour)
	if rr := do(h, http.MethodPost, "/host.a", "10.0.0.5:1"); rr.Code != 200 {
		t.Fatalf("code %d", rr.Code)
	}
	if ip, ok := st.LookupA("host.a"); !ok || ip.String() != "10.0.0.5" {
		t.Fatalf("got %v,%v", ip, ok)
	}
}

func TestPostIPName(t *testing.T) {
	st := store.New()
	h := New(st, now, time.Hour)
	do(h, http.MethodPut, "/10.1.2.3/host.a", "203.0.113.9:1")
	if ip, _ := st.LookupA("host.a"); !ip.Equal(net.ParseIP("10.1.2.3")) {
		t.Fatalf("got %v", ip)
	}
}

func TestDeleteName(t *testing.T) {
	st := store.New()
	st.Register("host.a", net.ParseIP("10.0.0.5"), now())
	h := New(st, now, time.Hour)
	do(h, http.MethodDelete, "/host.a", "10.0.0.5:1")
	if _, ok := st.LookupA("host.a"); ok {
		t.Fatal("expected deregistered")
	}
}

func TestGetNameForward(t *testing.T) {
	st := store.New()
	st.Register("host.a", net.ParseIP("10.0.0.7"), now())
	h := New(st, now, time.Hour)
	rr := do(h, http.MethodGet, "/host.a", "")
	if rr.Code != 200 || rr.Body.String() != "10.0.0.7\n" {
		t.Fatalf("code %d body %q", rr.Code, rr.Body.String())
	}
}

func TestGetNameMissing404(t *testing.T) {
	h := New(store.New(), now, time.Hour)
	if rr := do(h, http.MethodGet, "/nope", ""); rr.Code != 404 {
		t.Fatalf("code %d, want 404", rr.Code)
	}
}

func TestGetIPReverse(t *testing.T) {
	st := store.New()
	st.Register("host.a", net.ParseIP("10.0.0.7"), now())
	h := New(st, now, time.Hour)
	rr := do(h, http.MethodGet, "/10.0.0.7", "")
	if rr.Code != 200 || rr.Body.String() != "host.a\n" {
		t.Fatalf("code %d body %q", rr.Code, rr.Body.String())
	}
}

func TestExecList(t *testing.T) {
	st := store.New()
	st.Register("b.host", net.ParseIP("10.0.0.2"), now())
	st.Register("a.host", net.ParseIP("10.0.0.1"), now())
	h := New(st, now, time.Hour)
	rr := do(h, http.MethodGet, "/exec/list", "")
	ts := now().Format(time.RFC3339)
	// equal-width columns here, so a two-space join matches tabwriter exactly.
	want := strings.Join([]string{"a.host", "10.0.0.1", "3600", ts, "dynamic"}, "  ") + "\n" +
		strings.Join([]string{"b.host", "10.0.0.2", "3600", ts, "dynamic"}, "  ") + "\n"
	if rr.Code != 200 || rr.Body.String() != want {
		t.Fatalf("code %d body %q", rr.Code, rr.Body.String())
	}
}

func TestExecListFull(t *testing.T) {
	st := store.New()
	long := strings.Repeat("a", 70)
	st.Register(long, net.ParseIP("10.0.0.1"), now())
	h := New(st, now, time.Hour)

	capped := do(h, http.MethodGet, "/exec/list", "").Body.String()
	if got := strings.Fields(capped)[0]; len(got) != nameWidth {
		t.Fatalf("capped name len %d, want %d", len(got), nameWidth)
	}
	full := do(h, http.MethodGet, "/exec/list/full", "")
	if got := strings.Fields(full.Body.String())[0]; len(got) != 70 {
		t.Fatalf("full name len %d, want 70", len(got))
	}
	if !strings.HasPrefix(full.Body.String(), long+" 10.0.0.1 ") {
		t.Fatalf("full not single-space: %q", full.Body.String())
	}
}

func TestExecUnknown(t *testing.T) {
	h := New(store.New(), now, time.Hour)
	if rr := do(h, http.MethodGet, "/exec/bogus", ""); rr.Code != 400 {
		t.Fatalf("code %d, want 400", rr.Code)
	}
}

func TestExecNodes(t *testing.T) {
	admin.SetMembers(func() string {
		return "node-a\t10.0.0.1:7946\talive\t5s\n" +
			"node-bb\t10.0.0.2:7946\tsuspect\t1m\n"
	})
	h := New(store.New(), now, time.Hour)

	aligned := do(h, http.MethodGet, "/exec/nodes", "").Body.String()
	want := "node-a   10.0.0.1:7946  alive    5s\n" +
		"node-bb  10.0.0.2:7946  suspect  1m\n"
	if aligned != want {
		t.Fatalf("aligned %q want %q", aligned, want)
	}
	full := do(h, http.MethodGet, "/exec/nodes/full", "").Body.String()
	if full != "node-a 10.0.0.1:7946 alive 5s\nnode-bb 10.0.0.2:7946 suspect 1m\n" {
		t.Fatalf("full %q", full)
	}
}

func TestGetDualStackBothLines(t *testing.T) {
	st := store.New()
	st.Register("dual", net.ParseIP("10.0.0.7"), now())
	st.Register("dual", net.ParseIP("2001:db8::1"), now())
	h := New(st, now, time.Hour)
	rr := do(h, http.MethodGet, "/dual", "")
	if rr.Body.String() != "10.0.0.7\n2001:db8::1\n" {
		t.Fatalf("body %q", rr.Body.String())
	}
}
