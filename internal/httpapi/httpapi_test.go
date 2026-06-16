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
