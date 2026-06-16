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
