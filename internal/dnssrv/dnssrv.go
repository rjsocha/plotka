// Package dnssrv is plotka's authoritative, non-recursive DNS responder.
// It answers A/AAAA/PTR from the store with TTL 0 (local server, no caching)
// and NXDOMAIN for anything unknown. Registration via ':'-prefixed qnames is
// added in a later task.
package dnssrv

import (
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"
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

func (h *Handler) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(req)
	m.Authoritative = true

	if len(req.Question) == 1 && strings.HasPrefix(req.Question[0].Name, ":") {
		h.handleRegister(w, req, m)
		return
	}

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

func unfqdn(s string) string { return strings.TrimSuffix(strings.ToLower(s), ".") }

func ptrToIP(name string) net.IP {
	ip, err := reverseToIP(name)
	if err != nil {
		return nil
	}
	return ip
}

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
