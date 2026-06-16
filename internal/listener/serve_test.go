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

func TestDedicatedPorts(t *testing.T) {
	st := store.New()
	dnsP, httpP, tcpP := freePort(t), freePort(t), freePort(t)
	s := newServers(t, Config{DNSPort: dnsP, HTTPPort: httpP, TCPPort: tcpP}, st)
	defer s.Close()
	waitListening(t, tcpP)

	c, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", tcpP))
	fmt.Fprint(c, ":+[10.1.1.1].t.host")
	c.Close()
	time.Sleep(80 * time.Millisecond)
	if ip, ok := st.LookupA("t.host"); !ok || ip.String() != "10.1.1.1" {
		t.Fatalf("tcp register: %v,%v", ip, ok)
	}

	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn("t.host"), dns.TypeA)
	resp, err := dns.Exchange(m, fmt.Sprintf("127.0.0.1:%d", dnsP))
	if err != nil || len(resp.Answer) != 1 {
		t.Fatalf("dns resolve: %v %+v", err, resp)
	}

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
	if _, err := Start(Config{Bind: "127.0.0.1"}); err == nil {
		t.Fatal("expected error when no listener enabled")
	}
	if _, err := Start(Config{Bind: "127.0.0.1", MuxPort: 5300, DNSPort: 5300}); err == nil {
		t.Fatal("expected error on port collision")
	}
}
