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
	MuxPort  int
	DNSPort  int
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

// Start binds all listeners per cfg and serves. Errors on misconfiguration.
func Start(cfg Config) (*Servers, error) {
	ports := map[int]map[proto]bool{}
	assign := func(p proto, dedicated int) {
		port := dedicated
		if port == 0 {
			port = cfg.MuxPort
		}
		if port == 0 {
			return
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

func (s *Servers) startPort(cfg Config, port int, set map[proto]bool) error {
	hostport := fmt.Sprintf("%s:%d", cfg.Bind, port)

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
