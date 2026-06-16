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
		st.RegisterStatic(s.Name, s.IP, now)
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
