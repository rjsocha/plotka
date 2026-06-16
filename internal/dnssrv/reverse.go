package dnssrv

import (
	"fmt"
	"net"
	"strings"

	"github.com/miekg/dns"
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
