// Package netid derives the gossip advertise address: a host unicast IP that
// is not the service VIP. Gossip must reach a specific node, so it cannot use
// the anycast VIP.
package netid

import (
	"fmt"
	"net"
)

// pick returns the single candidate that is not vip, or an error if zero or
// more than one remain (caller must then set --advertise explicitly).
func pick(candidates []string, vip string) (string, error) {
	var keep []string
	for _, c := range candidates {
		if c != vip {
			keep = append(keep, c)
		}
	}
	switch len(keep) {
	case 1:
		return keep[0], nil
	case 0:
		return "", fmt.Errorf("no host IP other than the registry VIP %q; set --advertise", vip)
	default:
		return "", fmt.Errorf("multiple host IPs %v; set --advertise explicitly", keep)
	}
}

// Advertise enumerates the host's global-unicast IPs and picks the one that is
// not vip. Loopback and link-local addresses are skipped.
func Advertise(vip string) (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	var cands []string
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			continue
		}
		if ip.IsGlobalUnicast() {
			cands = append(cands, ip.String())
		}
	}
	return pick(cands, vip)
}
