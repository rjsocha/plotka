// Package server wires the store, listeners, and timers together.
package server

import (
	"fmt"
	"net"
	"time"

	"plotka/internal/protocol"
	"plotka/internal/store"
)

// Apply executes a parsed command against the store. For registration, the IP
// is the explicit cmd.Addr if present, otherwise the connection source IP.
func Apply(st *store.Store, cmd protocol.Command, src net.IP, now time.Time) error {
	switch cmd.Op {
	case protocol.OpDeregister:
		st.Delete(cmd.Name, now)
		return nil
	case protocol.OpRegister:
		ip := src
		if cmd.Addr != "" {
			ip = net.ParseIP(cmd.Addr)
			if ip == nil {
				return fmt.Errorf("invalid address %q", cmd.Addr)
			}
		}
		if ip == nil {
			return fmt.Errorf("no source IP and no explicit address")
		}
		st.Register(cmd.Name, ip, now)
		return nil
	default:
		return fmt.Errorf("unknown op %v", cmd.Op)
	}
}
