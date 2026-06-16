// Package raw implements the raw line registration channel: a ':'-prefixed
// token ("+name" / "-name" / "+[addr].name"), fire-and-forget, no response.
package raw

import (
	"net"
	"strings"
	"time"

	"plotka/internal/protocol"
	"plotka/internal/server"
	"plotka/internal/store"
)

// Handle parses one raw token and applies it. Malformed input is silently
// dropped (fire-and-forget, untrusted-but-closed network).
func Handle(st *store.Store, line []byte, src net.IP, now func() time.Time) {
	s := strings.TrimSpace(string(line))
	if !strings.HasPrefix(s, ":") {
		return
	}
	cmd, err := protocol.Parse(s[1:])
	if err != nil {
		return
	}
	_ = server.Apply(st, cmd, src, now())
}
