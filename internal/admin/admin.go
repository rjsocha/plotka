// Package admin is the local unix-socket control plane for `plotka client`.
// Line protocol: LIST | SET <name> <ip> | DELETE <name> | PURGE.
package admin

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"plotka/internal/store"
)

type Server struct {
	l    net.Listener
	sock string
}

// MaxTTL is consulted by PURGE; set by the server wiring. Default large.
var MaxTTL = 7 * 24 * time.Hour

// membersFn returns the cluster member table (preformatted lines); set by the
// server. nil => cluster info unavailable. Guarded because the server sets it
// after the accept loop has already started serving connections.
var (
	membersMu sync.Mutex
	membersFn func() string
)

// SetMembers registers the cluster member provider for the CLUSTER command.
func SetMembers(f func() string) {
	membersMu.Lock()
	membersFn = f
	membersMu.Unlock()
}

func members() func() string {
	membersMu.Lock()
	defer membersMu.Unlock()
	return membersFn
}

func Listen(sock string, st *store.Store, now func() time.Time) (*Server, error) {
	_ = os.Remove(sock) // stale socket from a previous run
	l, err := net.Listen("unix", sock)
	if err != nil {
		return nil, err
	}
	s := &Server{l: l, sock: sock}
	go s.accept(st, now)
	return s, nil
}

func (s *Server) accept(st *store.Store, now func() time.Time) {
	for {
		c, err := s.l.Accept()
		if err != nil {
			return
		}
		go handle(c, st, now)
	}
}

func handle(c net.Conn, st *store.Store, now func() time.Time) {
	defer c.Close()
	line, _ := bufio.NewReader(c).ReadString('\n')
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return
	}
	switch strings.ToUpper(fields[0]) {
	case "LIST":
		// Sort: statics first (never expire), then dynamics by remaining TTL
		// descending; ties broken by the rendered line (name is the first field).
		type row struct {
			static bool
			ttl    int64
			line   string
		}
		var rows []row
		for _, it := range st.List() {
			kind, ttlStr := "dynamic", "-"
			var ttl int64
			if it.Static {
				kind = "static" // never expires; ttl stays "-"
			} else {
				ttl = int64((MaxTTL - now().Sub(it.TS)).Seconds())
				if ttl < 0 {
					ttl = 0
				}
				ttlStr = strconv.FormatInt(ttl, 10)
			}
			line := fmt.Sprintf("%s\t%s\t%s\t%s\t%s", it.Name, it.IP, ttlStr, it.TS.Format(time.RFC3339), kind)
			rows = append(rows, row{it.Static, ttl, line})
		}
		sort.Slice(rows, func(i, j int) bool {
			a, b := rows[i], rows[j]
			if a.static != b.static {
				return a.static // statics on top
			}
			if !a.static && a.ttl != b.ttl {
				return a.ttl > b.ttl // dynamics: highest TTL first
			}
			return a.line < b.line
		})
		for _, r := range rows {
			fmt.Fprintln(c, r.line)
		}
	case "CLUSTER":
		mf := members()
		if mf == nil {
			fmt.Fprint(c, "ERR cluster info unavailable\n")
			return
		}
		fmt.Fprint(c, mf())
	case "SET":
		if len(fields) != 3 {
			fmt.Fprint(c, "ERR usage: SET <name> <ip>\n")
			return
		}
		ip := net.ParseIP(fields[2])
		if ip == nil {
			fmt.Fprint(c, "ERR invalid ip\n")
			return
		}
		st.Register(fields[1], ip, now())
		fmt.Fprint(c, "OK\n")
	case "DELETE":
		if len(fields) != 2 {
			fmt.Fprint(c, "ERR usage: DELETE <name>\n")
			return
		}
		st.Delete(fields[1], now())
		fmt.Fprint(c, "OK\n")
	case "PURGE":
		n := st.Purge(MaxTTL, now())
		fmt.Fprintf(c, "OK purged %d\n", n)
	default:
		fmt.Fprintf(c, "ERR unknown command %q\n", fields[0])
	}
}

func (s *Server) Close() error {
	err := s.l.Close()
	_ = os.Remove(s.sock)
	return err
}

// Call connects to the socket, sends one command line, and returns the reply.
func Call(sock, cmd string) (string, error) {
	c, err := net.Dial("unix", sock)
	if err != nil {
		return "", err
	}
	defer c.Close()
	fmt.Fprintln(c, cmd)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return b.String(), nil
}
