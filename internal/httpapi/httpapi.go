// Package httpapi implements the HTTP channel.
//   POST|PUT /name        -> register (source IP)
//   POST|PUT /ip/name     -> register (explicit IP)
//   DELETE   /name        -> deregister
//   GET /name             -> forward query: IP(s), 404 if none
//   GET /ip               -> reverse query: name, 404 if none
package httpapi

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"plotka/internal/protocol"
	"plotka/internal/server"
	"plotka/internal/store"
)

type Handler struct {
	st  *store.Store
	now func() time.Time
}

func New(st *store.Store, now func() time.Time) *Handler { return &Handler{st: st, now: now} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	segs := splitPath(r.URL.Path)
	src := hostIP(r.RemoteAddr)

	switch r.Method {
	case http.MethodPost, http.MethodPut:
		h.register(w, segs, src)
	case http.MethodDelete:
		if len(segs) != 1 {
			http.Error(w, "usage: DELETE /name", http.StatusBadRequest)
			return
		}
		if err := server.Apply(h.st, protocol.Command{Op: protocol.OpDeregister, Name: segs[0]}, src, h.now()); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		h.query(w, segs)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) register(w http.ResponseWriter, segs []string, src net.IP) {
	cmd := protocol.Command{Op: protocol.OpRegister}
	switch len(segs) {
	case 1:
		cmd.Name = segs[0]
	case 2:
		if net.ParseIP(segs[0]) == nil {
			http.Error(w, "first segment must be an IP", http.StatusBadRequest)
			return
		}
		cmd.Addr = segs[0]
		cmd.Name = segs[1]
	default:
		http.Error(w, "usage: POST /name or /ip/name", http.StatusBadRequest)
		return
	}
	if err := server.Apply(h.st, cmd, src, h.now()); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) query(w http.ResponseWriter, segs []string) {
	if len(segs) != 1 || segs[0] == "" {
		http.Error(w, "usage: GET /name or /ip", http.StatusBadRequest)
		return
	}
	seg := segs[0]
	if ip := net.ParseIP(seg); ip != nil {
		if name, ok := h.st.ReverseLookup(ip); ok {
			fmt.Fprintf(w, "%s\n", name)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	var wrote bool
	if ip, ok := h.st.LookupA(seg); ok {
		fmt.Fprintf(w, "%s\n", ip)
		wrote = true
	}
	if ip, ok := h.st.LookupAAAA(seg); ok {
		fmt.Fprintf(w, "%s\n", ip)
		wrote = true
	}
	if !wrote {
		w.WriteHeader(http.StatusNotFound)
	}
}

// splitPath splits "/a/b" -> ["a","b"]; "/" -> []. Empty segments dropped.
func splitPath(p string) []string {
	var out []string
	for _, s := range strings.Split(p, "/") {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func hostIP(remoteAddr string) net.IP {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	return net.ParseIP(host)
}
