// Package httpapi implements the HTTP channel.
//   POST|PUT /name        -> register (source IP)
//   POST|PUT /ip/name     -> register (explicit IP)
//   DELETE   /name        -> deregister
//   GET /name             -> forward query: IP(s), 404 if none
//   GET /ip               -> reverse query: name, 404 if none
//   GET /exec/list        -> list all records, aligned columns (name capped)
//   GET /exec/list/full   -> same records, single-space, uncapped, no alignment
package httpapi

import (
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"plotka/internal/protocol"
	"plotka/internal/server"
	"plotka/internal/store"
)

// nameWidth caps the name column in /exec/list (matches the CLI `list`).
const nameWidth = 64

type Handler struct {
	st     *store.Store
	now    func() time.Time
	maxTTL time.Duration
}

func New(st *store.Store, now func() time.Time, maxTTL time.Duration) *Handler {
	return &Handler{st: st, now: now, maxTTL: maxTTL}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
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
		if len(segs) >= 1 && segs[0] == "exec" {
			h.exec(w, segs[1:])
			return
		}
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

func (h *Handler) exec(w http.ResponseWriter, segs []string) {
	switch {
	case len(segs) == 1 && segs[0] == "list":
		h.list(w, false)
	case len(segs) == 2 && segs[0] == "list" && segs[1] == "full":
		h.list(w, true)
	default:
		http.Error(w, "usage: GET /exec/list[/full]", http.StatusBadRequest)
	}
}

// list renders all records like the CLI `list`: columns name, ip, ttl, ts, kind,
// statics first then dynamics by remaining TTL. Aligned with capped name unless
// full, in which case rows are single-space separated and the name is uncapped.
func (h *Handler) list(w http.ResponseWriter, full bool) {
	type row struct {
		static bool
		ttl    int64
		fields []string
	}
	var rows []row
	now := h.now()
	for _, it := range h.st.List() {
		kind, ttlStr := "dynamic", "-"
		var ttl int64
		if it.Static {
			kind = "static"
		} else {
			ttl = int64((h.maxTTL - now.Sub(it.TS)).Seconds())
			if ttl < 0 {
				ttl = 0
			}
			ttlStr = strconv.FormatInt(ttl, 10)
		}
		rows = append(rows, row{it.Static, ttl, []string{it.Name, it.IP, ttlStr, it.TS.Format(time.RFC3339), kind}})
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.static != b.static {
			return a.static
		}
		if !a.static && a.ttl != b.ttl {
			return a.ttl > b.ttl
		}
		return strings.Join(a.fields, "\t") < strings.Join(b.fields, "\t")
	})
	if full {
		for _, r := range rows {
			fmt.Fprintln(w, strings.Join(r.fields, " "))
		}
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, r := range rows {
		if rn := []rune(r.fields[0]); len(rn) > nameWidth {
			r.fields[0] = string(rn[:nameWidth])
		}
		fmt.Fprintln(tw, strings.Join(r.fields, "\t"))
	}
	tw.Flush()
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
