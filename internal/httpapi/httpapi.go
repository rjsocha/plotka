// Package httpapi implements the HTTP channel.
//   POST|PUT /name        -> register (source IP)
//   POST|PUT /ip/name     -> register (explicit IP)
//   DELETE   /name        -> deregister
//   GET /name             -> forward query: IP(s), 404 if none
//   GET /ip               -> reverse query: name, 404 if none
//   GET /exec/list        -> list all records, aligned columns (name capped)
//   GET /exec/list/full   -> same records, single-space, uncapped, no alignment
//   GET /exec/nodes       -> cluster members (like `cluster status`), aligned
//   GET /exec/nodes/full  -> same members, single-space, no alignment
package httpapi

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"text/tabwriter"
	"time"

	"plotka/internal/admin"
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
		writeTable(w, admin.Records(h.st, h.maxTTL, h.now()), false, nameWidth)
	case len(segs) == 2 && segs[0] == "list" && segs[1] == "full":
		writeTable(w, admin.Records(h.st, h.maxTTL, h.now()), true, 0)
	case len(segs) == 1 && segs[0] == "nodes":
		h.nodes(w, false)
	case len(segs) == 2 && segs[0] == "nodes" && segs[1] == "full":
		h.nodes(w, true)
	default:
		http.Error(w, "usage: GET /exec/list[/full] or /exec/nodes[/full]", http.StatusBadRequest)
	}
}

// nodes renders the cluster member table (same provider as `cluster status`).
func (h *Handler) nodes(w http.ResponseWriter, full bool) {
	raw, ok := admin.Members()
	if !ok {
		http.Error(w, "cluster info unavailable", http.StatusServiceUnavailable)
		return
	}
	writeTable(w, raw, full, 0)
}

// writeTable renders tab-separated rows. When full, fields are joined by a
// single space with no alignment; otherwise columns are space-aligned via
// tabwriter. capName>0 truncates the first column to capName runes.
func writeTable(w http.ResponseWriter, raw string, full bool, capName int) {
	lines := strings.Split(strings.TrimRight(raw, "\n"), "\n")
	if full {
		for _, ln := range lines {
			if ln == "" {
				continue
			}
			fmt.Fprintln(w, strings.Join(strings.Split(ln, "\t"), " "))
		}
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, ln := range lines {
		if ln == "" {
			continue
		}
		fields := strings.Split(ln, "\t")
		if capName > 0 && len(fields) > 0 {
			if rn := []rune(fields[0]); len(rn) > capName {
				fields[0] = string(rn[:capName])
			}
		}
		fmt.Fprintln(tw, strings.Join(fields, "\t"))
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
