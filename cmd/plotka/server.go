package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"plotka/internal/admin"
	"plotka/internal/dnssrv"
	"plotka/internal/gossip"
	"plotka/internal/httpapi"
	"plotka/internal/listener"
	"plotka/internal/netid"
	"plotka/internal/server"
	"plotka/internal/store"
)

type staticList []server.Static

func (s *staticList) String() string { return fmt.Sprintf("%v", *s) }
func (s *staticList) Set(v string) error {
	st, err := server.ParseStatic(v)
	if err != nil {
		return err
	}
	*s = append(*s, st)
	return nil
}

// loadRegisterFile appends static ip:name entries from path, one per line.
// Blank lines are skipped; a malformed line is warned about and skipped, not
// fatal. Each valid line uses the --register format.
func loadRegisterFile(path string, statics *staticList) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		st, err := server.ParseStatic(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "plotka: register-file %s:%d: skipping invalid line: %v\n", path, i+1, err)
			continue
		}
		*statics = append(*statics, st)
	}
	return nil
}

func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	regBind := fs.String("registry-bind", "10.53.53.53", "service bind IP (shared by all listeners)")
	muxPort := fs.Int("registry-port", 0, "mux port (DNS+HTTP+tcp on one port); 0 = mux off")
	dnsPort := fs.Int("registry-dns-port", 53, "dedicated DNS port (0 = on mux/off)")
	httpPort := fs.Int("registry-http-port", 80, "dedicated HTTP port (0 = on mux/off)")
	tcpPort := fs.Int("registry-tcp-port", 2000, "dedicated tcp-line registration port (0 = on mux/off)")
	sock := fs.String("admin-socket", "/run/plotka/admin", "unix admin socket path")
	maxttl := fs.Duration("purge-ttl", 24*time.Hour, "remove records not refreshed within this")
	purgeEvery := fs.Duration("purge-interval", 8*time.Hour, "how often the purge sweep runs")
	reassertEvery := fs.Duration("reassert-interval", time.Hour, "how often to refresh --register statics")
	var statics staticList
	fs.Var(&statics, "register", "static ip:name, repeatable")
	registerFile := fs.String("register-file", "", "file of static ip:name entries, one per line (same format as --register)")
	gBind := fs.String("bind", "", "cluster listen IP (\"\" = all interfaces)")
	gPort := fs.Int("port", 7946, "cluster port (TCP+UDP)")
	gAdvertise := fs.String("advertise", "", "cluster advertise IP (default: a host IP that is not --registry-bind)")
	gJoin := fs.String("join", "", "comma-separated seed peer host IPs (not the VIP)")
	gKey := fs.String("cluster-key", "", "shared cluster secret (16/24/32 bytes, base64); also PLOTKA_CLUSTER_KEY or --cluster-key-file")
	gKeyFile := fs.String("cluster-key-file", "", "file containing the cluster secret")
	nodeName := fs.String("node-name", "", "unique node name (default: hostname)")
	verbose := fs.Bool("verbose", false, "log record changes and gossip activity (joins/leaves, memberlist) to stderr")
	logTimestamp := fs.Bool("log-timestamp", false, "prefix memberlist log lines with date/time (off by default; the journal already timestamps)")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *registerFile != "" {
		if err := loadRegisterFile(*registerFile, &statics); err != nil {
			return err
		}
	}

	st := store.New()
	now := time.Now
	st.SetExpiry(*maxttl, now) // reject expired live deltas from stale peers
	admin.MaxTTL = *maxttl
	var eventLog io.Writer
	if *verbose {
		eventLog = os.Stderr
		st.SetEvents(func(e store.Event) {
			if e.IP != "" {
				fmt.Fprintf(os.Stderr, "plotka: %s %s -> %s\n", e.Kind, e.Name, e.IP)
			} else {
				fmt.Fprintf(os.Stderr, "plotka: %s %s\n", e.Kind, e.Name)
			}
		})
	}

	adm, err := admin.Listen(*sock, st, now)
	if err != nil {
		return fmt.Errorf("admin socket: %w", err)
	}
	defer adm.Close()

	lsrv, err := listener.Start(listener.Config{
		Bind:     *regBind,
		MuxPort:  *muxPort,
		DNSPort:  *dnsPort,
		HTTPPort: *httpPort,
		TCPPort:  *tcpPort,
		Store:    st,
		DNS:      dnssrv.New(st, now),
		HTTP:     httpapi.New(st, now),
		Now:      now,
	})
	if err != nil {
		return fmt.Errorf("listener: %w", err)
	}
	defer lsrv.Close()

	advertise := *gAdvertise
	if advertise == "" {
		a, err := netid.Advertise(*regBind)
		if err != nil {
			return fmt.Errorf("advertise: %w", err)
		}
		advertise = a
	}
	key, err := loadClusterKey(*gKey, *gKeyFile)
	if err != nil {
		return err
	}
	name := *nodeName
	if name == "" {
		if hn, _ := os.Hostname(); hn != "" {
			name = hn
		} else {
			name = advertise
		}
	}
	g, err := gossip.Create(gossip.Config{
		Name:          name,
		BindAddr:      *gBind,
		BindPort:      *gPort,
		AdvertiseAddr: advertise,
		SecretKey:     key,
		Store:         st,
		EventLog:      eventLog,
		LogTimestamp:  *logTimestamp,
	})
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}
	defer g.Close()
	seeds := splitNonEmpty(*gJoin, ",")
	if len(seeds) > 0 {
		if err := g.Join(seeds); err != nil {
			fmt.Fprintln(os.Stderr, "cluster join:", err) // non-fatal: the retry loop keeps trying while alone
		}
	}
	fmt.Fprintf(os.Stderr, "plotka: node up %q on :%d, %d member(s)\n", name, *gPort, g.Members())
	admin.SetMembers(func() string {
		var b strings.Builder
		for _, m := range g.MemberList() {
			fmt.Fprintf(&b, "%s\t%s\t%s\t%s\n", m.Name, m.Addr, m.State, m.Seen)
		}
		return b.String()
	})

	for _, s := range statics {
		fmt.Fprintf(os.Stderr, "plotka: %s registered as %s\n", s.IP, s.Name)
	}

	stop := make(chan struct{})
	go server.RunLoops(st, statics, *maxttl, *purgeEvery, *reassertEvery, now, stop)

	// Keep retrying the join while this node is alone, so cluster formation does
	// not depend on cold-start order (memberlist does not retry Join itself).
	if len(seeds) > 0 {
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					if g.Members() <= 1 {
						_ = g.Join(seeds) // best-effort; NotifyJoin logs success
					}
				}
			}
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stop)
	return nil
}

// loadClusterKey resolves the secret from flag, file, or PLOTKA_CLUSTER_KEY env
// (in that order). Empty => unencrypted cluster traffic. The value is base64-std.
func loadClusterKey(flagVal, file string) ([]byte, error) {
	raw := flagVal
	if raw == "" && file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("cluster-key-file: %w", err)
		}
		raw = strings.TrimSpace(string(b))
	}
	if raw == "" {
		raw = os.Getenv("PLOTKA_CLUSTER_KEY")
	}
	if raw == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("cluster key: not valid base64: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	default:
		return nil, fmt.Errorf("cluster key must decode to 16, 24, or 32 bytes, got %d", len(key))
	}
}

func splitNonEmpty(s, sep string) []string {
	var out []string
	for _, p := range strings.Split(s, sep) {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
