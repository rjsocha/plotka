package gossip

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/memberlist"
	"plotka/internal/store"
)

// Config holds the gossip parameters. PushPull defaults to memberlist's default
// when zero.
type Config struct {
	Name          string // unique node name
	BindAddr      string // host unicast IP to listen on ("" = all)
	BindPort      int    // gossip port
	AdvertiseAddr string // host unicast IP peers dial (never the VIP)
	SecretKey     []byte // 16/24/32 bytes, or nil
	PushPull      time.Duration
	Store         *store.Store
	EventLog      io.Writer // join/leave + memberlist log sink; nil = silent
	LogTimestamp  bool      // prefix memberlist log lines with date/time
}

type Gossip struct {
	ml       *memberlist.Memberlist
	events   *eventLogger
	bindPort int
}

// Create builds a memberlist node, wires the store's onChange to the broadcast
// queue, and starts gossiping. It does not join any peers (call Join).
func Create(c Config) (*Gossip, error) {
	q := &memberlist.TransmitLimitedQueue{RetransmitMult: 3}
	d := &delegate{store: c.Store, q: q}

	cfg := memberlist.DefaultWANConfig()
	cfg.Name = c.Name
	cfg.BindAddr = c.BindAddr
	cfg.BindPort = c.BindPort
	cfg.AdvertiseAddr = c.AdvertiseAddr
	cfg.AdvertisePort = c.BindPort
	cfg.Delegate = d
	ev := newEventLogger(c.EventLog)
	cfg.Events = ev
	if c.EventLog != nil {
		flags := 0
		if c.LogTimestamp {
			flags = log.LstdFlags
		}
		cfg.Logger = log.New(c.EventLog, "", flags)
	} else {
		cfg.LogOutput = logDiscard{}
	}
	if len(c.SecretKey) > 0 {
		cfg.SecretKey = c.SecretKey
	}
	if c.PushPull > 0 {
		cfg.PushPullInterval = c.PushPull
	}

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}
	q.NumNodes = func() int { return ml.NumMembers() }

	c.Store.SetOnChange(func(dl store.Delta) {
		q.QueueBroadcast(&broadcast{msg: encodeDelta(dl)})
	})

	return &Gossip{ml: ml, events: ev, bindPort: c.BindPort}, nil
}

// Join contacts seed peers (host IPs, never the VIP). Safe with an empty list.
func (g *Gossip) Join(seeds []string) error {
	if len(seeds) == 0 {
		return nil
	}
	_, err := g.ml.Join(seeds)
	return err
}

// Rejoin joins every seed that is not currently an alive member and returns how
// many seeds it tried. memberlist never rejoins dead nodes, so after a partition
// heals this is what merges the islands again. The own address is always an
// alive member, so a node never joins itself; a healthy cluster is a no-op.
func (g *Gossip) Rejoin(seeds []string) (int, error) {
	alive := map[string]bool{}
	for _, n := range g.ml.Members() {
		if n.State == memberlist.StateAlive {
			alive[net.JoinHostPort(n.Addr.String(), strconv.Itoa(int(n.Port)))] = true
		}
	}
	var missing []string
	for _, s := range seeds {
		if !g.seedAlive(s, alive) {
			missing = append(missing, s)
		}
	}
	if len(missing) == 0 {
		return 0, nil
	}
	return len(missing), g.Join(missing)
}

// seedAlive reports whether seed (ip, host, ip:port or host:port; no port =
// own cluster port, as in memberlist) resolves to an alive member address.
func (g *Gossip) seedAlive(seed string, alive map[string]bool) bool {
	host, port, err := net.SplitHostPort(seed)
	if err != nil {
		host, port = seed, strconv.Itoa(g.bindPort)
	}
	ips := []string{host}
	if net.ParseIP(host) == nil {
		if ips, err = net.LookupHost(host); err != nil {
			return false
		}
	}
	for _, ip := range ips {
		if alive[net.JoinHostPort(net.ParseIP(ip).String(), port)] {
			return true
		}
	}
	return false
}

func (g *Gossip) Members() int { return g.ml.NumMembers() }

// MemberList returns a snapshot of known cluster nodes.
func (g *Gossip) MemberList() []Member {
	var out []Member
	for _, n := range g.ml.Members() {
		seen := "-"
		if t, ok := g.events.lastSeen(n.Name); ok {
			seen = fmtAge(time.Since(t))
		}
		out = append(out, Member{
			Name:  n.Name,
			Addr:  fmt.Sprintf("%s:%d", n.Addr, n.Port),
			State: stateString(n.State),
			Seen:  seen,
		})
	}
	return out
}

func (g *Gossip) Close() error {
	_ = g.ml.Leave(time.Second)
	return g.ml.Shutdown()
}

// logDiscard silences memberlist's internal logger.
type logDiscard struct{}

func (logDiscard) Write(p []byte) (int, error) { return len(p), nil }
