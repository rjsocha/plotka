package gossip

import (
	"fmt"
	"io"
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
	EventLog io.Writer // join/leave log sink; nil = silent
}

type Gossip struct {
	ml *memberlist.Memberlist
}

// Create builds a memberlist node, wires the store's onChange to the broadcast
// queue, and starts gossiping. It does not join any peers (call Join).
func Create(c Config) (*Gossip, error) {
	q := &memberlist.TransmitLimitedQueue{RetransmitMult: 3}
	d := &delegate{store: c.Store, q: q}

	cfg := memberlist.DefaultLANConfig()
	cfg.Name = c.Name
	cfg.BindAddr = c.BindAddr
	cfg.BindPort = c.BindPort
	cfg.AdvertiseAddr = c.AdvertiseAddr
	cfg.AdvertisePort = c.BindPort
	cfg.Delegate = d
	if c.EventLog != nil {
		cfg.Events = eventLogger{w: c.EventLog}
	}
	cfg.LogOutput = logDiscard{}
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

	return &Gossip{ml: ml}, nil
}

// Join contacts seed peers (host IPs, never the VIP). Safe with an empty list.
func (g *Gossip) Join(seeds []string) error {
	if len(seeds) == 0 {
		return nil
	}
	_, err := g.ml.Join(seeds)
	return err
}

func (g *Gossip) Members() int { return g.ml.NumMembers() }

// MemberList returns a snapshot of known cluster nodes.
func (g *Gossip) MemberList() []Member {
	var out []Member
	for _, n := range g.ml.Members() {
		out = append(out, Member{
			Name:  n.Name,
			Addr:  fmt.Sprintf("%s:%d", n.Addr, n.Port),
			State: stateString(n.State),
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
