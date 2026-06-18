package gossip

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// Member is a snapshot of a cluster node for `client cluster status`.
type Member struct {
	Name  string `json:"name"`
	Addr  string `json:"addr"` // advertised ip:port
	State string `json:"state"`
	Seen  string `json:"seen"` // age since this node's last membership event
}

// eventLogger implements memberlist.EventDelegate. It optionally logs
// joins/leaves to w and records the time of each node's last membership event.
type eventLogger struct {
	w    io.Writer
	mu   sync.Mutex
	seen map[string]time.Time
}

func newEventLogger(w io.Writer) *eventLogger {
	return &eventLogger{w: w, seen: make(map[string]time.Time)}
}

func (e *eventLogger) mark(name string) {
	e.mu.Lock()
	e.seen[name] = time.Now()
	e.mu.Unlock()
}

func (e *eventLogger) lastSeen(name string) (time.Time, bool) {
	e.mu.Lock()
	t, ok := e.seen[name]
	e.mu.Unlock()
	return t, ok
}

func (e *eventLogger) NotifyJoin(n *memberlist.Node) {
	e.mark(n.Name)
	if e.w != nil {
		fmt.Fprintf(e.w, "plotka: node joined %q (%s:%d)\n", n.Name, n.Addr, n.Port)
	}
}

func (e *eventLogger) NotifyLeave(n *memberlist.Node) {
	e.mark(n.Name)
	if e.w != nil {
		fmt.Fprintf(e.w, "plotka: node left %q (%s:%d)\n", n.Name, n.Addr, n.Port)
	}
}

func (e *eventLogger) NotifyUpdate(n *memberlist.Node) {
	e.mark(n.Name)
}

func stateString(s memberlist.NodeStateType) string {
	switch s {
	case memberlist.StateAlive:
		return "alive"
	case memberlist.StateSuspect:
		return "suspect"
	case memberlist.StateDead:
		return "dead"
	case memberlist.StateLeft:
		return "left"
	default:
		return "unknown"
	}
}

// fmtAge renders a duration compactly: "5s", "3m", "2h", "4d".
func fmtAge(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
