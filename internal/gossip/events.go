package gossip

import (
	"fmt"
	"io"

	"github.com/hashicorp/memberlist"
)

// Member is a snapshot of a cluster node for `client cluster status`.
type Member struct {
	Name  string `json:"name"`
	Addr  string `json:"addr"` // advertised ip:port
	State string `json:"state"`
}

// eventLogger logs joins/leaves to w.
type eventLogger struct{ w io.Writer }

func (e eventLogger) NotifyJoin(n *memberlist.Node) {
	fmt.Fprintf(e.w, "plotka: node joined %q (%s:%d)\n", n.Name, n.Addr, n.Port)
}
func (e eventLogger) NotifyLeave(n *memberlist.Node) {
	fmt.Fprintf(e.w, "plotka: node left %q (%s:%d)\n", n.Name, n.Addr, n.Port)
}
func (e eventLogger) NotifyUpdate(*memberlist.Node) {}

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
