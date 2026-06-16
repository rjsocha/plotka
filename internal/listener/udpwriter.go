package listener

import (
	"net"

	"github.com/miekg/dns"
)

type udpWriter struct {
	conn *net.UDPConn
	addr *net.UDPAddr
}

func (w *udpWriter) WriteMsg(m *dns.Msg) error {
	b, err := m.Pack()
	if err != nil {
		return err
	}
	_, err = w.conn.WriteToUDP(b, w.addr)
	return err
}

func (w *udpWriter) Write(b []byte) (int, error) { return w.conn.WriteToUDP(b, w.addr) }
func (w *udpWriter) LocalAddr() net.Addr         { return w.conn.LocalAddr() }
func (w *udpWriter) RemoteAddr() net.Addr        { return w.addr }
func (w *udpWriter) Close() error                { return nil }
func (w *udpWriter) TsigStatus() error           { return nil }
func (w *udpWriter) TsigTimersOnly(bool)         {}
func (w *udpWriter) Hijack()                     {}
