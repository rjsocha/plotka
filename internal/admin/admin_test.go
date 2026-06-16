package admin

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"plotka/internal/store"
)

func now() time.Time { return time.Unix(1000, 0) }

func TestSetListDeletePurge(t *testing.T) {
	st := store.New()
	sock := filepath.Join(t.TempDir(), "plotka.sock")
	srv, err := Listen(sock, st, now)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	if out := call(t, sock, "SET host.a 10.0.0.5"); out != "OK\n" {
		t.Fatalf("SET => %q", out)
	}
	if out := call(t, sock, "LIST"); out == "" {
		t.Fatal("LIST empty")
	}
	if _, ok := st.LookupA("host.a"); !ok {
		t.Fatal("SET did not register")
	}
	call(t, sock, "DELETE host.a")
	if _, ok := st.LookupA("host.a"); ok {
		t.Fatal("DELETE did not remove")
	}
	if out := call(t, sock, "PURGE"); out == "" {
		t.Fatal("PURGE no response")
	}
}

func call(t *testing.T, sock, cmd string) string {
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Write([]byte(cmd + "\n"))
	buf := make([]byte, 4096)
	c.SetReadDeadline(time.Now().Add(time.Second))
	n, _ := c.Read(buf)
	return string(buf[:n])
}
