package admin

import (
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"plotka/internal/store"
)

func parseIP(s string) net.IP { return net.ParseIP(s) }

func TestClusterCommand(t *testing.T) {
	st := store.New()
	sock := filepath.Join(t.TempDir(), "p.sock")
	srv, err := Listen(sock, st, func() time.Time { return time.Unix(1000, 0) })
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	SetMembers(func() string { return "node-a\t10.0.0.1:7946\talive\n" })

	out, _ := Call(sock, "CLUSTER")
	if !strings.Contains(out, "node-a") {
		t.Fatalf("CLUSTER output = %q", out)
	}
}

func TestSetMembersConcurrent(t *testing.T) {
	// Under -race this catches an unguarded membersFn: the server sets it while
	// the accept loop is already serving CLUSTER requests.
	st := store.New()
	sock := filepath.Join(t.TempDir(), "p.sock")
	srv, err := Listen(sock, st, func() time.Time { return time.Unix(1000, 0) })
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			SetMembers(func() string { return "x\t1.1.1.1:7946\talive\n" })
		}
		close(done)
	}()
	for i := 0; i < 50; i++ {
		_, _ = Call(sock, "CLUSTER")
	}
	<-done
}

func TestListShowsStatic(t *testing.T) {
	st := store.New()
	st.RegisterStatic("reg.vm", parseIP("10.0.0.1"), time.Unix(1000, 0))
	sock := filepath.Join(t.TempDir(), "p.sock")
	srv, _ := Listen(sock, st, func() time.Time { return time.Unix(1000, 0) })
	defer srv.Close()
	out, _ := Call(sock, "LIST")
	if !strings.Contains(out, "static") {
		t.Fatalf("LIST should mark static: %q", out)
	}
}
