package main

import (
	"path/filepath"
	"testing"
	"time"

	"plotka/internal/admin"
	"plotka/internal/store"
)

func TestClientSetViaSocket(t *testing.T) {
	st := store.New()
	sock := filepath.Join(t.TempDir(), "p.sock")
	srv, err := admin.Listen(sock, st, func() time.Time { return time.Unix(1000, 0) })
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	if err := clientCmd(sock, false, []string{"set", "host.a", "10.0.0.5"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.LookupA("host.a"); !ok {
		t.Fatal("client set did not reach store")
	}
}
