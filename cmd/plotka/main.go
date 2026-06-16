package main

import (
	"fmt"
	"os"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func dispatch(args []string) error {
	if len(args) == 0 {
		return runHelp()
	}
	switch args[0] {
	case "server":
		return runServer(args[1:])
	case "client":
		return runClient(args[1:])
	case "version":
		fmt.Println("plotka", version)
		return nil
	case "help", "-h", "--help":
		return runHelp()
	default:
		return fmt.Errorf("unknown subcommand %q (try: server, client, help, version)", args[0])
	}
}

func runHelp() error {
	fmt.Println(`plotka - small dynamic DNS registry

usage:
  plotka server [flags]   run the daemon
  plotka client <op> ...  admin against the local server
                          (list | set | delete | purge | cluster status)
  plotka version
  plotka help

run 'plotka server -h' or 'plotka client -h' for details`)
	return nil
}

func main() {
	if err := dispatch(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "plotka:", err)
		os.Exit(1)
	}
}
