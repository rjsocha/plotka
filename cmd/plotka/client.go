package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"plotka/internal/admin"
)

// nameWidth caps the name column in `list` unless --full is given.
const nameWidth = 64

const clientUsage = `usage: plotka client [--admin-socket PATH] [--full] <op>

operations:
  list                  list all records (name, ip, ttl-secs, last-seen, static|dynamic)
  set <name> <ip>       create/update a record
  delete <name>         remove a record
  purge                 run the purge sweep now
  cluster status        list cluster members (name, addr, state)`

func runClient(args []string) error {
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), clientUsage)
		fmt.Fprintln(fs.Output(), "\nflags:")
		fs.PrintDefaults()
	}
	sock := fs.String("admin-socket", "/run/plotka/admin", "unix admin socket path")
	full := fs.Bool("full", false, "list: do not truncate the name column to 64 chars")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	return clientCmd(*sock, *full, fs.Args())
}

// clientCmd maps a client subcommand to an admin line and prints the reply.
func clientCmd(sock string, full bool, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", clientUsage)
	}
	var line string
	tabular := false
	switch args[0] {
	case "list":
		line, tabular = "LIST", true
	case "set":
		if len(args) != 3 {
			return fmt.Errorf("usage: plotka client set <name> <ip>")
		}
		line = fmt.Sprintf("SET %s %s", args[1], args[2])
	case "delete":
		if len(args) != 2 {
			return fmt.Errorf("usage: plotka client delete <name>")
		}
		line = "DELETE " + args[1]
	case "purge":
		line = "PURGE"
	case "cluster":
		if len(args) != 2 || args[1] != "status" {
			return fmt.Errorf("usage: plotka client cluster status")
		}
		line, tabular = "CLUSTER", true
	default:
		return fmt.Errorf("unknown client op %q", args[0])
	}
	out, err := admin.Call(sock, line)
	if err != nil {
		return err
	}
	if tabular {
		printTable(out, args[0] == "list" && !full)
		return nil
	}
	fmt.Print(strings.TrimRight(out, "\n") + "\n")
	return nil
}

// printTable aligns tab-separated rows into columns. When truncate is set, the
// first column (the name) is capped at nameWidth runes.
func printTable(out string, truncate bool) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, ln := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if ln == "" {
			continue
		}
		fields := strings.Split(ln, "\t")
		if truncate && len(fields) > 0 {
			if r := []rune(fields[0]); len(r) > nameWidth {
				fields[0] = string(r[:nameWidth])
			}
		}
		fmt.Fprintln(tw, strings.Join(fields, "\t"))
	}
	tw.Flush()
}
