// Command bigfleet is the BigFleet binary. Subcommands:
//
//	bigfleet shard         run a single shard controller
//	bigfleet coordinator   run the global coordinator (Raft + BoltDB)
//	bigfleet all-in-one    run coordinator + shard in one process (dev / quickstart)
package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "shard":
		if err := runShard(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "bigfleet shard:", err)
			os.Exit(1)
		}
	case "coordinator":
		if err := runCoordinator(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "bigfleet coordinator:", err)
			os.Exit(1)
		}
	case "all-in-one":
		if err := runAllInOne(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "bigfleet all-in-one:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintln(os.Stderr, "bigfleet: unknown subcommand:", os.Args[1])
		printUsage()
		os.Exit(2)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: bigfleet <subcommand> [args]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "subcommands:")
	fmt.Fprintln(os.Stderr, "  shard         run a single shard controller")
	fmt.Fprintln(os.Stderr, "  coordinator   run the global coordinator (Raft + BoltDB)")
	fmt.Fprintln(os.Stderr, "  all-in-one    run coordinator + shard in one process (dev / quickstart)")
}

// signalContext returns a context that is cancelled on SIGINT or SIGTERM.
func signalContext() (cancel func(), wait <-chan os.Signal) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return func() { signal.Stop(ch); close(ch) }, ch
}

// errFlagParse is a sentinel returned by sub-runners when the user's
// flags didn't parse — the caller renders the message but skips the
// "bigfleet shard:" prefix. Importing errors keeps a stable home for
// any future sentinel additions.
var errFlagParse = errors.New("flag parse")
var _ = errFlagParse
