// Command bigfleet is the BigFleet binary. Subcommands:
//
//	bigfleet shard         run a single shard controller
//	bigfleet coordinator   (stub for M6) run the global coordinator
//	bigfleet all-in-one    (stub) run shard and coordinator in one process
//
// In M3 only `shard` is implemented. The other subcommands print a
// not-yet-implemented message and exit with status 2.
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
		fmt.Fprintln(os.Stderr, "coordinator: not yet implemented (M6)")
		os.Exit(2)
	case "all-in-one":
		fmt.Fprintln(os.Stderr, "all-in-one: not yet implemented (M6)")
		os.Exit(2)
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
	fmt.Fprintln(os.Stderr, "  coordinator   run the global coordinator (not yet implemented)")
	fmt.Fprintln(os.Stderr, "  all-in-one    run shard and coordinator in one process (not yet implemented)")
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
