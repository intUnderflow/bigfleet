// Command fauxctl is the BigFleet simulator CLI. Modelled on Borg's
// Fauxmaster, it drives the same decision / shard / inventory / needs
// packages as the production binary against an in-memory provider,
// against the scenarios registered in sim/scenario.
//
// Subcommands:
//
//	fauxctl list                    — list registered scenarios
//	fauxctl run <name>              — run a scenario, print trace + assertions
//	fauxctl run-all                 — run every scenario; non-zero exit if any fail
//	fauxctl record <name> [path]    — record a golden trace to sim/golden/<name>.jsonl
//	fauxctl verify <name>           — diff current trace against the recorded golden
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/intUnderflow/bigfleet/sim"
	"github.com/intUnderflow/bigfleet/sim/scenario"
)

const goldenDir = "sim/golden"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "list":
		cmdList()
	case "run":
		if err := cmdRun(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "fauxctl run:", err)
			os.Exit(1)
		}
	case "run-all":
		if err := cmdRunAll(); err != nil {
			fmt.Fprintln(os.Stderr, "fauxctl run-all:", err)
			os.Exit(1)
		}
	case "record":
		if err := cmdRecord(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "fauxctl record:", err)
			os.Exit(1)
		}
	case "verify":
		if err := cmdVerify(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "fauxctl verify:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintln(os.Stderr, "fauxctl: unknown subcommand:", os.Args[1])
		printUsage()
		os.Exit(2)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: fauxctl <subcommand> [args]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "subcommands:")
	fmt.Fprintln(os.Stderr, "  list                       list registered scenarios")
	fmt.Fprintln(os.Stderr, "  run <name>                 run a scenario, print trace + assertions")
	fmt.Fprintln(os.Stderr, "  run-all                    run every scenario; non-zero exit if any fail")
	fmt.Fprintln(os.Stderr, "  record <name> [path]       record a golden trace to sim/golden/<name>.jsonl")
	fmt.Fprintln(os.Stderr, "  verify <name>              diff current trace against the recorded golden")
}

func cmdList() {
	for _, n := range scenario.Names() {
		sc := scenario.Must(n)
		fmt.Printf("  %-28s %s\n", sc.Name, sc.Description)
	}
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("missing <name>")
	}
	name := fs.Arg(0)
	sc, ok := scenario.Lookup(name)
	if !ok {
		return fmt.Errorf("unknown scenario %q (have: %v)", name, scenario.Names())
	}
	res, err := sim.Run(context.Background(), sc())
	if err != nil {
		return err
	}
	// Trace to stdout.
	if _, err := res.Trace.WriteTo(os.Stdout); err != nil {
		return err
	}
	// Assertions to stderr (so trace stdout is grep-friendly).
	failed := 0
	for _, a := range res.Assertions {
		if a.Pass {
			fmt.Fprintf(os.Stderr, "PASS  %s\n", a.Name)
			continue
		}
		failed++
		fmt.Fprintf(os.Stderr, "FAIL  %s: %v\n", a.Name, a.Err)
	}
	if failed > 0 {
		return fmt.Errorf("%d assertion(s) failed", failed)
	}
	return nil
}

func cmdRunAll() error {
	failed := 0
	for _, name := range scenario.Names() {
		sc := scenario.Must(name)
		res, err := sim.Run(context.Background(), sc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%-28s ERROR  %v\n", name, err)
			failed++
			continue
		}
		if res.AllPassed() {
			fmt.Fprintf(os.Stderr, "%-28s PASS\n", name)
			continue
		}
		failed++
		for _, a := range res.Assertions {
			if !a.Pass {
				fmt.Fprintf(os.Stderr, "%-28s FAIL  %s: %v\n", name, a.Name, a.Err)
			}
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d scenario(s) failed", failed)
	}
	return nil
}

func cmdRecord(args []string) error {
	fs := flag.NewFlagSet("record", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("missing <name>")
	}
	name := fs.Arg(0)
	path := defaultGoldenPath(name)
	if fs.NArg() >= 2 {
		path = fs.Arg(1)
	}
	sc, ok := scenario.Lookup(name)
	if !ok {
		return fmt.Errorf("unknown scenario %q", name)
	}
	res, err := sim.Run(context.Background(), sc())
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := res.Trace.WriteTo(f); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d events)\n", path, len(res.Trace))
	return nil
}

func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("missing <name>")
	}
	name := fs.Arg(0)
	sc, ok := scenario.Lookup(name)
	if !ok {
		return fmt.Errorf("unknown scenario %q", name)
	}

	want, err := os.ReadFile(defaultGoldenPath(name))
	if err != nil {
		return fmt.Errorf("read golden: %w (run 'fauxctl record %s' first)", err, name)
	}
	res, err := sim.Run(context.Background(), sc())
	if err != nil {
		return err
	}
	var got bytes.Buffer
	if _, err := res.Trace.WriteTo(&got); err != nil {
		return err
	}
	if !bytes.Equal(want, got.Bytes()) {
		return fmt.Errorf("trace differs from golden\n--- want\n%s\n--- got\n%s",
			truncate(string(want), 1000), truncate(got.String(), 1000))
	}
	if !res.AllPassed() {
		return errors.New("assertions failed")
	}
	fmt.Fprintf(os.Stderr, "%s OK (%d events, %d assertions)\n",
		name, len(res.Trace), len(res.Assertions))
	return nil
}

func defaultGoldenPath(name string) string {
	return filepath.Join(goldenDir, name+".jsonl")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..." + strings.Repeat("", 0)
}
