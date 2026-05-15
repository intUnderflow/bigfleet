// Package adrlint asserts structural invariants of the ADR set in
// docs/adr/. The tests run as part of `go test ./...` (and therefore
// as part of `make verify`, the PR CI gate), so a missing index row
// or a typo'd slug fails CI rather than waiting to be spotted by eye.
package adrlint

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// adrFilePattern matches an ADR filename of the form NNNN-kebab-slug.md.
// Anchored, lower-case-only, hyphen-separated — matches the convention
// every existing ADR file follows.
var adrFilePattern = regexp.MustCompile(`^(\d{4})-[a-z0-9-]+\.md$`)

// indexLinkPattern matches /adr/SLUG/ occurrences in docs/adr/index.md.
// Each row in the index table renders to one of these.
var indexLinkPattern = regexp.MustCompile(`/adr/(\d{4}-[a-z0-9-]+)/`)

// TestADRIndexCoversAllADRs asserts docs/adr/index.md has a row for
// every NNNN-*.md file in docs/adr/. The index is what /adr/ renders
// on the site; a missing row means readers using the site literally
// can't find the new ADR. Add the row in the same commit as the ADR.
//
// Row format: `| [N](/adr/SLUG/) | Title |` where Title is the ADR's
// H1 with the "ADR-NNNN: " prefix stripped.
func TestADRIndexCoversAllADRs(t *testing.T) {
	t.Parallel()
	adrDir := adrDir(t)

	slugs := readADRSlugs(t, adrDir)
	index := readIndex(t, adrDir)

	var missing []string
	for _, slug := range slugs {
		if !strings.Contains(index, "/adr/"+slug+"/") {
			missing = append(missing, slug)
		}
	}
	if len(missing) > 0 {
		t.Errorf("docs/adr/index.md is missing rows for %d ADR(s): %s\n\n"+
			"Add a row in docs/adr/index.md in the same commit that adds the ADR file. Row format:\n"+
			"  | [N](/adr/SLUG/) | Title (ADR H1 with the 'ADR-NNNN: ' prefix stripped) |",
			len(missing), strings.Join(missing, ", "))
	}
}

// TestADRIndexLinksResolve asserts every /adr/SLUG/ link in
// docs/adr/index.md points to an actual NNNN-*.md file. Catches typos
// in slug names — those would 404 on the site without this check.
func TestADRIndexLinksResolve(t *testing.T) {
	t.Parallel()
	adrDir := adrDir(t)
	index := readIndex(t, adrDir)

	matches := indexLinkPattern.FindAllStringSubmatch(index, -1)
	for _, m := range matches {
		slug := m[1]
		if _, err := os.Stat(filepath.Join(adrDir, slug+".md")); err != nil {
			t.Errorf("docs/adr/index.md links to /adr/%s/ but docs/adr/%s.md does not exist", slug, slug)
		}
	}
}

// adrDir returns the absolute path of docs/adr/, located by walking up
// from the test's working directory until go.mod is found.
func adrDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, "docs", "adr")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repo root (containing go.mod) not found above %s", wd)
		}
		dir = parent
	}
}

func readADRSlugs(t *testing.T, adrDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(adrDir)
	if err != nil {
		t.Fatalf("read %s: %v", adrDir, err)
	}
	var slugs []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if adrFilePattern.MatchString(e.Name()) {
			slugs = append(slugs, strings.TrimSuffix(e.Name(), ".md"))
		}
	}
	sort.Strings(slugs)
	return slugs
}

func readIndex(t *testing.T, adrDir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(adrDir, "index.md"))
	if err != nil {
		t.Fatalf("read index.md: %v", err)
	}
	return string(b)
}
