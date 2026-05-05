package operator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// TestNewFileTemplateRenderer_RendersContext: the M21 file-backed
// renderer parses a Go text/template at startup and substitutes the
// BootstrapRendererInput context on every call.
func TestNewFileTemplateRenderer_RendersContext(t *testing.T) {
	tpl := `cluster: {{ .ClusterID }}
{{- range .Requirements }}
  - {{ .Key }} {{ .Operator }} {{ .Values }}
{{- end }}
`
	path := filepath.Join(t.TempDir(), "bootstrap.tmpl")
	if err := os.WriteFile(path, []byte(tpl), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	r, err := NewFileTemplateRenderer(path)
	if err != nil {
		t.Fatalf("NewFileTemplateRenderer: %v", err)
	}
	if r == nil {
		t.Fatalf("renderer is nil")
	}
	out, err := r(context.Background(), BootstrapRendererInput{
		ClusterID: machine.ClusterID("prod-eu-1"),
		Requirements: []RequirementInput{
			{Key: "node.kubernetes.io/instance-type", Operator: "In", Values: []string{"a3-highgpu-8g"}},
		},
	})
	if err != nil {
		t.Fatalf("renderer call: %v", err)
	}
	got := string(out.UserData)
	if !strings.Contains(got, "cluster: prod-eu-1") {
		t.Errorf("missing ClusterID substitution; got:\n%s", got)
	}
	if !strings.Contains(got, "node.kubernetes.io/instance-type In") {
		t.Errorf("missing requirement substitution; got:\n%s", got)
	}
}

// TestNewFileTemplateRenderer_EmptyPathReturnsNil: empty path is the
// signal "use the operator's built-in stub renderer". The cmd binary
// passes the result straight to operator.Config.BootstrapTemplate;
// nil there means the stub takes over.
func TestNewFileTemplateRenderer_EmptyPathReturnsNil(t *testing.T) {
	r, err := NewFileTemplateRenderer("")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if r != nil {
		t.Errorf("renderer = non-nil, want nil for empty path")
	}
}

// TestNewFileTemplateRenderer_BadTemplate: a parse error at startup is
// surfaced so the operator binary fails fast instead of silently
// shipping a broken renderer.
func TestNewFileTemplateRenderer_BadTemplate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.tmpl")
	if err := os.WriteFile(path, []byte(`{{ .Unclosed `), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := NewFileTemplateRenderer(path)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
}
