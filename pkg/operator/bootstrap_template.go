package operator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"text/template"
)

// NewFileTemplateRenderer parses a Go text/template at the given path
// and returns a BootstrapRenderer that renders it on every
// BootstrapRequest. M21: this is the path the helm chart wires up
// when the user supplies a `bootstrapTemplate` values block — they
// get to declare userdata generation without forking the operator
// binary.
//
// Template context (mirrors BootstrapRendererInput verbatim):
//
//	{{ .ClusterID }}
//	{{ range .Requirements }}{{ .Key }} {{ .Operator }} {{ .Values }}{{ end }}
//
// Template execution errors are forwarded to the shard as the
// BootstrapBlobResponse.Error field — the shard treats a non-empty
// Error as an unsatisfiable requirement and falls back to a
// shortfall, so a bad template gates capacity instead of crashing
// the operator.
//
// Empty path → returns nil; New() then falls back to
// stubBootstrapRenderer. This keeps unit tests and embedders that
// supply their own callback unchanged.
func NewFileTemplateRenderer(path string) (BootstrapRenderer, error) {
	if path == "" {
		return nil, nil
	}
	src, err := os.ReadFile(path) //nolint:gosec // path is operator-supplied at startup
	if err != nil {
		return nil, fmt.Errorf("read bootstrap-template-file: %w", err)
	}
	tpl, err := template.New("bootstrap").Parse(string(src))
	if err != nil {
		return nil, fmt.Errorf("parse bootstrap-template-file: %w", err)
	}
	return func(_ context.Context, in BootstrapRendererInput) (BootstrapRendererOutput, error) {
		var buf bytes.Buffer
		if err := tpl.Execute(&buf, in); err != nil {
			return BootstrapRendererOutput{}, err
		}
		return BootstrapRendererOutput{UserData: buf.Bytes()}, nil
	}, nil
}
