// Package tlstest mints throwaway CAs and leaf certificates for
// exercising the ADR-0048 mTLS layer in tests. Test fixture only —
// never deployed, never checked-in key material (the same posture as
// pkg/provider/fake). It deliberately imports nothing from BigFleet so
// both internal and external grpcutil tests can use it.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// CA is an in-memory certificate authority.
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
}

// LeafOpts shapes an issued leaf certificate.
type LeafOpts struct {
	// CommonName for the subject. Optional.
	CommonName string
	// URIs become URI SANs verbatim — pass bigfleet:// identities
	// here (one for the normal case; several to exercise the
	// exactly-one rejection path; none for an identity-free cert).
	URIs []string
	// DNSNames / IPs are the usual serving SANs. Server leaves in
	// tests typically want IPs: 127.0.0.1.
	DNSNames []string
	IPs      []net.IP
}

// NewCA mints a fresh CA.
func NewCA(t testing.TB) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstest: generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          randomSerial(t),
		Subject:               pkix.Name{CommonName: "bigfleet-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("tlstest: create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("tlstest: parse CA cert: %v", err)
	}
	return &CA{
		Cert:    cert,
		Key:     key,
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// Issue returns a PEM (cert, key) pair signed by the CA. Every leaf
// carries both ServerAuth and ClientAuth EKUs because BigFleet
// processes use one certificate for every edge they serve or dial
// (ADR-0048 symmetric-flags design).
func (ca *CA) Issue(t testing.TB, opts LeafOpts) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstest: generate leaf key: %v", err)
	}
	uris := make([]*url.URL, 0, len(opts.URIs))
	for _, raw := range opts.URIs {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("tlstest: parse URI SAN %q: %v", raw, err)
		}
		uris = append(uris, u)
	}
	cn := opts.CommonName
	if cn == "" {
		cn = "bigfleet-test-leaf"
	}
	tmpl := &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         uris,
		DNSNames:     opts.DNSNames,
		IPAddresses:  opts.IPs,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		t.Fatalf("tlstest: create leaf cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("tlstest: marshal leaf key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// WriteFiles issues a leaf per opts and writes tls.crt / tls.key /
// ca.crt under dir, returning the three paths in --tls-cert /
// --tls-key / --tls-ca order. dir should be unique per identity
// (e.g. filepath.Join(t.TempDir(), "operator")).
func (ca *CA) WriteFiles(t testing.TB, dir string, opts LeafOpts) (certPath, keyPath, caPath string) {
	t.Helper()
	certPEM, keyPEM := ca.Issue(t, opts)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("tlstest: mkdir %s: %v", dir, err)
	}
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")
	caPath = filepath.Join(dir, "ca.crt")
	for path, data := range map[string][]byte{
		certPath: certPEM,
		keyPath:  keyPEM,
		caPath:   ca.CertPEM,
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("tlstest: write %s: %v", path, err)
		}
	}
	return certPath, keyPath, caPath
}

func randomSerial(t testing.TB) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("tlstest: serial: %v", err)
	}
	return serial
}
