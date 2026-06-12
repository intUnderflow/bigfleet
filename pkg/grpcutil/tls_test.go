package grpcutil

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/intUnderflow/bigfleet/pkg/grpcutil/tlstest"
)

// startHealthServer brings up a grpc_health_v1 server with the given
// TLS config and an interceptor that captures the PeerIdentity result
// observed on each call.
type identityCapture struct {
	mu   sync.Mutex
	uri  string
	mtls bool
	err  error
}

func (ic *identityCapture) get() (string, bool, error) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	return ic.uri, ic.mtls, ic.err
}

func startHealthServer(t *testing.T, cfg TLSConfig) (addr string, capture *identityCapture) {
	t.Helper()
	opts, err := cfg.ServerOptions()
	if err != nil {
		t.Fatalf("ServerOptions: %v", err)
	}
	capture = &identityCapture{}
	opts = append(opts, grpc.UnaryInterceptor(
		func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			uri, mtls, err := PeerIdentity(ctx)
			capture.mu.Lock()
			capture.uri, capture.mtls, capture.err = uri, mtls, err
			capture.mu.Unlock()
			return handler(ctx, req)
		}))
	srv := grpc.NewServer(opts...)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), capture
}

func healthCheck(t *testing.T, addr string, cfg TLSConfig) error {
	t.Helper()
	opts, err := cfg.DialOptions()
	if err != nil {
		t.Fatalf("DialOptions: %v", err)
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func serverFiles(t *testing.T, ca *tlstest.CA, dir string, uri string) TLSConfig {
	t.Helper()
	cert, key, caPath := ca.WriteFiles(t, dir, tlstest.LeafOpts{
		URIs: []string{uri},
		IPs:  []net.IP{net.ParseIP("127.0.0.1")},
	})
	return TLSConfig{CertFile: cert, KeyFile: key, CAFile: caPath}
}

func clientFiles(t *testing.T, ca *tlstest.CA, dir string, uris ...string) TLSConfig {
	t.Helper()
	cert, key, caPath := ca.WriteFiles(t, dir, tlstest.LeafOpts{URIs: uris})
	return TLSConfig{CertFile: cert, KeyFile: key, CAFile: caPath}
}

// TestTLSConfig_PlaintextDefault: the zero value keeps today's
// behaviour — plaintext both ways, and PeerIdentity reports mtls=false
// so identity checks are skipped.
func TestTLSConfig_PlaintextDefault(t *testing.T) {
	var cfg TLSConfig
	if cfg.Enabled() {
		t.Fatal("zero TLSConfig must not be Enabled")
	}
	addr, capture := startHealthServer(t, cfg)
	if err := healthCheck(t, addr, cfg); err != nil {
		t.Fatalf("plaintext health check: %v", err)
	}
	uri, mtls, err := capture.get()
	if mtls || err != nil || uri != "" {
		t.Fatalf("plaintext PeerIdentity = (%q, %v, %v); want (\"\", false, nil)", uri, mtls, err)
	}
}

// TestTLSConfig_PartialFlagsError: any partial combination is a
// startup error from both ServerOptions and DialOptions.
func TestTLSConfig_PartialFlagsError(t *testing.T) {
	partials := []TLSConfig{
		{CertFile: "a"},
		{KeyFile: "b"},
		{CAFile: "c"},
		{CertFile: "a", KeyFile: "b"},
		{CertFile: "a", CAFile: "c"},
		{KeyFile: "b", CAFile: "c"},
	}
	for _, cfg := range partials {
		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate(%+v): want error, got nil", cfg)
		}
		if _, err := cfg.ServerOptions(); err == nil {
			t.Errorf("ServerOptions(%+v): want error, got nil", cfg)
		}
		if _, err := cfg.DialOptions(); err == nil {
			t.Errorf("DialOptions(%+v): want error, got nil", cfg)
		}
	}
}

// TestMTLS_HandshakeSuccess: server and client certs from the same CA
// handshake successfully, and the server observes the client's
// bigfleet:// URI SAN through PeerIdentity.
func TestMTLS_HandshakeSuccess(t *testing.T) {
	ca := tlstest.NewCA(t)
	dir := t.TempDir()
	serverCfg := serverFiles(t, ca, filepath.Join(dir, "server"), ShardURI("shard-0"))
	clientCfg := clientFiles(t, ca, filepath.Join(dir, "client"), ClusterURI("prod-eu-1"))

	addr, capture := startHealthServer(t, serverCfg)
	if err := healthCheck(t, addr, clientCfg); err != nil {
		t.Fatalf("mTLS health check: %v", err)
	}
	uri, mtls, err := capture.get()
	if !mtls || err != nil {
		t.Fatalf("PeerIdentity = (%q, %v, %v); want mtls=true, nil error", uri, mtls, err)
	}
	if want := ClusterURI("prod-eu-1"); uri != want {
		t.Fatalf("PeerIdentity uri = %q; want %q", uri, want)
	}
}

// TestMTLS_WrongCARejected: a client certificate from a different CA
// fails the handshake — the server never sees the call.
func TestMTLS_WrongCARejected(t *testing.T) {
	ca := tlstest.NewCA(t)
	rogue := tlstest.NewCA(t)
	dir := t.TempDir()
	serverCfg := serverFiles(t, ca, filepath.Join(dir, "server"), ShardURI("shard-0"))

	// Rogue leaf, but trusting the real CA as server root so the
	// failure is unambiguously the server rejecting the client.
	rogueCert, rogueKey, _ := rogue.WriteFiles(t, filepath.Join(dir, "rogue"), tlstest.LeafOpts{
		URIs: []string{ClusterURI("prod-eu-1")},
	})
	clientCfg := TLSConfig{CertFile: rogueCert, KeyFile: rogueKey, CAFile: serverCfg.CAFile}

	addr, _ := startHealthServer(t, serverCfg)
	if err := healthCheck(t, addr, clientCfg); err == nil {
		t.Fatal("health check with wrong-CA client cert: want handshake error, got nil")
	}
}

// TestMTLS_NoClientCertRejected: mTLS requires the client
// certificate; a TLS client that trusts the server but presents no
// leaf of its own is refused at the handshake.
func TestMTLS_NoClientCertRejected(t *testing.T) {
	ca := tlstest.NewCA(t)
	dir := t.TempDir()
	serverCfg := serverFiles(t, ca, filepath.Join(dir, "server"), ShardURI("shard-0"))
	addr, _ := startHealthServer(t, serverCfg)

	pool, err := loadCAPool(serverCfg.CAFile)
	if err != nil {
		t.Fatalf("loadCAPool: %v", err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool})))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err == nil {
		t.Fatal("certificate-less TLS client against mTLS server: want error, got nil")
	}
}

// TestPeerIdentity_MultipleURISANs: a certificate with two bigfleet://
// URI SANs violates the exactly-one rule; PeerIdentity errors and the
// caller rejects.
func TestPeerIdentity_MultipleURISANs(t *testing.T) {
	ca := tlstest.NewCA(t)
	dir := t.TempDir()
	serverCfg := serverFiles(t, ca, filepath.Join(dir, "server"), ShardURI("shard-0"))
	clientCfg := clientFiles(t, ca, filepath.Join(dir, "client"),
		ClusterURI("a"), ClusterURI("b"))

	addr, capture := startHealthServer(t, serverCfg)
	if err := healthCheck(t, addr, clientCfg); err != nil {
		t.Fatalf("health check: %v (the transport itself should succeed)", err)
	}
	_, mtls, err := capture.get()
	if !mtls {
		t.Fatal("expected mtls=true")
	}
	if err == nil {
		t.Fatal("PeerIdentity with two bigfleet:// URI SANs: want error, got nil")
	}
}

// TestPeerIdentity_NoURISAN: mTLS with a SAN-less client cert is also
// an identity error (mtls=true, err!=nil) — the cert authenticates a
// caller but asserts no BigFleet identity.
func TestPeerIdentity_NoURISAN(t *testing.T) {
	ca := tlstest.NewCA(t)
	dir := t.TempDir()
	serverCfg := serverFiles(t, ca, filepath.Join(dir, "server"), ShardURI("shard-0"))
	clientCfg := clientFiles(t, ca, filepath.Join(dir, "client")) // no URIs

	addr, capture := startHealthServer(t, serverCfg)
	if err := healthCheck(t, addr, clientCfg); err != nil {
		t.Fatalf("health check: %v", err)
	}
	_, mtls, err := capture.get()
	if !mtls || err == nil {
		t.Fatalf("PeerIdentity = (mtls=%v, err=%v); want mtls=true with identity error", mtls, err)
	}
}

// TestFileCertSource_ReloadOnMtime: the source serves the cached pair
// until a file's mtime changes, then re-reads — the rotation contract
// cert-manager relies on (ADR-0048).
func TestFileCertSource_ReloadOnMtime(t *testing.T) {
	ca := tlstest.NewCA(t)
	dir := t.TempDir()
	certPEM1, keyPEM1 := ca.Issue(t, tlstest.LeafOpts{CommonName: "leaf-1"})
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	writeFile(t, certPath, certPEM1)
	writeFile(t, keyPath, keyPEM1)
	// Pin mtimes well in the past so the rotation below is a
	// guaranteed mtime change regardless of filesystem granularity.
	past := time.Now().Add(-time.Hour)
	chtimes(t, certPath, past)
	chtimes(t, keyPath, past)

	src, err := newFileCertSource(certPath, keyPath)
	if err != nil {
		t.Fatalf("newFileCertSource: %v", err)
	}
	first, err := src.current()
	if err != nil {
		t.Fatalf("current: %v", err)
	}

	// Unchanged mtime: same cached object back.
	again, err := src.current()
	if err != nil {
		t.Fatalf("current (cached): %v", err)
	}
	if again != first {
		t.Fatal("expected cached *tls.Certificate while mtimes are unchanged")
	}

	// Rotate: new pair, fresh mtimes.
	certPEM2, keyPEM2 := ca.Issue(t, tlstest.LeafOpts{CommonName: "leaf-2"})
	writeFile(t, certPath, certPEM2)
	writeFile(t, keyPath, keyPEM2)
	rotated, err := src.current()
	if err != nil {
		t.Fatalf("current (rotated): %v", err)
	}
	if rotated == first {
		t.Fatal("expected reload after mtime change")
	}
	if string(rotated.Certificate[0]) == string(first.Certificate[0]) {
		t.Fatal("rotated source still serves the old leaf")
	}

	// Half-written rotation: garbage key with a fresh mtime must not
	// break handshakes — the previous coherent pair keeps serving.
	writeFile(t, keyPath, []byte("not a key"))
	fallback, err := src.current()
	if err != nil {
		t.Fatalf("current (broken rotation): %v", err)
	}
	if string(fallback.Certificate[0]) != string(rotated.Certificate[0]) {
		t.Fatal("broken rotation must keep serving the last coherent pair")
	}
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func chtimes(t *testing.T, path string, mtime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}
