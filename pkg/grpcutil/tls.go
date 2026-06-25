package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
)

// ADR-0048: file-based mTLS, opt-in, symmetric flags. Every BigFleet
// process takes the same three flags (--tls-cert, --tls-key, --tls-ca)
// and applies them to every gRPC edge it serves or dials. All three
// set = mTLS with client-certificate verification required; none set =
// plaintext (the quickstart and the scaletest harness stay
// zero-config); a partial set is a startup error so a typo can't
// silently downgrade a deployment to plaintext.
//
// Caller identity rides on a URI SAN of the certificate:
//
//	bigfleet://cluster/<cluster_id>  — cluster operator
//	bigfleet://shard/<shard_id>      — shard (to coordinator, provider)
//	bigfleet://admin                 — admin CLI and coordinator replicas
//
// Exactly one bigfleet:// URI SAN per certificate. Servers bind the
// caller-asserted protobuf identity (Hello.cluster_id,
// ShardReport.shard_id) to that SAN; in plaintext mode the binding is
// skipped — identity is only as strong as the transport.

// TLSConfig holds the three symmetric mTLS flag values. The zero
// value means plaintext.
type TLSConfig struct {
	// CertFile is this process's PEM certificate (leaf, optionally
	// followed by intermediates).
	CertFile string
	// KeyFile is the PEM private key for CertFile.
	KeyFile string
	// CAFile is the PEM CA bundle that verifies the remote side —
	// clients verify servers and servers verify clients against the
	// same bundle. Re-read only at startup; rotating the CA requires
	// a restart (leaf certs reload live, see fileCertSource).
	CAFile string
}

// RegisterFlags wires the symmetric --tls-cert/--tls-key/--tls-ca
// flags into fs. Every binary registers the same three names so a
// deployment's cert mounts look identical across components.
func (c *TLSConfig) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.CertFile, "tls-cert", "", "path to this process's PEM certificate. ADR-0048: set all of --tls-cert/--tls-key/--tls-ca to enable mTLS on every gRPC edge this process serves or dials; set none for plaintext (the default). A partial set is a startup error. The certificate must carry this process's bigfleet:// identity URI SAN (cluster operator: bigfleet://cluster/<cluster-id>; shard: bigfleet://shard/<shard-id>; coordinator / admin CLI: bigfleet://admin).")
	fs.StringVar(&c.KeyFile, "tls-key", "", "path to the PEM private key for --tls-cert")
	fs.StringVar(&c.CAFile, "tls-ca", "", "path to the PEM CA bundle used to verify the remote side's certificate (clients verify servers and servers verify clients against the same bundle)")
}

// Enabled reports whether mTLS is configured (all three files set).
func (c TLSConfig) Enabled() bool {
	return c.CertFile != "" && c.KeyFile != "" && c.CAFile != ""
}

// Validate returns an error when the flag set is partial. All-or-none
// is the contract: a partial set is far more likely a typo than an
// intent, and silently running plaintext would be the worst outcome.
func (c TLSConfig) Validate() error {
	if c.Enabled() || (c.CertFile == "" && c.KeyFile == "" && c.CAFile == "") {
		return nil
	}
	return fmt.Errorf("tls: partial configuration (cert=%q key=%q ca=%q): set all of --tls-cert/--tls-key/--tls-ca for mTLS, or none for plaintext", c.CertFile, c.KeyFile, c.CAFile)
}

// ServerOptions returns the full grpc.ServerOption set for a BigFleet
// server under this TLS config: the shared wire limits plus transport
// credentials (mTLS with required client-cert verification when
// enabled, plaintext otherwise). Fails fast on a partial flag set or
// unreadable cert material.
func (c TLSConfig) ServerOptions() ([]grpc.ServerOption, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	opts := ServerOptions()
	if !c.Enabled() {
		return opts, nil
	}
	pool, err := loadCAPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	src, err := newFileCertSource(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
		// GetCertificate (not Certificates) so cert-manager-style
		// rotation is picked up without a restart — the source
		// re-reads the files when their mtime changes.
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return src.current()
		},
	}
	return append(opts, grpc.Creds(credentials.NewTLS(tlsCfg))), nil
}

// DialOptions returns the full grpc.DialOption set for a BigFleet
// client under this TLS config: the shared wire limits plus transport
// credentials (mTLS when enabled, plaintext otherwise). Fails fast on
// a partial flag set or unreadable cert material.
func (c TLSConfig) DialOptions() ([]grpc.DialOption, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	opts := DialOptions()
	if !c.Enabled() {
		return append(opts, grpc.WithTransportCredentials(insecure.NewCredentials())), nil
	}
	pool, err := loadCAPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	src, err := newFileCertSource(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    pool,
		// GetClientCertificate mirrors the server side: live reload on
		// mtime change so rotation never needs a reconnect storm.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return src.current()
		},
	}
	return append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))), nil
}

func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tls: read CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tls: no certificates found in CA bundle %s", path)
	}
	return pool, nil
}

// fileCertSource serves the (cert, key) pair from disk, re-reading the
// files when either one's mtime changes. Stat-per-handshake is cheap
// (no parsing unless something changed) and matches how cert-manager
// rotates: it rewrites the mounted Secret files in place, so the next
// handshake after a rotation picks up the new pair with no restart.
type fileCertSource struct {
	certFile, keyFile string

	mu      sync.Mutex
	cached  *tls.Certificate
	certMod time.Time
	keyMod  time.Time
}

// newFileCertSource constructs the source and loads the pair once so
// bad material fails at startup, not on the first handshake.
func newFileCertSource(certFile, keyFile string) (*fileCertSource, error) {
	s := &fileCertSource{certFile: certFile, keyFile: keyFile}
	if _, err := s.current(); err != nil {
		return nil, fmt.Errorf("tls: load certificate: %w", err)
	}
	return s, nil
}

func (s *fileCertSource) current() (*tls.Certificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	certInfo, certErr := os.Stat(s.certFile)
	keyInfo, keyErr := os.Stat(s.keyFile)
	if s.cached != nil {
		if certErr != nil || keyErr != nil {
			// Mid-rotation blip (Kubernetes swaps Secret mounts via a
			// symlink dance): keep serving the cached pair rather than
			// failing live handshakes. The next call re-stats.
			return s.cached, nil
		}
		if certInfo.ModTime().Equal(s.certMod) && keyInfo.ModTime().Equal(s.keyMod) {
			return s.cached, nil
		}
	}
	cert, err := tls.LoadX509KeyPair(s.certFile, s.keyFile)
	if err != nil {
		if s.cached != nil {
			// Half-written rotation (new cert, old key): serve the
			// previous coherent pair; mtimes stay stale so the next
			// handshake retries the load.
			return s.cached, nil
		}
		return nil, err
	}
	s.cached = &cert
	if certErr == nil {
		s.certMod = certInfo.ModTime()
	}
	if keyErr == nil {
		s.keyMod = keyInfo.ModTime()
	}
	return s.cached, nil
}

// URI SAN identity helpers (ADR-0048).

// AdminURI is the URI SAN that authorizes the coordinator admin
// surface. Coordinator replicas carry it too — they call
// JoinRaftCluster on each other (ADR-0047) and are inherently the
// admin domain.
const AdminURI = "bigfleet://admin"

// ReadonlyURI is the URI SAN that authorizes the coordinator's READ
// surface only (ListShards / ListDomainAssignments / ListQuotas /
// ListProviders / ListShardReports) and NOTHING that mutates the fleet
// (ADR-0060). It exists so read-only operator tooling — dashboards, CLIs,
// alerting, capacity scripts — can query the coordinator without holding an
// admin certificate that could also call the write RPCs. AdminURI is a
// superset: an admin certificate passes every read check too.
const ReadonlyURI = "bigfleet://readonly"

// ClusterURI is the URI SAN a cluster operator's certificate must
// carry to open a Session as clusterID.
func ClusterURI(clusterID string) string { return "bigfleet://cluster/" + clusterID }

// ShardURI is the URI SAN a shard's certificate must carry to report
// as shardID (and to identify itself to a provider).
func ShardURI(shardID string) string { return "bigfleet://shard/" + shardID }

// ErrNoIdentity is returned by PeerIdentity when the connection is
// mTLS but the client certificate does not carry exactly one
// bigfleet:// URI SAN.
var ErrNoIdentity = errors.New("client certificate must carry exactly one bigfleet:// URI SAN")

// PeerIdentity extracts the caller's bigfleet:// URI SAN from the
// verified client certificate on the connection behind ctx.
//
//   - mtls=false: the transport is not mTLS (plaintext, or TLS without
//     a verified client chain). Identity checks are skipped — ADR-0048
//     documents that identity is only as strong as the transport.
//   - mtls=true, err=nil: uri is the certificate's single bigfleet://
//     URI SAN, e.g. "bigfleet://cluster/prod-eu-1".
//   - mtls=true, err!=nil: the certificate carries zero or multiple
//     bigfleet:// URI SANs; the caller should reject with
//     PermissionDenied.
func PeerIdentity(ctx context.Context) (uri string, mtls bool, err error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return "", false, nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return "", false, nil
	}
	leaf := tlsInfo.State.VerifiedChains[0][0]
	for _, u := range leaf.URIs {
		if u.Scheme != "bigfleet" {
			continue
		}
		if uri != "" {
			return "", true, fmt.Errorf("%w: found at least %q and %q", ErrNoIdentity, uri, u.String())
		}
		uri = u.String()
	}
	if uri == "" {
		return "", true, ErrNoIdentity
	}
	return uri, true, nil
}
