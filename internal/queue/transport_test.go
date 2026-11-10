package queue_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"soupqueue/internal/queue"
)

func fakeCertificate(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	must(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-db"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	must(t, err)
	private, err := x509.MarshalPKCS8PrivateKey(key)
	must(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	cert, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}))
	must(t, err)
	path := filepath.Join(t.TempDir(), "root.crt")
	must(t, os.WriteFile(path, certPEM, 0600))
	return cert, path
}

func fakeDatabase(t *testing.T, cert tls.Certificate, serve func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(5 * time.Second))
		var request [8]byte
		if _, err = io.ReadFull(c, request[:]); err != nil {
			return
		}
		// PostgreSQL's SSL negotiation precedes its authentication exchange.
		if binary.BigEndian.Uint32(request[4:]) != 80877103 {
			t.Error("client attempted plaintext startup")
			return
		}
		if _, err = c.Write([]byte("S")); err != nil {
			return
		}
		secured := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err = secured.Handshake(); err != nil {
			return
		}
		serve(secured)
	}()
	t.Cleanup(func() { listener.Close(); <-done })
	return listener.Addr().String()
}

func TestRejectInsecureDatabaseMode(t *testing.T) {
	for _, mode := range []string{"disable", "allow", "prefer", "require", "verify-ca"} {
		if s, err := queue.Open("postgres://u:p@localhost/db?sslmode=" + mode); err == nil {
			s.DB.Close()
			t.Errorf("accepted %s", mode)
		}
	}
}

func TestUntrustedDatabaseCannotRequestPassword(t *testing.T) {
	cert, _ := fakeCertificate(t)
	_, otherRoot := fakeCertificate(t)
	startup := make(chan bool, 1)
	addr := fakeDatabase(t, cert, func(c net.Conn) {
		var b [1]byte
		_, err := c.Read(b[:])
		startup <- err == nil
	})
	s, err := queue.Open("postgres://user:secret@" + addr + "/db?sslmode=verify-full&sslrootcert=" + otherRoot)
	must(t, err)
	defer s.DB.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := s.DB.PingContext(ctx); err == nil {
		t.Fatal("untrusted server accepted")
	}
	select {
	case sent := <-startup:
		if sent {
			t.Fatal("startup reached untrusted database")
		}
	default:
	}
}

func TestMalformedDatabaseFrameReturnsError(t *testing.T) {
	cert, root := fakeCertificate(t)
	addr := fakeDatabase(t, cert, func(c net.Conn) {
		var length [4]byte
		if _, err := io.ReadFull(c, length[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(length[:])
		if n < 4 || n > 8192 {
			t.Errorf("unexpected startup size %d", n)
			return
		}
		if _, err := io.CopyN(io.Discard, c, int64(n)-4); err != nil {
			return
		}
		// Accept startup, then return a zero-length frame when Claim sends its query.
		if _, err := c.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0, 'Z', 0, 0, 0, 5, 'I'}); err != nil {
			return
		}
		var query [1]byte
		if _, err := io.ReadFull(c, query[:]); err != nil {
			return
		}
		c.Write([]byte{'D', 0, 0, 0, 0})
	})
	s, err := queue.Open("postgres://user:secret@" + addr + "/db?sslmode=verify-full&sslrootcert=" + root)
	must(t, err)
	defer s.DB.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err = s.Claim(ctx, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "invalid message length") {
		t.Fatalf("expected protocol length error, got %v", err)
	}
}
