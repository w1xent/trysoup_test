// Package bootstrap prepares credentials in separate Docker volumes.
package bootstrap

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

type secrets struct {
	Admin  string
	API    string
	Worker string
	Token  string
}

func write(path string, data []byte, uid int, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".bootstrap-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Chmod(mode); err != nil {
		return err
	}
	if err = f.Chown(uid, uid); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func certificateFiles(root string) error {
	if _, err := os.Stat(filepath.Join(root, "db", "server.key")); err == nil {
		return nil
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Queue local CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(10, 0, 0),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "db"},
		DNSNames: []string{"db", "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(1, 0, 0),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	for _, dir := range []string{"api", "worker", "db"} {
		if err := write(filepath.Join(root, dir, "ca.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0, 0644); err != nil {
			return err
		}
	}
	if err := write(filepath.Join(root, "db", "server.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 70, 0644); err != nil {
		return err
	}
	return write(filepath.Join(root, "db", "server.key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 70, 0600)
}

func databaseURL(role, password, cert string) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(role, password), Host: "db:5432", Path: "/queue"}
	q := u.Query()
	q.Set("sslmode", "verify-full")
	q.Set("sslrootcert", cert)
	u.RawQuery = q.Encode()
	return u.String()
}

func Run(root string) error {
	for _, dir := range []string{"api", "worker", "db"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			return err
		}
	}
	seedPath := filepath.Join(root, "db", "seed.json")
	data, err := os.ReadFile(seedPath)
	var values secrets
	if errors.Is(err, os.ErrNotExist) {
		var entropy [128]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return err
		}
		values = secrets{Admin: hex.EncodeToString(entropy[0:32]), API: hex.EncodeToString(entropy[32:64]),
			Worker: hex.EncodeToString(entropy[64:96]), Token: hex.EncodeToString(entropy[96:128])}
		data, err = json.Marshal(values)
		if err != nil {
			return err
		}
		if err := write(seedPath, data, 0, 0600); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if err = json.Unmarshal(data, &values); err != nil {
		return err
	}
	if err := certificateFiles(root); err != nil {
		return err
	}
	tokens, _ := json.Marshal(map[string]string{"operator": values.Token})
	files := []struct {
		path, value string
		uid         int
	}{
		{"api/tokens.json", string(tokens), 65532},
		{"api/database_url", databaseURL("queue_api", values.API, "/run/config/ca.crt"), 65532},
		{"worker/database_url", databaseURL("queue_worker", values.Worker, "/run/config/ca.crt"), 65532},
		{"db/test_database_url", databaseURL("queue_admin", values.Admin, "/run/db/ca.crt"), 0},
		{"db/admin_password", values.Admin, 70}, {"db/api_password", values.API, 0}, {"db/worker_password", values.Worker, 0},
	}
	for _, f := range files {
		if err := write(filepath.Join(root, f.path), []byte(f.value), f.uid, 0600); err != nil {
			return err
		}
	}
	hba := "local all all trust\nhostssl queue queue_api,queue_worker,queue_admin all scram-sha-256\nhostnossl all all all reject\n"
	return write(filepath.Join(root, "db", "pg_hba.conf"), []byte(hba), 70, 0600)
}
