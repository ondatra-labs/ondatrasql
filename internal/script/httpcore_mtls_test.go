// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testCA generates an ephemeral CA + client certificate for mTLS testing.
// Returns paths to the CA cert, client cert, and client key files in tmpDir.
func testCA(t *testing.T) (caCertPath, clientCertPath, clientKeyPath string) {
	t.Helper()
	tmpDir := t.TempDir()

	// Generate CA key
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatal(err)
	}

	// Write CA cert PEM
	caCertPath = filepath.Join(tmpDir, "ca.crt")
	writePEM(t, caCertPath, "CERTIFICATE", caCertDER)

	// Generate client key
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Test Client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	// Write client cert PEM
	clientCertPath = filepath.Join(tmpDir, "client.crt")
	writePEM(t, clientCertPath, "CERTIFICATE", clientCertDER)

	// Write client key PEM
	clientKeyPath = filepath.Join(tmpDir, "client.key")
	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, clientKeyPath, "EC PRIVATE KEY", keyDER)

	return caCertPath, clientCertPath, clientKeyPath
}

func writePEM(t *testing.T, path, blockType string, data []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: data}); err != nil {
		t.Fatal(err)
	}
}

// newMTLSServer creates an httptest TLS server that requires client certificates
// signed by the given CA.
func newMTLSServer(t *testing.T, caCertPath string, handler http.Handler) *httptest.Server {
	t.Helper()

	caCertPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		t.Fatal(err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCertPEM) {
		t.Fatal("failed to parse CA cert")
	}

	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		ClientCAs:  caPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func TestMTLSClientCert(t *testing.T) {
	caCertPath, clientCertPath, clientKeyPath := testCA(t)

	srv := newMTLSServer(t, caCertPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{"mtls":"ok"}`))
	}))

	opts := httpOptions{
		Timeout:  5 * time.Second,
		Insecure: true, // self-signed server cert
		ClientCert: &clientCertConfig{
			CertFile: clientCertPath,
			KeyFile:  clientKeyPath,
		},
	}

	resp, err := DoHTTP(context.Background(), "GET", srv.URL, nil, map[string]string{}, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if string(resp.Body) != `{"mtls":"ok"}` {
		t.Errorf("body = %q", string(resp.Body))
	}
}

func TestMTLSMissingCert(t *testing.T) {
	caCertPath, _, _ := testCA(t)

	srv := newMTLSServer(t, caCertPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	opts := httpOptions{
		Timeout:  5 * time.Second,
		Insecure: true,
		// No ClientCert — server should reject the TLS handshake
	}

	_, err := DoHTTP(context.Background(), "GET", srv.URL, nil, map[string]string{}, opts)
	if err == nil {
		t.Fatal("expected TLS handshake error, got nil")
	}
}

func TestMTLSCustomCA(t *testing.T) {
	caCertPath, clientCertPath, clientKeyPath := testCA(t)

	srv := newMTLSServer(t, caCertPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))

	// Use ca kwarg instead of Insecure — the custom CA should trust the server cert.
	// However, httptest TLS servers use a self-signed server cert from httptest's own CA,
	// not our test CA, so we still need Insecure for the server cert validation.
	// The ca kwarg adds our CA to RootCAs, which is used for server cert verification.
	// To properly test ca kwarg, we verify it loads without error and the request succeeds.
	opts := httpOptions{
		Timeout:  5 * time.Second,
		Insecure: true,
		ClientCert: &clientCertConfig{
			CertFile: clientCertPath,
			KeyFile:  clientKeyPath,
			CAFile:   caCertPath,
		},
	}

	resp, err := DoHTTP(context.Background(), "GET", srv.URL, nil, map[string]string{}, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestMTLSInvalidCertPath(t *testing.T) {
	opts := httpOptions{
		Timeout: 5 * time.Second,
		ClientCert: &clientCertConfig{
			CertFile: "/nonexistent/client.crt",
			KeyFile:  "/nonexistent/client.key",
		},
	}

	_, err := DoHTTP(context.Background(), "GET", "https://localhost:1", nil, map[string]string{}, opts)
	if err == nil {
		t.Fatal("expected error for invalid cert path, got nil")
	}
}

func TestMTLSInvalidCAPath(t *testing.T) {
	_, clientCertPath, clientKeyPath := testCA(t)

	opts := httpOptions{
		Timeout: 5 * time.Second,
		ClientCert: &clientCertConfig{
			CertFile: clientCertPath,
			KeyFile:  clientKeyPath,
			CAFile:   "/nonexistent/ca.crt",
		},
	}

	_, err := DoHTTP(context.Background(), "GET", "https://localhost:1", nil, map[string]string{}, opts)
	if err == nil {
		t.Fatal("expected error for invalid CA path, got nil")
	}
}
