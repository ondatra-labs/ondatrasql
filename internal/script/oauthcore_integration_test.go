// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDoOAuthRequestSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
		}

		body, _ := io.ReadAll(r.Body)
		if got := string(body); got == "" {
			t.Error("expected form body")
		}

		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "tok123",
			"token_type":   "Bearer",
			"expires_in":   float64(3600),
		})
	}))
	defer srv.Close()

	form := map[string][]string{
		"grant_type":    {"client_credentials"},
		"client_id":     {"id"},
		"client_secret": {"secret"},
	}

	result, err := DoOAuthRequest(context.Background(), srv.URL, form)
	if err != nil {
		t.Fatal(err)
	}
	if result["access_token"] != "tok123" {
		t.Errorf("access_token = %v", result["access_token"])
	}
	if result["token_type"] != "Bearer" {
		t.Errorf("token_type = %v", result["token_type"])
	}
}

func TestDoOAuthRequestError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":"invalid_client"}`))
	}))
	defer srv.Close()

	form := map[string][]string{"grant_type": {"client_credentials"}}
	_, err := DoOAuthRequest(context.Background(), srv.URL, form)
	if err == nil {
		t.Fatal("expected error for 401")
	}
}

func TestExchangeJWTForTokenSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodyStr := string(body)
		if !strings.Contains(bodyStr, "grant_type=urn") {
			t.Errorf("missing grant_type in body: %s", bodyStr)
		}
		if !strings.Contains(bodyStr, "assertion=test.jwt.token") {
			t.Errorf("missing assertion in body: %s", bodyStr)
		}

		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "google-tok",
			"token_type":   "Bearer",
			"expires_in":   float64(3600),
		})
	}))
	defer srv.Close()

	result, err := ExchangeJWTForToken(context.Background(), srv.URL, "test.jwt.token")
	if err != nil {
		t.Fatal(err)
	}
	if result["access_token"] != "google-tok" {
		t.Errorf("access_token = %v", result["access_token"])
	}
	if result["token_type"] != "Bearer" {
		t.Errorf("token_type = %v", result["token_type"])
	}
}

func TestExchangeJWTForTokenError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()

	_, err := ExchangeJWTForToken(context.Background(), srv.URL, "bad.jwt")
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
}

func TestCreateGoogleJWT(t *testing.T) {
	// Generate a test RSA key
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Encode as PKCS8 PEM (what Google service account JSON contains)
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	pemBlock := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Bytes})

	key := ServiceAccountKey{
		ClientEmail: "test@project.iam.gserviceaccount.com",
		TokenURI:    "https://oauth2.googleapis.com/token",
		PrivateKey:  string(pemBlock),
	}

	jwt, err := CreateGoogleJWT(key, "https://www.googleapis.com/auth/analytics.readonly")
	if err != nil {
		t.Fatalf("CreateGoogleJWT: %v", err)
	}

	// JWT should have 3 parts
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatalf("JWT has %d parts, want 3", len(parts))
	}

	// Verify header
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	var header map[string]string
	json.Unmarshal(headerJSON, &header)
	if header["alg"] != "RS256" {
		t.Errorf("alg = %q, want RS256", header["alg"])
	}
	if header["typ"] != "JWT" {
		t.Errorf("typ = %q, want JWT", header["typ"])
	}

	// Verify claims
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	var claims map[string]interface{}
	json.Unmarshal(claimsJSON, &claims)
	if claims["iss"] != "test@project.iam.gserviceaccount.com" {
		t.Errorf("iss = %v", claims["iss"])
	}
	if claims["scope"] != "https://www.googleapis.com/auth/analytics.readonly" {
		t.Errorf("scope = %v", claims["scope"])
	}

	// Verify signature with the public key
	signInput := parts[0] + "." + parts[1]
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	hash := sha256.Sum256([]byte(signInput))
	if err := rsa.VerifyPKCS1v15(&privKey.PublicKey, crypto.SHA256, hash[:], sigBytes); err != nil {
		t.Errorf("signature verification failed: %v", err)
	}
}

func TestCreateGoogleJWTInvalidPEM(t *testing.T) {
	key := ServiceAccountKey{PrivateKey: "not-a-pem"}
	_, err := CreateGoogleJWT(key, "scope")
	if err == nil {
		t.Fatal("expected error for invalid PEM")
	}
}

func TestDoOAuthRequestServerDown(t *testing.T) {
	// Use a closed server to simulate connection error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()

	form := map[string][]string{"grant_type": {"client_credentials"}}
	_, err := DoOAuthRequest(context.Background(), srv.URL, form)
	if err == nil {
		t.Fatal("expected error for closed server")
	}
}

func TestDoOAuthRequestInvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("not json"))
	}))
	defer srv.Close()

	form := map[string][]string{"grant_type": {"client_credentials"}}
	_, err := DoOAuthRequest(context.Background(), srv.URL, form)
	if err == nil {
		t.Fatal("expected error for invalid JSON response")
	}
}

func TestExchangeJWTForTokenInvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("not json"))
	}))
	defer srv.Close()

	_, err := ExchangeJWTForToken(context.Background(), srv.URL, "test.jwt")
	if err == nil {
		t.Fatal("expected error for invalid JSON response")
	}
}

func TestCreateGoogleJWTInvalidPKCS8(t *testing.T) {
	// Valid PEM block but invalid PKCS8 content
	pemBlock := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: []byte("not a valid key"),
	})

	key := ServiceAccountKey{PrivateKey: string(pemBlock)}
	_, err := CreateGoogleJWT(key, "scope")
	if err == nil {
		t.Fatal("expected error for invalid PKCS8 key")
	}
}

func TestExchangeJWTForTokenServerDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()

	_, err := ExchangeJWTForToken(context.Background(), srv.URL, "test.jwt")
	if err == nil {
		t.Fatal("expected error for closed server")
	}
}
