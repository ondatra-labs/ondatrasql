// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package oauth2host

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("skipping: cannot bind IPv4 loopback: %v", err)
	}
	srv := &httptest.Server{
		Listener: l,
		Config:   &http.Server{Handler: handler},
	}
	srv.Start()
	return srv
}

func TestFetchProviderConfig(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/providers/fortnox.json" {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(ProviderConfig{
			Name:        "fortnox",
			AuthURL:     "https://apps.fortnox.se/oauth-v1/auth",
			TokenURL:    "https://apps.fortnox.se/oauth-v1/token",
			ClientID:    "test-client-id",
			Scope:       "companyinformation",
			RedirectURI: "https://oauth2.ondatra.sh/oauth/callback",
		})
	}))
	defer srv.Close()

	cfg, err := FetchProviderConfig(context.Background(), srv.URL, "fortnox")
	if err != nil {
		t.Fatalf("FetchProviderConfig: %v", err)
	}
	if cfg.Name != "fortnox" {
		t.Errorf("name = %q, want fortnox", cfg.Name)
	}
	if cfg.ClientID != "test-client-id" {
		t.Errorf("client_id = %q, want test-client-id", cfg.ClientID)
	}
}

func TestFetchProviderConfig_NotFound(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.NotFoundHandler())
	defer srv.Close()

	_, err := FetchProviderConfig(context.Background(), srv.URL, "unknown")
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
}

func TestRegister(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/oauth/register" {
			http.NotFound(w, r)
			return
		}
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		if body["license_key"] == "bad" {
			w.WriteHeader(403)
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer srv.Close()

	err := Register(context.Background(), srv.URL, "fortnox", "abc123", "good-key", "")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	err = Register(context.Background(), srv.URL, "fortnox", "abc123", "bad", "")
	if err == nil {
		t.Fatal("expected error for bad key")
	}
}

func TestPoll_Pending(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.NotFoundHandler())
	defer srv.Close()

	_, err := Poll(context.Background(), srv.URL, "abc123", "test-key")
	if err != ErrPending {
		t.Fatalf("expected ErrPending, got %v", err)
	}
}

func TestPoll_Success(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(PollResult{
			Provider:     "fortnox",
			RefreshToken: "RT_test",
		})
	}))
	defer srv.Close()

	result, err := Poll(context.Background(), srv.URL, "abc123", "test-key")
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if result.RefreshToken != "RT_test" {
		t.Errorf("refresh_token = %q, want RT_test", result.RefreshToken)
	}
}

func TestRefresh(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/oauth/refresh" {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(RefreshResult{
			AccessToken:  "AT_new",
			RefreshToken: "RT_new",
			ExpiresIn:    3600,
		})
	}))
	defer srv.Close()

	result, err := Refresh(context.Background(), srv.URL, "fortnox", "RT_old", "key")
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if result.AccessToken != "AT_new" {
		t.Errorf("access_token = %q, want AT_new", result.AccessToken)
	}
	if result.RefreshToken != "RT_new" {
		t.Errorf("refresh_token = %q, want RT_new", result.RefreshToken)
	}
}

func TestListProviders(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]string{"fortnox", "google-sheets"})
	}))
	defer srv.Close()

	providers, err := ListProviders(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("ListProviders: %v", err)
	}
	if len(providers) != 2 {
		t.Errorf("len = %d, want 2", len(providers))
	}
}

func TestListProviders_Error(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	_, err := ListProviders(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestExchangeCode(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.FormValue("grant_type") != "authorization_code" {
			t.Errorf("grant_type = %q", r.FormValue("grant_type"))
		}
		if r.FormValue("code") != "AUTH_CODE" {
			t.Errorf("code = %q", r.FormValue("code"))
		}
		if r.FormValue("client_secret") != "secret" {
			t.Errorf("client_secret = %q", r.FormValue("client_secret"))
		}
		json.NewEncoder(w).Encode(RefreshResult{
			AccessToken:  "AT_new",
			RefreshToken: "RT_new",
			ExpiresIn:    3600,
		})
	}))
	defer srv.Close()

	result, err := ExchangeCode(context.Background(), srv.URL, "client-id", "secret", "AUTH_CODE", "http://localhost/callback")
	if err != nil {
		t.Fatalf("ExchangeCode: %v", err)
	}
	if result.AccessToken != "AT_new" {
		t.Errorf("access_token = %q", result.AccessToken)
	}
	if result.RefreshToken != "RT_new" {
		t.Errorf("refresh_token = %q", result.RefreshToken)
	}
}

func TestExchangeCode_Error(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()

	_, err := ExchangeCode(context.Background(), srv.URL, "id", "secret", "bad", "http://localhost/callback")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRefreshLocal(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.FormValue("grant_type") != "refresh_token" {
			t.Errorf("grant_type = %q", r.FormValue("grant_type"))
		}
		if r.FormValue("refresh_token") != "RT_old" {
			t.Errorf("refresh_token = %q", r.FormValue("refresh_token"))
		}
		json.NewEncoder(w).Encode(RefreshResult{
			AccessToken:  "AT_refreshed",
			RefreshToken: "RT_rotated",
			ExpiresIn:    3600,
		})
	}))
	defer srv.Close()

	result, err := RefreshLocal(context.Background(), srv.URL, "client-id", "secret", "RT_old")
	if err != nil {
		t.Fatalf("RefreshLocal: %v", err)
	}
	if result.AccessToken != "AT_refreshed" {
		t.Errorf("access_token = %q", result.AccessToken)
	}
	if result.RefreshToken != "RT_rotated" {
		t.Errorf("refresh_token = %q", result.RefreshToken)
	}
}

func TestRefreshLocal_Error(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()

	_, err := RefreshLocal(context.Background(), srv.URL, "id", "secret", "bad")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecryptToken(t *testing.T) {
	t.Parallel()
	// Generate a known key, encrypt, then decrypt
	keyHex := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	// Manually construct encrypted token using Go's crypto
	plaintext := "RT_test_refresh_token"
	keyBytes, _ := hex.DecodeString(keyHex)
	block, _ := aes.NewCipher(keyBytes)
	gcm, _ := cipher.NewGCM(block)
	iv := make([]byte, 12)
	for i := range iv {
		iv[i] = byte(i)
	}
	ciphertext := gcm.Seal(nil, iv, []byte(plaintext), nil)
	encrypted := hex.EncodeToString(iv) + ":" + hex.EncodeToString(ciphertext)

	decrypted, err := DecryptToken(encrypted, keyHex)
	if err != nil {
		t.Fatalf("DecryptToken: %v", err)
	}
	if decrypted != plaintext {
		t.Errorf("decrypted = %q, want %q", decrypted, plaintext)
	}
}

func TestDecryptToken_WrongKey(t *testing.T) {
	t.Parallel()
	keyHex := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	wrongKey := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

	keyBytes, _ := hex.DecodeString(keyHex)
	block, _ := aes.NewCipher(keyBytes)
	gcm, _ := cipher.NewGCM(block)
	iv := make([]byte, 12)
	ciphertext := gcm.Seal(nil, iv, []byte("secret"), nil)
	encrypted := hex.EncodeToString(iv) + ":" + hex.EncodeToString(ciphertext)

	_, err := DecryptToken(encrypted, wrongKey)
	if err == nil {
		t.Fatal("expected error with wrong key")
	}
}

func TestDecryptToken_InvalidFormat(t *testing.T) {
	t.Parallel()
	_, err := DecryptToken("not-valid", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
}

func TestRegisterWithEphemeralKey(t *testing.T) {
	t.Parallel()
	var receivedKey string
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		receivedKey = body["ephemeral_key"]
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer srv.Close()

	err := Register(context.Background(), srv.URL, "fortnox", "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", "key", "aabbccdd0123456789abcdef0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if receivedKey != "aabbccdd0123456789abcdef0123456789abcdef0123456789abcdef01234567" {
		t.Errorf("ephemeral_key = %q", receivedKey)
	}
}

func TestRegisterWithoutEphemeralKey(t *testing.T) {
	t.Parallel()
	var receivedBody map[string]interface{}
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&receivedBody)
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer srv.Close()

	err := Register(context.Background(), srv.URL, "fortnox", "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", "key", "")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, exists := receivedBody["ephemeral_key"]; exists {
		t.Error("ephemeral_key should not be sent when empty")
	}
}

func TestHost_Default(t *testing.T) {
	t.Setenv("ONDATRA_OAUTH_HOST", "")
	if got := Host(); got != DefaultHost {
		t.Errorf("Host() = %q, want %q", got, DefaultHost)
	}
}

func TestHost_Override(t *testing.T) {
	t.Setenv("ONDATRA_OAUTH_HOST", "https://custom.example.com")
	if got := Host(); got != "https://custom.example.com" {
		t.Errorf("Host() = %q, want custom", got)
	}
}
