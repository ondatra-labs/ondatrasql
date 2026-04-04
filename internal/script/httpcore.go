// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// HTTPResponse holds the result of an HTTP request in pure Go types.
type HTTPResponse struct {
	StatusCode int
	Body       []byte
	Headers    http.Header
	JSON       interface{}
	Links      map[string]string // Parsed Link header (RFC 5988)
}

// DoHTTPWithRetry performs an HTTP request with retry and exponential backoff.
func DoHTTPWithRetry(ctx context.Context, method, urlStr string, body []byte, headers map[string]string, opts httpOptions) (*HTTPResponse, error) {
	var lastErr error
	maxAttempts := opts.Retry + 1

	retryAfterWait := time.Duration(0)

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			var wait time.Duration
			if retryAfterWait > 0 {
				wait = retryAfterWait
				retryAfterWait = 0
			} else {
				backoffMs := opts.Backoff * (1 << (attempt - 1))
				jitter := rand.Intn(backoffMs/2 + 1)
				wait = time.Duration(backoffMs+jitter) * time.Millisecond
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
		}

		result, err := DoHTTP(ctx, method, urlStr, body, headers, opts)
		if err != nil {
			lastErr = err
			continue
		}

		// Handle Digest Auth challenge
		if result.StatusCode == 401 && opts.DigestAuth != nil {
			challenge := result.Headers.Get("Www-Authenticate")
			if strings.HasPrefix(challenge, "Digest ") {
				authHeader := computeDigestResponse(opts.DigestAuth, challenge, method, urlStr)
				headers["Authorization"] = authHeader
				opts.DigestAuth = nil // don't retry digest again
				continue
			}
		}

		// Retry on 429 (rate limit) or 5xx errors
		if result.StatusCode == 429 || (result.StatusCode >= 500 && result.StatusCode < 600) {
			lastErr = fmt.Errorf("HTTP %d", result.StatusCode)
			retryAfterWait = parseRetryAfter(result.Headers.Get("Retry-After"))
			continue
		}

		return result, nil
	}

	return nil, fmt.Errorf("request failed after %d attempts: %w", maxAttempts, lastErr)
}

// DoHTTP performs a single HTTP request with cancellation support.
func DoHTTP(ctx context.Context, method, urlStr string, body []byte, headers map[string]string, opts httpOptions) (*HTTPResponse, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlStr, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set default headers
	req.Header.Set("User-Agent", "OndatraSQL/1.0")
	if body != nil && headers["Content-Type"] == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	transport := &http.Transport{}
	tlsConfig := &tls.Config{}

	if opts.Insecure {
		tlsConfig.InsecureSkipVerify = true
	}

	if opts.ClientCert != nil {
		cert, err := tls.LoadX509KeyPair(opts.ClientCert.CertFile, opts.ClientCert.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}

		if opts.ClientCert.CAFile != "" {
			caCert, err := os.ReadFile(opts.ClientCert.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read CA certificate: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}
	}

	transport.TLSClientConfig = tlsConfig

	client := &http.Client{
		Timeout:   opts.Timeout,
		Transport: transport,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	result := &HTTPResponse{
		StatusCode: resp.StatusCode,
		Body:       respBody,
		Headers:    resp.Header,
	}

	// Try to parse as JSON
	var jsonData interface{}
	if json.Unmarshal(respBody, &jsonData) == nil {
		result.JSON = jsonData
	}

	// Parse Link header for pagination
	if linkHeader := resp.Header.Get("Link"); linkHeader != "" {
		result.Links = ParseLinkHeader(linkHeader)
	}

	return result, nil
}

// parseRetryAfter parses the Retry-After header value.
// Supports seconds (integer) and HTTP-date (RFC 1123) formats.
// Returns 0 if the header is empty or unparseable.
func parseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}

	// Try as seconds first
	if secs, err := strconv.Atoi(value); err == nil {
		return time.Duration(secs) * time.Second
	}

	// Try as HTTP-date (RFC 1123)
	if t, err := time.Parse(time.RFC1123, value); err == nil {
		wait := time.Until(t)
		if wait > 0 {
			return wait
		}
	}

	return 0
}

// DoMultipartUpload performs a multipart form-data POST with a file and optional fields.
func DoMultipartUpload(ctx context.Context, urlStr, filePath, fieldName, fileName string, headers map[string]string, fields map[string]string, opts httpOptions) (*HTTPResponse, error) {
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", filePath, err)
	}

	if fileName == "" {
		fileName = filepath.Base(filePath)
	}

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Add the file field
	part, err := writer.CreateFormFile(fieldName, fileName)
	if err != nil {
		return nil, fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write(fileData); err != nil {
		return nil, fmt.Errorf("write file data: %w", err)
	}

	// Add extra form fields
	for k, v := range fields {
		if err := writer.WriteField(k, v); err != nil {
			return nil, fmt.Errorf("write field %s: %w", k, err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	headers["Content-Type"] = writer.FormDataContentType()
	return DoHTTPWithRetry(ctx, "POST", urlStr, buf.Bytes(), headers, opts)
}

// ParseLinkHeader parses RFC 5988 Link headers into a map of rel -> URL.
func ParseLinkHeader(header string) map[string]string {
	links := make(map[string]string)

	parts := strings.Split(header, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)

		urlStart := strings.Index(part, "<")
		urlEnd := strings.Index(part, ">")
		if urlStart == -1 || urlEnd == -1 || urlEnd <= urlStart {
			continue
		}

		linkURL := part[urlStart+1 : urlEnd]

		relStart := strings.Index(part, `rel="`)
		if relStart == -1 {
			continue
		}
		relStart += 5
		relEnd := strings.Index(part[relStart:], `"`)
		if relEnd == -1 {
			continue
		}

		rel := part[relStart : relStart+relEnd]
		links[rel] = linkURL
	}

	return links
}
