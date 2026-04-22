// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// Default HTTP client settings
const (
	defaultTimeout    = 30 * time.Second
	defaultMaxRetries = 0
	defaultBackoffMs  = 1000
)

// httpOptions holds HTTP request options
type httpOptions struct {
	Timeout    time.Duration
	Retry      int
	Backoff    int
	Insecure   bool
	DigestAuth *digestCredentials
	ClientCert *clientCertConfig
}

// clientCertConfig holds mTLS client certificate configuration.
type clientCertConfig struct {
	CertFile string
	KeyFile  string
	CAFile   string // optional, for custom CA
}

// httpModule provides HTTP client functions with kwargs support.
// apiConfig is optional -- when non-nil, base_url/auth/headers/timeout/retry/backoff
// are injected as defaults into every request. Per-call kwargs override.
func httpModule(ctx context.Context, apiConfig ...*apiHTTPConfig) *starlarkstruct.Module {
	var cfg *apiHTTPConfig
	if len(apiConfig) > 0 {
		cfg = apiConfig[0]
	}
	return &starlarkstruct.Module{
		Name: "http",
		Members: starlark.StringDict{
			"get":    starlark.NewBuiltin("http.get", httpRequest(ctx, "GET", cfg)),
			"post":   starlark.NewBuiltin("http.post", httpRequest(ctx, "POST", cfg)),
			"put":    starlark.NewBuiltin("http.put", httpRequest(ctx, "PUT", cfg)),
			"delete": starlark.NewBuiltin("http.delete", httpRequest(ctx, "DELETE", cfg)),
			"patch":  starlark.NewBuiltin("http.patch", httpRequest(ctx, "PATCH", cfg)),
			"upload": starlark.NewBuiltin("http.upload", httpUpload(ctx, cfg)),
		},
	}
}

// APIHTTPConfig holds defaults injected into every http.* call from the API dict.
type APIHTTPConfig struct {
	BaseURL    string
	Headers    map[string]string
	Timeout    int // seconds, 0 = use default
	Retry      int
	Backoff    int
	Auth       map[string]any // auth config from API dict
	ProjectDir string         // for OAuth provider flow
	Ctx        context.Context // for OAuth provider flow
}

// apiHTTPConfig is an alias for internal use.
type apiHTTPConfig = APIHTTPConfig

// injectAPIAuth resolves auth config from API dict and sets headers or query params.
func injectAPIAuth(auth map[string]any, headers map[string]string, urlStr *string, cfg *apiHTTPConfig) error {
	// Google service account: google_key_file or google_key_file_env
	keyFile, _ := auth["google_key_file"].(string)
	if envName, ok := auth["google_key_file_env"].(string); ok && envName != "" {
		keyFile = os.Getenv(envName)
	}
	if keyFile != "" {
		keyData, err := os.ReadFile(keyFile)
		if err != nil {
			return fmt.Errorf("auth: read google_key_file %q: %w", keyFile, err)
		}
		var key ServiceAccountKey
		if err := json.Unmarshal(keyData, &key); err != nil {
			return fmt.Errorf("auth: parse google_key_file %q: %w", keyFile, err)
		}
		scope, _ := auth["scope"].(string)
		tp := &tokenProvider{
			ctx:         cfg.Ctx,
			googleSAKey: &key,
			scope:       scope,
		}
		tok, err := tp.AccessToken()
		if err != nil {
			return fmt.Errorf("auth: google service account: %w", err)
		}
		headers["Authorization"] = "Bearer " + tok
		return nil
	}

	// provider: OAuth2 managed token (refreshes automatically)
	if provider, ok := auth["provider"].(string); ok && provider != "" {
		tp := &tokenProvider{
			ctx:        cfg.Ctx,
			provider:   provider,
			projectDir: cfg.ProjectDir,
		}
		tok, err := tp.AccessToken()
		if err != nil {
			return fmt.Errorf("auth: oauth token for %q: %w", provider, err)
		}
		headers["Authorization"] = "Bearer " + tok
		return nil
	}

	// env: API key from environment variable
	if envKey, ok := auth["env"].(string); ok && envKey != "" {
		token := os.Getenv(envKey)
		if token != "" {
			if param, ok := auth["param"].(string); ok && param != "" {
				// Query parameter auth
				u, err := url.Parse(*urlStr)
				if err == nil {
					q := u.Query()
					q.Set(param, token)
					u.RawQuery = q.Encode()
					*urlStr = u.String()
				}
			} else if header, ok := auth["header"].(string); ok && header != "" {
				// Custom header
				headers[header] = token
			} else {
				// Default: Authorization: Bearer
				headers["Authorization"] = "Bearer " + token
			}
		}
	}

	// env_user + env_pass: basic auth
	if userKey, ok := auth["env_user"].(string); ok && userKey != "" {
		if passKey, ok := auth["env_pass"].(string); ok && passKey != "" {
			user := os.Getenv(userKey)
			pass := os.Getenv(passKey)
			if user != "" {
				headers["Authorization"] = BasicAuth(user, pass)
			}
		}
	}
	return nil
}

// httpRequest creates a unified handler for all HTTP methods using kwargs.
// Signature: http.method(url, headers?, json?, data?, timeout?, retry?)
func httpRequest(ctx context.Context, method string, apiCfg *apiHTTPConfig) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var (
			urlStr      string
			headersDict *starlark.Dict
			paramsDict  *starlark.Dict
			jsonBody    starlark.Value
			dataDict    *starlark.Dict
			bodyStr     string
			timeout     starlark.Value // float seconds
			retry       int
			backoff     starlark.Value // float seconds
			auth        *starlark.Tuple
			cert        string
			key         string
			ca          string
		)
		if err := starlark.UnpackArgs(fn.Name(), args, kwargs,
			"url", &urlStr,
			"headers?", &headersDict,
			"params?", &paramsDict,
			"json?", &jsonBody,
			"data?", &dataDict,
			"body?", &bodyStr,
			"timeout?", &timeout,
			"retry?", &retry,
			"backoff?", &backoff,
			"auth?", &auth,
			"cert?", &cert,
			"key?", &key,
			"ca?", &ca,
		); err != nil {
			return nil, err
		}

		// Inject API config defaults (base_url, headers, timeout, retry, backoff)
		if apiCfg != nil {
			// base_url: prepend to relative URLs
			if apiCfg.BaseURL != "" && !strings.HasPrefix(urlStr, "http://") && !strings.HasPrefix(urlStr, "https://") {
				urlStr = strings.TrimRight(apiCfg.BaseURL, "/") + "/" + strings.TrimLeft(urlStr, "/")
			}
			// timeout default (only if not set by caller)
			if timeout == nil && apiCfg.Timeout > 0 {
				timeout = starlark.Float(float64(apiCfg.Timeout))
			}
			// retry default (only if not set by caller -- 0 means "not set" since UnpackArgs inits to 0)
			if retry == 0 && apiCfg.Retry > 0 {
				retry = apiCfg.Retry
			}
			// backoff default
			if backoff == nil && apiCfg.Backoff > 0 {
				backoff = starlark.Float(float64(apiCfg.Backoff))
			}
		}

		// Append query params to URL
		if paramsDict != nil {
			u, err := url.Parse(urlStr)
			if err != nil {
				return nil, fmt.Errorf("%s: invalid url: %w", fn.Name(), err)
			}
			q := u.Query()
			for _, item := range paramsDict.Items() {
				k, ok := starlark.AsString(item[0])
				if !ok {
					return nil, fmt.Errorf("%s: params key must be string, got %s", fn.Name(), item[0].Type())
				}
				v, ok := starlark.AsString(item[1])
				if !ok {
					return nil, fmt.Errorf("%s: params value must be string, got %s", fn.Name(), item[1].Type())
				}
				if k != "" {
					q.Set(k, v)
				}
			}
			u.RawQuery = q.Encode()
			urlStr = u.String()
		}

		// Validate: cert and key must be provided together
		if (cert != "") != (key != "") {
			return nil, fmt.Errorf("%s: cert and key must be provided together", fn.Name())
		}
		if ca != "" && cert == "" {
			return nil, fmt.Errorf("%s: ca requires cert and key", fn.Name())
		}

		// Validate: json, data, and body are mutually exclusive
		bodyArgCount := 0
		if jsonBody != nil && jsonBody != starlark.None {
			bodyArgCount++
		}
		if dataDict != nil {
			bodyArgCount++
		}
		if bodyStr != "" {
			bodyArgCount++
		}
		if bodyArgCount > 1 {
			return nil, fmt.Errorf("%s: json, data, and body are mutually exclusive", fn.Name())
		}

		// Build headers: API config defaults first, then per-call overrides win
		headers := make(map[string]string)
		if apiCfg != nil {
			for k, v := range apiCfg.Headers {
				headers[k] = v
			}
		}
		if headersDict != nil {
			for _, item := range headersDict.Items() {
				k, ok := starlark.AsString(item[0])
				if !ok {
					return nil, fmt.Errorf("%s: header key must be string, got %s", fn.Name(), item[0].Type())
				}
				v, ok := starlark.AsString(item[1])
				if !ok {
					return nil, fmt.Errorf("%s: header value must be string, got %s", fn.Name(), item[1].Type())
				}
				if k != "" {
					headers[k] = v
				}
			}
		}

		// Inject auth from API dict (only if caller didn't set auth= kwarg)
		if auth == nil && apiCfg != nil && apiCfg.Auth != nil {
			if authErr := injectAPIAuth(apiCfg.Auth, headers, &urlStr, apiCfg); authErr != nil {
				return nil, authErr
			}
		}

		// Build body
		var body []byte
		if jsonBody != nil && jsonBody != starlark.None {
			switch jb := jsonBody.(type) {
			case *starlark.Dict:
				goVal, err := starlarkToGo(jb)
				if err != nil {
					return nil, err
				}
				body, err = json.Marshal(goVal)
				if err != nil {
					return nil, fmt.Errorf("%s: marshal json body: %w", fn.Name(), err)
				}
			case *starlark.List:
				goVal, err := starlarkToGo(jb)
				if err != nil {
					return nil, err
				}
				body, err = json.Marshal(goVal)
				if err != nil {
					return nil, fmt.Errorf("%s: marshal json body: %w", fn.Name(), err)
				}
			case starlark.String:
				body = []byte(string(jb))
			default:
				return nil, fmt.Errorf("%s: json must be a dict, list, or string, got %s", fn.Name(), jsonBody.Type())
			}
			if headers["Content-Type"] == "" {
				headers["Content-Type"] = "application/json"
			}
		} else if dataDict != nil {
			form := url.Values{}
			for _, item := range dataDict.Items() {
				k, ok := starlark.AsString(item[0])
				if !ok {
					return nil, fmt.Errorf("%s: data key must be string, got %s", fn.Name(), item[0].Type())
				}
				v, ok := starlark.AsString(item[1])
				if !ok {
					return nil, fmt.Errorf("%s: data value must be string, got %s", fn.Name(), item[1].Type())
				}
				form.Set(k, v)
			}
			body = []byte(form.Encode())
			if headers["Content-Type"] == "" {
				headers["Content-Type"] = "application/x-www-form-urlencoded"
			}
		}

		// Raw body string (for CSV, XML, etc.)
		if bodyStr != "" && body == nil {
			body = []byte(bodyStr)
		}

		// Build options
		opts := httpOptions{
			Timeout:  defaultTimeout,
			Retry:    retry,
			Backoff:  defaultBackoffMs,
			Insecure: false,
		}
		if timeout != nil {
			switch v := timeout.(type) {
			case starlark.Float:
				opts.Timeout = time.Duration(float64(v) * float64(time.Second))
			case starlark.Int:
				i, _ := v.Int64()
				opts.Timeout = time.Duration(i) * time.Second
			}
		}
		if backoff != nil {
			switch v := backoff.(type) {
			case starlark.Float:
				opts.Backoff = int(float64(v) * 1000) // seconds → ms
			case starlark.Int:
				i, _ := v.Int64()
				opts.Backoff = int(i) * 1000 // seconds → ms
			}
		}

		// Parse auth kwarg: (user, pass) or (user, pass, "basic") or (user, pass, "digest")
		if auth != nil {
			n := auth.Len()
			if n < 2 || n > 3 {
				return nil, fmt.Errorf("%s: auth must be a tuple of (user, pass) or (user, pass, scheme)", fn.Name())
			}
			user, ok := starlark.AsString(auth.Index(0))
			if !ok {
				return nil, fmt.Errorf("%s: auth username must be string, got %s", fn.Name(), auth.Index(0).Type())
			}
			pass, ok := starlark.AsString(auth.Index(1))
			if !ok {
				return nil, fmt.Errorf("%s: auth password must be string, got %s", fn.Name(), auth.Index(1).Type())
			}
			if user == "" {
				return nil, fmt.Errorf("%s: auth username must be a non-empty string", fn.Name())
			}

			scheme := "basic"
			if n == 3 {
				s, ok := starlark.AsString(auth.Index(2))
				if !ok {
					return nil, fmt.Errorf("%s: auth scheme must be string, got %s", fn.Name(), auth.Index(2).Type())
				}
				scheme = s
			}

			switch scheme {
			case "basic":
				headers["Authorization"] = BasicAuth(user, pass)
			case "digest":
				opts.DigestAuth = &digestCredentials{Username: user, Password: pass}
				// Digest needs at least 1 retry for the challenge-response
				if opts.Retry < 1 {
					opts.Retry = 1
				}
			default:
				return nil, fmt.Errorf("%s: auth scheme must be \"basic\" or \"digest\", got %q", fn.Name(), scheme)
			}
		}

		// Build client cert config
		if cert != "" {
			opts.ClientCert = &clientCertConfig{
				CertFile: cert,
				KeyFile:  key,
				CAFile:   ca,
			}
		}

		resp, err := DoHTTPWithRetry(ctx, method, urlStr, body, headers, opts)
		if err != nil {
			return nil, err
		}
		return httpResponseToStarlark(resp), nil
	}
}

// httpUpload creates a multipart form-data POST request.
// Signature: http.upload(url, file, field?, filename?, headers?, fields?, timeout?, retry?, backoff?, auth?)
//
// Parameters:
//   - url: target URL
//   - file: path to file on disk
//   - field: form field name for the file (default: "file")
//   - filename: override filename sent in the form (default: basename of file)
//   - headers: extra headers dict
//   - fields: dict of additional form fields (string key-value pairs)
//   - timeout, retry, backoff, auth: same as http.post
func httpUpload(ctx context.Context, apiCfg *apiHTTPConfig) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var (
			urlStr      string
			filePath    string
			field       string
			filename    string
			headersDict *starlark.Dict
			fieldsDict  *starlark.Dict
			timeout     starlark.Value
			retry       int
			backoff     starlark.Value
			authVal     starlark.Value
		)
		if err := starlark.UnpackArgs(fn.Name(), args, kwargs,
			"url", &urlStr,
			"file", &filePath,
			"field?", &field,
			"filename?", &filename,
			"headers?", &headersDict,
			"fields?", &fieldsDict,
			"timeout?", &timeout,
			"retry?", &retry,
			"backoff?", &backoff,
			"auth?", &authVal,
		); err != nil {
			return nil, err
		}

		if field == "" {
			field = "file"
		}

		// Build headers
		headers := make(map[string]string)
		if headersDict != nil {
			for _, item := range headersDict.Items() {
				k, ok := starlark.AsString(item[0])
				if !ok {
					return nil, fmt.Errorf("%s: header key must be string", fn.Name())
				}
				v, ok := starlark.AsString(item[1])
				if !ok {
					return nil, fmt.Errorf("%s: header value must be string", fn.Name())
				}
				if k != "" {
					headers[k] = v
				}
			}
		}

		// Build extra form fields
		var extraFields map[string]string
		if fieldsDict != nil {
			extraFields = make(map[string]string)
			for _, item := range fieldsDict.Items() {
				k, ok := starlark.AsString(item[0])
				if !ok {
					return nil, fmt.Errorf("%s: fields key must be string", fn.Name())
				}
				v, ok := starlark.AsString(item[1])
				if !ok {
					return nil, fmt.Errorf("%s: fields value must be string", fn.Name())
				}
				extraFields[k] = v
			}
		}

		// Inject API config defaults (same as httpRequest)
		if apiCfg != nil {
			// base_url: prepend to relative URLs
			if apiCfg.BaseURL != "" && !strings.HasPrefix(urlStr, "http://") && !strings.HasPrefix(urlStr, "https://") {
				urlStr = strings.TrimRight(apiCfg.BaseURL, "/") + "/" + strings.TrimLeft(urlStr, "/")
			}
			// headers: merge (per-call overrides)
			if apiCfg.Headers != nil {
				for k, v := range apiCfg.Headers {
					if _, exists := headers[k]; !exists {
						headers[k] = v
					}
				}
			}
			// auth: inject if caller didn't set auth= kwarg
			if (authVal == nil || authVal == starlark.None) && apiCfg.Auth != nil {
				if authErr := injectAPIAuth(apiCfg.Auth, headers, &urlStr, apiCfg); authErr != nil {
					return nil, authErr
				}
			}
			// timeout default
			if timeout == nil && apiCfg.Timeout > 0 {
				timeout = starlark.Float(float64(apiCfg.Timeout))
			}
			// retry default
			if retry == 0 && apiCfg.Retry > 0 {
				retry = apiCfg.Retry
			}
			// backoff default
			if backoff == nil && apiCfg.Backoff > 0 {
				backoff = starlark.Float(float64(apiCfg.Backoff))
			}
		}

		// Build options
		opts := httpOptions{
			Timeout: defaultTimeout,
			Retry:   retry,
			Backoff: defaultBackoffMs,
		}
		if timeout != nil {
			switch v := timeout.(type) {
			case starlark.Float:
				opts.Timeout = time.Duration(float64(v) * float64(time.Second))
			case starlark.Int:
				i, _ := v.Int64()
				opts.Timeout = time.Duration(i) * time.Second
			}
		}
		if backoff != nil {
			switch v := backoff.(type) {
			case starlark.Float:
				opts.Backoff = int(float64(v) * 1000)
			case starlark.Int:
				i, _ := v.Int64()
				opts.Backoff = int(i) * 1000
			}
		}

		// Parse auth (same logic as httpRequest)
		if authVal != nil && authVal != starlark.None {
			auth, ok := authVal.(*starlark.Tuple)
			if !ok {
				// Starlark runtime may pass Tuple (not *Tuple)
				if t, ok2 := authVal.(starlark.Tuple); ok2 {
					auth = &t
				} else {
					return nil, fmt.Errorf("%s: auth must be a tuple", fn.Name())
				}
			}
			n := auth.Len()
			if n < 2 || n > 3 {
				return nil, fmt.Errorf("%s: auth must be (user, pass) or (user, pass, scheme)", fn.Name())
			}
			user, ok := starlark.AsString(auth.Index(0))
			if !ok {
				return nil, fmt.Errorf("%s: auth username must be string", fn.Name())
			}
			pass, ok := starlark.AsString(auth.Index(1))
			if !ok {
				return nil, fmt.Errorf("%s: auth password must be string", fn.Name())
			}

			scheme := "basic"
			if n == 3 {
				s, ok := starlark.AsString(auth.Index(2))
				if !ok {
					return nil, fmt.Errorf("%s: auth scheme must be string", fn.Name())
				}
				scheme = s
			}

			switch scheme {
			case "basic":
				headers["Authorization"] = BasicAuth(user, pass)
			case "digest":
				opts.DigestAuth = &digestCredentials{Username: user, Password: pass}
				if opts.Retry < 1 {
					opts.Retry = 1
				}
			default:
				return nil, fmt.Errorf("%s: auth scheme must be \"basic\" or \"digest\", got %q", fn.Name(), scheme)
			}
		}

		resp, err := DoMultipartUpload(ctx, urlStr, filePath, field, filename, headers, extraFields, opts)
		if err != nil {
			return nil, err
		}
		return httpResponseToStarlark(resp), nil
	}
}

// httpResponseToStarlark converts an HTTPResponse to a Starlark struct.
func httpResponseToStarlark(resp *HTTPResponse) *starlarkstruct.Struct {
	// Build response headers dict
	headersDict := starlark.NewDict(len(resp.Headers))
	for k, v := range resp.Headers {
		if len(v) == 1 {
			headersDict.SetKey(starlark.String(k), starlark.String(v[0]))
		} else {
			elems := make([]starlark.Value, len(v))
			for i, val := range v {
				elems[i] = starlark.String(val)
			}
			headersDict.SetKey(starlark.String(k), starlark.NewList(elems))
		}
	}

	// Add parsed Link header
	if len(resp.Links) > 0 {
		linkDict := starlark.NewDict(len(resp.Links))
		for rel, u := range resp.Links {
			linkDict.SetKey(starlark.String(rel), starlark.String(u))
		}
		headersDict.SetKey(starlark.String("_links"), linkDict)
	}

	ok := resp.StatusCode >= 200 && resp.StatusCode < 300

	members := starlark.StringDict{
		"status_code": starlark.MakeInt(resp.StatusCode),
		"text":        starlark.String(resp.Body),
		"ok":          starlark.Bool(ok),
		"headers":     headersDict,
		"json":        starlark.None,
	}

	if resp.JSON != nil {
		jsonObj, err := goToStarlark(resp.JSON)
		if err == nil {
			members["json"] = jsonObj
		}
	}

	return starlarkstruct.FromStringDict(starlark.String("response"), members)
}
