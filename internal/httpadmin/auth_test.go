package httpadmin

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestCloudflareAuthenticatorVerifiesAccessJWT(t *testing.T) {
	now := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks, err := json.Marshal(map[string]any{"keys": []any{map[string]any{
		"kty": "RSA", "kid": "key-1", "use": "sig", "alg": "RS256",
		"n": base64.RawURLEncoding.EncodeToString(privateKey.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(privateKey.E)).Bytes()),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "https://team.example/cdn-cgi/access/certs" {
			t.Fatalf("JWKS URL=%s", request.URL)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(jwks)), Header: make(http.Header)}, nil
	})}
	authenticator := newCloudflareAuthenticator("team.example", "admin-audience", client, func() time.Time { return now })
	claims := map[string]any{"iss": "https://team.example", "aud": []string{"admin-audience"}, "sub": "user-1",
		"email": "admin@example.com", "type": "app", "nbf": now.Add(-time.Minute).Unix(), "exp": now.Add(time.Minute).Unix()}
	token := signAccessToken(t, privateKey, claims)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://admin.example/admin", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Cf-Access-Jwt-Assertion", token)
	identity, err := authenticator.Authenticate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Subject != "user-1" || identity.Email != "admin@example.com" {
		t.Fatalf("principal=%+v", identity)
	}

	claims["exp"] = now.Add(-time.Hour).Unix()
	request.Header.Set("Cf-Access-Jwt-Assertion", signAccessToken(t, privateKey, claims))
	if _, err := authenticator.Authenticate(context.Background(), request); err == nil {
		t.Fatal("expected expired token rejection")
	}
}

func signAccessToken(t *testing.T, key *rsa.PrivateKey, claims any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": "key-1", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}
