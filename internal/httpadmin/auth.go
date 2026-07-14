package httpadmin

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

var errUnauthenticated = errors.New("admin identity is not authenticated")

type principal struct {
	Subject string
	Email   string
}

type authenticator interface {
	Authenticate(context.Context, *http.Request) (principal, error)
}

type developmentAuthenticator struct{ email string }

func (a developmentAuthenticator) Authenticate(_ context.Context, _ *http.Request) (principal, error) {
	return principal{Subject: "development:" + a.email, Email: a.email}, nil
}

type audienceClaim []string

func (a *audienceClaim) UnmarshalJSON(data []byte) error {
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		*a = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return errors.New("invalid audience claim")
	}
	*a = many
	return nil
}

type jwtHeader struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
}

type accessClaims struct {
	Audience  audienceClaim `json:"aud"`
	Email     string        `json:"email"`
	Expires   int64         `json:"exp"`
	NotBefore int64         `json:"nbf"`
	Issuer    string        `json:"iss"`
	Subject   string        `json:"sub"`
	Type      string        `json:"type"`
}

type cloudflareAuthenticator struct {
	issuer, audience, certsURL string
	client                     *http.Client
	now                        func() time.Time
	mu                         sync.RWMutex
	keys                       map[string]*rsa.PublicKey
	keysExpireAt               time.Time
	lastRefreshAt              time.Time
}

func newCloudflareAuthenticator(teamDomain, audience string, client *http.Client, now func() time.Time) *cloudflareAuthenticator {
	issuer := strings.TrimRight(teamDomain, "/")
	if !strings.Contains(issuer, "://") {
		issuer = "https://" + issuer
	}
	return &cloudflareAuthenticator{
		issuer: issuer, audience: audience, certsURL: issuer + "/cdn-cgi/access/certs",
		client: client, now: now, keys: make(map[string]*rsa.PublicKey),
	}
}

func (a *cloudflareAuthenticator) Authenticate(ctx context.Context, request *http.Request) (principal, error) {
	token := request.Header.Get("Cf-Access-Jwt-Assertion")
	if token == "" {
		if cookie, err := request.Cookie("CF_Authorization"); err == nil {
			token = cookie.Value
		}
	}
	if token == "" || len(token) > 16<<10 {
		return principal{}, errUnauthenticated
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return principal{}, errUnauthenticated
	}
	var header jwtHeader
	if err := decodeJWTPart(parts[0], &header); err != nil || header.Algorithm != "RS256" || header.KeyID == "" {
		return principal{}, errUnauthenticated
	}
	var claims accessClaims
	if err := decodeJWTPart(parts[1], &claims); err != nil {
		return principal{}, errUnauthenticated
	}
	key, err := a.key(ctx, header.KeyID)
	if err != nil {
		return principal{}, errUnauthenticated
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return principal{}, errUnauthenticated
	}
	now := a.now()
	if claims.Issuer != a.issuer || !containsAudience(claims.Audience, a.audience) || claims.Type != "app" ||
		claims.Subject == "" || claims.Email == "" || claims.Expires == 0 || now.After(time.Unix(claims.Expires, 0).Add(30*time.Second)) ||
		(claims.NotBefore != 0 && now.Before(time.Unix(claims.NotBefore, 0).Add(-30*time.Second))) {
		return principal{}, errUnauthenticated
	}
	return principal{Subject: claims.Subject, Email: claims.Email}, nil
}

func decodeJWTPart(value string, target any) error {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(data) > 16<<10 {
		return errUnauthenticated
	}
	return json.Unmarshal(data, target)
}

func containsAudience(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (a *cloudflareAuthenticator) key(ctx context.Context, id string) (*rsa.PublicKey, error) {
	a.mu.RLock()
	key, found := a.keys[id]
	fresh := a.now().Before(a.keysExpireAt)
	a.mu.RUnlock()
	if found && fresh {
		return key, nil
	}
	if err := a.refresh(ctx, !found); err != nil {
		return nil, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	key, found = a.keys[id]
	if !found {
		return nil, errUnauthenticated
	}
	return key, nil
}

func (a *cloudflareAuthenticator) refresh(ctx context.Context, force bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	if len(a.keys) > 0 && now.Before(a.keysExpireAt) && (!force || now.Sub(a.lastRefreshAt) < time.Minute) {
		return nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, a.certsURL, nil)
	if err != nil {
		return err
	}
	response, err := a.client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("cloudflare JWKS returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 256<<10))
	if err != nil {
		return err
	}
	var document struct {
		Keys []jsonWebKey `json:"keys"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		return err
	}
	if len(document.Keys) == 0 || len(document.Keys) > 16 {
		return errors.New("cloudflare JWKS has an invalid key count")
	}
	keys := make(map[string]*rsa.PublicKey, len(document.Keys))
	for _, item := range document.Keys {
		key, err := item.publicKey()
		if err != nil {
			return err
		}
		if _, exists := keys[item.KeyID]; exists {
			return errors.New("cloudflare JWKS contains duplicate key IDs")
		}
		keys[item.KeyID] = key
	}
	a.keys = keys
	a.lastRefreshAt = now
	a.keysExpireAt = now.Add(time.Hour)
	return nil
}

type jsonWebKey struct {
	KeyType, KeyID, Use, Algorithm string
	Modulus, Exponent              string
}

func (k *jsonWebKey) UnmarshalJSON(data []byte) error {
	var value struct {
		KeyType     string   `json:"kty"`
		KeyID       string   `json:"kid"`
		Use         string   `json:"use"`
		Algorithm   string   `json:"alg"`
		Modulus     string   `json:"n"`
		Exponent    string   `json:"e"`
		Certificate []string `json:"x5c"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	k.KeyType, k.KeyID, k.Use, k.Algorithm = value.KeyType, value.KeyID, value.Use, value.Algorithm
	k.Modulus, k.Exponent = value.Modulus, value.Exponent
	if k.Modulus == "" && len(value.Certificate) > 0 {
		certificate, err := base64.StdEncoding.DecodeString(value.Certificate[0])
		if err != nil {
			return err
		}
		parsed, err := x509.ParseCertificate(certificate)
		if err != nil {
			return err
		}
		public, ok := parsed.PublicKey.(*rsa.PublicKey)
		if !ok {
			return errors.New("cloudflare certificate is not RSA")
		}
		k.Modulus = base64.RawURLEncoding.EncodeToString(public.N.Bytes())
		k.Exponent = base64.RawURLEncoding.EncodeToString(big.NewInt(int64(public.E)).Bytes())
	}
	return nil
}

func (k jsonWebKey) publicKey() (*rsa.PublicKey, error) {
	if k.KeyType != "RSA" || k.KeyID == "" || k.Algorithm != "RS256" || (k.Use != "" && k.Use != "sig") {
		return nil, errors.New("cloudflare JWKS contains an unsupported key")
	}
	modulus, err := base64.RawURLEncoding.DecodeString(k.Modulus)
	if err != nil || len(modulus) < 256 {
		return nil, errors.New("cloudflare JWKS contains an invalid modulus")
	}
	exponentBytes, err := base64.RawURLEncoding.DecodeString(k.Exponent)
	if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
		return nil, errors.New("cloudflare JWKS contains an invalid exponent")
	}
	exponent := 0
	for _, value := range exponentBytes {
		exponent = exponent<<8 | int(value)
	}
	if exponent < 3 {
		return nil, errors.New("cloudflare JWKS contains an invalid exponent")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: exponent}, nil
}
