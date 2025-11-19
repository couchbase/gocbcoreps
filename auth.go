package gocbcoreps

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"sync/atomic"
)

type Authenticator interface {
	isAuthenticator()
}

type BasicAuthenticator struct {
	encodedData atomic.Pointer[string]
}

// NewBasicAuthenticator creates PerRPCCredentials from the given username and password.
func NewBasicAuthenticator(username, password string) *BasicAuthenticator {
	basicAuth := username + ":" + password
	authValue := base64.StdEncoding.EncodeToString([]byte(basicAuth))

	auth := &BasicAuthenticator{}

	auth.encodedData.Store(&authValue)

	return auth
}

func (j *BasicAuthenticator) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	encodedData := j.encodedData.Load()
	return map[string]string{
		"authorization": "Basic " + *encodedData,
	}, nil
}

func (j *BasicAuthenticator) RequireTransportSecurity() bool {
	return false
}

func (j *BasicAuthenticator) UpdateCredentials(username, password string) {
	basicAuth := username + ":" + password
	authValue := base64.StdEncoding.EncodeToString([]byte(basicAuth))

	j.encodedData.Store(&authValue)
}

func (j *BasicAuthenticator) isAuthenticator() {}

type CertificateAuthenticator struct {
	certificate atomic.Pointer[tls.Certificate]
}

func NewCertificateAuthenticator(cert *tls.Certificate) *CertificateAuthenticator {
	auth := &CertificateAuthenticator{}
	auth.certificate.Store(cert)
	return auth
}

func (j *CertificateAuthenticator) GetClientCertificate(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cert := j.certificate.Load()

	return cert, nil
}

func (j *CertificateAuthenticator) UpdateCertificate(cert *tls.Certificate) {
	j.certificate.Store(cert)
}

func (j *CertificateAuthenticator) isAuthenticator() {}
