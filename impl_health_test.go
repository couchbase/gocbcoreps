package gocbcoreps

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func dialTestRoutingClient(t *testing.T, username, password string) *RoutingClient {
	return dialTestRoutingClientAt(t, TestOpts.CngConnstr, username, password)
}

func dialTestRoutingClientAt(t *testing.T, target, username, password string) *RoutingClient {
	client, err := Dial(target, &DialOptions{
		Authenticator:      NewBasicAuthenticator(username, password),
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

func TestHealthCheck(t *testing.T) {
	type test struct {
		description string
		password    string
	}

	tests := []test{
		{
			description: "ValidCredentials",
			password:    TestOpts.Password,
		},
		{
			// Health checks are sent without credentials, so they succeed regardless.
			description: "InvalidCredentials",
			password:    TestOpts.Password + "-invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			client := dialTestRoutingClient(t, TestOpts.Username, tt.password)

			resp, err := client.HealthV1().Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
			requireRpcStatus(t, err, codes.OK)
			require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
		})
	}
}

func TestInvalidCredentialsStillSentForNonHealthCheckRequests(t *testing.T) {
	client := dialTestRoutingClient(t, TestOpts.Username, TestOpts.Password+"-invalid")

	_, err := client.KvV1().Get(context.Background(), &kv_v1.GetRequest{
		BucketName:     TestOpts.BucketName,
		ScopeName:      TestOpts.ScopeName,
		CollectionName: TestOpts.CollectionName,
		Key:            RandomDocId(),
	})
	requireRpcStatus(t, err, codes.PermissionDenied)
}

// authRecorder is a server interceptor that records the authorization header received for each method.
type authRecorder struct {
	lock sync.Mutex
	auth map[string][]string
}

func (r *authRecorder) record(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)

	r.lock.Lock()
	r.auth[method] = md.Get("authorization")
	r.lock.Unlock()
}

func (r *authRecorder) Get(method string) ([]string, bool) {
	r.lock.Lock()
	defer r.lock.Unlock()

	auth, ok := r.auth[method]
	return auth, ok
}

func (r *authRecorder) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	r.record(ctx, info.FullMethod)
	return handler(ctx, req)
}

// selfSignedCert generates a throwaway certificate for a local TLS server.
func selfSignedCert(t *testing.T) tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TestHealthCheckSendsNoCredentials dials a local gRPC server and verifies the authorization header the server
// actually receives.
func TestHealthCheckSendsNoCredentials(t *testing.T) {
	recorder := &authRecorder{auth: map[string][]string{}}
	cert := selfSignedCert(t)

	srv := grpc.NewServer(
		grpc.Creds(credentials.NewServerTLSFromCert(&cert)),
		grpc.UnaryInterceptor(recorder.unary),
	)
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	kv_v1.RegisterKvServiceServer(srv, kv_v1.UnimplementedKvServiceServer{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	client := dialTestRoutingClientAt(t, lis.Addr().String(), "Administrator", "password")

	resp, err := client.HealthV1().Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	requireRpcStatus(t, err, codes.OK)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	auth, ok := recorder.Get(grpc_health_v1.Health_Check_FullMethodName)
	require.True(t, ok, "health check did not reach the server")
	require.Empty(t, auth)

	_, err = client.KvV1().Get(context.Background(), &kv_v1.GetRequest{Key: "key"})
	requireRpcStatus(t, err, codes.Unimplemented)

	auth, ok = recorder.Get(kv_v1.KvService_Get_FullMethodName)
	require.True(t, ok, "kv get did not reach the server")
	require.Equal(t, []string{"Basic " + base64.StdEncoding.EncodeToString([]byte("Administrator:password"))}, auth)
}
