package gocbcoreps

import (
	"context"
	"crypto/x509"
	"net"
	"sync"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/resolver"

	grpc_logsettable "github.com/grpc-ecosystem/go-grpc-middleware/logging/settable"
	"go.uber.org/zap/zapgrpc"

	"github.com/couchbase/goprotostellar/genproto/view_v1"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"

	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/couchbase/goprotostellar/genproto/analytics_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/goprotostellar/genproto/routing_v2"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"go.uber.org/zap"
)

type RoutingClient struct {
	routing *atomicRoutingTable
	lock    sync.Mutex
	logger  *zap.Logger
	auth    Authenticator
}

// Verify that RoutingClient implements Conn
var _ Conn = (*RoutingClient)(nil)

type DialOptions struct {
	RootCAs            *x509.CertPool
	Authenticator      Authenticator
	Logger             *zap.Logger
	InsecureSkipVerify bool
	PoolSize           uint32
	TracerProvider     trace.TracerProvider
	MeterProvider      metric.MeterProvider
	OptimizedRouting   bool
}

func Dial(target string, opts *DialOptions) (*RoutingClient, error) {
	return DialContext(context.Background(), target, opts)
}

func DialContext(ctx context.Context, target string, opts *DialOptions) (*RoutingClient, error) {
	// use port 18091 by default
	{
		_, _, err := net.SplitHostPort(target)
		if err != nil {
			// if we couldn't split the host/port, assume there is no port
			target = target + ":18098"
		}
	}

	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	// Setup grpc level logging, so that we can pipe connection level issues into our logs.
	grpc_logsettable.ReplaceGrpcLoggerV2().Set(zapgrpc.NewLogger(logger))

	var conns []*routingConn

	var poolSize uint32 = 1
	if opts.PoolSize > 0 {
		poolSize = opts.PoolSize
	}

	balancer.Register(CustomLBBuilder{
		logger: logger,
	})

	auth, _ := opts.Authenticator.(*BasicAuthenticator)
	resolver.Register(&CustomResolverBuilder{
		ctx:             ctx,
		logger:          logger,
		basicAuth:       auth,
		resolveInterval: defaultResolveInterval,
	})

	if opts.OptimizedRouting {
		target = OptimizedRoutingScheme + ":///" + target
	}

	for i := uint32(0); i < poolSize; i++ {
		conn, err := dialRoutingConn(ctx, target, &routingConnOptions{
			RootCAs:            opts.RootCAs,
			Authenticator:      opts.Authenticator,
			InsecureSkipVerify: opts.InsecureSkipVerify,
			TracerProvider:     opts.TracerProvider,
			MeterProvider:      opts.MeterProvider,
		})
		if err != nil {
			return nil, err
		}

		conns = append(conns, conn)
	}

	routing := &atomicRoutingTable{}
	routing.Store(&routingTable{
		Conns: newRoutingConnPool(conns),
	})

	return &RoutingClient{
		routing: routing,
		logger:  logger,
		auth:    opts.Authenticator,
	}, nil
}

type ReconfigureAuthenticatorOptions struct {
	Authenticator Authenticator
}

func (c *RoutingClient) ReconfigureAuthenticator(opts ReconfigureAuthenticatorOptions) error {
	auth := opts.Authenticator
	c.lock.Lock()
	defer c.lock.Unlock()

	switch a := c.auth.(type) {
	case *BasicAuthenticator:
		switch na := auth.(type) {
		case *BasicAuthenticator:
			data := na.encodedData.Load()
			a.encodedData.Store(data)
		default:
			return ErrAuthenticatorMismatch
		}
	case *CertificateAuthenticator:
		switch na := auth.(type) {
		case *CertificateAuthenticator:
			cert := na.certificate.Load()
			a.certificate.Store(cert)
		default:
			return ErrAuthenticatorMismatch
		}
	default:
		return ErrAuthenticatorUnsupported
	}

	return nil
}

func (c *RoutingClient) ConnectionState() ConnState {
	r := c.routing.Load()

	return r.Conns.State()
}

func (c *RoutingClient) fetchConn() *routingConn {
	// TODO(brett19): We should probably be more clever here...
	r := c.routing.Load()

	return r.Conns.Conn()
}

func (c *RoutingClient) fetchConnForBucket(bucketName string) *routingConn {
	// TODO(brett19): Implement routing of bucket-specific requests
	return c.fetchConn()
}

func (c *RoutingClient) fetchConnForKey(bucketName string, key string) *routingConn {
	// TODO(brett19): Implement routing of key-specific requests.
	return c.fetchConn()
}

func (c *RoutingClient) RoutingV2() routing_v2.RoutingServiceClient {
	return &routingImpl_RoutingV2{c}
}

func (c *RoutingClient) KvV1() kv_v1.KvServiceClient {
	return &routingImpl_KvV1{c}
}

func (c *RoutingClient) QueryV1() query_v1.QueryServiceClient {
	return &routingImpl_QueryV1{c}
}

func (c *RoutingClient) CollectionV1() admin_collection_v1.CollectionAdminServiceClient {
	return &routingImpl_CollectionV1{c}
}
func (c *RoutingClient) BucketV1() admin_bucket_v1.BucketAdminServiceClient {
	return &routingImpl_BucketV1{c}
}

func (c *RoutingClient) AnalyticsV1() analytics_v1.AnalyticsServiceClient {
	return &routingImpl_AnalyticsV1{c}
}

func (c *RoutingClient) SearchV1() search_v1.SearchServiceClient {
	return &routingImpl_SearchV1{c}
}

func (c *RoutingClient) ViewV1() view_v1.ViewServiceClient {
	return &routingImpl_ViewV1{c}
}

func (c *RoutingClient) QueryAdminV1() admin_query_v1.QueryAdminServiceClient {
	return &routingImpl_QueryAdminV1{c}
}

func (c *RoutingClient) SearchAdminV1() admin_search_v1.SearchAdminServiceClient {
	return &routingImpl_SearchAdminV1{c}
}

func (c *RoutingClient) Close() error {
	table := c.routing.Load()
	if table == nil {
		// We're already closed.
		return nil
	}
	c.lock.Lock()
	closeErr := table.Conns.Close()
	c.routing.Store(nil)

	c.lock.Unlock()

	return closeErr
}
