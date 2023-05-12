package gocbcoreps

import (
	"crypto/tls"
	"crypto/x509"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/couchbase/goprotostellar/genproto/analytics_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/goprotostellar/genproto/routing_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type routingConnOptions struct {
	InsecureSkipVerify bool // used for enabling TLS, but skipping verification
	ClientCertificate  *x509.CertPool
	Username           string
	Password           string
}

type routingConn struct {
	conn         *grpc.ClientConn
	routingV1    routing_v1.RoutingServiceClient
	kvV1         kv_v1.KvServiceClient
	queryV1      query_v1.QueryServiceClient
	collectionV1 admin_collection_v1.CollectionAdminServiceClient
	bucketV1     admin_bucket_v1.BucketAdminServiceClient
	analyticsV1  analytics_v1.AnalyticsServiceClient
}

// Verify that routingConn implements Conn
var _ Conn = (*routingConn)(nil)

func dialRoutingConn(address string, opts *routingConnOptions) (*routingConn, error) {
	var transportDialOpt grpc.DialOption
	var perRpcDialOpt grpc.DialOption

	if opts.ClientCertificate != nil { // use tls
		transportDialOpt = grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(opts.ClientCertificate, ""))
	} else if opts.InsecureSkipVerify { // use tls, but skip verification
		creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
		transportDialOpt = grpc.WithTransportCredentials(creds)
	} else { // plain text
		transportDialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	// setup basic auth.
	if opts.Username != "" && opts.Password != "" {
		basicAuthCreds, err := NewGrpcBasicAuth(opts.Username, opts.Password)
		if err != nil {
			return nil, err
		}
		perRpcDialOpt = grpc.WithPerRPCCredentials(basicAuthCreds)

	} else {
		perRpcDialOpt = nil
	}

	dialOpts := []grpc.DialOption{transportDialOpt}
	if perRpcDialOpt != nil {
		dialOpts = append(dialOpts, perRpcDialOpt)
	}

	conn, err := grpc.Dial(address, dialOpts...)
	if err != nil {
		return nil, err
	}

	return &routingConn{
		conn:      conn,
		routingV1: routing_v1.NewRoutingServiceClient(conn),
		kvV1:      kv_v1.NewKvServiceClient(conn),
		queryV1:   query_v1.NewQueryServiceClient(conn),
	}, nil
}

func (c *routingConn) RoutingV1() routing_v1.RoutingServiceClient {
	return c.routingV1
}

func (c *routingConn) KvV1() kv_v1.KvServiceClient {
	return c.kvV1
}

func (c *routingConn) QueryV1() query_v1.QueryServiceClient {
	return c.queryV1
}

func (c *routingConn) CollectionV1() admin_collection_v1.CollectionAdminServiceClient {
	return c.collectionV1
}

func (c *routingConn) BucketV1() admin_bucket_v1.BucketAdminServiceClient {
	return c.bucketV1
}

func (c *routingConn) AnalyticsV1() analytics_v1.AnalyticsServiceClient {
	return c.analyticsV1
}
