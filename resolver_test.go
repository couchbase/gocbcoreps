package gocbcoreps

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"testing"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const numNodes = 3

var _ resolver.ClientConn = (*mockClientConn)(nil)

type mockClientConn struct {
	t       *testing.T
	buckets []string
	doneCh  chan struct{}
}

func (mcc *mockClientConn) UpdateState(s resolver.State) error {
	// We are running against local host and should resolve the ipv4 and ipv6
	// address
	require.Len(mcc.t, s.Endpoints, 2)

	for _, ep := range s.Endpoints {
		numVbs := ep.Attributes.Value("numvbs")
		require.NotNil(mcc.t, numVbs)

		numvbsMap, ok := numVbs.(map[string]uint32)
		require.True(mcc.t, ok)

		localVbs := ep.Attributes.Value("localvbs")
		require.NotNil(mcc.t, localVbs)

		localVbsMap, ok := localVbs.(map[string][]uint32)
		require.True(mcc.t, ok)

		for _, bucket := range mcc.buckets {
			bucketNumVbs, ok := numvbsMap[bucket]
			require.True(mcc.t, ok)
			require.Equal(mcc.t, uint32(1024), bucketNumVbs)

			bucketLocalVbs, ok := localVbsMap[bucket]
			require.True(mcc.t, ok)
			require.GreaterOrEqual(mcc.t, uint32(len(bucketLocalVbs)), bucketNumVbs/(numNodes+1))
			require.LessOrEqual(mcc.t, uint32(len(bucketLocalVbs)), bucketNumVbs/(numNodes-1))
		}
	}

	mcc.doneCh <- struct{}{}
	return nil
}

func (mcc *mockClientConn) ReportError(err error) {}

func (mcc *mockClientConn) NewAddress(addresses []resolver.Address) {}

func (mcc *mockClientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	require.Equal(mcc.t, `{"loadBalancingPolicy": "optimized_load_balancer"}`, serviceConfigJSON)
	return &serviceconfig.ParseResult{}
}

func TestResolve(t *testing.T) {
	rb := CustomResolverBuilder{
		ctx:             context.Background(),
		auth:            NewBasicAuthenticator(TestOpts.Username, TestOpts.Password),
		resolveInterval: defaultResolveInterval,
	}

	target := resolver.Target{
		URL: url.URL{
			Scheme: OptimizedRoutingScheme,
			Path:   fmt.Sprintf("/%s", TestOpts.CngConnstr),
		},
	}

	conn := mockClientConn{
		t:       t,
		buckets: []string{TestOpts.BucketName},
		doneCh:  make(chan struct{}),
	}

	r, err := rb.Build(
		target,
		&conn,
		resolver.BuildOptions{
			DialCreds: credentials.NewTLS(
				&tls.Config{
					InsecureSkipVerify: true,
				},
			)},
	)
	require.NoError(t, err)

	_, ok := r.(*customResolver)
	require.True(t, ok)

	// Wait for resolver to do the resolving
	<-conn.doneCh

	testBucket := "optimisedRoutingBucket"
	conn.buckets = append(conn.buckets, testBucket)

	// Create a new bucket and check that the resolver picks it up
	bucketClient := admin_bucket_v1.NewBucketAdminServiceClient(TestOpts.ClientConn)
	_, err = bucketClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName: testBucket,
	})
	requireRpcStatus(t, err, codes.OK)
	checkBucketCreated(t, bucketClient, testBucket)
	scheduleBucketCleanup(t, bucketClient, testBucket)

	// Wait for resolver to do the resolving
	<-conn.doneCh
}
