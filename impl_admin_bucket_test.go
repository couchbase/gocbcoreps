package gocbcoreps

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestBucketCreate(t *testing.T) {
	bucketClient := admin_bucket_v1.NewBucketAdminServiceClient(TestOpts.ClientConn)

	type test struct {
		description   string
		modifyDefault func(*admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest
		expect        codes.Code
	}

	tests := []test{
		{
			description: "Success",
			expect:      codes.OK,
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketName = "CreateBucketSuccess"
				return def
			},
		},
		{
			description: "BucketAlreadyExists",
			expect:      codes.AlreadyExists,
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketName = TestOpts.BucketName
				return def
			},
		},
		{
			description: "InvalidRamQuota",
			expect:      codes.InvalidArgument,
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				ramQuota := uint64(0)
				def.BucketName = "InvalidRamQuotaBucket"
				def.RamQuotaMb = &ramQuota
				return def
			},
		},
		{
			description: "InvalidNumReplicas",
			expect:      codes.InvalidArgument,
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				numReplicas := uint32(5)
				def.BucketName = "InvalidNumReplicasBucket"
				def.NumReplicas = &numReplicas
				return def
			},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			req := test.modifyDefault(&admin_bucket_v1.CreateBucketRequest{})
			_, err := bucketClient.CreateBucket(context.Background(), req)

			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				checkBucketCreated(t, bucketClient, req.BucketName)
				scheduleBucketCleanup(t, bucketClient, req.BucketName)
			}
		})
	}
}

func TestBucketUpdate(t *testing.T) {
	bucketClient := admin_bucket_v1.NewBucketAdminServiceClient(TestOpts.ClientConn)

	testBucket := "updateBucket"
	_, err := bucketClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName: testBucket,
	})
	requireRpcStatus(t, err, codes.OK)
	checkBucketCreated(t, bucketClient, testBucket)
	scheduleBucketCleanup(t, bucketClient, testBucket)

	type test struct {
		description string
		modifyReq   func(*admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest
		expect      codes.Code
	}

	tests := []test{
		{
			description: "RamQuota",
			modifyReq: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				ramQuota := uint64(123)
				def.RamQuotaMb = &ramQuota
				return def
			},
			expect: codes.OK,
		},
		{
			description: "BucketNotFound",
			modifyReq: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = "notFound"
				return def
			},
			expect: codes.NotFound,
		},
		{
			description: "InvalidRamQuota",
			modifyReq: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				ramQuota := uint64(1)
				def.RamQuotaMb = &ramQuota
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "InvalidNumReplicas",
			modifyReq: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				numReplicas := uint32(7)
				def.NumReplicas = &numReplicas
				return def
			},
			expect: codes.InvalidArgument,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			defaultReq := &admin_bucket_v1.UpdateBucketRequest{
				BucketName: testBucket,
			}
			req := test.modifyReq(defaultReq)

			_, err := bucketClient.UpdateBucket(context.Background(), req)
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.Eventually(t, func() bool {
					resp, err := bucketClient.ListBuckets(context.Background(), &admin_bucket_v1.ListBucketsRequest{})
					require.NoError(t, err, "failed to list buckets")

					for _, bucket := range resp.Buckets {
						if bucket.BucketName == req.BucketName {
							return bucket.RamQuotaMb == *req.RamQuotaMb
						}
					}

					return false
				}, time.Second*30, time.Second*5)
			}
		})
	}
}

func TestBucketFlush(t *testing.T) {
	bucketClient := admin_bucket_v1.NewBucketAdminServiceClient(TestOpts.ClientConn)

	flushEnabled := false
	testBucket := "UnflushableBucket"
	_, err := bucketClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName:   testBucket,
		FlushEnabled: &flushEnabled,
	})
	requireRpcStatus(t, err, codes.OK)
	checkBucketCreated(t, bucketClient, testBucket)
	scheduleBucketCleanup(t, bucketClient, testBucket)

	type test struct {
		description string
		bucketName  string
		expect      codes.Code
	}

	tests := []test{
		{
			description: "Success",
			bucketName:  TestOpts.BucketName,
			expect:      codes.OK,
		},
		{
			description: "BucketNotFound",
			bucketName:  "nonexistent",
			expect:      codes.NotFound,
		},
		{
			description: "FlushDisabled",
			bucketName:  testBucket,
			expect:      codes.FailedPrecondition,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			_, err := bucketClient.FlushBucket(context.Background(), &admin_bucket_v1.FlushBucketRequest{
				BucketName: test.bucketName,
			})
			requireRpcStatus(t, err, test.expect)
		})
	}

}

func TestBucketDelete(t *testing.T) {
	bucketClient := admin_bucket_v1.NewBucketAdminServiceClient(TestOpts.ClientConn)

	testBucket := "DeleteBucket"
	_, err := bucketClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName: testBucket,
	})
	requireRpcStatus(t, err, codes.OK)
	checkBucketCreated(t, bucketClient, testBucket)

	type test struct {
		description string
		bucketName  string
		expect      codes.Code
	}

	tests := []test{
		{
			description: "NotFound",
			bucketName:  "notFound",
			expect:      codes.NotFound,
		},
		{
			description: "Success",
			bucketName:  testBucket,
			expect:      codes.OK,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			_, err := bucketClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
				BucketName: test.bucketName,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				checkBucketDeleted(t, bucketClient, test.bucketName)
			}
		})
	}
}

func checkBucketCreated(t *testing.T, bucketClient admin_bucket_v1.BucketAdminServiceClient, bucketName string) {
	checkBucket(t, bucketClient, bucketName, false)
}

func checkBucketDeleted(t *testing.T, bucketClient admin_bucket_v1.BucketAdminServiceClient, bucketName string) {
	checkBucket(t, bucketClient, bucketName, true)
}

func checkBucket(
	t *testing.T,
	bucketClient admin_bucket_v1.BucketAdminServiceClient,
	bucketName string,
	wantMissing bool,
) {
	require.Eventually(t, func() bool {
		resp, err := bucketClient.ListBuckets(context.Background(), &admin_bucket_v1.ListBucketsRequest{})
		require.NoError(t, err, "failed to list buckets")

		for _, bucket := range resp.Buckets {
			if bucket.BucketName == bucketName {
				return !wantMissing
			}
		}

		return wantMissing
	}, time.Second*30, time.Second*5)
}

func scheduleBucketCleanup(t *testing.T, bucketClient admin_bucket_v1.BucketAdminServiceClient, bucketName string) {
	t.Cleanup(func() {
		_, err := bucketClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
			BucketName: bucketName,
		})
		requireRpcStatus(t, err, codes.OK)
	})
}
