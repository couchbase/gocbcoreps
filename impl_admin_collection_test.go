package gocbcoreps

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestScopeCreate(t *testing.T) {
	collectionClient := admin_collection_v1.NewCollectionAdminServiceClient(TestOpts.ClientConn)

	testScope := "testScope"
	t.Cleanup(func() {
		_, err := collectionClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: TestOpts.BucketName,
			ScopeName:  testScope,
		})
		requireRpcStatus(t, err, codes.OK)
	})

	type test struct {
		description string
		bucketName  string
		scopeName   string
		expect      codes.Code
	}

	tests := []test{
		{
			description: "Success",
			bucketName:  TestOpts.BucketName,
			scopeName:   testScope,
			expect:      codes.OK,
		},
		{
			description: "AlreadyExists",
			bucketName:  TestOpts.BucketName,
			scopeName:   testScope,
			expect:      codes.AlreadyExists,
		},
		{
			description: "BucketNotFound",
			bucketName:  "missingBucket",
			scopeName:   "newScope",
			expect:      codes.NotFound,
		},
		// TO DO - ING-1400 blank scope name returns invalid argument error
		// {
		// 	description: "ScopeNameBlank",
		// 	bucketName:  TestOpts.BucketName,
		// 	scopeName:   "",
		// 	expect:      codes.InvalidArgument,
		// },
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			_, err := collectionClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
				BucketName: test.bucketName,
				ScopeName:  test.scopeName,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.Eventually(t, func() bool {
					resp, err := collectionClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
						BucketName: test.bucketName,
					})
					require.NoError(t, err, "failed to list collections")

					for _, scope := range resp.Scopes {
						if scope.Name == test.scopeName {
							return true
						}
					}

					return false
				}, time.Second*30, time.Second*5)
			}
		})
	}
}

func TestScopeDelete(t *testing.T) {
	collectionClient := admin_collection_v1.NewCollectionAdminServiceClient(TestOpts.ClientConn)

	testScope := "testDeleteScope"
	_, err := collectionClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
		BucketName: TestOpts.BucketName,
		ScopeName:  testScope,
	})
	require.NoError(t, err, "failed to create collection")

	type test struct {
		description string
		bucketName  string
		scopeName   string
		expect      codes.Code
	}

	tests := []test{
		{
			description: "BucketNotFound",
			bucketName:  "missingBucket",
			scopeName:   testScope,
			expect:      codes.NotFound,
		},
		{
			description: "ScopeNotFound",
			bucketName:  TestOpts.BucketName,
			scopeName:   "missingScope",
			expect:      codes.NotFound,
		},
		{
			description: "Success",
			bucketName:  TestOpts.BucketName,
			scopeName:   testScope,
			expect:      codes.OK,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			_, err := collectionClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
				BucketName: test.bucketName,
				ScopeName:  test.scopeName,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.Eventually(t, func() bool {
					resp, err := collectionClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
						BucketName: TestOpts.BucketName,
					})
					require.NoError(t, err, "failed to list collections")

					for _, scope := range resp.Scopes {
						if scope.Name == test.scopeName {
							return false
						}
					}

					return true
				}, time.Second*30, time.Second*5)
			}
		})
	}
}

func TestCollectionCreate(t *testing.T) {
	collectionClient := admin_collection_v1.NewCollectionAdminServiceClient(TestOpts.ClientConn)

	testCollection := "testCollection"
	t.Cleanup(func() {
		_, err := collectionClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     TestOpts.BucketName,
			ScopeName:      TestOpts.ScopeName,
			CollectionName: testCollection,
		})
		requireRpcStatus(t, err, codes.OK)
	})

	type test struct {
		description string
		bucketName  string
		scopeName   string
		expect      codes.Code
	}

	tests := []test{
		{
			description: "Success",
			expect:      codes.OK,
		},
		{
			description: "AlreadyExists",
			expect:      codes.AlreadyExists,
		},
		{
			description: "BucketNotFound",
			bucketName:  "missingBucket",
			expect:      codes.NotFound,
		},
		{
			description: "ScopeNotFound",
			scopeName:   "missingScope",
			expect:      codes.NotFound,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			if test.bucketName == "" {
				test.bucketName = TestOpts.BucketName
			}

			if test.scopeName == "" {
				test.scopeName = TestOpts.ScopeName
			}

			_, err := collectionClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
				BucketName:     test.bucketName,
				ScopeName:      test.scopeName,
				CollectionName: testCollection,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.Eventually(t, func() bool {
					resp, err := collectionClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
						BucketName: test.bucketName,
					})
					require.NoError(t, err, "failed to list collections")

					for _, scope := range resp.Scopes {
						if scope.Name == test.scopeName {
							for _, col := range scope.Collections {
								if col.Name == testCollection {
									return true
								}
							}
						}
					}
					return false
				}, time.Second*30, time.Second*5)
			}
		})
	}
}

func TestCollectionUpdate(t *testing.T) {
	// Create a temporary magma bucket so that we can update history retention
	tempBucket := "tempBucket"
	backend := admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA
	bucketClient := admin_bucket_v1.NewBucketAdminServiceClient(TestOpts.ClientConn)
	_, err := bucketClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName:     tempBucket,
		StorageBackend: &backend,
	})
	require.NoError(t, err, "failed to create temp magma bucket")

	collectionClient := admin_collection_v1.NewCollectionAdminServiceClient(TestOpts.ClientConn)
	tempCollection := "updateCollection"
	_, err = collectionClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
		BucketName:     tempBucket,
		ScopeName:      TestOpts.ScopeName,
		CollectionName: tempCollection,
	})
	require.NoError(t, err, "failed to create collection")

	t.Cleanup(func() {
		_, err = bucketClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
			BucketName: tempBucket,
		})
		requireRpcStatus(t, err, codes.OK)
	})

	type test struct {
		description       string
		bucketName        string
		scopeName         string
		colName           string
		historyRetEnabled *bool
		expect            codes.Code
	}

	historyRetEnabled := false
	tests := []test{
		{
			description:       "Success",
			bucketName:        tempBucket,
			scopeName:         TestOpts.ScopeName,
			historyRetEnabled: &historyRetEnabled,
			expect:            codes.OK,
		},
		{
			description: "BucketNotFound",
			bucketName:  "missingBucket",
			expect:      codes.NotFound,
		},
		{
			description: "ScopeNotFound",
			scopeName:   "missingScope",
			expect:      codes.NotFound,
		},
		{
			description: "CollectionNotFound",
			colName:     "missingCollection",
			expect:      codes.NotFound,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			if test.bucketName == "" {
				test.bucketName = TestOpts.BucketName
			}

			if test.scopeName == "" {
				test.scopeName = TestOpts.ScopeName
			}

			if test.colName == "" {
				test.colName = tempCollection
			}

			_, err := collectionClient.UpdateCollection(context.Background(), &admin_collection_v1.UpdateCollectionRequest{
				BucketName:              test.bucketName,
				ScopeName:               test.scopeName,
				CollectionName:          test.colName,
				HistoryRetentionEnabled: test.historyRetEnabled,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.Eventually(t, func() bool {
					resp, err := collectionClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
						BucketName: test.bucketName,
					})
					require.NoError(t, err, "failed to list collections")

					for _, scope := range resp.Scopes {
						if scope.Name == test.scopeName {
							for _, col := range scope.Collections {
								if col.Name == test.colName {
									return col.HistoryRetentionEnabled == nil
								}
							}
						}
					}
					return false
				}, time.Second*30, time.Second*5)
			}
		})
	}
}

func TestCollectionDelete(t *testing.T) {
	collectionClient := admin_collection_v1.NewCollectionAdminServiceClient(TestOpts.ClientConn)

	testCollection := "testDeleteCollection"
	_, err := collectionClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
		BucketName:     TestOpts.BucketName,
		ScopeName:      TestOpts.ScopeName,
		CollectionName: testCollection,
	})
	require.NoError(t, err, "failed to create collection")

	type test struct {
		description string
		bucketName  string
		scopeName   string
		colName     string
		expect      codes.Code
	}

	tests := []test{
		{
			description: "BucketNotFound",
			bucketName:  "missingBucket",
			expect:      codes.NotFound,
		},
		{
			description: "ScopeNotFound",
			scopeName:   "missingScope",
			expect:      codes.NotFound,
		},
		{
			description: "CollectionNotFound",
			colName:     "missingCollection",
			expect:      codes.NotFound,
		},
		{
			description: "Success",
			expect:      codes.OK,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			if test.bucketName == "" {
				test.bucketName = TestOpts.BucketName
			}

			if test.scopeName == "" {
				test.scopeName = TestOpts.ScopeName
			}

			if test.colName == "" {
				test.colName = testCollection
			}

			_, err := collectionClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
				BucketName:     test.bucketName,
				ScopeName:      test.scopeName,
				CollectionName: test.colName,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.Eventually(t, func() bool {
					resp, err := collectionClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
						BucketName: TestOpts.BucketName,
					})
					require.NoError(t, err, "failed to list collections")

					for _, scope := range resp.Scopes {
						if scope.Name == test.scopeName {
							for _, col := range scope.Collections {
								if col.Name == test.colName {
									return false
								}
							}
						}
					}

					return true
				}, time.Second*30, time.Second*5)
			}
		})
	}
}
