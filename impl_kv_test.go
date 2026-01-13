package gocbcoreps

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
)

var TEST_CONTENT = []byte(`{"foo": "bar","obj":{"num":14,"arr":[2,5,8],"str":"zz"},"num":11,"arr":[3,6,9,12]}`)
var UPDATE_CONTENT = []byte(`{"fizz": "buzz","obj":{"num":89,"arr":[1,2,3],"str":"xy"},"num":198,"arr":[4,8,16,20]}`)
var TEST_CONTENT_FLAGS = uint32(0x01000000)

var incorrectCas = uint64(123)
var zeroCas = uint64(0)
var initial = int64(987)
var negativeInitial = int64(-1)

// TODO - add testing around grpc creds, wouldl need to open a few connections
// in the test opts me thinks
type commonErrorTestData struct {
	ScopeName      string
	BucketName     string
	CollectionName string
	Key            string
}

func RunCommonErrorCases(
	t *testing.T,
	fn func(ctx context.Context, opts *commonErrorTestData) (interface{}, error),
) {
	t.Run("BucketNotFound", func(t *testing.T) {
		_, err := fn(context.Background(), &commonErrorTestData{
			BucketName:     "missingBucket",
			ScopeName:      TestOpts.ScopeName,
			CollectionName: TestOpts.CollectionName,
			Key:            RandomDocId(),
		})
		requireRpcStatus(t, err, codes.NotFound)
		requireRpcErrorDetails(t, err, func(d *epb.ResourceInfo) {
			require.Equal(t, "bucket", d.ResourceType)
		})
	})

	t.Run("ScopeNotFound", func(t *testing.T) {
		_, err := fn(context.Background(), &commonErrorTestData{
			BucketName:     TestOpts.BucketName,
			ScopeName:      "missingScope",
			CollectionName: TestOpts.CollectionName,
			Key:            RandomDocId(),
		})
		requireRpcStatus(t, err, codes.NotFound)
		requireRpcErrorDetails(t, err, func(d *epb.ResourceInfo) {
			require.Equal(t, "scope", d.ResourceType)
		})
	})

	t.Run("CollectionNotFound", func(t *testing.T) {
		_, err := fn(context.Background(), &commonErrorTestData{
			BucketName:     TestOpts.BucketName,
			ScopeName:      TestOpts.ScopeName,
			CollectionName: "missingCollection",
			Key:            RandomDocId(),
		})
		requireRpcStatus(t, err, codes.NotFound)
		requireRpcErrorDetails(t, err, func(d *epb.ResourceInfo) {
			require.Equal(t, "collection", d.ResourceType)
		})
	})
}

func TestKvServiceGet(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)
	docId := createTestDoc(t)

	type test struct {
		description string
		id          string
		compression *kv_v1.CompressionEnabled
		expect      codes.Code
		checkResp   func(*testing.T, *kv_v1.GetResponse)
		checkErr    func(*epb.ResourceInfo)
	}

	tests := []test{
		{
			description: "Basic",
			id:          docId,
			expect:      codes.OK,
			checkResp: func(t *testing.T, resp *kv_v1.GetResponse) {
				require.Equal(t, TEST_CONTENT, resp.GetContentUncompressed())
				require.Nil(t, resp.GetContentCompressed())
			},
		},
		{
			description: "ContentCompressed",
			id:          docId,
			compression: kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS.Enum(),
			expect:      codes.OK,
			checkResp: func(t *testing.T, resp *kv_v1.GetResponse) {
				require.Nil(t, resp.GetContentUncompressed())
				require.Equal(t, TEST_CONTENT, decompressContent(t, resp.GetContentCompressed()))
			},
		},
		{
			description: "DocNotFound",
			id:          "missingDoc",
			expect:      codes.NotFound,
			checkErr: func(d *epb.ResourceInfo) {
				require.Equal(t, "document", d.ResourceType)
			},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			if test.id == "" {
				test.id = docId
			}

			resp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      "_default",
				CollectionName: "_default",
				Key:            test.id,
				Compression:    test.compression,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				test.checkResp(t, resp)
				require.Equal(t, TEST_CONTENT_FLAGS, resp.ContentFlags)
				require.Nil(t, resp.Expiry)
			} else {
				requireRpcErrorDetails(t, err, test.checkErr)
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Get(ctx, &kv_v1.GetRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
		})
	})
}

func TestKvServiceGetAndTouch(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	type test struct {
		description string
		id          string
		expiry      *kv_v1.GetAndTouchRequest_ExpirySecs
		compression *kv_v1.CompressionEnabled
		expect      codes.Code
		checkResp   func(*testing.T, *kv_v1.GetAndTouchResponse)
		checkErr    func(*epb.ResourceInfo)
	}

	tests := []test{
		{
			description: "Basic",
			id:          createTestDoc(t),
			expiry:      &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: 20},
			expect:      codes.OK,
			checkResp: func(t *testing.T, resp *kv_v1.GetAndTouchResponse) {
				require.Equal(t, TEST_CONTENT, resp.GetContentUncompressed())
				require.Nil(t, resp.GetContentCompressed())
			},
		},
		{
			description: "NoExpiry",
			id:          createTestDoc(t),
			expect:      codes.InvalidArgument,
			checkResp: func(t *testing.T, resp *kv_v1.GetAndTouchResponse) {
				require.Equal(t, TEST_CONTENT, resp.GetContentUncompressed())
				require.Nil(t, resp.GetContentCompressed())
			},
		},
		{
			description: "ContentCompressed",
			id:          createTestDoc(t),
			compression: kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS.Enum(),
			expiry:      &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: 20},
			expect:      codes.OK,
			checkResp: func(t *testing.T, resp *kv_v1.GetAndTouchResponse) {
				require.Nil(t, resp.GetContentUncompressed())
				require.Equal(t, TEST_CONTENT, decompressContent(t, resp.GetContentCompressed()))
			},
		},
		{
			description: "DocNotFound",
			id:          "missingDoc",
			expiry:      &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: 20},
			expect:      codes.NotFound,
			checkErr: func(d *epb.ResourceInfo) {
				require.Equal(t, "document", d.ResourceType)
			},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			resp, err := kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      "_default",
				CollectionName: "_default",
				Key:            test.id,
				Expiry:         test.expiry,
				Compression:    test.compression,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				test.checkResp(t, resp)
				require.Equal(t, TEST_CONTENT_FLAGS, resp.ContentFlags)
				require.Nil(t, resp.Expiry)

				// Validate that expiry has been set
				getResp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
					BucketName:     TestOpts.BucketName,
					ScopeName:      "_default",
					CollectionName: "_default",
					Key:            test.id,
				})
				requireRpcStatus(t, err, codes.OK)
				require.NotZero(t, getResp.Cas)
				expiryTime := time.Unix(getResp.Expiry.Seconds, int64(getResp.Expiry.Nanos))
				expirySecs := int(time.Until(expiryTime) / time.Second)
				assert.Greater(t, expirySecs, 0)
				assert.LessOrEqual(t, expirySecs, 20+1)
			} else {
				if test.checkErr != nil {
					requireRpcErrorDetails(t, err, test.checkErr)
				}
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.GetAndTouch(ctx, &kv_v1.GetAndTouchRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Expiry:         &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: 20},
		})
	})
}

func TestKvServiceGetAndLock(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	type test struct {
		description string
		id          string
		compression *kv_v1.CompressionEnabled
		lockTime    uint32
		expect      codes.Code
		checkResp   func(*testing.T, *kv_v1.GetAndLockResponse)
		checkErr    func(*epb.ResourceInfo)
	}

	tests := []test{
		{
			description: "Basic",
			id:          createTestDoc(t),
			lockTime:    uint32(30),
			expect:      codes.OK,
			checkResp: func(t *testing.T, resp *kv_v1.GetAndLockResponse) {
				require.Equal(t, TEST_CONTENT, resp.GetContentUncompressed())
				require.Nil(t, resp.GetContentCompressed())
			},
		},
		{
			description: "ContentCompressed",
			id:          createTestDoc(t),
			compression: kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS.Enum(),
			expect:      codes.OK,
			checkResp: func(t *testing.T, resp *kv_v1.GetAndLockResponse) {
				require.Nil(t, resp.GetContentUncompressed())
				require.Equal(t, TEST_CONTENT, decompressContent(t, resp.GetContentCompressed()))
			},
		},
		{
			description: "DocNotFound",
			id:          "missingDoc",
			expect:      codes.NotFound,
			checkErr: func(d *epb.ResourceInfo) {
				require.Equal(t, "document", d.ResourceType)
			},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			resp, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      "_default",
				CollectionName: "_default",
				Key:            test.id,
				LockTime:       test.lockTime,
				Compression:    test.compression,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				test.checkResp(t, resp)
				require.Equal(t, TEST_CONTENT_FLAGS, resp.ContentFlags)
				require.Nil(t, resp.Expiry)

				// Validate that the document is locked and we can't do updates
				_, err = kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     TestOpts.BucketName,
					ScopeName:      "_default",
					CollectionName: "_default",
					Key:            test.id,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
				})
				requireRpcStatus(t, err, codes.FailedPrecondition)
				requireRpcErrorDetails(t, err, func(d *epb.PreconditionFailure) {
					assert.Len(t, d.Violations, 1)
					assert.Equal(t, "LOCKED", d.Violations[0].Type)
				})
			} else {
				if test.checkErr != nil {
					requireRpcErrorDetails(t, err, test.checkErr)
				}
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.GetAndLock(ctx, &kv_v1.GetAndLockRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
		})
	})
}

func TestKvServiceUnlock(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	key, cas := createLockedDoc(t)

	type test struct {
		description string
		cas         *uint64
		key         string
		expect      codes.Code
	}

	tests := []test{
		{
			description: "IncorrectCas",
			cas:         &incorrectCas,
			expect:      codes.Aborted,
		},
		{
			description: "Basic",
		},
		{
			description: "InvalidCas",
			cas:         &zeroCas,
			expect:      codes.InvalidArgument,
		},
		{
			description: "AlreadyUnlocked",
			// Prior to 7.6, we expect that the KV operation yields a
			// TmpFail, which leads to internal retries until timeout.  After 7.6,
			// we instead expect to receive a PreconditionFailure with a violation
			// type of "NOT_LOCKED".
			expect: func() codes.Code {
				if IsOlderServerVersion(t, "7.6.0") {
					return codes.DeadlineExceeded
				} else {
					return codes.FailedPrecondition
				}
			}(),
		},
		{
			description: "DocNotFound",
			key:         "missingDoc",
			expect:      codes.NotFound,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			if test.key == "" {
				test.key = key
			}

			if test.cas == nil {
				test.cas = &cas
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err := kvClient.Unlock(ctx, &kv_v1.UnlockRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Cas:            *test.cas,
			})
			requireRpcStatus(t, err, test.expect)
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Unlock(ctx, &kv_v1.UnlockRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Cas:            cas,
		})
	})
}

func TestKvServiceTouch(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)
	key := createTestDoc(t)

	type test struct {
		description string
		key         string
		expiry      *kv_v1.TouchRequest_ExpirySecs
		expect      codes.Code
	}

	tests := []test{
		{
			description: "Basic",
			expiry:      &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: 20},
			expect:      codes.OK,
		},
		{
			description: "NoExpiry",
			expect:      codes.InvalidArgument,
		},
		{
			description: "DocNotFound",
			key:         "missingDoc",
			expiry:      &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: 20},
			expect:      codes.NotFound,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			if test.key == "" {
				test.key = key
			}

			_, err := kvClient.Touch(context.Background(), &kv_v1.TouchRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Expiry:         test.expiry,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				// check that the expiry was actually set
				getResp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
					BucketName:     TestOpts.BucketName,
					ScopeName:      TestOpts.ScopeName,
					CollectionName: TestOpts.CollectionName,
					Key:            test.key,
				})
				requireRpcStatus(t, err, test.expect)
				require.NotZero(t, getResp.Cas)
				expiryTime := time.Unix(getResp.Expiry.Seconds, int64(getResp.Expiry.Nanos))
				expirySecs := int(time.Until(expiryTime) / time.Second)
				assert.Greater(t, expirySecs, 0)
				assert.LessOrEqual(t, expirySecs, 20+1)
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Touch(ctx, &kv_v1.TouchRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Expiry:         &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: 20},
		})
	})
}

func TestKvServiceExists(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)
	key := createTestDoc(t)

	type test struct {
		description string
		key         string
		exists      bool
	}

	tests := []test{
		{
			description: "DocExists",
			key:         key,
			exists:      true,
		},
		{
			description: "DocNotFound",
			key:         "missingDoc",
			exists:      false,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			resp, err := kvClient.Exists(context.Background(), &kv_v1.ExistsRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
			})
			requireRpcStatus(t, err, codes.OK)
			require.Equal(t, resp.Result, test.exists)
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Exists(ctx, &kv_v1.ExistsRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
		})
	})
}

func TestKvServiceInsert(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	type test struct {
		description string
		key         string
		expiry      *kv_v1.InsertRequest_ExpirySecs
		expiryCheck expiryCheckType
		expect      codes.Code
	}

	successDocId := RandomDocId()
	tests := []test{
		{
			description: "Basic",
			key:         successDocId,
			expiryCheck: expiryCheckType_None,
			expect:      codes.OK,
		},
		{
			description: "DocExists",
			key:         successDocId,
			expect:      codes.AlreadyExists,
		},
		{
			description: "WithExpiry",
			key:         RandomDocId(),
			expiry: &kv_v1.InsertRequest_ExpirySecs{
				ExpirySecs: 100,
			},
			expiryCheck: expiryCheckType_Future,
			expect:      codes.OK,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			resp, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Content: &kv_v1.InsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				Expiry:       test.expiry,
				ContentFlags: TEST_CONTENT_FLAGS,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.NotZero(t, resp.Cas)
				requireValidMutationToken(t, resp.MutationToken, TestOpts.BucketName)

				checkDocument(t, checkDocumentOptions{
					BucketName:     TestOpts.BucketName,
					ScopeName:      TestOpts.ScopeName,
					CollectionName: TestOpts.CollectionName,
					DocId:          test.key,
					Content:        TEST_CONTENT,
					ContentFlags:   TEST_CONTENT_FLAGS,
					expiry:         test.expiryCheck,
				})
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Insert(ctx, &kv_v1.InsertRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content: &kv_v1.InsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
		})
	})
}

func TestKvServiceUpsert(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	type test struct {
		description string
		key         string
		content     *kv_v1.UpsertRequest_ContentUncompressed
		expiry      *kv_v1.UpsertRequest_ExpirySecs
		expiryCheck expiryCheckType
		expect      codes.Code
	}

	successDocId := RandomDocId()
	tests := []test{
		{
			description: "InsertDoc",
			key:         successDocId,
			content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			expect: codes.OK,
		},
		{
			description: "UpdateDoc",
			key:         successDocId,
			content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: UPDATE_CONTENT,
			},
			expect: codes.OK,
		},
		{
			description: "MissingContent",
			key:         successDocId,
			expect:      codes.InvalidArgument,
		},
		{
			description: "WithExpiry",
			key:         RandomDocId(),
			content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			expiry: &kv_v1.UpsertRequest_ExpirySecs{
				ExpirySecs: 100,
			},
			expiryCheck: expiryCheckType_Future,
			expect:      codes.OK,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Content:        test.content,
				Expiry:         test.expiry,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.NotZero(t, resp.Cas)
				requireValidMutationToken(t, resp.MutationToken, TestOpts.BucketName)

				checkDocument(t, checkDocumentOptions{
					BucketName:     TestOpts.BucketName,
					ScopeName:      TestOpts.ScopeName,
					CollectionName: TestOpts.CollectionName,
					DocId:          test.key,
					Content:        test.content.ContentUncompressed,
					ContentFlags:   TEST_CONTENT_FLAGS,
					expiry:         test.expiryCheck,
				})
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Upsert(ctx, &kv_v1.UpsertRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
		})
	})
}

func TestKvServiceReplace(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	type test struct {
		description string
		key         string
		content     *kv_v1.ReplaceRequest_ContentUncompressed
		expiry      *kv_v1.ReplaceRequest_ExpirySecs
		expiryCheck expiryCheckType
		expect      codes.Code
	}

	key := createTestDoc(t)
	tests := []test{
		{
			description: "Basic",
			key:         key,
			content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: UPDATE_CONTENT,
			},
			expect: codes.OK,
		},
		{
			description: "DocNotFound",
			key:         "missingDoc",
			content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			expect: codes.NotFound,
		},
		{
			description: "MissingContent",
			key:         key,
			expect:      codes.InvalidArgument,
		},
		{
			description: "WithExpiry",
			key:         key,
			content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			expiry: &kv_v1.ReplaceRequest_ExpirySecs{
				ExpirySecs: 100,
			},
			expiryCheck: expiryCheckType_Future,
			expect:      codes.OK,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			resp, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Content:        test.content,
				Expiry:         test.expiry,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.NotZero(t, resp.Cas)
				requireValidMutationToken(t, resp.MutationToken, TestOpts.BucketName)

				checkDocument(t, checkDocumentOptions{
					BucketName:     TestOpts.BucketName,
					ScopeName:      TestOpts.ScopeName,
					CollectionName: TestOpts.CollectionName,
					DocId:          test.key,
					Content:        test.content.ContentUncompressed,
					ContentFlags:   TEST_CONTENT_FLAGS,
					expiry:         test.expiryCheck,
				})
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Replace(ctx, &kv_v1.ReplaceRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
		})
	})
}

func TestKvServiceRemove(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	type test struct {
		description string
		key         string
		cas         *uint64
		expect      codes.Code
	}

	key := createTestDoc(t)
	lockedDoc, _ := createLockedDoc(t)
	tests := []test{
		{
			description: "IncorrectCas",
			key:         key,
			cas:         &incorrectCas,
			expect:      codes.Aborted,
		},
		{
			description: "Basic",
			key:         key,
			expect:      codes.OK,
		},
		{
			description: "DocNotFound",
			key:         "missingDocument",
			expect:      codes.NotFound,
		},
		{
			description: "DocLocked",
			key:         lockedDoc,
			expect:      codes.FailedPrecondition,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			_, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Cas:            test.cas,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				checkDocument(t, checkDocumentOptions{
					BucketName:     TestOpts.BucketName,
					ScopeName:      TestOpts.ScopeName,
					CollectionName: TestOpts.CollectionName,
					DocId:          test.key,
				})
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Remove(ctx, &kv_v1.RemoveRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
		})
	})
}

func TestKvServiceIncrement(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	key := createBinaryDoc(t, []byte("5"))

	nonBinary := createTestDoc(t)

	type test struct {
		description     string
		key             string
		expectedContent []byte
		delta           uint64
		initial         *int64
		expect          codes.Code
	}

	tests := []test{
		{
			description:     "Basic",
			key:             key,
			delta:           uint64(12),
			expectedContent: []byte("17"),
			expect:          codes.OK,
		},
		{
			description: "NonBinaryDoc",
			key:         nonBinary,
			expect:      codes.FailedPrecondition,
		},
		{
			description:     "DocMissing",
			key:             RandomDocId(),
			initial:         &initial,
			delta:           uint64(1),
			expectedContent: []byte("987"),
			expect:          codes.OK,
		},
		{
			description: "NegativeInitial",
			key:         RandomDocId(),
			initial:     &negativeInitial,
			expect:      codes.InvalidArgument,
		},
		{
			description: "DocMissingWithoutInitial",
			key:         "missingDoc",
			expect:      codes.NotFound,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			_, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Delta:          test.delta,
				Initial:        test.initial,
				Key:            test.key,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				checkDocument(t, checkDocumentOptions{
					BucketName:     TestOpts.BucketName,
					ScopeName:      TestOpts.ScopeName,
					CollectionName: TestOpts.CollectionName,
					DocId:          test.key,
					Content:        test.expectedContent,
				})
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Increment(ctx, &kv_v1.IncrementRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
		})
	})
}

func TestKvServiceDecrement(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	key := createBinaryDoc(t, []byte("5"))

	nonBinary := createTestDoc(t)

	type test struct {
		description     string
		key             string
		expectedContent []byte
		delta           uint64
		initial         *int64
		expect          codes.Code
	}

	tests := []test{
		{
			description:     "Basic",
			key:             key,
			delta:           uint64(3),
			expectedContent: []byte("2"),
			expect:          codes.OK,
		},
		{
			description: "NonBinaryDoc",
			key:         nonBinary,
			expect:      codes.FailedPrecondition,
		},
		{
			description:     "DocMissing",
			key:             RandomDocId(),
			initial:         &initial,
			delta:           uint64(1),
			expectedContent: []byte("987"),
			expect:          codes.OK,
		},
		{
			description: "NegativeInitial",
			key:         RandomDocId(),
			initial:     &negativeInitial,
			expect:      codes.InvalidArgument,
		},
		{
			description: "DocMissingWithoutInitial",
			key:         "missingDoc",
			expect:      codes.NotFound,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			_, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Delta:          test.delta,
				Initial:        test.initial,
				Key:            test.key,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				checkDocument(t, checkDocumentOptions{
					BucketName:     TestOpts.BucketName,
					ScopeName:      TestOpts.ScopeName,
					CollectionName: TestOpts.CollectionName,
					DocId:          test.key,
					Content:        test.expectedContent,
				})
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Decrement(ctx, &kv_v1.DecrementRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
		})
	})
}

func TestKvServiceAppend(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	key := createBinaryDoc(t, []byte("hello"))
	lockedDoc, _ := createLockedDoc(t)

	type test struct {
		description   string
		key           string
		content       []byte
		cas           *uint64
		expectContent []byte
		expect        codes.Code
	}

	tests := []test{
		{
			description:   "Basic",
			key:           key,
			content:       []byte("world"),
			expectContent: []byte("helloworld"),
			expect:        codes.OK,
		},
		{
			description: "DocNotFound",
			key:         "missingDoc",
			expect:      codes.NotFound,
		},
		{
			description: "IncorrectCas",
			key:         key,
			cas:         &incorrectCas,
			expect:      codes.Aborted,
		},
		{
			description: "DocLocked",
			key:         lockedDoc,
			expect:      codes.FailedPrecondition,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			_, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Content:        test.content,
				Cas:            test.cas,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				checkDocument(t, checkDocumentOptions{
					BucketName:     TestOpts.BucketName,
					ScopeName:      TestOpts.ScopeName,
					CollectionName: TestOpts.CollectionName,
					DocId:          test.key,
					Content:        test.expectContent,
				})
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Append(ctx, &kv_v1.AppendRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content:        []byte("content"),
		})
	})
}

func TestKvServicePrepend(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	key := createBinaryDoc(t, []byte("world"))
	lockedDoc, _ := createLockedDoc(t)

	type test struct {
		description   string
		key           string
		content       []byte
		cas           *uint64
		expectContent []byte
		expect        codes.Code
	}

	tests := []test{
		{
			description:   "Basic",
			key:           key,
			content:       []byte("hello"),
			expectContent: []byte("helloworld"),
			expect:        codes.OK,
		},
		{
			description: "DocNotFound",
			key:         "missingDoc",
			expect:      codes.NotFound,
		},
		{
			description: "IncorrectCas",
			key:         key,
			cas:         &incorrectCas,
			expect:      codes.Aborted,
		},
		{
			description: "DocLocked",
			key:         lockedDoc,
			expect:      codes.FailedPrecondition,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			_, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Content:        test.content,
				Cas:            test.cas,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				checkDocument(t, checkDocumentOptions{
					BucketName:     TestOpts.BucketName,
					ScopeName:      TestOpts.ScopeName,
					CollectionName: TestOpts.CollectionName,
					DocId:          test.key,
					Content:        test.expectContent,
				})
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Prepend(ctx, &kv_v1.PrependRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content:        []byte("content"),
		})
	})
}

func TestKvServiceLookupIn(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	key := createTestDoc(t)

	type test struct {
		description   string
		key           string
		spec          *kv_v1.LookupInRequest_Spec
		expectContent []byte
		checkFn       func(*kv_v1.LookupInResponse)
		expect        codes.Code
	}

	tests := []test{
		{
			description: "Get",
			spec: &kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
				Path:      "foo",
			},
			expectContent: []byte(`"bar"`),
			expect:        codes.OK,
		},
		{
			description: "GetArrayElement",
			spec: &kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
				Path:      "arr[3]",
			},
			expectContent: []byte("12"),
			expect:        codes.OK,
		},
		{
			description: "Count",
			spec: &kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_COUNT,
				Path:      "arr",
			},
			expectContent: []byte("4"),
			expect:        codes.OK,
		},
		{
			description: "Exists",
			spec: &kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_EXISTS,
				Path:      "num",
			},
			expectContent: []byte("true"),
			expect:        codes.OK,
		},
		{
			description: "ExistsNotFound",
			spec: &kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_EXISTS,
				Path:      "notANum",
			},
			expectContent: []byte("false"),
			expect:        codes.OK,
		},
		{
			description: "NoSpecs",
			expect:      codes.InvalidArgument,
		},
		{
			description: "DocNotFound",
			key:         "missingDoc",
			spec: &kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
				Path:      "foo",
			},
			expect: codes.NotFound,
		},
		{
			description: "PathMismatch",
			key:         key,
			spec: &kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
				Path:      "arr.missing",
			},
			checkFn: func(resp *kv_v1.LookupInResponse) {
				assert.Len(t, resp.Specs, 1)
				assertStatusProto(t, resp.Specs[0].Status, codes.FailedPrecondition)
				assertStatusProtoDetails(t, resp.Specs[0].Status, func(d *epb.PreconditionFailure) {
					assert.Len(t, d.Violations, 1)
					assert.Equal(t, "PATH_MISMATCH", d.Violations[0].Type)
				})
			},
			expect: codes.OK,
		},
		{
			description: "DocNotJson",
			key:         createBinaryDoc(t, []byte("hello")),
			spec: &kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
				Path:      "foo",
			},
			checkFn: func(resp *kv_v1.LookupInResponse) {
				assert.Len(t, resp.Specs, 1)
				assertStatusProto(t, resp.Specs[0].Status, codes.FailedPrecondition)
				assertStatusProtoDetails(t, resp.Specs[0].Status, func(d *epb.PreconditionFailure) {
					assert.Len(t, d.Violations, 1)
					assert.Equal(t, "DOC_NOT_JSON", d.Violations[0].Type)
				})
			},
			expect: codes.OK,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			if test.key == "" {
				test.key = key
			}

			if test.checkFn == nil {
				test.checkFn = func(resp *kv_v1.LookupInResponse) {
					require.Len(t, resp.Specs, 1)
					require.Nil(t, resp.Specs[0].Status)
					require.Equal(t, test.expectContent, resp.Specs[0].Content)
				}
			}

			var specs []*kv_v1.LookupInRequest_Spec
			if test.spec != nil {
				specs = []*kv_v1.LookupInRequest_Spec{test.spec}
			}

			resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Specs:          specs,
			})
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				test.checkFn(resp)
			}
		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.LookupIn(ctx, &kv_v1.LookupInRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "foo",
				},
			},
		})
	})
}

func TestKvServiceMutateIn(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)
	key := createTestDoc(t)

	checkDocumentPath := func(docId, path string, content []byte) {
		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     TestOpts.BucketName,
			ScopeName:      TestOpts.ScopeName,
			CollectionName: TestOpts.CollectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      path,
				},
			},
		})
		requireRpcStatus(t, err, codes.OK)

		assert.Len(t, resp.Specs, 1)
		if content != nil {
			assert.Nil(t, resp.Specs[0].Status)
			assert.Equal(t, content, resp.Specs[0].Content)
		} else {
			assert.Equal(t, int32(codes.NotFound), resp.Specs[0].Status.Code)
		}
	}

	type test struct {
		description   string
		key           string
		spec          *kv_v1.MutateInRequest_Spec
		expectContent []byte
		checkFn       func(*kv_v1.MutateInResponse, error)
		expect        codes.Code
	}

	tests := []test{
		{
			description: "Insert",
			key:         key,
			spec: &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
				Path:      "newfoo",
				Content:   []byte(`"baz"`),
			},
			checkFn: func(resp *kv_v1.MutateInResponse, err error) {
				checkDocumentPath(key, "newfoo", []byte(`"baz"`))
			},
			expect: codes.OK,
		},
		{
			description: "Upsert",
			key:         key,
			spec: &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
				Path:      "newfoo",
				Content:   []byte(`"newbar"`),
			},
			checkFn: func(resp *kv_v1.MutateInResponse, err error) {
				checkDocumentPath(key, "newfoo", []byte(`"newbar"`))
			},
			expect: codes.OK,
		},
		{
			description: "Remove",
			key:         key,
			spec: &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_REMOVE,
				Path:      "newfoo",
			},
			checkFn: func(resp *kv_v1.MutateInResponse, err error) {
				checkDocumentPath(key, "newfoo", nil)
			},
			expect: codes.OK,
		},
		{
			description: "ArrayAppend",
			key:         key,
			spec: &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_APPEND,
				Path:      "arr",
				Content:   []byte(`15,18`),
			},
			checkFn: func(resp *kv_v1.MutateInResponse, err error) {
				checkDocumentPath(key, "arr", []byte(`[3,6,9,12,15,18]`))
			},
			expect: codes.OK,
		},
		{
			description: "DocNotFound",
			key:         "missingDoc",
			spec: &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
				Path:      "newfoo",
				Content:   []byte(`"newbar"`),
			},
			expect: codes.NotFound,
		},
		{
			description: "DocNotJSON",
			key:         createBinaryDoc(t, []byte("hello")),
			spec: &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
				Path:      "foo",
				Content:   []byte(`2`),
			},
			checkFn: func(resp *kv_v1.MutateInResponse, err error) {
				requireRpcErrorDetails(t, err, func(d *epb.PreconditionFailure) {
					assert.Len(t, d.Violations, 1)
					assert.Equal(t, "DOC_NOT_JSON", d.Violations[0].Type)
				})
			},
			expect: codes.FailedPrecondition,
		},
		{
			description: "InsertPathExists",
			key:         key,
			spec: &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
				Path:      "foo",
				Content:   []byte(`2`),
			},
			checkFn: func(resp *kv_v1.MutateInResponse, err error) {
				requireRpcErrorDetails(t, err, func(d *epb.ResourceInfo) {
					assert.Equal(t, "path", d.ResourceType)
				})
			},
			expect: codes.AlreadyExists,
		},
		{
			description: "PathMismatch",
			key:         key,
			spec: &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
				Path:      "arr.missing",
				Content:   []byte(`2`),
			},
			checkFn: func(resp *kv_v1.MutateInResponse, err error) {
				requireRpcErrorDetails(t, err, func(d *epb.PreconditionFailure) {
					assert.Len(t, d.Violations, 1)
					assert.Equal(t, "PATH_MISMATCH", d.Violations[0].Type)
				})
			},
			expect: codes.FailedPrecondition,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			if test.key == "" {
				test.key = key
			}

			var specs []*kv_v1.MutateInRequest_Spec
			if test.spec != nil {
				specs = []*kv_v1.MutateInRequest_Spec{test.spec}
			}

			resp, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
				BucketName:     TestOpts.BucketName,
				ScopeName:      TestOpts.ScopeName,
				CollectionName: TestOpts.CollectionName,
				Key:            test.key,
				Specs:          specs,
			})
			requireRpcStatus(t, err, test.expect)
			if test.checkFn != nil {
				test.checkFn(resp, err)
			}

		})
	}

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.MutateIn(ctx, &kv_v1.MutateInRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
					Path:      "foo",
				},
			},
		})
	})
}

func TestKvServiceGetAllReplicas(t *testing.T) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)
	key := createTestDoc(t)

	// TODO - should check the number of replicas here

	type test struct {
		description string
		key         string
		expect      codes.Code
	}

	getAllReplicas := func() (grpc.ServerStreamingClient[kv_v1.GetAllReplicasResponse], error) {
		return kvClient.GetAllReplicas(context.Background(), &kv_v1.GetAllReplicasRequest{
			BucketName:     TestOpts.BucketName,
			ScopeName:      TestOpts.ScopeName,
			CollectionName: TestOpts.CollectionName,
			Key:            key,
		})
	}

	t.Run("Basic", func(t *testing.T) {
		resp, err := getAllReplicas()
		requireRpcStatus(t, err, codes.OK)

		successful := 0
		numResponses := 0
		for {
			itemResp, err := resp.Recv()
			if err != nil {
				// EOF is not a response, it indicates that the stream has been
				// closed with no errors
				if !errors.Is(err, io.EOF) {
					numResponses++
				}
				break
			}

			require.NotZero(t, itemResp.Cas)
			require.Equal(t, TEST_CONTENT, itemResp.Content)
			require.Equal(t, TEST_CONTENT_FLAGS, itemResp.ContentFlags)
			numResponses++
			successful++
		}

		// Since we are testing with two replicas we should get at least 2 responses,
		// in the case where both replica reads return an error.
		require.GreaterOrEqual(t, numResponses, 2)

		// since the document is at least written to the master, we must get
		// at least a single response, and more is acceptable.
		require.Greater(t, successful, 0)
	})

	t.Run("WaitForAllReplicas", func(t *testing.T) {
		require.Eventually(t, func() bool {
			resp, err := getAllReplicas()
			requireRpcStatus(t, err, codes.OK)

			successful := 0
			for {
				itemResp, err := resp.Recv()
				if err != nil {
					break
				}

				require.NotZero(t, itemResp.Cas)
				require.Equal(t, TEST_CONTENT, itemResp.Content)
				require.Equal(t, TEST_CONTENT_FLAGS, itemResp.ContentFlags)
				successful++
			}
			return successful == 3
		},
			time.Second*60,
			time.Second*5)
	})

	RunCommonErrorCases(t, func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		resp, err := kvClient.GetAllReplicas(ctx, &kv_v1.GetAllReplicasRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
		})
		requireRpcStatus(t, err, codes.OK)

		var results []interface{}
		for {
			itemResp, err := resp.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return nil, err
			}

			results = append(results, itemResp)
		}

		return results, nil
	})
}
