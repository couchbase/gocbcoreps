package gocbcoreps

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
	"github.com/stretchr/testify/require"
)

func TestQueryAdmin(t *testing.T) {
	queryClient := admin_query_v1.NewQueryAdminServiceClient(TestOpts.ClientConn)

	primaryIndex := "testPrimaryIndex"
	t.Run("CreatePrimaryIndex", func(t *testing.T) {
		_, err := queryClient.CreatePrimaryIndex(context.Background(), &admin_query_v1.CreatePrimaryIndexRequest{
			BucketName: TestOpts.BucketName,
			Name:       &primaryIndex,
		})
		require.NoError(t, err, "failed to create primary index")

		require.Eventually(t, func() bool {
			resp, err := queryClient.GetAllIndexes(context.Background(), &admin_query_v1.GetAllIndexesRequest{
				BucketName: &TestOpts.BucketName,
			})
			require.NoError(t, err, "failed to get all indexes")

			for _, index := range resp.Indexes {
				if index.Name == primaryIndex {
					return true
				}
			}
			return false
		}, time.Second*30, time.Second*5)
	})

	t.Run("DropPrimaryIndex", func(t *testing.T) {
		_, err := queryClient.DropPrimaryIndex(context.Background(), &admin_query_v1.DropPrimaryIndexRequest{
			BucketName: TestOpts.BucketName,
			Name:       &primaryIndex,
		})
		require.NoError(t, err, "failed to drop primary index")

		require.Eventually(t, func() bool {
			resp, err := queryClient.GetAllIndexes(context.Background(), &admin_query_v1.GetAllIndexesRequest{
				BucketName: &TestOpts.BucketName,
			})
			require.NoError(t, err, "failed to get all indexes")

			for _, index := range resp.Indexes {
				if index.Name == primaryIndex {
					return false
				}
			}
			return true
		}, time.Second*30, time.Second*5)
	})

	indexName := "testIndex"
	deferred := true
	t.Run("CreateIndex", func(t *testing.T) {
		_, err := queryClient.CreateIndex(context.Background(), &admin_query_v1.CreateIndexRequest{
			BucketName: TestOpts.BucketName,
			Name:       indexName,
			Fields:     []string{"foo", "bar"},
			Deferred:   &deferred,
		})
		require.NoError(t, err, "failed to create index")

		require.Eventually(t, func() bool {
			resp, err := queryClient.GetAllIndexes(context.Background(), &admin_query_v1.GetAllIndexesRequest{
				BucketName: &TestOpts.BucketName,
			})
			require.NoError(t, err, "failed to get all indexes")

			for _, index := range resp.Indexes {
				if index.Name == indexName && index.State == admin_query_v1.IndexState_INDEX_STATE_DEFERRED {
					return true
				}
			}
			return false
		}, time.Second*30, time.Second*5)
	})

	t.Run("BuildDeferredIndexes", func(t *testing.T) {
		_, err := queryClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
			BucketName: TestOpts.BucketName,
		})
		require.NoError(t, err, "failed to build deferred indexes")

		require.Eventually(t, func() bool {
			resp, err := queryClient.GetAllIndexes(context.Background(), &admin_query_v1.GetAllIndexesRequest{
				BucketName: &TestOpts.BucketName,
			})
			require.NoError(t, err, "failed to get all indexes")

			for _, index := range resp.Indexes {
				if index.Name == indexName && index.State == admin_query_v1.IndexState_INDEX_STATE_BUILDING {
					return true
				}
			}
			return false
		}, time.Second*30, time.Second*5)
	})

	t.Run("WaitForIndexOnline", func(t *testing.T) {
		_, err := queryClient.WaitForIndexOnline(context.Background(), &admin_query_v1.WaitForIndexOnlineRequest{
			BucketName: TestOpts.BucketName,
			Name:       indexName,
		})
		require.NoError(t, err, "failed to wait for index online")

		resp, err := queryClient.GetAllIndexes(context.Background(), &admin_query_v1.GetAllIndexesRequest{
			BucketName: &TestOpts.BucketName,
		})
		require.NoError(t, err, "failed to get all indexes")

		for _, index := range resp.Indexes {
			if index.Name == indexName && index.State == admin_query_v1.IndexState_INDEX_STATE_BUILDING {
				require.Equal(t, admin_query_v1.IndexState_INDEX_STATE_ONLINE, index.State, "index is not online")
			}
		}
	})

	t.Run("DropIndex", func(t *testing.T) {
		_, err := queryClient.DropIndex(context.Background(), &admin_query_v1.DropIndexRequest{
			BucketName: TestOpts.BucketName,
			Name:       indexName,
		})
		require.NoError(t, err, "failed to drop index")

		resp, err := queryClient.GetAllIndexes(context.Background(), &admin_query_v1.GetAllIndexesRequest{
			BucketName: &TestOpts.BucketName,
		})

		require.Eventually(t, func() bool {
			for _, index := range resp.Indexes {
				if index.Name == indexName {
					return false
				}
			}
			return true
		}, time.Second*30, time.Second*5)
	})
}
