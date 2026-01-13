package gocbcoreps

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
	"github.com/stretchr/testify/require"
)

func TestSearchAdminClient(t *testing.T) {
	searchClient := admin_search_v1.NewSearchAdminServiceClient(TestOpts.ClientConn)

	bucket, scope := initialiseBucketAndScope(t)

	indexName := "testSearchIndex"
	sourceType := "couchbase"
	t.Run("CreateIndex", func(t *testing.T) {
		_, err := searchClient.CreateIndex(context.Background(), &admin_search_v1.CreateIndexRequest{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: &sourceType,
			SourceName: &TestOpts.BucketName,
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to create search index")
	})

	t.Run("AnalyzeDocument", func(t *testing.T) {
		testDoc := `{"someText": "Analyze this please"}`
		docBytes, err := json.Marshal(testDoc)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, err := searchClient.AnalyzeDocument(context.Background(), &admin_search_v1.AnalyzeDocumentRequest{
				Name:       indexName,
				Doc:        docBytes,
				BucketName: bucket,
				ScopeName:  scope,
			})
			return err == nil
		}, time.Second*120, time.Second*10)
	})

	var indexUuid string
	t.Run("GetIndex", func(t *testing.T) {
		require.Eventually(t, func() bool {
			resp, err := searchClient.GetIndex(context.Background(), &admin_search_v1.GetIndexRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			})

			if err == nil {
				indexUuid = resp.Index.Uuid
				return true
			}

			return false
		}, time.Second*30, time.Second*5)
	})

	gcbSourceTye := "gocbcore"
	t.Run("UpdateIndex", func(t *testing.T) {
		_, err := searchClient.UpdateIndex(context.Background(), &admin_search_v1.UpdateIndexRequest{
			Index: &admin_search_v1.Index{
				Uuid:       indexUuid,
				Name:       indexName,
				Type:       "fulltext-index",
				SourceType: &gcbSourceTye,
				SourceName: &TestOpts.BucketName,
			},
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to update search index")

		require.Eventually(t, func() bool {
			resp, err := searchClient.GetIndex(context.Background(), &admin_search_v1.GetIndexRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			})
			require.NoError(t, err, "failed to get search index")
			return *resp.Index.SourceType == gcbSourceTye
		}, time.Second*5, time.Second*30)
	})

	t.Run("FreezeIndexPlan", func(t *testing.T) {
		_, err := searchClient.FreezeIndexPlan(context.Background(), &admin_search_v1.FreezeIndexPlanRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to freeze search index plan")
	})

	t.Run("UnfreezeIndexPlan", func(t *testing.T) {
		_, err := searchClient.UnfreezeIndexPlan(context.Background(), &admin_search_v1.UnfreezeIndexPlanRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to unfreeze search index plan")
	})

	t.Run("DisallowIndexQuerying", func(t *testing.T) {
		_, err := searchClient.DisallowIndexQuerying(context.Background(), &admin_search_v1.DisallowIndexQueryingRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to disallow search index querying")
	})

	t.Run("AllowIndexQuerying", func(t *testing.T) {
		_, err := searchClient.AllowIndexQuerying(context.Background(), &admin_search_v1.AllowIndexQueryingRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to allow search index querying")
	})

	t.Run("PauseIndexIngest", func(t *testing.T) {
		_, err := searchClient.PauseIndexIngest(context.Background(), &admin_search_v1.PauseIndexIngestRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to pause search index ingest")
	})

	t.Run("ResumeIndexIngest", func(t *testing.T) {
		_, err := searchClient.ResumeIndexIngest(context.Background(), &admin_search_v1.ResumeIndexIngestRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to resume search index ingest")
	})

	t.Run("GetIndexedDocumentsCount", func(t *testing.T) {
		_, err := searchClient.GetIndexedDocumentsCount(context.Background(), &admin_search_v1.GetIndexedDocumentsCountRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to get indexed documents count")
	})

	t.Run("DeleteIndex", func(t *testing.T) {
		_, err := searchClient.DeleteIndex(context.Background(), &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		})
		require.NoError(t, err, "failed to delete search index")

		require.Eventually(t, func() bool {
			resp, err := searchClient.ListIndexes(context.Background(), &admin_search_v1.ListIndexesRequest{
				BucketName: bucket,
				ScopeName:  scope,
			})
			require.NoError(t, err, "failed to list search indexes")

			for _, index := range resp.Indexes {
				if index.Name == indexName {
					return false
				}
			}
			return true
		}, time.Second*30, time.Second*5)
	})
}
