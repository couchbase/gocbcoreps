package gocbcoreps

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestQueryService(t *testing.T) {
	queryClient := query_v1.NewQueryServiceClient(TestOpts.ClientConn)
	docId := createTestDoc(t)

	readQueryStream := func(client query_v1.QueryService_QueryClient) ([][]byte, *query_v1.QueryResponse_MetaData, error) {
		var rows [][]byte
		var md *query_v1.QueryResponse_MetaData

		for {
			resp, err := client.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return rows, md, err
			}

			if len(resp.Rows) > 0 {
				rows = append(rows, resp.Rows...)
			}
			if resp.MetaData != nil {
				md = resp.MetaData
			}
		}

		return rows, md, nil
	}

	type test struct {
		description  string
		statement    string
		bucket       *string
		scope        *string
		expectedRows int
		expect       codes.Code
	}

	missingBucket := "missingBucket"
	tests := []test{
		{
			description:  "Basic",
			statement:    "SELECT * FROM default._default._default WHERE META().id='" + docId + "'",
			expectedRows: 1,
			expect:       codes.OK,
		},
		{
			description: "InvalidQueryStatement",
			statement:   "FINAGLE * FROM default._default._default",
			expect:      codes.InvalidArgument,
		},
		{
			description: "BucketNotFound",
			bucket:      &missingBucket,
			expect:      codes.NotFound,
		},
		{
			description: "ScopeNotFound",
			bucket:      &missingBucket,
			expect:      codes.NotFound,
		},
		{
			description: "DropIndexMissing",
			statement:   "DROP INDEX `MISSING` ON default",
			expect:      codes.NotFound,
		},
		{
			description:  "LongRunningQuery",
			statement:    "SELECT SUM(t + t2) AS total FROM ARRAY_RANGE(0,4000) AS t, ARRAY_RANGE(0,4000) AS t2",
			expectedRows: 1,
			expect:       codes.OK,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.description, func(t *testing.T) {
			qCli, err := queryClient.Query(context.Background(), &query_v1.QueryRequest{
				Statement:  test.statement,
				BucketName: test.bucket,
				ScopeName:  test.scope,
			})
			requireRpcStatus(t, err, codes.OK)

			rows, md, err := readQueryStream(qCli)
			requireRpcStatus(t, err, test.expect)

			if test.expect == codes.OK {
				require.Eventually(t, func() bool {
					if md != nil && len(rows) == test.expectedRows {
						return true
					}

					qCli, err := queryClient.Query(context.Background(), &query_v1.QueryRequest{
						Statement:  test.statement,
						BucketName: test.bucket,
						ScopeName:  test.scope,
					})
					requireRpcStatus(t, err, codes.OK)

					rows, md, err = readQueryStream(qCli)
					return false
				}, time.Second*30, time.Second*5)

			}
		})
	}
}
