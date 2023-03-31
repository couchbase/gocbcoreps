package gocbps

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"google.golang.org/grpc"
)

type routingImpl_AdminCollectionV1 struct {
	client *RoutingClient
}

var _ admin_collection_v1.CollectionAdminServiceClient = (*routingImpl_AdminCollectionV1)(nil)

func (c *routingImpl_AdminCollectionV1) ListCollections(ctx context.Context, in *admin_collection_v1.ListCollectionsRequest, opts ...grpc.CallOption) (*admin_collection_v1.ListCollectionsResponse, error) {
	return c.client.fetchConnForBucket(in.BucketName).AdminCollectionV1().ListCollections(ctx, in, opts...)
}
func (c *routingImpl_AdminCollectionV1) CreateScope(ctx context.Context, in *admin_collection_v1.CreateScopeRequest, opts ...grpc.CallOption) (*admin_collection_v1.CreateScopeResponse, error) {
	return c.client.fetchConnForBucket(in.BucketName).AdminCollectionV1().CreateScope(ctx, in, opts...)
}
func (c *routingImpl_AdminCollectionV1) DeleteScope(ctx context.Context, in *admin_collection_v1.DeleteScopeRequest, opts ...grpc.CallOption) (*admin_collection_v1.DeleteScopeResponse, error) {
	return c.client.fetchConnForBucket(in.BucketName).AdminCollectionV1().DeleteScope(ctx, in, opts...)
}
func (c *routingImpl_AdminCollectionV1) CreateCollection(ctx context.Context, in *admin_collection_v1.CreateCollectionRequest, opts ...grpc.CallOption) (*admin_collection_v1.CreateCollectionResponse, error) {
	return c.client.fetchConnForBucket(in.BucketName).AdminCollectionV1().CreateCollection(ctx, in, opts...)
}
func (c *routingImpl_AdminCollectionV1) DeleteCollection(ctx context.Context, in *admin_collection_v1.DeleteCollectionRequest, opts ...grpc.CallOption) (*admin_collection_v1.DeleteCollectionResponse, error) {
	return c.client.fetchConnForBucket(in.BucketName).AdminCollectionV1().DeleteCollection(ctx, in, opts...)
}
