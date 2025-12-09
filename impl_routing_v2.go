package gocbcoreps

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/routing_v2"
	"google.golang.org/grpc"
)

type routingImpl_RoutingV2 struct {
	client *RoutingClient
}

var _ routing_v2.RoutingServiceClient = (*routingImpl_RoutingV2)(nil)

func (c *routingImpl_RoutingV2) WatchRouting(ctx context.Context, in *routing_v2.WatchRoutingRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[routing_v2.WatchRoutingResponse], error) {
	return c.client.fetchConn().RoutingV2().WatchRouting(ctx, in, opts...)
}
