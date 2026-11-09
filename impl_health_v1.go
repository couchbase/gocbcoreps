package gocbcoreps

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// noCredentials replaces the per-RPC credentials set as a default call option, so that no Authorization header is
// sent. Health checks are sent unauthenticated.
type noCredentials struct{}

func (noCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return nil, nil
}

func (noCredentials) RequireTransportSecurity() bool {
	return false
}

func withoutCredentials(opts []grpc.CallOption) []grpc.CallOption {
	return append(opts, grpc.PerRPCCredentials(noCredentials{}))
}

type routingImpl_HealthV1 struct {
	client *RoutingClient
}

var _ grpc_health_v1.HealthClient = (*routingImpl_HealthV1)(nil)

func (c *routingImpl_HealthV1) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	return c.client.fetchConn().HealthV1().Check(ctx, in, withoutCredentials(opts)...)
}

func (c *routingImpl_HealthV1) List(ctx context.Context, in *grpc_health_v1.HealthListRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthListResponse, error) {
	return c.client.fetchConn().HealthV1().List(ctx, in, withoutCredentials(opts)...)
}

func (c *routingImpl_HealthV1) Watch(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[grpc_health_v1.HealthCheckResponse], error) {
	return c.client.fetchConn().HealthV1().Watch(ctx, in, withoutCredentials(opts)...)
}
