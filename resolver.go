package gocbcoreps

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/routing_v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

const OptimizedRoutingScheme = "couchbase2+optimized"

const defaultResolveInterval = time.Second * 30

// TO DO - move this into the load balancing implementation when added.
const customLBName = "optimized_load_balancer"

type CustomResolverBuilder struct {
	logger          *zap.Logger
	ctx             context.Context
	basicAuth       *BasicAuthenticator
	resolveInterval time.Duration
}

func (*CustomResolverBuilder) Scheme() string { return OptimizedRoutingScheme }

func (c *CustomResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, rOpts resolver.BuildOptions) (resolver.Resolver, error) {
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(rOpts.DialCreds)}
	if c.basicAuth != nil {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(c.basicAuth))
	}

	r := &customResolver{
		ctx:             c.ctx,
		logger:          c.logger,
		done:            make(chan struct{}),
		resolveNow:      make(chan struct{}),
		resolveInterval: c.resolveInterval,

		target:   target,
		dialOpts: dialOpts,
		cc:       cc,

		targetToConn:   make(map[string]*grpc.ClientConn),
		routingInfoMap: make(map[string]*routing_v2.WatchRoutingResponse),
		routingUpdates: make(chan routingUpdate),
	}

	// Start a routine to watch for updates from the watch routing streams and
	// update our map.
	go func() {
		for u := range r.routingUpdates {
			r.shouldUpdate.Store(true)

			r.routingLock.Lock()
			if u.err != nil {
				delete(r.routingInfoMap, u.key)
				r.routingLock.Unlock()
				continue
			}

			r.routingInfoMap[u.key] = u.resp
			r.routingLock.Unlock()
		}
	}()

	go r.watch()

	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil
}

type customResolver struct {
	ctx             context.Context
	logger          *zap.Logger
	resolveNow      chan struct{}
	done            chan struct{}
	resolveInterval time.Duration

	target   resolver.Target
	dialOpts []grpc.DialOption
	cc       resolver.ClientConn

	targetToConn   map[string]*grpc.ClientConn
	routingInfoMap map[string]*routing_v2.WatchRoutingResponse
	routingLock    sync.Mutex
	routingUpdates chan routingUpdate

	addrs        []string
	shouldUpdate atomic.Bool
}

type routingUpdate struct {
	key  string
	resp *routing_v2.WatchRoutingResponse
	err  error
}

func (r *customResolver) resolve() error {
	addrs, err := resolveAddrs(r.target.Endpoint())
	if err != nil {
		return err
	}

	eps := make([]resolver.Endpoint, len(addrs))
	conns := make([]*grpc.ClientConn, len(addrs))
	for i, addr := range addrs {
		eps[i] = resolver.Endpoint{
			Addresses: []resolver.Address{{Addr: addr}},
		}

		// Try to reuse connections we have already established in previous
		// resolutions.
		conn, ok := r.targetToConn[addr]
		if !ok {
			// We've found a new address so we need to update our LB state
			r.shouldUpdate.Store(true)

			conn, err = grpc.NewClient(addr, r.dialOpts...)
			if err != nil {
				return fmt.Errorf("failed to create connection for '%s': %w", addr, err)
			}

			r.targetToConn[addr] = conn
		}

		conns[i] = conn
	}

	bucketClient := admin_bucket_v1.NewBucketAdminServiceClient(conns[0])
	resp, err := bucketClient.ListBuckets(r.ctx, &admin_bucket_v1.ListBucketsRequest{})
	if err != nil {
		return fmt.Errorf("listing buckets: %w", err)
	}

	for i, conn := range conns {
		bucketToLocalVBs := make(map[string][]uint32)
		bucketToNumVBs := make(map[string]uint32)

		for _, b := range resp.Buckets {
			r.routingLock.Lock()
			resp, ok := r.routingInfoMap[constructKey(b.BucketName, conn.Target())]
			r.routingLock.Unlock()
			if !ok {
				// We've found a new bucket so we need to update our LB
				r.shouldUpdate.Store(true)

				rClient := routing_v2.NewRoutingServiceClient(conn)
				rStream, err := rClient.WatchRouting(r.ctx, &routing_v2.WatchRoutingRequest{
					BucketName: &b.BucketName,
				})
				if err != nil {
					return fmt.Errorf("watch routing request failed: %w", err)
				}

				resp, err = rStream.Recv()
				if err != nil {
					return fmt.Errorf("failed to receive watch routing result: %w", err)
				}

				// Send update down channel to update the routingInfo map
				key := constructKey(b.BucketName, conn.Target())
				r.routingUpdates <- routingUpdate{
					key:  key,
					resp: resp,
				}

				// Start a routine to watch for future updates
				go func(key string) {
					for {
						resp, err := rStream.Recv()
						select {
						case r.routingUpdates <- routingUpdate{
							key:  key,
							resp: resp,
							err:  err,
						}:
							if err != nil {
								return
							}
						case <-r.done:
							return
						}
					}
				}(key)
			}

			bucketToLocalVBs[b.BucketName] = resp.VbucketDataRouting.LocalVbuckets
			bucketToNumVBs[b.BucketName] = resp.VbucketDataRouting.NumVbuckets
		}

		eps[i].Attributes = eps[i].Attributes.WithValue("localvbs", bucketToLocalVBs)
		eps[i].Attributes = eps[i].Attributes.WithValue("numvbs", bucketToNumVBs)
	}

	if r.shouldUpdate.Load() {
		r.shouldUpdate.Store(false)
		r.cc.UpdateState(resolver.State{
			Endpoints:     eps,
			ServiceConfig: r.cc.ParseServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, customLBName)),
		})
	}

	return nil
}

func (r *customResolver) watch() {
	ticker := time.NewTicker(r.resolveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ticker.Stop()
			if err := r.resolve(); err != nil {
				r.logger.Error("optimized routing resolution failed", zap.Error(err))
			}
		case <-r.resolveNow:
			ticker.Stop()
			if err := r.resolve(); err != nil {
				r.logger.Error("optimized routing resolution failed", zap.Error(err))
			}
		case <-r.done:
			close(r.routingUpdates)
			return
		}

		ticker = time.NewTicker(r.resolveInterval)
	}
}

func (r *customResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.resolveNow <- struct{}{}
}

func (r *customResolver) Close() {
	close(r.done)
}

func resolveAddrs(target string) ([]string, error) {
	var addrs []string
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return addrs, fmt.Errorf("parsing target endpoint: %w", err)
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return addrs, fmt.Errorf("resolving addresses from hostname: %w", err)
	}

	for _, ip := range ips {
		if ip.To4() != nil {
			addrs = append(addrs, ip.String()+":"+port)
		} else if ip.To16() != nil {
			addrs = append(addrs, "["+ip.String()+"]:"+port)
		}
	}

	return addrs, nil
}

func constructKey(bucketName, addr string) string {
	return bucketName + ":" + addr
}
