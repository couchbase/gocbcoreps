package gocbcoreps

import (
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

const customServiceConfig = `{
  "loadBalancingPolicy": "custom_load_balancer" 
}`

// TODO - this should probably be named something better than this, same applies
// above
const customScheme = "custom"

type CustomResolverBuilder struct {
	logger *zap.Logger
}

func (c *CustomResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, rOpts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &customResolver{
		target: target,
		cc:     cc,
		done:   make(chan struct{}),
	}
	r.start()
	return r, nil
}

func (*CustomResolverBuilder) Scheme() string { return customScheme }

type customResolver struct {
	target resolver.Target
	cc     resolver.ClientConn
	done   chan struct{}
	logger *zap.Logger
}

func (r *customResolver) start() {
	host, port, err := net.SplitHostPort(r.target.Endpoint())
	if err != nil {
		// TO DO - properly  handle this error
		fmt.Println("Split Error:", err)
		return
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	var ipv4s []string
	for _, ip := range ips {
		if ip.To4() != nil {
			ipv4s = append(ipv4s, ip.String())
		}
	}

	addrs := make([]resolver.Endpoint, len(ipv4s))
	for i, ip := range ipv4s {
		addr := ip + ":" + port
		fmt.Println(addr)

		addrs[i] = resolver.Endpoint{
			Addresses: []resolver.Address{{Addr: addr}},
		}

	}

	// Watch for changes in the resolution of the hostname
	go r.watch()

	// Make the gRPC channel use our sutom load balancing policy
	cfg := r.cc.ParseServiceConfig(customServiceConfig)

	r.cc.UpdateState(resolver.State{
		Endpoints:     addrs,
		ServiceConfig: cfg,
	})
}

// TODO - make the watch mechanism smarter so we don't update the state when
// the addresses haven't changed.
func (r *customResolver) watch() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.start()
		case <-r.done:
			return
		}
	}
}

func (*customResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *customResolver) Close() {
	close(r.done)
}
