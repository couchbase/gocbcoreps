package gocbcoreps

import (
	"context"
	"errors"
	"hash/crc32"
	"net"
	"sync"

	"github.com/couchbase/goprotostellar/genproto/routing_v1"
	"go.uber.org/zap"
)

type routingManager struct {
	client routing_v1.RoutingServiceClient

	logger *zap.Logger

	// For each bucket we store an array of routingInfo, one entry per vBucket
	bucketToRouting map[string][1024]RoutingInfo

	updateLock sync.Mutex
}

type RoutingInfo struct {
	// LocalServers is the list of servers that have the vBucket local to them
	LocalServers []string `json:"localServers"`

	// TODO add support for listing servers that have the vBucket in their server group.
}

func newRoutingManager(client routing_v1.RoutingServiceClient, logger *zap.Logger) *routingManager {
	return &routingManager{
		client:          client,
		logger:          logger,
		bucketToRouting: make(map[string][1024]RoutingInfo),
	}
}

func (r *routingManager) FindFor(ctx context.Context, bucket, key string) RoutingInfo {
	// We don't want to perform an RPC every time we call this so we should have some
	// watch thread on a per bucket basis that updates a map in our manger that we
	// can quickly access (under a lock) here.
	r.updateLock.Lock()
	vbRoutingInfo, ok := r.bucketToRouting[bucket]
	r.updateLock.Unlock()
	if !ok {
		go r.watchBucket(ctx, bucket)
		// TODO - decide what the behaviour should be here, do we wait until we get the routing
		// information or do we just let the watch thread do it's thing in the background and the
		// next request to the bucket will have an entry. For now we do the latter.
		return RoutingInfo{}
	}

	vbId := vBidFromKey(key)

	return vbRoutingInfo[vbId]
}

func vBidFromKey(key string) uint16 {
	crc := crc32.ChecksumIEEE([]byte(key))
	crcMidBits := uint16(crc>>16) & ^uint16(0x8000)
	// Stole this from VbucketByKey in gocbcorex, there we mod by the number of
	// entries in the vBucketMap, can we just hardcode 1024 here?
	return crcMidBits % uint16(1024)
}

func (r *routingManager) watchBucket(ctx context.Context, bucket string) error {
	routingStream, err := r.client.WatchRouting(context.Background(), &routing_v1.WatchRoutingRequest{
		BucketName: &bucket,
	})
	if err != nil {
		return err
	}

	outputCh := make(chan [1024]RoutingInfo)
	go func() {
		for {
			routingResp, err := routingStream.Recv()
			if err != nil {
				r.logger.Error("failed to recv updated topology", zap.Error(err))
				break
			}

			vbIdToLocalServer, err := r.vbIdToLocalServersFromResp(*routingResp)
			if err != nil {
				r.logger.Error("failed to extract routing infor from response", zap.Error(err))
				break
			}

			outputCh <- vbIdToLocalServer
		}
	}()

	// We want to leave this running so that any vBucketMap changes pushed
	// to us on teh watch routing stream can be reflected in the manager.
	for {
		select {
		// TO DO - handle retries/timeouts here. May need to create a new
		// context and pass to watchRouting because if we use the top level
		//config passed in then this will get canceled when the top level op
		// finishes.
		// case <-ctx.Done():
		// 	return nil
		case vbIdToLocalSrv := <-outputCh:
			r.updateLock.Lock()
			r.bucketToRouting[bucket] = vbIdToLocalSrv
			r.updateLock.Unlock()
		}
	}

}

// This takes a watch routing response and builds a map from vBucket ID to
// ip address of the cng instance to which that vBucket is local.
func (r *routingManager) vbIdToLocalServersFromResp(resp routing_v1.WatchRoutingResponse) ([1024]RoutingInfo, error) {
	var vbIdToLocalServer [1024]RoutingInfo

	vBucketRoutingStrategy, ok := resp.DataRouting.(*routing_v1.WatchRoutingResponse_VbucketDataRouting)
	if !ok {
		return vbIdToLocalServer, errors.New("unexpected data routing implementation in response")
	}

	for _, ep := range vBucketRoutingStrategy.VbucketDataRouting.Endpoints {
		// The server will tell us the host name's of nodes along with the port
		// we need to split the host from the port and resolve the ip addr.
		host, _, err := net.SplitHostPort(resp.Endpoints[ep.EndpointIdx].Address)
		if err != nil {
			r.logger.Error("splitting host name from port", zap.String("address", resp.Endpoints[ep.EndpointIdx].Address))
			continue
		}

		ipAdr, err := net.LookupIP(host)
		if err != nil {
			r.logger.Error("looking up ip for node host", zap.String("host", host))
			continue
		}

		// TO DO - atm this assumes that each vBucket is only local to one CNG.
		for _, vbId := range ep.LocalVbuckets {
			vbIdToLocalServer[vbId] = RoutingInfo{
				LocalServers: []string{ipAdr[0].String()},
			}
		}
	}

	return vbIdToLocalServer, nil
}
