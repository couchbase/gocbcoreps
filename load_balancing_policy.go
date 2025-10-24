package gocbcoreps

import (
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"

	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/endpointsharding"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/serviceconfig"
)

const customLBName = "custom_load_balancer"

type customLBConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`
}

type CustomLBBuilder struct {
	logger *zap.Logger
}

func (CustomLBBuilder) Name() string {
	return customLBName
}

func (CustomLBBuilder) ParseConfig(s json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	lbConfig := &customLBConfig{}

	if err := json.Unmarshal(s, lbConfig); err != nil {
		return nil, fmt.Errorf(customLBName+": unable to unmarshal customLBConfig: %v", err)
	}
	return lbConfig, nil
}

func (clbb CustomLBBuilder) Build(cc balancer.ClientConn, bOpts balancer.BuildOptions) balancer.Balancer {
	cb := &customLB{
		ClientConn: cc,
		bOpts:      bOpts,
		logger:     clbb.logger,
	}

	cb.Balancer = endpointsharding.NewBalancer(cb, bOpts, balancer.Get(roundrobin.Name).Build, endpointsharding.Options{})
	return cb
}

type customLB struct {
	// All state and operations on this balancer are either initialized at build
	// time and read only after, or are only accessed as part of its
	// balancer.Balancer API (UpdateState from children only comes in from
	// balancer.Balancer calls as well, and children are called one at a time),
	// in which calls are guaranteed to come synchronously. Thus, no extra
	// synchronization is required in this balancer.
	balancer.Balancer
	balancer.ClientConn
	bOpts  balancer.BuildOptions
	logger *zap.Logger

	cfg atomic.Pointer[customLBConfig]
}

// Called by the gRPC channel to inform the load balancing policy of:
// - updated backend addresses
// - updated service config (if any)
func (clb *customLB) UpdateClientConnState(state balancer.ClientConnState) error {
	clbCfg, ok := state.BalancerConfig.(*customLBConfig)
	if !ok {
		return balancer.ErrBadResolverState
	}

	clb.cfg.Store(clbCfg)
	// A call to UpdateClientConnState should always produce a new Picker.  That
	// is guaranteed to happen since the aggregator will always call
	// UpdateChildState in its UpdateClientConnState.
	return clb.Balancer.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: state.ResolverState,
	})
}

// Called by the load balancing poicy to update the connectivity state of the
// balancer and initialise a new picker. It is called when:
// - New addresses/config received
// - Subchannel state changes
func (clb *customLB) UpdateState(state balancer.State) {
	if state.ConnectivityState == connectivity.Ready {
		childStates := endpointsharding.ChildStatesFromPicker(state.Picker)
		var readyPickers []balancer.Picker
		addressToPicker := make(map[string]balancer.Picker)
		for _, childState := range childStates {
			if childState.State.ConnectivityState == connectivity.Ready {
				readyPickers = append(readyPickers, childState.State.Picker)

				// TODO - I think just using the first address is alright here
				// because of how we resolve things, but need to verify.
				host, _, err := net.SplitHostPort(childState.Endpoint.Addresses[0].Addr)
				if err != nil {
					// TO DO - properly  handle this error
					clb.logger.Error("failed to split host address", zap.String("address", childState.Endpoint.Addresses[0].Addr))
					continue
				}

				addressToPicker[host] = childState.State.Picker
			}
		}

		picker := &customLoadBalancerPicker{
			addressToPicker: addressToPicker,
			logger:          clb.logger,
		}
		clb.ClientConn.UpdateState(balancer.State{
			ConnectivityState: connectivity.Ready,
			Picker:            picker,
		})
		return
	}

	// Delegate to default behavior/picker from below.
	clb.ClientConn.UpdateState(state)
}

type customLoadBalancerPicker struct {
	addressToPicker map[string]balancer.Picker

	logger *zap.Logger
}

func (clbp *customLoadBalancerPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	// TODO - optimized the performance of this, any slowness here is very bad
	// since this is called for every RPC
	md, ok := metadata.FromOutgoingContext(info.Ctx)
	if ok {
		routingInfo := md["routinginfo"]

		if len(routingInfo) == 0 {
			return clbp.pickFirst().Pick(info)
		}

		var routingInfoStruct RoutingInfo

		// TODO - would it be more performant to just check if the substring of
		// an address in within the routing information instead of unmarshaling?
		err := json.Unmarshal([]byte(routingInfo[0]), &routingInfoStruct)
		if err != nil {
			// TODO - wrt performance concerns maybe we shouldn't be logging in
			// Pick()
			clbp.logger.Error("failed to unmarshal routing info", zap.Any("routingInfo", routingInfo[0]))
			return clbp.pickFirst().Pick(info)
		}

		fmt.Printf("%+v\n", routingInfoStruct)

		for k, _ := range clbp.addressToPicker {
			fmt.Println(k)
		}

		for _, addr := range routingInfoStruct.LocalServers {
			bestPicker, ok := clbp.addressToPicker[addr]
			if ok {
				fmt.Println("BEST PICKER FOUND")
				return bestPicker.Pick(info)
			}
		}
	}

	childPicker := clbp.pickFirst()
	return childPicker.Pick(info)
}

// TO DO - probably want to implement round robin picking here.
func (clbp *customLoadBalancerPicker) pickFirst() balancer.Picker {
	for _, pkr := range clbp.addressToPicker {
		return pkr
	}
	return nil
}
