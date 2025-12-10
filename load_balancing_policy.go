package gocbcoreps

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sync/atomic"

	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/endpointsharding"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/serviceconfig"
)

const customLBName = "optimized_load_balancer"

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
	balancer.Balancer
	balancer.ClientConn
	bOpts  balancer.BuildOptions
	logger *zap.Logger
	cfg    atomic.Pointer[customLBConfig]
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
// balancer and initialise a new picker.
func (clb *customLB) UpdateState(state balancer.State) {
	if state.ConnectivityState == connectivity.Ready {
		childStates := endpointsharding.ChildStatesFromPicker(state.Picker)

		var readyPickers []balancer.Picker
		bucketAndVbIdToPicker := make(map[string][]*balancer.Picker)
		for _, childState := range childStates {
			if childState.State.ConnectivityState == connectivity.Ready {
				readyPickers = append(readyPickers, childState.State.Picker)

				localVBs := childState.Endpoint.Attributes.Value("localvbs")
				if localVBs == nil {
					continue
				}

				numVBsAttr := childState.Endpoint.Attributes.Value("numvbs")
				if numVBsAttr == nil {
					continue
				}

				vbMaps, _ := localVBs.(map[string][]uint32)
				numVBs, _ := numVBsAttr.(map[string]uint32)

				for bucketName, vBIDs := range vbMaps {
					bucketEntry, ok := bucketAndVbIdToPicker[bucketName]
					if !ok {
						bucketEntry = make([]*balancer.Picker, numVBs[bucketName])
					}

					for _, vbID := range vBIDs {
						bucketEntry[vbID] = &childState.State.Picker
					}

					bucketAndVbIdToPicker[bucketName] = bucketEntry
				}
			}
		}

		picker := &customLoadBalancerPicker{
			pickers:                   readyPickers,
			bucketNameAndvBIDToPicker: bucketAndVbIdToPicker,
			logger:                    clb.logger,
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
	lastUsed                  int
	pickers                   []balancer.Picker
	bucketNameAndvBIDToPicker map[string][]*balancer.Picker

	logger *zap.Logger
}

func (clbp *customLoadBalancerPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	md, ok := metadata.FromOutgoingContext(info.Ctx)
	if !ok {
		return clbp.defaultStrategy(info)
	}

	if len(md["bucketname"]) == 0 || len(md["key"]) == 0 {
		return clbp.defaultStrategy(info)
	}

	bucketName := md["bucketname"][0]
	vBIdToPicker, ok := clbp.bucketNameAndvBIDToPicker[bucketName]
	if !ok {
		return clbp.defaultStrategy(info)
	}

	vBID := vBIdFromKey(md["key"][0], len(vBIdToPicker))
	optimalPicker := vBIdToPicker[vBID]
	if optimalPicker == nil {
		return clbp.defaultStrategy(info)
	}

	return (*optimalPicker).Pick(info)
}

func (clbp *customLoadBalancerPicker) defaultStrategy(info balancer.PickInfo) (balancer.PickResult, error) {
	picker := clbp.pickers[clbp.lastUsed]
	clbp.lastUsed = (clbp.lastUsed + 1) % len(clbp.pickers)
	return picker.Pick(info)
}

func vBIdFromKey(key string, numVBs int) uint16 {
	crc := crc32.ChecksumIEEE([]byte(key))
	crcMidBits := uint16(crc>>16) & ^uint16(0x8000)
	return crcMidBits % uint16(numVBs)
}
