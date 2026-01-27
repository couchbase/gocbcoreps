# Gocbcoreps

A golang protostellar client used to communicate with Couchbase server through the Cloud-Native-Gateway (CNG).

## Optimized routing

Cloud-Native-Gateway is often deployed in clusters and depending on the topology of the CNG cluster and the underlying Couchbase
Server cluster some instances of CNG will be able to serve requests more efficiently than others. The aim of optimized routing 
in gocbcoreps is to identify which instance of CNG can optimally serve a given request and route that request to the optimal
instance of CNG. This is achieved by the implementation of a custom grpc resolver and load balancing policy.

### Resolver

resolver.go implements the grpc resolver interface and is used when a grpc client is created using a connection string 
prepended with "couchbase2+optimized:///". This custom resolver will: 

1. Resolve all ip addresses from the hostname - imagine a cluster of CNG instances are deployed behind a network load balancer, 
   each ip address will correspond to an instance of CNG
2. Gather information on which vBuckets are optimal for each instance of CNG using wathcRouting RPCs
3. Continually monitor and update the vBucket routing information when new buckets/CNGs are added/removed

### Load Balancing Policy

load_balancing_policy.go implements the grpc load balancer interface. This load balancing policy will: 

1. Use the routing information gathered by the resolver to build a mapping from bucket name and vBucket ID to grpc subchannel 
   (these grpc subchannels are represented by pickers)
2. Extract bucket name and document key from incoming request metadata and use these to find the optimal grpc subchannel in the
   mapping from above
3. When no metadata or optimal subchannel are found fall back on a sensible default routing strategy

For more implementational details and design decisions please consult the [design doc](https://docs.google.com/document/d/1B5Fk8Ncvc41YxOG0JE0rfqiZN9gk-Sgrle8SDyqghPo).