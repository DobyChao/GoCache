package gocache

import pb "gocache/cachepb"

// PeerPicker is the interface that must be implemented to locate
// the peer that owns a specific key.
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter is the interface that must be implemented by a peer.
type PeerGetter interface {
	// Get returns the value form the group.
	Get(in *pb.Request, out *pb.Response) error
}
