package coral

import (
	inet "github.com/libp2p/go-libp2p-net"
	ma "github.com/multiformats/go-multiaddr"
	mstream "github.com/multiformats/go-multistream"
)

// netNotifiee defines methods to be used with the IpfscoralNode
type netNotifiee coralNode

func (nn *netNotifiee) coralNode() *coralNode {
	return (*coralNode)(nn)
}

func (nn *netNotifiee) Connected(n inet.Network, v inet.Conn) {
	coralNode := nn.coralNode()

	p := v.RemotePeer()
	//	protos, err := coralNode.peerstore.SupportsProtocols(p, coralNode.protocolStrs()...)

	// We lock here for consistency with the lock in testConnection.
	// This probably isn't necessary because (dis)connect
	// notifications are serialized but it's nice to be consistent.
	//coralNode.plk.Lock()
	//defer coralNode.plk.Unlock()
	if coralNode.host.Network().Connectedness(p) == inet.Connected {
		coralNode.Update(coralNode.ctx, p)

	}

	// Note: Unfortunately, the peerstore may not yet know that this peer is
	// a coralNode server. So, if it didn't return a positive response above, test
	// manually.
	go nn.testConnection(v)
}

func (nn *netNotifiee) testConnection(v inet.Conn) {
	coralNode := nn.coralNode()
	p := v.RemotePeer()

	// Forcibly use *this* connection. Otherwise, if we have two connections, we could:
	// 1. Test it twice.
	// 2. Have it closed from under us leaving the second (open) connection untested.
	s, err := v.NewStream()
	if err != nil {
		// Connection error
		return
	}
	defer inet.FullClose(s)

	selected, err := mstream.SelectOneOf(coralNode.protocolStrs(), s)
	if err != nil {
		// Doesn't support the protocol
		return
	}
	// Remember this choice (makes subsequent negotiations faster)
	coralNode.peerstore.AddProtocols(p, selected)

	// We lock here as we race with disconnect. If we didn't lock, we could
	// finish processing a connect after handling the associated disconnect
	// event and add the peer to the routing table after removing it.
	//	coralNode.plk.Lock()
	//defer coralNode.plk.Unlock()
	if coralNode.host.Network().Connectedness(p) == inet.Connected {
		coralNode.Update(coralNode.ctx, p)
	}
}

func (nn *netNotifiee) Disconnected(n inet.Network, v inet.Conn) {
	coralNode := nn.coralNode()
	select {
	case <-coralNode.proc.Closing():
		return
	default:
	}

	p := v.RemotePeer()

	// Lock and check to see if we're still connected. We lock to make sure
	// we don't concurrently process a connect event.
	//	coralNode.plk.Lock()
	//	defer coralNode.plk.Unlock()
	if coralNode.host.Network().Connectedness(p) == inet.Connected {
		// We're still connected.
		return
	}

	coralNode.levelTwo.routingTable.Remove(p)

	//	coralNode.smlk.Lock()
	//	defer coralNode.smlk.Unlock()
	ms, ok := coralNode.strmap[p]
	if !ok {
		return
	}
	delete(coralNode.strmap, p)

	// Do this asynchronously as ms.lk can block for a while.
	go func() {
		ms.lk.Lock()
		defer ms.lk.Unlock()
		ms.invalidate()
	}()
}
func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr)      {}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {}
