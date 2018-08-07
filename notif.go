package coral

import (
	inet "github.com/libp2p/go-libp2p-net"

)

// netNotifiee defines methods to be used with the IpfsDHT
type netNotifiee coralNode

func (nn *netNotifiee) DHT() *coralNode {
	return (*coralNode)(nn)
}

func (nn *netNotifiee) Connected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()

	p := v.RemotePeer()

		if dht.host.Network().Connectedness(p) == inet.Connected {
			dht.Update(dht.ctx, p)
		}
		return
}
