package coral

import (
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

type ClusterID string

type Cluster struct {
	routingTable *kb.RoutingTable
	clusterID    ClusterID
}

func NewCluster(bucketsize int, localID kb.ID, latency time.Duration, m pstore.Metrics, clustID ClusterID) *Cluster {
	c := new(Cluster)
	c.routingTable = kb.NewRoutingTable(bucketsize, localID, latency, m)
	c.clusterID = clustID
	return c
}

func (clust *Cluster) ClustSize() int {

	return clust.routingTable.Size()

}

//get the "destination" node for the given id in the level 2 routing table
//closest in XOR distance
//The type ID signifies that its contents have been hashed from either a
//peer.ID or a util.Key. This unifies the keyspace
func (clust *Cluster) getDestinationNode(key string) peer.ID {
	tgtID := kb.ConvertKey(key)
	return clust.routingTable.NearestPeer(tgtID)
}

//get the "next" node on way to the destination node
func (clust *Cluster) getNextNode(key string, pID peer.ID) peer.ID {

	tgtID := kb.XORMidpoint(pID, key)
	return clust.routingTable.NearestPeer(tgtID)

}
