package coral

import (
	"math/big"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"

	kb "github.com/libp2p/go-libp2p-kbucket"
)

type coralNode struct {

	// host host.Host        // the network services we need
	id peer.ID // Local peer (yourself)
	// ctx  context.Context

	// datastore ds.Datastore // Local data

	// peerstore pstore.Peerstore // Peer Registry

	// birth time.Time // When this peer started up

	peerToClust map[peer.ID]ClusterID //should be a triple, for now just implemented for level 2

	levelTwo  *Cluster //peers are stored by their lowest common denominator
	levelOne  *Cluster //cluster. for example, if a peer has the same level 2
	levelZero *Cluster //cluster id, it will be stored in the level 2 routing table.

}

func NewCNode(id peer.ID) *coralNode {
	n := new(coralNode)
	m := pstore.NewMetrics()
	n.levelTwo = NewCluster(10, kb.ConvertPeerID(id), time.Hour, m, "2") //going to want to join clusters immediately via discovery
	n.levelOne = NewCluster(10, kb.ConvertPeerID(id), time.Hour, m, "1")
	n.levelZero = NewCluster(10, kb.ConvertPeerID(id), time.Hour, m, "0")
	n.peerToClust = make(map[peer.ID]ClusterID)
	return n
}

func (cNode *coralNode) insert(key string, id peer.ID) {
	fullAndLoaded := false
	totaldist := kb.Dist(cNode.id, key) //what is the total distance to the key
	dist := totaldist
	node := cNode.id
	for !fullAndLoaded && dist != big.NewInt(1) {
		//once you get the next node, ADD IT to your routingTable
		if dist != totaldist {
			cNode.addCNode(node, cNode.levelTwo.clusterID)
		}
		node = cNode.levelTwo.getNextNode(key, node) //in case your routing table had something closer originally
		//send a QUERY of type getNextNode to another node, they respond with results of getNextNode(key, self) or they will respond
		//that the node is full and loaded for that key
		//QUERYNODE
		dist = kb.Dist(node, key)

	}

	//putValuetopeer
}

//need to implement these, maybe where we implement the getNextNode RPC
func (cNode *coralNode) isFull() bool {
	return false
}

func (cNode *coralNode) isLoaded() bool {
	return false
}

//local node joins cluster
func (cNode *coralNode) joinCluster(clustID ClusterID) ClusterID {

	cNode.levelTwo.clusterID = clustID
	return clustID

}

func (cNode *coralNode) levelTwoClustSize() int {

	return cNode.levelTwo.ClustSize()

}
func (cNode *coralNode) levelZeroClustSize() int {

	return cNode.levelZero.ClustSize()

}

func (cNode *coralNode) levelOneClustSize() int {

	return cNode.levelOne.ClustSize()

}

//peer cluster info is kept track of in a map
//peer id info is kept track of in routing table
func (cNode *coralNode) addCNode(p peer.ID, clustID ClusterID) {
	cNode.levelTwo.routingTable.Update(p) //change
	cNode.peerToClust[p] = clustID
	//update peer store?
}

//lookup a peer's cluster info
//for now just implemented for level 2 only
func (cNode *coralNode) lookupClustID(p peer.ID) ClusterID {
	return cNode.peerToClust[p]
}

//possible code to reuse

//ping/pong
//find_value
//store
//find_node
//what is different about the way cNodes need to talk to each other?
//need to pass around cluster information in RPC header
//cluster ids and cluster size
//and save it in map

//need to be implemented:
//insertion/retrieval
//every CNode needs a stack for forward/reverse phase of insertion

//merging/splitting clusters
