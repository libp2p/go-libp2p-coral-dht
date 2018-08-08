package coral

import (
	"context"
	"math/big"
	"time"

	host "github.com/libp2p/go-libp2p-host"
	opts "github.com/libp2p/go-libp2p-kad-dht/opts"
	kb "github.com/libp2p/go-libp2p-kbucket"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

type coralNode struct {
	host host.Host // the network services we need
	id   peer.ID   // Local peer (yourself)
	ctx  context.Context

	// datastore ds.Datastore // Local data

	peerstore pstore.Peerstore // Peer Registry

	// birth time.Time // When this peer started up

	peerToClust map[peer.ID]ClusterID //should be a triple, for now just implemented for level 2

	levelTwo  *Cluster //peers are stored by their lowest common denominator
	levelOne  *Cluster //cluster. for example, if a peer has the same level 2
	levelZero *Cluster //cluster id, it will be stored in the level 2 routing table.
	//protocols []protocol.ID // DHT protocols
}

func New(ctx context.Context, h host.Host, options ...opts.Option) (*coralNode, error) {
	n := new(coralNode)
	n.id = h.ID()
	n.peerstore = h.Peerstore()
	n.host = h
	n.ctx = ctx
	m := pstore.NewMetrics()
	n.levelTwo = NewCluster(10, kb.ConvertPeerID(n.id), time.Hour, m, "2") //going to want to join clusters immediately via discovery
	n.levelOne = NewCluster(10, kb.ConvertPeerID(n.id), time.Hour, m, "1")
	n.levelZero = NewCluster(10, kb.ConvertPeerID(n.id), time.Hour, m, "0")
	n.peerToClust = make(map[peer.ID]ClusterID)
	return n, nil
}

func (cNode *coralNode) Update(ctx context.Context, p peer.ID) {
	cNode.levelTwo.routingTable.Update(p)
}

func (cNode *coralNode) PutValue(ctx context.Context, key string, value []byte) error {
	//cNode.insert(key, value)
	return nil
}

func (cNode *coralNode) GetValue(ctx context.Context, key string) ([]byte, error) {

	return []byte("world"), nil
}

//find node in own level two routing table
//return slice of type peer ids
// func (cNode *coralNode) findMidpointNode(key string, distance big.Int) []peer.ID {
//
// }
func (cNode *coralNode) insert(ctx context.Context, key string, storeid peer.ID) {
	totaldist := kb.Dist(cNode.id, key) //what is the total distance to the key
	epsilon := big.NewInt(1)
	var nodeStack []peer.ID
	dist := totaldist
	nextKey := key
	var chosenNode peer.ID
	for dist != epsilon {

		nextKey = calculateMidpointKey(nextKey, dist) //add dist to previous Nextkey
		if dist != totaldist {                        //for first round
			findAndAddNextNode(nextKey, chosenNode)
		}
		//query routing table
		chosenNode = cNode.levelTwo.getNextNode(nextKey, cNode.id)
		nodeStack = append(nodeStack, chosenNode)
		dist = dist.Div(dist, big.NewInt(2))

	}

	putValueToPeer(ctx, nodeStack, key, storeid)

}

func putValueToPeer(ctx context.Context, nodeStack []peer.ID, key string, value peer.ID) {
	//	fullAndLoaded := true
	// var chosenNode peer.ID
	// for fullAndLoaded {
	// 	chosenNode, nodeStack = nodeStack[len(nodeStack)-1], nodeStack[:len(nodeStack)-1]
	//pmes := pb.NewMessage(pb.Message_PUT_VALUE, key, 0)
	//set up record with value
	//pmes.Record = rec
	// rpmes, err := dht.sendRequest(ctx, chosenNode, pmes)
	// if err != nil {
	// 	fullAndLoaded = true
	// } else {
	// 	fullAndLoaded = false
	// }

	//can I store the data here?
	//fullAndLoaded = dht.sendRequest(ctx, ChosenNode, pmes)
	//	}

}

func findAndAddNextNode(nextKey string, ChosenNode peer.ID) {

}

func calculateMidpointKey(nextKey string, dist *big.Int) string {
	return nextKey
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
