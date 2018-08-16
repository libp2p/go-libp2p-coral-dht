package coral

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	host "github.com/libp2p/go-libp2p-host"
	opts "github.com/libp2p/go-libp2p-kad-dht/opts"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	protocol "github.com/libp2p/go-libp2p-protocol"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
)

type coralNode struct {
	host      host.Host // the network services we need
	id        peer.ID   // Local peer (yourself)
	ctx       context.Context
	proc      goprocess.Process
	datastore ds.Datastore // Local data

	peerstore pstore.Peerstore // Peer Registry
	Validator record.Validator
	// birth time.Time // When this peer started up

	peerToClust map[peer.ID]ClusterID //should be a triple, for now just implemented for level 2
	strmap      map[peer.ID]*messageSender
	levelTwo    *Cluster      //peers are stored by their lowest common denominator
	levelOne    *Cluster      //cluster. for example, if a peer has the same level 2
	levelZero   *Cluster      //cluster id, it will be stored in the level 2 routing table.
	protocols   []protocol.ID // cNode protocols
}

func New(ctx context.Context, h host.Host, options ...opts.Option) (*coralNode, error) {

	var cfg opts.Options
	if err := cfg.Apply(append([]opts.Option{opts.Defaults}, options...)...); err != nil {
		return nil, err
	}

	cfg.Protocols = []protocol.ID{protocol.ID("/ipfs/coral")}
	n := makeNode(ctx, h, cfg.Datastore, cfg.Protocols)
	// register for network notifs.
	n.host.Network().Notify((*netNotifiee)(n))
	n.Validator = cfg.Validator
	n.proc = goprocessctx.WithContextAndTeardown(ctx, func() error {
		// remove ourselves from network notifs.
		n.host.Network().StopNotify((*netNotifiee)(n))
		return nil
	})

	for _, p := range cfg.Protocols {
		fmt.Printf("%s\n", p)
		h.SetStreamHandler(p, n.handleNewStream)
	}

	return n, nil
}

func makeNode(ctx context.Context, h host.Host, dstore ds.Batching, protocols []protocol.ID) *coralNode {
	n := new(coralNode)
	n.id = h.ID()
	n.peerstore = h.Peerstore()
	n.host = h
	n.datastore = dstore
	n.ctx = ctx
	m := pstore.NewMetrics()
	n.levelTwo = NewCluster(10, kb.ConvertPeerID(n.id), time.Hour, m, 2, "2") //going to want to join clusters immediately via discovery
	n.levelOne = NewCluster(10, kb.ConvertPeerID(n.id), time.Hour, m, 1, "1")
	n.levelZero = NewCluster(10, kb.ConvertPeerID(n.id), time.Hour, m, 0, "0")
	n.peerToClust = make(map[peer.ID]ClusterID)
	n.strmap = make(map[peer.ID]*messageSender)
	n.protocols = protocols
	return n
}

func (cNode *coralNode) protocolStrs() []string {
	pstrs := make([]string, len(cNode.protocols))
	for idx, proto := range cNode.protocols {
		pstrs[idx] = string(proto)
	}

	return pstrs
}

func (cNode *coralNode) Update(ctx context.Context, p peer.ID) {
	cNode.levelTwo.routingTable.Update(p)
}

func (cNode *coralNode) PutValue(ctx context.Context, key string, value []byte) error {
	return cNode.insert(ctx, key, value)
}

func (cNode *coralNode) GetValue(ctx context.Context, key string) ([]byte, error) {

	return []byte("world"), nil
}

//find node in own level two routing table
//return slice of type peer ids
// func (cNode *coralNode) findMidpointNode(key string, distance big.Int) []peer.ID {
//
// }
func (cNode *coralNode) insert(ctx context.Context, key string, value []byte) error {
	totaldist := kb.Dist(cNode.id, key)
	dist := kb.Dist(cNode.id, key)
	//what is the total distance to the key
	epsilon := big.NewInt(1)
	//fmt.Printf("Distance from %s to %s is %s, %d\n", cNode.id.String(), key, totaldist.String(), totaldist.Cmp(epsilon))

	var nodeStack []peer.ID

	nextKey := key
	var chosenNode peer.ID
	for dist.Cmp(epsilon) > 0 {
		fmt.Printf("Entered top of insert for loop\n")
		nextKey = kb.CalculateMidpointKey(nextKey, dist) //add dist to previous Nextkey
		if dist.Cmp(totaldist) != 0 {
			fmt.Printf("chosen node: %s\n", chosenNode) //for first round, this is messy fix it
			_, err := cNode.findAndAddNextNode(ctx, nextKey, chosenNode)
			if err != nil {
				return err
			}
		}
		//query routing table
		chosenNode = cNode.levelTwo.routingTable.NearestPeer(kb.ConvertKey(nextKey))
		nodeStack = append(nodeStack, chosenNode)
		dist = dist.Div(dist, big.NewInt(2))

	}
	rec := record.MakePutRecord(key, value)
	cNode.putValueToPeer(ctx, nodeStack, rec, key)
	return nil
}

func (cNode *coralNode) putValueToPeer(ctx context.Context, nodeStack []peer.ID, rec *recpb.Record, key string) error {
	fullAndLoaded := true
	var chosenNode peer.ID
	if len(nodeStack) == 0 {
		return errors.New("Attempted to put value with empty node stack.")
	}
	for fullAndLoaded {

		chosenNode, nodeStack = nodeStack[len(nodeStack)-1], nodeStack[:len(nodeStack)-1]
		pmes := pb.NewMessage(pb.Message_PUT_VALUE, []byte(key), 0)
		pmes.Record = rec
		rpmes, err := cNode.sendRequest(ctx, chosenNode, pmes)

		if err != nil {
			if err == ErrReadTimeout {
				fmt.Printf("read timeout: %s %s", chosenNode.Pretty(), key)
			}
			return err
		}

		if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
			return errors.New("value not put correctly")
		}
		fullAndLoaded = false
	}
	return nil
}
func (cNode *coralNode) nearestPeersToQuery(pmes *pb.Message, count int) []peer.ID {
	closer := cNode.levelTwo.routingTable.NearestPeers(kb.ConvertKey(string(pmes.GetKey())), count)
	return closer
}

func (cNode *coralNode) findAndAddNextNode(ctx context.Context, key string, receiverNode peer.ID) (peer.ID, error) {
	fmt.Printf("Entered findandAddNextNode for key: %s, and recievernode: %s \n", key, receiverNode)
	pmes := pb.NewMessage(pb.Message_FIND_NODE, []byte(key), 0)
	resp, err := cNode.sendRequest(ctx, receiverNode, pmes)
	if err != nil {
		return peer.ID(""), err
	}

	closer := resp.GetCloserPeers()
	clpeers := pb.PBPeersToPeerInfos(closer)
	var nextNode peer.ID
	if len(clpeers) > 0 {
		nextNode = clpeers[0].ID
	} else {
		nextNode = cNode.id
	}
	///	once you get the next node, add it to your routing table
	cNode.addCNode(nextNode, cNode.levelTwo.clusterID)
	fmt.Printf("Found node id: %s\n", nextNode.Pretty())
	//return receiverNode, nil
	return nextNode, nil
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
