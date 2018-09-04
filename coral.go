package coral

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
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
	notif "github.com/libp2p/go-libp2p-routing/notifications"
	"github.com/whyrusleeping/base32"
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
	plk         sync.Mutex
	smlk        sync.Mutex
	waitlk      sync.Mutex
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

func (cNode *coralNode) GetValue(ctx context.Context, key string) (<-chan []byte, error) {

	Value := make(chan []byte)
	go func() {
		rec, err := cNode.getLocal(key)
		if err != nil {

		}
		if rec != nil {
			value := rec.GetValue()
			Value <- value

		} else {
			lastNodeReached := cNode.clusterSearch(ctx, cNode.id, Value, 2, key)
			//level 1 cluster search pick up from where you left off
			lastNodeReached = cNode.clusterSearch(ctx, lastNodeReached, Value, 1, key)
			//level 0 cluster search pick up from where you left off
			lastNodeReached = cNode.clusterSearch(ctx, lastNodeReached, Value, 0, key)
		}
		//close(value)
	}()
	return Value, nil
}

func (cNode *coralNode) Close() error {
	return cNode.proc.Close()
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

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func(key string, p peer.ID) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				defer wg.Done()

				peer, err := cNode.findAndAddNextNode(ctx, nextKey, chosenNode)
				if err != nil {
					//log.Debugf("failed putting value to peer: %s", err)
				}
				notif.PublishQueryEvent(ctx, &notif.QueryEvent{
					Type: notif.Value,
					ID:   peer,
				})

			}(nextKey, chosenNode)

			wg.Wait()
		}

		chosenNode = cNode.levelTwo.routingTable.NearestPeer(kb.ConvertKey(nextKey))
		nodeStack = append(nodeStack, chosenNode)
		dist = dist.Div(dist, big.NewInt(2))

	}
	rec := record.MakePutRecord(key, value)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(p []peer.ID, rec *recpb.Record, key string) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer wg.Done()

		peer, err := cNode.putValueToPeer(ctx, nodeStack, rec, key)
		if err != nil {
			//log.Debugf("failed putting value to peer: %s", err)
		}
		notif.PublishQueryEvent(ctx, &notif.QueryEvent{
			Type: notif.Value,
			ID:   peer,
		})

	}(nodeStack, rec, key)

	wg.Wait()

	return nil
}

func (cNode *coralNode) putValueToPeer(ctx context.Context, nodeStack []peer.ID, rec *recpb.Record, key string) (peer.ID, error) {
	fullAndLoaded := true
	var chosenNode peer.ID
	if len(nodeStack) == 0 {
		return chosenNode, errors.New("Attempted to put value with empty node stack.")
	}
	for fullAndLoaded {

		chosenNode = nodeStack[len(nodeStack)-1]
		pmes := pb.NewMessage(pb.Message_PUT_VALUE, []byte(key), 0)
		pmes.Record = rec
		rpmes, err := cNode.sendRequest(ctx, chosenNode, pmes)

		fmt.Printf("rpmes info %s\n", rpmes.GetType())
		fmt.Printf("Put to node %s\n", chosenNode)
		fmt.Printf("Local node is %s\n", cNode.id)
		if err != nil {
			if err == ErrReadTimeout {
				fmt.Printf("read timeout: %s %s", chosenNode.Pretty(), key)
			}
			//	fmt.Printf("here")
			return chosenNode, err
		}

		// if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
		// 	return errors.New("value not put correctly")
		// }
		fullAndLoaded = false
	}
	return chosenNode, nil
}

func (cNode *coralNode) getLocal(key string) (*recpb.Record, error) {
	//log.Debugf("getLocal %s", key)
	rec, err := cNode.getRecordFromDatastore(mkDsKey(key))
	if err != nil {
		//	log.Warningf("getLocal: %s", err)
		return nil, err
	}

	// Double check the key. Can't hurt.
	if rec != nil && string(rec.GetKey()) != key {
		//	log.Errorf("BUG getLocal: found a DHT record that didn't match it's key: %s != %s", rec.GetKey(), key)
		return nil, nil

	}
	return rec, nil
}
func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

func (cNode *coralNode) nearestPeersToQuery(pmes *pb.Message, p peer.ID, count int, cluster int) []peer.ID {
	var closer []peer.ID
	closer = cNode.levelTwo.routingTable.NearestPeers(kb.ConvertKey(string(pmes.GetKey())), count)

	// no node? nil
	if closer == nil {
		fmt.Printf("betterPeersToQuery: no closer peers to send: %s", p)
		return nil
	}

	filtered := make([]peer.ID, 0, len(closer))
	for _, clp := range closer {

		// == to self? thats bad
		if clp == cNode.id {
			fmt.Printf("BUG betterPeersToQuery: attempted to return self! this shouldn't happen...")
			return nil
		}
		// Dont send a peer back themselves
		if clp == p {
			continue
		}

		filtered = append(filtered, clp)
	}
	return filtered

}

func (cNode *coralNode) findAndAddNextNode(ctx context.Context, key string, receiverNode peer.ID) (peer.ID, error) {
	fmt.Printf("Entered findandAddNextNode for key: %s, and recievernode: %s \n", key, receiverNode)
	pmes := pb.NewMessage(pb.Message_FIND_NODE, []byte(key), 2)
	resp, err := cNode.sendRequest(ctx, receiverNode, pmes)
	if err != nil {
		return peer.ID(""), err
	}

	closer := resp.GetCloserPeers()
	clpeers := pb.PBPeersToPeerInfos(closer)
	var nextNode peer.ID
	if len(clpeers) > 0 {
		nextNode = clpeers[0].ID
		cNode.peerstore.AddAddrs(clpeers[0].ID, clpeers[0].Addrs, 6*time.Hour)
	} else {
		nextNode = receiverNode
	}
	///	once you get the next node, add it to your routing table
	cNode.sortNode(nextNode)

	fmt.Printf("Found node id: %s\n", nextNode.Pretty())
	//return receiverNode, nil
	return nextNode, nil
}

//Cluster search searches a given cluster that a node belongs to for a key
//Want to surface most local values first
func (cNode *coralNode) clusterSearch(ctx context.Context, nodeid peer.ID, Value chan []byte, clusterLevel int, key string) peer.ID {
	totaldist := kb.Dist(nodeid, key)
	dist := kb.Dist(nodeid, key)
	goalkey := key

	epsilon := big.NewInt(1)
	var chosenNode peer.ID
	nextKey := key
	for dist.Cmp(epsilon) > 0 {
		fmt.Printf("Entered top of cluster search for loop %d\n", clusterLevel)
		nextKey = kb.CalculateMidpointKey(nextKey, dist) //add dist to previous Nextkey
		if dist.Cmp(totaldist) != 0 {
			fmt.Printf("chosen node: %s\n", chosenNode)
			value := cNode.findNextNodeAndVal(ctx, goalkey, nextKey, chosenNode, clusterLevel)
			//fmt.Printf("here\n")
			if value != nil {
				Value <- value
			}
		}
		//query routing table
		chosenNode = cNode.levelTwo.routingTable.NearestPeer(kb.ConvertKey(nextKey))
		dist = dist.Div(dist, big.NewInt(2))
	}
	fmt.Printf("finished")
	return chosenNode

}

func (cNode *coralNode) findNextNodeAndVal(ctx context.Context, goalkey string, fakekey string, nodeid peer.ID, clusterlevel int) []byte {
	var value []byte
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(key string, p peer.ID, clusterlevel int) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer wg.Done()
		//		fmt.Printf("request from %s\n", p)
		pmes := pb.NewMessage(pb.Message_GET_VALUE, []byte(goalkey), clusterlevel)
		resp, err := cNode.sendRequest(ctx, p, pmes)
		if err != nil {
			fmt.Printf("%s", err)
		}
		value = resp.GetRecord().GetValue()
		// notif.PublishQueryEvent(ctx, &notif.QueryEvent{
		// 	Type: notif.Value,
		// 	ID:   peer,
		// })

	}(goalkey, nodeid, clusterlevel)

	wg.Wait()

	wg = sync.WaitGroup{}
	wg.Add(1)
	go func(key string, p peer.ID, clusterlevel int) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer wg.Done()

		pmesnode := pb.NewMessage(pb.Message_FIND_NODE, []byte(fakekey), clusterlevel)
		resp, err := cNode.sendRequest(ctx, nodeid, pmesnode)
		if err != nil {
			fmt.Printf("%s", err)
		}

		closer := resp.GetCloserPeers()
		clpeers := pb.PBPeersToPeerInfos(closer)
		var nextNode peer.ID
		if len(clpeers) > 0 {
			nextNode = clpeers[0].ID
			cNode.peerstore.AddAddrs(clpeers[0].ID, clpeers[0].Addrs, 6*time.Hour)
		} else {
			nextNode = nodeid
		}
		//once you get the next node, add it to your routing table
		cNode.sortNode(nextNode)
	}(fakekey, nodeid, clusterlevel)

	wg.Wait()

	if value == nil {
		return nil
	} else {
		return value
	}
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
func (cNode *coralNode) addCNode(p peer.ID, clust int) {
	if clust == 2 {
		cNode.levelTwo.routingTable.Update(p) //change
		cNode.peerToClust[p] = cNode.levelTwo.clusterID
	} else if clust == 1 {
		cNode.levelOne.routingTable.Update(p)
		cNode.peerToClust[p] = cNode.levelOne.clusterID
	} else {
		cNode.levelZero.routingTable.Update(p)
		cNode.peerToClust[p] = cNode.levelZero.clusterID
	}

	//update peer store?
}

//lookup a peer's cluster info
//for now just implemented for level 2 only
func (cNode *coralNode) lookupClustID(p peer.ID) ClusterID {
	return cNode.peerToClust[p]
}

func (cNode *coralNode) sortAllNodes() {
	peers := cNode.levelTwo.routingTable.ListPeers()

	for _, p := range peers {
		cNode.sortNode(p)
	}
}
func (cNode *coralNode) sortNode(p peer.ID) {

	//timeOne := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(p peer.ID) {
		ctx, cancel := context.WithCancel(cNode.ctx)
		defer cancel()
		defer wg.Done()
		pmes := pb.NewMessage(pb.Message_PING, nil, 0)
		_, err := cNode.sendRequest(ctx, p, pmes)
		if err != nil {
		}

	}(p)
	wg.Wait()
	//rTT := time.Now().Sub(timeOne)
	//seconds := rTT.Seconds()
	//if seconds >= 80000 {
	//		cNode.levelZero.routingTable.Update(p)
	//level 0
	//	} else if seconds >= 20000 {
	//	cNode.levelOne.routingTable.Update(p)
	//level 1
	//	} else {
	cNode.levelTwo.routingTable.Update(p)
	// level 2
}

//}

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
