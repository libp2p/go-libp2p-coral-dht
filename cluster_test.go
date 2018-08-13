package coral

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"testing"
	"time"

	opts "github.com/libp2p/go-libp2p-kad-dht/opts"

	cid "github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	logging "github.com/ipfs/go-log"

	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"

	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"

	ma "github.com/multiformats/go-multiaddr"
)

var testCaseValues = map[string][]byte{}
var testCaseCids []*cid.Cid
var log = logging.Logger("dht")

func init() {
	for i := 0; i < 100; i++ {
		v := fmt.Sprintf("%d -- value", i)

		mhv := u.Hash([]byte(v))
		testCaseCids = append(testCaseCids, cid.NewCidV0(mhv))
	}
}

type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

type testValidator struct{}

func (testValidator) Select(_ string, bs [][]byte) (int, error) {
	index := -1
	for i, b := range bs {
		if bytes.Compare(b, []byte("newer")) == 0 {
			index = i
		} else if bytes.Compare(b, []byte("valid")) == 0 {
			if index == -1 {
				index = i
			}
		}
	}
	if index == -1 {
		return -1, errors.New("no rec found")
	}
	return index, nil
}
func (testValidator) Validate(_ string, b []byte) error {
	if bytes.Compare(b, []byte("expired")) == 0 {
		return errors.New("expired")
	}
	return nil
}

func setupDHT(ctx context.Context, t *testing.T, client bool) *coralNode {
	c, err := New(
		ctx,
		bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		opts.Client(client),
		opts.NamespacedValidator("v", blankValidator{}),
	)

	if err != nil {
		t.Fatal(err)
	}
	return c
}

func setupDHTS(ctx context.Context, n int, t *testing.T) ([]ma.Multiaddr, []peer.ID, []*coralNode) {
	addrs := make([]ma.Multiaddr, n)
	dhts := make([]*coralNode, n)
	peers := make([]peer.ID, n)

	sanityAddrsMap := make(map[string]struct{})
	sanityPeersMap := make(map[string]struct{})

	for i := 0; i < n; i++ {
		dhts[i] = setupDHT(ctx, t, false)
		peers[i] = dhts[i].id
		addrs[i] = dhts[i].peerstore.Addrs(dhts[i].id)[0]

		if _, lol := sanityAddrsMap[addrs[i].String()]; lol {
			t.Fatal("While setting up DHTs address got duplicated.")
		} else {
			sanityAddrsMap[addrs[i].String()] = struct{}{}
		}
		if _, lol := sanityPeersMap[peers[i].String()]; lol {
			t.Fatal("While setting up DHTs peerid got duplicated.")
		} else {
			sanityPeersMap[peers[i].String()] = struct{}{}
		}
	}

	return addrs, peers, dhts
}

func connectNoSync(t *testing.T, ctx context.Context, a, b *coralNode) {
	t.Helper()

	idB := b.id
	addrB := b.peerstore.Addrs(idB)
	//a.levelTwo.routingTable.Update(idB)
	if len(addrB) == 0 {
		t.Fatal("peers setup incorrectly: no local address")
	}
	// nn1 := (*netNotifiee)(a)
	// nn2 := (*netNotifiee)(b)

	a.peerstore.AddAddrs(idB, addrB, pstore.TempAddrTTL)
	pi := pstore.PeerInfo{ID: idB}
	if err := a.host.Connect(ctx, pi); err != nil {
		t.Fatal(err)
	}

	// c12 := a.host.Network().ConnsToPeer(b.id)[0]
	// c21 := b.host.Network().ConnsToPeer(a.id)[0]

	// Pretend to reestablish/re-kill connection
	// nn1.Connected(a.host.Network(), c12)
	// nn2.Connected(b.host.Network(), c21)

}

func wait(t *testing.T, ctx context.Context, a, b *coralNode) {
	t.Helper()
	fmt.Printf("wait")
	// loop until connection notification has been received.
	// under high load, this may not happen as immediately as we would like.
	for a.levelTwo.routingTable.Find(b.id) == "" {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond * 5):
		}
	}
}

func connect(t *testing.T, ctx context.Context, a, b *coralNode) {
	t.Helper()
	fmt.Printf("before connect no sync")
	connectNoSync(t, ctx, a, b)
	wait(t, ctx, a, b)
	wait(t, ctx, b, a)
}

func TestValueGetSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fmt.Printf("before connect")
	var dhts [5]*coralNode

	for i := range dhts {
		dhts[i] = setupDHT(ctx, t, false)
		//defer dhts[i].Close()
		defer dhts[i].host.Close()
	}

	fmt.Printf("before connect")
	connect(t, ctx, dhts[0], dhts[1])

	t.Log("adding value on: ", dhts[0].id)
	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := dhts[0].PutValue(ctxT, "/v/hello", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("requesting value on dhts: ", dhts[1].id)
	ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()

	val, err := dhts[1].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}
	//edit this to be a channel
	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}

	// // late connect
	// connect(t, ctx, dhts[2], dhts[0])
	// connect(t, ctx, dhts[2], dhts[1])

	// t.Log("requesting value (offline) on dhts: ", dhts[2].id)
	// vala, err := dhts[2].GetValue(ctxT, "/v/hello")
	// if vala != nil {
	// 	t.Fatalf("offline get should have failed, got %s", string(vala))
	// }
	// if err != routing.ErrNotFound {
	// 	t.Fatalf("offline get should have failed with ErrNotFound, got: %s", err)
	// }
	//
	// t.Log("requesting value (online) on dhts: ", dhts[2].id)
	// val, err = dhts[2].GetValue(ctxT, "/v/hello")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	//
	// if string(val) != "world" {
	// 	t.Fatalf("Expected 'world' got '%s'", string(val))
	// }
	//
	// for _, d := range dhts[:3] {
	// 	connect(t, ctx, dhts[3], d)
	// }
	// connect(t, ctx, dhts[4], dhts[3])
	//
	// t.Log("requesting value (requires peer routing) on dhts: ", dhts[4].id)
	// val, err = dhts[4].GetValue(ctxT, "/v/hello")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	//
	// if string(val) != "world" {
	// 	t.Fatalf("Expected 'world' got '%s'", string(val))
	// }
}

//
// import (
// 	"testing"
//
// 	tu "github.com/libp2p/go-testutil"
// )
//
// func TestCluster(t *testing.T) {
// 	pid := tu.RandPeerIDFatal(t)
// 	n := NewCNode(pid)
//
// 	t.Logf("Cluster Two size: '%d'", n.levelTwoClustSize())
// 	t.Logf("Cluster One size: '%d'", n.levelOneClustSize())
// 	t.Logf("Cluster Zero size: '%d'", n.levelZeroClustSize())
//
// 	pid = tu.RandPeerIDFatal(t)
// 	n.addCNode(pid, "2")
// 	pid = tu.RandPeerIDFatal(t)
// 	n.addCNode(pid, "2")
// 	t.Logf("Cluster Two size: '%d'", n.levelTwoClustSize())
// 	if n.levelTwoClustSize() != 2 {
// 		t.Fatalf("Expected cluster size of 2, got %d ", n.levelTwoClustSize())
// 	}
// 	cid := n.lookupClustID(pid)
// 	t.Logf("Lookup returned '%s'", cid)
//
// 	n.joinCluster("345")
// 	t.Logf("New ClusterID '%s'", n.levelTwo.clusterID)
// 	t.Logf("Node id is %s", n.id)
// 	pid = n.levelTwo.getNextNode("1", n.id)
// 	t.Logf("Next node is '%s'", pid)
// 	//n.insert("134", pid)
// }
