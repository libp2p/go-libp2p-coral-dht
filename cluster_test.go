package coral

import (
	"testing"

	tu "github.com/libp2p/go-testutil"
)

func TestCluster(t *testing.T) {
	pid := tu.RandPeerIDFatal(t)
	n := NewCNode(pid)

	t.Logf("Cluster Two size: '%d'", n.levelTwoClustSize())
	t.Logf("Cluster One size: '%d'", n.levelOneClustSize())
	t.Logf("Cluster Zero size: '%d'", n.levelZeroClustSize())

	pid = tu.RandPeerIDFatal(t)
	n.addCNode(pid, "2")
	pid = tu.RandPeerIDFatal(t)
	n.addCNode(pid, "2")
	t.Logf("Cluster Two size: '%d'", n.levelTwoClustSize())

	cid := n.lookupClustID(pid)
	t.Logf("Lookup returned '%s'", cid)

	n.joinCluster("345")
	t.Logf("New ClusterID '%s'", n.levelTwo.clusterID)

	pid = n.levelTwo.getNextNode("0x00", n.id)
	t.Logf("Next node is '%s'", pid)
	n.insert("134", pid)
}
