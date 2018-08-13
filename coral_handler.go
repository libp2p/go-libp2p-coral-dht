
package coral
import (
	"context"
	"errors"
	"time"
	ds "github.com/ipfs/go-datastore"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
recpb "github.com/libp2p/go-libp2p-record/pb"
	peer "github.com/libp2p/go-libp2p-peer"
base32 "github.com/whyrusleeping/base32"
proto "github.com/gogo/protobuf/proto"
u "github.com/ipfs/go-ipfs-util"
pstore "github.com/libp2p/go-libp2p-peerstore"
inet "github.com/libp2p/go-libp2p-net"

)

type cNodeHandler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func (cNode *coralNode) handlerForMsgType(t pb.Message_MessageType) cNodeHandler {
	switch t {
	//case pb.Message_GET_VALUE:
		//return cNode.handleGetValue
	case pb.Message_PUT_VALUE:
		return cNode.handlePutValue
	 case pb.Message_FIND_NODE:
	  return cNode.handleFindNode
	// case pb.Message_ADD_PROVIDER:
	// 	return cNode.handleAddProvider
	// case pb.Message_GET_PROVIDERS:
	// 	return cNode.handleGetProviders
	// case pb.Message_PING:
	// 	return cNode.handlePing
	default:
		return nil
	}
}
func cleanRecord(rec *recpb.Record) {
	rec.XXX_unrecognized = nil
	rec.TimeReceived = nil
}

func convertToDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

func (cNode *coralNode) handlePutValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {

	rec := pmes.GetRecord()
	if rec == nil {
		//log.Infof("Got nil record from: %s", p.Pretty())
		return nil, errors.New("nil record")
	}

	if pmes.GetKey() != rec.GetKey() {
		return nil, errors.New("put key doesn't match record key")
	}

	cleanRecord(rec)

	// Make sure the record is valid (not expired, valid signature etc)
	if err = cNode.Validator.Validate(rec.GetKey(), rec.GetValue()); err != nil {
		//log.Warningf("Bad dht record in PUT from: %s. %s", p.Pretty(), err)
		return nil, err
	}

	dskey := convertToDsKey(rec.GetKey())

	// Make sure the new record is "better" than the record we have locally.
	// This prevents a record with for example a lower sequence number from
	// overwriting a record with a higher sequence number.
	// existing, err := cNode.getRecordFromDatastore(dskey)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// if existing != nil {
	// 	recs := [][]byte{rec.GetValue(), existing.GetValue()}
	// 	i, err := cNode.Validator.Select(rec.GetKey(), recs)
	// 	if err != nil {
	// 		//log.Warningf("Bad dht record in PUT from %s: %s", p.Pretty(), err)
	// 		return nil, err
	// 	}
	// 	if i != 0 {
	// 		//log.Infof("DHT record in PUT from %s is older than existing record. Ignoring", p.Pretty())
	// 		return nil, errors.New("old record")
	// 	}
	// }

	// record the time we receive every record
	rec.TimeReceived = proto.String(u.FormatRFC3339(time.Now()))

	data, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}

	err = cNode.datastore.Put(dskey, data)
	//log.Debugf("%s handlePutValue %v", dht.self, dskey)
	return pmes, err
}

func (cNode *coralNode) handleFindNode(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {


 resp := pb.NewMessage(pmes.GetType(), "", pmes.GetClusterLevel())
 var nearest []peer.ID


 // if looking for self... special case where we send it on CloserPeers.
	targetPid := peer.ID(pmes.GetKey())

	if targetPid == cNode.id {
		nearest = []peer.ID{cNode.id}
	} else {
		nearest = cNode.nearestPeersToQuery(pmes, 1)

		// Never tell a peer about itself.
		if targetPid != p {
			// If we're connected to the target peer, report their
			// peer info. This makes FindPeer work even if the
			// target peer isn't in our routing table.
			//
			// Alternatively, we could just check our peerstore.
			// However, we don't want to return out of date
			// information. We can change this in the future when we
			// add a progressive, asynchronous `SearchPeer` function
			// and improve peer routing in the host.
			switch cNode.host.Network().Connectedness(targetPid) {
			case inet.Connected, inet.CanConnect:
				nearest = append(nearest, targetPid)
			}
		}
	}

	if nearest == nil {
		//log.Infof("%s handleFindPeer %s: could not find anything.", cNode.id, p)
		return resp, nil
	}

	nearestinfos := pstore.PeerInfos(cNode.peerstore, nearest)
	// possibly an over-allocation but this array is temporary anyways.
	withAddresses := make([]pstore.PeerInfo, 0, len(nearestinfos))
	for _, pi := range nearestinfos {
		if len(pi.Addrs) > 0 {
			withAddresses = append(withAddresses, pi)
		//	log.Debugf("handleFindPeer: sending back '%s'", pi.ID)
		}
	}

	resp.CloserPeers = pb.PeerInfosToPBPeers(cNode.host.Network(), withAddresses)
	return resp, nil
}
