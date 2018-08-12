
package coral
import (
	"context"



	pb "github.com/libp2p/go-libp2p-kad-dht/pb"

	peer "github.com/libp2p/go-libp2p-peer"



)

type cNodeHandler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func (cNode *coralNode) handlerForMsgType(t pb.Message_MessageType) cNodeHandler {
	switch t {
	case pb.Message_GET_VALUE:
		return cNode.handleGetValue
	case pb.Message_PUT_VALUE:
		return cNode.handlePutValue
	// case pb.Message_FIND_NODE:
	//   return cNode.handleFindPeer
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

func (cNode *coralNode) handlePutValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	//store in local data store

 return pmes, err

}

func (cNode *coralNode) handleGetValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {


 cNode.nearestPeersToQuery(pmes, 1)
 return pmes
}
