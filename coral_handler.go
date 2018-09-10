package coral

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	proto "github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	u "github.com/ipfs/go-ipfs-util"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	base32 "github.com/whyrusleeping/base32"
	//inet "github.com/libp2p/go-libp2p-net"
)

type cNodeHandler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func (cNode *coralNode) handlerForMsgType(t pb.Message_MessageType) cNodeHandler {
	switch t {
	case pb.Message_GET_VALUE: //gets value
		return cNode.handleGetValue
	case pb.Message_PUT_VALUE: //puts value
		return cNode.handlePutValue
	case pb.Message_FIND_NODE: //returns closer peers
		return cNode.handleFindPeer
	// case pb.Message_ADD_PROVIDER:
	// 	return cNode.handleAddProvider
	// case pb.Message_GET_PROVIDERS:
	// 	return cNode.handleGetProviders
	case pb.Message_PING:
		return cNode.handlePing
	default:
		return nil
	}
}
func cleanRecord(rec *recpb.Record) {
	rec.XXX_unrecognized = nil
	rec.TimeReceived = ""
}

func convertToDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

func (cNode *coralNode) handleGetValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {

	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	// first, is there even a key?
	k := pmes.GetKey()
	if len(k) == 0 {
		return nil, errors.New("handleGetValue but no key was provided")
		// TODO: send back an error response? could be bad, but the other node's hanging.
	}

	rec, err := cNode.checkLocalDatastore(k)
	if err != nil {
		fmt.Printf("error")
		return nil, err
	}
	resp.Record = rec
	return resp, nil

}

func (cNode *coralNode) checkLocalDatastore(k []byte) (*recpb.Record, error) {
	//	log.Debugf("%s handleGetValue looking into ds", dht.self)
	dskey := convertToDsKey(string(k))
	buf, err := cNode.datastore.Get(dskey)
	//log.Debugf("%s handleGetValue looking into ds GOT %v", dht.self, buf)

	if err == ds.ErrNotFound {
		return nil, nil
	}

	// if we got an unexpected error, bail.
	if err != nil {
		return nil, err
	}

	// if we have the value, send it back
	//log.Debugf("%s handleGetValue success!", dht.self)

	rec := new(recpb.Record)
	s := buf.([]byte)
	err = proto.Unmarshal(s, rec)
	if err != nil {
		//fmt.Printf("failed to unmarshal DHT record from datastore")
		return nil, err
	}

	var recordIsBad bool
	_, err = u.ParseRFC3339(rec.GetTimeReceived())
	if err != nil {
		//fmt.Printf("either no receive time set on record, or it was invalid: %s", err)
		recordIsBad = true
	}

	// if time.Now().Sub(recvtime) > MaxRecordAge {
	// 	//	log.Debug("old record found, tossing.")
	// 	recordIsBad = true
	// }

	// NOTE: We do not verify the record here beyond checking these timestamps.
	// we put the burden of checking the records on the requester as checking a record
	// may be computationally expensive

	if recordIsBad {
		err := cNode.datastore.Delete(dskey)
		if err != nil {
			//fmt.Printf("Failed to delete bad record from datastore: %s", err)
		}

		return nil, nil // can treat this as not having the record at all
	}

	return rec, nil
}

func (cNode *coralNode) handlePutValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	resp := pb.NewMessage(pmes.GetType(), nil, pmes.GetClusterLevel())
	rec := pmes.GetRecord()
	if rec == nil {
		//log.Infof("Got nil record from: %s", p.Pretty())
		return nil, errors.New("nil record")
	}

	if !bytes.Equal([]byte(pmes.GetKey()), rec.GetKey()) {
		return nil, errors.New("put key doesn't match record key")
	}

	cleanRecord(rec)

	// Make sure the record is valid (not expired, valid signature etc)
	if err = cNode.Validator.Validate(string(rec.GetKey()), rec.GetValue()); err != nil {
		//log.Warningf("Bad dht record in PUT from: %s. %s", p.Pretty(), err)
		return nil, err
	}

	dskey := convertToDsKey(string(rec.GetKey()))
	//TODO: add this check back in
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
	rec.TimeReceived = u.FormatRFC3339(time.Now())

	data, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}

	err = cNode.datastore.Put(dskey, data)
	//log.Debugf("%s handlePutValue %v", dht.self, dskey)
	return resp, err
}

func (cNode *coralNode) handleFindPeer(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {

	resp := pb.NewMessage(pmes.GetType(), nil, pmes.GetClusterLevel())
	var nearest []peer.ID

	nearest = cNode.nearestPeersToQuery(pmes, p, 2, pmes.GetClusterLevel())

	nearestinfos := pstore.PeerInfos(cNode.peerstore, nearest)
	// possibly an over-allocation but this array is temporary anyways.
	withAddresses := make([]pstore.PeerInfo, 0, len(nearestinfos))
	for _, pi := range nearestinfos {

		if len(pi.Addrs) > 0 {
			withAddresses = append(withAddresses, pi)

			//fmt.Printf("handleFindPeer: sending back '%s' %s\n", pi.ID, pi.Addrs)
		}
	}

	resp.CloserPeers = pb.PeerInfosToPBPeers(cNode.host.Network(), withAddresses)
	//fmt.Printf("Response closer peers %s\n", resp.CloserPeers)
	return resp, nil
}

func (cNode *coralNode) getRecordFromDatastore(dskey ds.Key) (*recpb.Record, error) {
	buf, err := cNode.datastore.Get(dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		//log.Errorf("Got error retrieving record with key %s from datastore: %s", dskey, err)
		return nil, err
	}
	rec := new(recpb.Record)
	s := buf.([]byte)
	err = proto.Unmarshal(s, rec)
	if err != nil {
		// Bad data in datastore, log it but don't return an error, we'll just overwrite it
		//log.Errorf("Bad record data stored in datastore with key %s: could not unmarshal record", dskey)
		return nil, nil
	}

	err = cNode.Validator.Validate(string(rec.GetKey()), rec.GetValue())
	if err != nil {
		// Invalid record in datastore, probably expired but don't return an error,
		// we'll just overwrite it
		//log.Debugf("Local record verify failed: %s (discarded)", err)
		return nil, nil
	}

	return rec, nil
}

func (cNode *coralNode) handlePing(_ context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	//fmt.Printf("here")
	resp := pb.NewMessage(pmes.GetType(), nil, pmes.GetClusterLevel())
	return resp, nil
}
