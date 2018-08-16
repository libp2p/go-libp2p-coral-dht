package coral

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	ggio "github.com/gogo/protobuf/io"
	ctxio "github.com/jbenet/go-context/io"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
)

var cNodeReadMessageTimeout = time.Minute
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// handleNewStream implements the inet.StreamHandler
func (cNode *coralNode) handleNewStream(s inet.Stream) {
	go cNode.handleNewMessage(s)
}

func (cNode *coralNode) handleNewMessage(s inet.Stream) {
	ctx := cNode.ctx
	cr := ctxio.NewReader(ctx, s) // ok to use. we defer close stream in this func
	cw := ctxio.NewWriter(ctx, s) // ok to use. we defer close stream in this func
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		// receive msg
		pmes := new(pb.Message)
		switch err := r.ReadMsg(pmes); err {
		case io.EOF:
			s.Close()
			return
		case nil:
		default:
			s.Reset()
			fmt.Printf("Error unmarshaling data: %s", err)
			return
		}

		// update the peer (on valid msgs only)
		cNode.updateFromMessage(ctx, mPeer, pmes)

		// get handler for this msg type.
		handler := cNode.handlerForMsgType(pmes.GetType())
		if handler == nil {
			s.Reset()
			fmt.Printf("got back nil handler from handlerForMsgType")
			return
		}

		// dispatch handler.
		rpmes, err := handler(ctx, mPeer, pmes)
		if err != nil {
			s.Reset()
			fmt.Printf("handle message error: %s", err)
			return
		}

		// if nil response, return it before serializing
		if rpmes == nil {
			//log.Debug("got back nil response from request")
			continue
		}

		// send out response msg
		if err := w.WriteMsg(rpmes); err != nil {
			s.Reset()
			fmt.Printf("send response error: %s", err)
			return
		}
	}
}

// sendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (cNode *coralNode) sendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {

	ms, err := cNode.messageSenderForPeer(p)
	if err != nil {
		return nil, err
	}

	start := time.Now()

	rpmes, err := ms.SendRequest(ctx, pmes)
	if err != nil {
		return nil, err
	}

	// update the peer (on valid msgs only)
	cNode.updateFromMessage(ctx, p, rpmes)

	cNode.peerstore.RecordLatency(p, time.Since(start))
	//log.Event(ctx, "cNode ReceivedMessage", cNode .self, p, rpmes)
	return rpmes, nil
}

// sendMessage sends out a message
func (cNode *coralNode) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ms, err := cNode.messageSenderForPeer(p)
	if err != nil {
		return err
	}

	if err := ms.SendMessage(ctx, pmes); err != nil {
		return err
	}
	//log.Event(ctx, "cNode SentMessage", cNode .self, p, pmes)
	return nil
}

func (cNode *coralNode) updateFromMessage(ctx context.Context, p peer.ID, mes *pb.Message) error {
	// Make sure that this node is actually a cNode server, not just a client.
	protos, err := cNode.peerstore.SupportsProtocols(p, cNode.protocolStrs()...)
	if err == nil && len(protos) > 0 {
		cNode.Update(ctx, p)
	}
	return nil
}

func (cNode *coralNode) messageSenderForPeer(p peer.ID) (*messageSender, error) {
	// cNode.smlk.Lock()
	ms := cNode.strmap[p]
	// if ok {
	// 	cNode.smlk.Unlock()
	// 	return ms, nil
	// }
	ms = &messageSender{p: p, cNode: cNode}
	cNode.strmap[p] = ms
	//cNode .smlk.Unlock()

	if err := ms.prepOrInvalidate(); err != nil {
		//cNode .smlk.Lock()
		//defer cNode .smlk.Unlock()

		msCur := cNode.strmap[p]
		// Changed. Use the new one, old one is invalid and
		// not in the map so we can just throw it away.
		if ms != msCur {
			return msCur, nil
		}
		// Not changed, remove the now invalid stream from the
		// map.
		delete(cNode.strmap, p)
		return nil, err
	}
	// Invalid but not in map. Must have been removed by a disconnect.

	// All ready to go.
	return ms, nil
}

type messageSender struct {
	s     inet.Stream
	r     ggio.ReadCloser
	w     ggio.WriteCloser
	lk    sync.Mutex
	p     peer.ID
	cNode *coralNode

	invalid   bool
	singleMes int
}

// invalidate is called before this messageSender is removed from the strmap.
// It prevents the messageSender from being reused/reinitialized and then
// forgotten (leaving the stream open).
func (ms *messageSender) invalidate() {
	ms.invalid = true
	if ms.s != nil {
		ms.s.Reset()
		ms.s = nil
	}
}

func (ms *messageSender) prepOrInvalidate() error {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	if err := ms.prep(); err != nil {
		ms.invalidate()
		return err
	}
	return nil
}

func (ms *messageSender) prep() error {
	if ms.invalid {
		return fmt.Errorf("message sender has been invalidated")
	}
	if ms.s != nil {
		return nil
	}

	nstr, err := ms.cNode.host.NewStream(ms.cNode.ctx, ms.p, ms.cNode.protocols...)
	if err != nil {
		return err
	}

	ms.r = ggio.NewDelimitedReader(nstr, inet.MessageSizeMax)
	ms.w = ggio.NewDelimitedWriter(nstr)
	ms.s = nstr

	return nil
}

// streamReuseTries is the number of times we will try to reuse a stream to a
// given peer before giving up and reverting to the old one-message-per-stream
// behaviour.
const streamReuseTries = 3

func (ms *messageSender) SendMessage(ctx context.Context, pmes *pb.Message) error {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	retry := false
	for {
		if err := ms.prep(); err != nil {
			return err
		}

		if err := ms.w.WriteMsg(pmes); err != nil {
			ms.s.Reset()
			ms.s = nil

			if retry {
				//	log.Info("error writing message, bailing: ", err)
				return err
			} else {
				//	log.Info("error writing message, trying again: ", err)
				retry = true
				continue
			}
		}

		//log.Event(ctx, "cNode SentMessage", ms.cNode .self, ms.p, pmes)

		if ms.singleMes > streamReuseTries {
			go inet.FullClose(ms.s)
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return nil
	}
}

func (ms *messageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	retry := false
	for {
		if err := ms.prep(); err != nil {
			return nil, err
		}

		if err := ms.w.WriteMsg(pmes); err != nil {
			ms.s.Reset()
			ms.s = nil

			if retry {
				//log.Info("error writing message, bailing: ", err)
				return nil, err
			} else {
				//	log.Info("error writing message, trying again: ", err)
				retry = true
				continue
			}
		}

		mes := new(pb.Message)
		if err := ms.ctxReadMsg(ctx, mes); err != nil {
			ms.s.Reset()
			ms.s = nil

			if retry {
				fmt.Printf("error reading message, bailing: %s\n", err)

				return nil, err
			} else {
				fmt.Printf("error reading message, trying again: %s\n", err)
				retry = true
				continue
			}
		}

		//log.Event(ctx, "cNode SentMessage", ms.cNode .self, ms.p, pmes)

		if ms.singleMes > streamReuseTries {
			go inet.FullClose(ms.s)
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return mes, nil
	}
}

func (ms *messageSender) ctxReadMsg(ctx context.Context, mes *pb.Message) error {
	fmt.Printf("ctxReadMsg\n")
	errc := make(chan error, 1)
	go func(r ggio.ReadCloser) {
		errc <- r.ReadMsg(mes)
	}(ms.r)

	t := time.NewTimer(cNodeReadMessageTimeout)
	defer t.Stop()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}
}
