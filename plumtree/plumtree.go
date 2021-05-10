package plumtree

import (
	"flood/shared"
	"math/rand"
	"reflect"
	"time"

	"github.com/jinzhu/copier"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

//Messages types follow x1xx
const (
	PlumtreeProtoID = 21002
	name            = "Plumtree"

	IHaveTimeout = 1 * time.Second
	// PruneBackoff = 1 * time.Second
)

type Plumtree struct {
	r      *rand.Rand
	babel  protocolManager.ProtocolManager
	logger *logrus.Logger
	size   int

	// pruneBackoff   map[string]time.Time
	view           map[string]peer.Peer
	lazyPushPeers  map[string]peer.Peer
	eagerPushPeers map[string]peer.Peer

	missingMessages  map[uint32]map[string]messageSource
	mids             []uint32
	receivedMessages map[uint32]message.Message
	ongoingTimers    map[uint32]int

	lazyQueue  []addressedMsg
	eagerQueue map[string]message.Message

	useUDP bool
}

type messageSource = struct {
	p peer.Peer
	r uint32
}

type addressedMsg = struct {
	d  peer.Peer
	m  message.Message
	ts time.Time
}

func (f *Plumtree) ID() protocol.ID {
	return PlumtreeProtoID
}

func (f *Plumtree) Name() string {
	return name
}

func (f *Plumtree) Logger() *logrus.Logger {
	return f.logger
}

func (f *Plumtree) Start() {
	t := SendIHaveTimer{
		duration: 1 * time.Second,
	}
	f.babel.RegisterPeriodicTimer(f.ID(), t, false)
}

func (f *Plumtree) Init() {
	f.babel.RegisterMessageHandler(PlumtreeProtoID, shared.GossipMessage{}, f.uponReceiveGossipMessage)
	f.babel.RegisterNotificationHandler(PlumtreeProtoID, shared.NeighborDownNotification{}, f.uponNeighborDownNotification)
	f.babel.RegisterNotificationHandler(PlumtreeProtoID, shared.NeighborUpNotification{}, f.uponNeighborUpNotification)
	f.babel.RegisterRequestHandler(PlumtreeProtoID, shared.BroadcastRequestType, f.uponBroadcastRequest)

	f.babel.RegisterMessageHandler(f.ID(), shared.GossipMessage{}, f.uponReceiveGossipMessage)
	f.babel.RegisterMessageHandler(f.ID(), PruneMessage{}, f.uponReceivePruneMessage)
	f.babel.RegisterMessageHandler(f.ID(), shared.GraftMessage{}, f.uponReceiveGraftMessage)
	f.babel.RegisterMessageHandler(f.ID(), shared.IHaveMessage{}, f.uponReceiveIHaveMessage)

	f.babel.RegisterTimerHandler(f.ID(), IHaveTimeoutTimerType, f.uponIHaveTimeout)
	f.babel.RegisterTimerHandler(f.ID(), SendIHaveTimerType, f.uponSendIHaveTimer)

}

func (f *Plumtree) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return false //panic("this shouldn't run!")
}

func (f *Plumtree) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	return false //panic("this shouldn't run!")
}

func (f *Plumtree) DialFailed(peer peer.Peer) {
	//panic("this shouldn't run!")
}

func (f *Plumtree) OutConnDown(peer peer.Peer) {
	//panic("this shouldn't run!")
}

func (f *Plumtree) MessageDelivered(message message.Message, peer peer.Peer) {
	// do nothing really
}

func (f *Plumtree) MessageDeliveryErr(message message.Message, peer peer.Peer, err errors.Error) {
	f.logger.Errorf("Couldn't send message of type %s to %s due to: %s", reflect.TypeOf(message), peer.String(), err.ToString())
}

/* -------------------- MESSAGE HANDLERS -----------------------------------*/
func (f *Plumtree) uponReceiveGossipMessage(sender peer.Peer, m message.Message) {
	gossipMsg := m.(shared.GossipMessage)
	_, alreadyReceived := f.receivedMessages[gossipMsg.MID]
	if !alreadyReceived {
		// f.logger.Infof("Received new message %d from %s", gossipMsg.MID, sender)
		// f.logger.Infof("Received gossip message %d from %s", gossipMsg.MID, sender)
		f.babel.SendNotification(shared.DeliverMessageNotification{
			Message: gossipMsg,
			From:    sender,
		})
		f.receivedMessages[gossipMsg.MID] = gossipMsg
		f.mids = append(f.mids, gossipMsg.MID)
		if len(f.mids) > f.size {
			delete(f.receivedMessages, f.mids[0])
			f.mids = f.mids[1:]
		}

		if tID, ok := f.ongoingTimers[gossipMsg.MID]; ok {
			f.babel.CancelTimer(tID)
			delete(f.ongoingTimers, uint32(tID))
		}

		f.addToEager(sender)
		f.removeFromLazy(sender)

		f.eagerPush(gossipMsg, gossipMsg.Hop+1, sender)
		f.lazyPush(gossipMsg, gossipMsg.Hop+1, sender)

	} else {
		// lastTime, ok := f.pruneBackoff[sender.String()]
		// if ok {
		// if time.Since(lastTime) < PruneBackoff {
		// 	return
		// }
		// }
		// f.pruneBackoff[sender.String()] = time.Now()
		// f.logger.Infof("Received duplicate message %d from %s", gossipMsg.MID, sender)
		f.addToLazy(sender)
		f.removeFromEager(sender)
		f.sendMessage(PruneMessage{}, sender)
	}
}

func (f *Plumtree) uponReceiveIHaveMessage(sender peer.Peer, m message.Message) {
	graftMsg := m.(shared.IHaveMessage)

	if _, ok := f.receivedMessages[graftMsg.MID]; !ok {
		// f.logger.Infof("Received IHave for MISSING message %d from %s", graftMsg.MID, sender)
		if _, ok := f.ongoingTimers[graftMsg.MID]; !ok {
			tID := f.babel.RegisterTimer(f.ID(), IHaveTimeoutTimer{
				duration: IHaveTimeout,
				mid:      graftMsg.MID,
			})
			f.ongoingTimers[graftMsg.MID] = tID
		}
		if _, ok := f.missingMessages[graftMsg.MID]; !ok {
			f.missingMessages[graftMsg.MID] = make(map[string]messageSource)
		}
		// f.logger.Infof("Set up IHaveTimeoutTimer for message %d", graftMsg.MID)
		f.missingMessages[graftMsg.MID][sender.String()] = messageSource{
			p: sender,
			r: graftMsg.Round,
		}
	}
}

func (f *Plumtree) uponSendIHaveTimer(t timer.Timer) {
	tmp := []addressedMsg{}
	for _, v := range f.lazyQueue {
		if time.Since(v.ts) < 1*time.Second {
			tmp = append(tmp, v)
			continue
		}
		if _, ok := f.view[v.d.String()]; !ok {
			f.logger.Warnf("Not sending IHave as peer %s is not in view ", v.d.String())
			continue
		}
		f.logger.Info("Sending IHave message  to " + v.d.String())
		f.sendMessage(v.m, v.d)
	}
	f.lazyQueue = nil
}

func (f *Plumtree) uponReceivePruneMessage(sender peer.Peer, m message.Message) {
	// f.logger.Infof("Received prune message from %s", sender)
	f.removeFromEager(sender)
	f.addToLazy(sender)
}

func (f *Plumtree) uponReceiveGraftMessage(sender peer.Peer, m message.Message) {
	f.logger.Infof("Received graft message from %s", sender)
	graftMsg := m.(shared.GraftMessage)
	f.addToEager(sender)
	f.removeFromLazy(sender)

	toSend, ok := f.receivedMessages[graftMsg.MID]
	if !ok {
		f.logger.Errorf("Do not have message %d in order to respond to graft message", graftMsg.MID)
		return
	}
	// f.logger.Infof("Replying to graft message %d from %s", graftMsg.MID, sender.String())
	f.sendMessage(toSend, sender)
}

func (f *Plumtree) uponIHaveTimeout(t timer.Timer) {
	iHaveTimeoutTimer := t.(IHaveTimeoutTimer)

	if _, ok := f.receivedMessages[iHaveTimeoutTimer.mid]; !ok {
		// f.logger.Infof("IHaveTimeoutTimer trigger for missing message %d", iHaveTimeoutTimer.mid)
		messageSources, ok := f.missingMessages[iHaveTimeoutTimer.mid]
		if !ok {
			f.logger.Errorf("Source missing %d", iHaveTimeoutTimer.mid)
			return
		}

		if len(messageSources) == 0 {
			f.logger.Error("Message source is empty")
			return
		}
		newTimerID := f.babel.RegisterTimer(f.ID(), iHaveTimeoutTimer)
		f.ongoingTimers[iHaveTimeoutTimer.mid] = newTimerID
		for k, messageSource := range messageSources {
			// f.logger.Infof("Sending GraftMessage for mid %d to %s", iHaveTimeoutTimer.mid, messageSource.p)
			if _, ok := f.view[messageSource.p.String()]; !ok {
				delete(messageSources, k)
				continue
			}
			f.sendMessage(shared.GraftMessage{
				MID:   iHaveTimeoutTimer.mid,
				Round: messageSource.r,
			}, messageSource.p)
			break
		}
	}
}

func (f *Plumtree) sendMessage(m message.Message, target peer.Peer) {
	if f.useUDP {
		f.babel.SendMessageSideStream(m, target, target.ToUDPAddr(), f.ID(), f.ID())
	} else {
		f.babel.SendMessage(m, target, f.ID(), f.ID(), true)
	}
}

/* -------------------- REQUESTS HANDLERS -----------------------------------*/

func (f *Plumtree) uponBroadcastRequest(request request.Request) request.Reply {
	r := request.(shared.BroadcastRequest)
	mid := f.r.Uint32()
	msg := shared.GossipMessage{
		Content:  r.Content,
		TimeSent: uint64(time.Now().UnixNano()),
		MID:      mid,
		Hop:      0,
	}
	f.babel.SendNotification(shared.DeliverMessageNotification{
		Message: msg,
		From:    f.babel.SelfPeer(),
	})
	f.receivedMessages[mid] = msg
	f.mids = append(f.mids, mid)
	if len(f.mids) > f.size {
		delete(f.receivedMessages, f.mids[0])
		f.mids = f.mids[1:]
	}
	f.eagerPush(msg, 1, f.babel.SelfPeer())
	return nil
}

/* -------------------- NOTIFICATIONS HANDLERS -----------------------------------*/

func (f *Plumtree) uponNeighborDownNotification(n notification.Notification) {
	notif := shared.NeighborDownNotification{}
	err := copier.Copy(&notif, n)
	if err != nil {
		f.logger.Panic(err)
	}
	// delete(f.neighbors, notif.PeerDown.String())
	f.logger.Info("PEER DOWN!! - " + notif.PeerDown.String())
	f.view = notif.View
	f.removeFromEager(notif.PeerDown)
	f.removeFromLazy(notif.PeerDown)
}

func (f *Plumtree) uponNeighborUpNotification(n notification.Notification) {
	notif := shared.NeighborUpNotification{}
	err := copier.Copy(&notif, n)
	if err != nil {
		f.logger.Panic(err)
	}
	f.view = notif.View
	f.addToEager(notif.PeerUp)
	f.logger.Info("PEER UP!! - " + notif.PeerUp.String())
}

/* -------------------------------------------------------*/

func NewPlumTreeProtocol(babel protocolManager.ProtocolManager, useUDP bool) protocol.Protocol {
	logger := logs.NewLogger(name)
	// logger.SetLevel(logrus.InfoLevel)
	return &Plumtree{
		r:                rand.New(rand.NewSource(time.Now().UnixNano())),
		babel:            babel,
		logger:           logger,
		size:             100_000,
		view:             map[string]peer.Peer{},
		lazyPushPeers:    map[string]peer.Peer{},
		eagerPushPeers:   map[string]peer.Peer{},
		missingMessages:  map[uint32]map[string]messageSource{},
		mids:             []uint32{},
		receivedMessages: map[uint32]message.Message{},
		ongoingTimers:    make(map[uint32]int),
		lazyQueue:        []addressedMsg{},
		eagerQueue:       map[string]message.Message{},
		useUDP:           useUDP,
	}
}
