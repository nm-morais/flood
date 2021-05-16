package flood

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"flood/shared"
	"math/rand"
	"reflect"
	"time"

	"github.com/jinzhu/copier"
	demmonProto "github.com/nm-morais/demmon/core/membership/protocol"
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

type messageSource = struct {
	p peer.Peer
	r uint32
}

type addressedMsg = struct {
	m  message.Message
	ts time.Time
}

//Messages types follow x1xx
const (
	IHaveTimeout = 1 * time.Second
	FloodProtoID = 21000
	name         = "Flood"
)

type Flood struct {
	isLandmark       bool
	mids             []uint32
	size             int
	r                *rand.Rand
	babel            protocolManager.ProtocolManager
	logger           *logrus.Logger
	neighbors        map[string]peer.Peer       //of peers
	receivedMessages map[uint32]message.Message //of ints
	demmonView       demmonProto.InView
	useUDP           bool
	isDemmon         bool
	missingMessages  map[uint32]*missingMessage
	lazyQueue        []addressedMsg
}

type missingMessage struct {
	ts      time.Time
	sources map[string]messageSource
}

func (f *Flood) ID() protocol.ID {
	return FloodProtoID
}

func (f *Flood) Name() string {
	return name
}

func (f *Flood) Logger() *logrus.Logger {
	return f.logger
}

func (f *Flood) Start() {
	f.babel.RegisterPeriodicTimer(f.ID(), SendIHaveTimer{
		duration: 1 * time.Second,
	}, false)
	f.babel.RegisterPeriodicTimer(f.ID(), IHaveTimeoutTimer{
		duration: IHaveTimeout / 2,
	}, false)
}

func (f *Flood) Init() {
	f.babel.RegisterMessageHandler(FloodProtoID, shared.GossipMessage{}, f.uponReceiveGossipMessage)
	f.babel.RegisterNotificationHandler(FloodProtoID, shared.NeighborDownNotification{}, f.uponNeighborDownNotification)
	f.babel.RegisterNotificationHandler(FloodProtoID, shared.NeighborUpNotification{}, f.uponNeighborUpNotification)
	f.babel.RegisterRequestHandler(FloodProtoID, shared.BroadcastRequestType, f.uponBroadcastRequest)
	f.babel.RegisterNotificationHandler(FloodProtoID, demmonProto.NodeUpNotification{}, f.uponDemmonNodeUpNotification)
	f.babel.RegisterNotificationHandler(FloodProtoID, demmonProto.NodeDownNotification{}, f.uponDemmonNodeDownNotification)

	f.babel.RegisterMessageHandler(f.ID(), shared.GraftMessage{}, f.uponReceiveGraftMessage)
	f.babel.RegisterMessageHandler(f.ID(), shared.IHaveMessage{}, f.uponReceiveIHaveMessage)

	f.babel.RegisterTimerHandler(f.ID(), IHaveTimeoutTimerType, f.uponIHaveTimeout)
	f.babel.RegisterTimerHandler(f.ID(), SendIHaveTimerType, f.uponSendIHaveTimer)

}

func (f *Flood) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return false //panic("this shouldn't run!")
}

func (f *Flood) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	return false //panic("this shouldn't run!")
}

func (f *Flood) DialFailed(peer peer.Peer) {
	//panic("this shouldn't run!")
}

func (f *Flood) OutConnDown(peer peer.Peer) {
	//panic("this shouldn't run!")
}

func (f *Flood) MessageDelivered(message message.Message, peer peer.Peer) {
	// do nothing really
}

func (f *Flood) MessageDeliveryErr(message message.Message, peer peer.Peer, err errors.Error) {
	f.logger.Errorf("Couldn't send message of type %s to %s due to: %s", reflect.TypeOf(message), peer.String(), err.ToString())
}

func (f *Flood) broadcastTo(m message.Message, p peer.Peer) {
	if f.useUDP {
		f.babel.SendMessageSideStream(m, p, p.ToUDPAddr(), FloodProtoID, FloodProtoID)
	} else {
		f.babel.SendMessage(m, p, f.ID(), f.ID(), true)
	}
}

func (f *Flood) lazyPush(m shared.IHaveMessage, p peer.Peer) {
	f.lazyQueue = append(f.lazyQueue, addressedMsg{
		m:  shared.IHaveMessage{MID: m.MID, Round: m.Round},
		ts: time.Now(),
	})
}

func (f *Flood) demmonFlood(m shared.GossipMessage, sender peer.Peer) {
	iHaveMsg := shared.IHaveMessage{
		MID:   m.MID,
		Round: m.Hop,
	}
	for _, p := range f.demmonView.Children {
		if peer.PeersEqual(p, sender) {
			continue
		}
		f.broadcastTo(m, p)
	}

	if f.demmonView.Parent != nil {
		if !peer.PeersEqual(f.demmonView.Parent, sender) {
			f.broadcastTo(m, f.demmonView.Parent)
		}
	}

	if f.isLandmark {
		isSibling, _, _ := f.getPeerRelationshipType(sender)
		if !isSibling {
			for _, p := range f.demmonView.Siblings {
				if peer.PeersEqual(p, sender) {
					continue
				}
				f.broadcastTo(m, p)
			}
		}
	}

	f.lazyPush(iHaveMsg, sender)
}

func (f *Flood) regularFlood(m message.Message, sender peer.Peer) {
	rand.Seed(time.Now().UnixNano())
	for _, p := range f.neighbors {
		if !peer.PeersEqual(sender, p) {
			f.broadcastTo(m, p)
			// f.logger.Info("Sending gossip message  to " + p.String())
		}
	}
}

/* -------------------- MESSAGE HANDLERS -----------------------------------*/

func (f *Flood) uponReceiveGossipMessage(sender peer.Peer, m message.Message) {
	// f.logger.Infof("Received gossip message from %s", sender)
	gossipMsg := m.(shared.GossipMessage)

	delete(f.missingMessages, gossipMsg.MID)
	if _, ok := f.receivedMessages[gossipMsg.MID]; ok {
		return
	}
	f.babel.SendNotification(shared.DeliverMessageNotification{
		Message: gossipMsg,
		From:    sender,
	})
	gossipMsg.Hop += 1
	f.receivedMessages[gossipMsg.MID] = gossipMsg
	f.mids = append(f.mids, gossipMsg.MID)
	if len(f.mids) > f.size {
		toPop := f.mids[0]
		f.mids = f.mids[1:]
		delete(f.receivedMessages, toPop)
	}
	if f.isDemmon {
		f.demmonFlood(gossipMsg, sender)
	} else {
		f.regularFlood(gossipMsg, sender)
	}
}

func (f *Flood) uponReceiveIHaveMessage(sender peer.Peer, m message.Message) {
	iHaveMsg := m.(shared.IHaveMessage)
	if _, ok := f.receivedMessages[iHaveMsg.MID]; !ok {
		// f.logger.Infof("Received IHave for MISSING message %d from %s", iHaveMsg.MID, sender)
		if _, ok := f.missingMessages[iHaveMsg.MID]; !ok {
			f.missingMessages[iHaveMsg.MID] = &missingMessage{
				ts: time.Now(),
				sources: make(map[string]struct {
					p peer.Peer
					r uint32
				}),
			}
		}
		// f.logger.Infof("Set up IHaveTimeoutTimer for message %d", iHaveMsg.MID)
		f.missingMessages[iHaveMsg.MID].sources[sender.String()] = messageSource{
			p: sender,
			r: iHaveMsg.Round,
		}
	}
}

func (f *Flood) uponIHaveTimeout(t timer.Timer) {
	for mid, missingMsg := range f.missingMessages {
		// f.logger.Infof("IHaveTimeoutTimer trigger for missing message %d", iHaveTimeoutTimer.MID)
		if time.Since(missingMsg.ts) < IHaveTimeout {
			continue
		}

		for k, messageSource := range missingMsg.sources {
			if sibling, child, parent := f.getPeerRelationshipType(messageSource.p); !sibling && !child && !parent {
				delete(missingMsg.sources, k)
				continue
			}
			// f.logger.Infof("Sending GraftMessage for mid %d to %s", mid, messageSource.p)
			f.babel.SendMessage(shared.GraftMessage{
				MID:   mid,
				Round: messageSource.r,
			}, messageSource.p, f.ID(), f.ID(), false)
			missingMsg.ts = time.Now()
			break
		}
		if len(missingMsg.sources) == 0 {
			delete(f.missingMessages, mid)
		}
	}
}

func (f *Flood) uponSendIHaveTimer(t timer.Timer) {
	tmp := []addressedMsg{}
	for _, v := range f.lazyQueue {
		if time.Since(v.ts) < 1*time.Second {
			tmp = append(tmp, v)
			continue
		}
		for _, p := range f.demmonView.Children {
			f.broadcastTo(v.m, p)
		}
		if f.demmonView.Parent != nil {
			f.broadcastTo(v.m, f.demmonView.Parent)
		}
		if f.isLandmark {
			for _, p := range f.demmonView.Siblings {
				f.broadcastTo(v.m, p)
			}
		}
	}
	f.lazyQueue = tmp
}

func (f *Flood) uponReceiveGraftMessage(sender peer.Peer, m message.Message) {
	graftMsg := m.(shared.GraftMessage)
	toSend, ok := f.receivedMessages[graftMsg.MID]
	if !ok {
		f.logger.Panicf("Do not have message %d in order to respond to graft message", graftMsg.MID)
		return
	}
	sibling, child, parent := f.getPeerRelationshipType(sender)
	if !sibling && !child && !parent {
		f.logger.Warnf("Received graft message from a peer not in view %s", sender)
		return
	}
	// f.logger.Infof("Received graft message from %s", sender)
	// f.logger.Infof("Replying to graft message %d from %s", graftMsg.MID, sender.String())
	f.babel.SendMessage(toSend, sender, f.ID(), f.ID(), true)
}

/* -------------------- REQUESTS HANDLERS -----------------------------------*/

func (f *Flood) uponBroadcastRequest(request request.Request) request.Reply {
	r := request.(shared.BroadcastRequest)
	msg := shared.GossipMessage{
		TimeSent: uint64(time.Now().UnixNano()),
		Hop:      0,
		MID:      f.r.Uint32(),
		Content:  r.Content,
	}
	f.uponReceiveGossipMessage(f.babel.SelfPeer(), msg)
	return nil
}

/* -------------------- NOTIFICATIONS HANDLERS -----------------------------------*/

func (f *Flood) uponDemmonNodeDownNotification(n notification.Notification) {
	notif := n.(demmonProto.NodeDownNotification)
	f.demmonView = notif.InView
	f.logger.Info("PEER DOWN!! - " + notif.PeerDown.String())
	f.logger.Infof("View: %+v", notif.InView)
}

func (f *Flood) uponDemmonNodeUpNotification(n notification.Notification) {
	notif := n.(demmonProto.NodeUpNotification)
	f.demmonView = notif.InView
	f.logger.Info("PEER UP!! - " + notif.PeerUp.String())
	f.logger.Infof("View: %+v", notif.InView)
}

/* -------------------- NOTIFICATIONS HANDLERS -----------------------------------*/

func (f *Flood) uponNeighborDownNotification(n notification.Notification) {
	notif := shared.NeighborDownNotification{}
	err := copier.Copy(&notif, n)
	if err != nil {
		f.logger.Panic(err)
	}
	f.neighbors = notif.View
	f.logger.Info("PEER DOWN!! - " + notif.PeerDown.String())
	f.logger.Infof("View: %+v", f.neighbors)
}

func (f *Flood) uponNeighborUpNotification(n notification.Notification) {
	notif := shared.NeighborUpNotification{}
	err := copier.Copy(&notif, n)
	if err != nil {
		f.logger.Panic(err)
	}
	f.neighbors = notif.View
	f.logger.Info("PEER UP!! - " + notif.PeerUp.String())
	f.logger.Infof("View: %+v", f.neighbors)
}

func (f *Flood) getPeerRelationshipType(p peer.Peer) (isSibling, isChildren, isParent bool) {
	for _, sibling := range f.demmonView.Siblings {
		if peer.PeersEqual(sibling, p) {
			return true, false, false
		}
	}

	for _, children := range f.demmonView.Children {
		if peer.PeersEqual(children, p) {
			return false, true, false
		}
	}

	if peer.PeersEqual(p, f.demmonView.Parent) {
		return false, false, true
	}
	return false, false, false
}

/* -------------------------------------------------------*/

func NewFloodProtocol(babel protocolManager.ProtocolManager, useC, isDemmon, isLandmark bool) protocol.Protocol {
	logger := logs.NewLogger(name)
	logger.SetLevel(logrus.InfoLevel)
	var b [8]byte
	_, err := crypto_rand.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	return &Flood{
		mids:             []uint32{},
		size:             100_000,
		r:                rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(b[:])))),
		babel:            babel,
		logger:           logger,
		neighbors:        make(map[string]peer.Peer),
		receivedMessages: make(map[uint32]message.Message),
		demmonView:       demmonProto.InView{},
		useUDP:           useC,
		isDemmon:         isDemmon,
		missingMessages:  make(map[uint32]*missingMessage),
		isLandmark:       isLandmark,
	}
}
