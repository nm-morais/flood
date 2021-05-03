package flood

import (
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
	d peer.Peer
	m message.Message
}

//Messages types follow x1xx
const (
	SendIHaveDelay = 1 * time.Second
	IHaveTimeout   = 1 * time.Second
	FloodProtoID   = 21000
	name           = "Flood"
)

type Flood struct {
	mids             []uint32
	size             int
	r                *rand.Rand
	babel            protocolManager.ProtocolManager
	logger           *logrus.Logger
	neighbors        map[string]peer.Peer       //of peers
	receivedMessages map[uint32]message.Message //of ints
	demmonView       *demmonProto.InView
	useUDP           bool
	isDemmon         bool
	missingMessages  map[uint32]map[string]messageSource
	ongoingTimers    map[uint32]int
	lazyQueue        []addressedMsg
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
	t := SendIHaveTimer{
		duration: 1 * time.Second,
	}
	f.babel.RegisterPeriodicTimer(f.ID(), t, false)
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

func (f *Flood) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
	f.logger.Errorf("Couldn't message of type %s to %s ", reflect.TypeOf(message), peer.String())
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
		d: p,
		m: shared.IHaveMessage{
			MID:   m.MID,
			Round: m.Round,
		},
	})
}

func (f *Flood) demmonFlood(m shared.GossipMessage, sender peer.Peer) {
	if f.demmonView == nil {
		return
	}

	if peer.PeersEqual(sender, f.babel.SelfPeer()) {
		for _, p := range f.demmonView.Children {
			f.broadcastTo(m, p)
		}

		for _, p := range f.demmonView.Siblings {
			f.broadcastTo(m, p)

		}
		if f.demmonView.Parent != nil {
			f.broadcastTo(m, f.demmonView.Parent)
		}
		return
	}

	iHaveMsg := shared.IHaveMessage{
		MID:   m.MID,
		Round: m.Hop,
	}
	sibling, child, parent := f.getPeerRelationshipType(sender)
	if parent || sibling {
		for _, p := range f.demmonView.Siblings {
			if peer.PeersEqual(p, sender) {
				continue
			}
			f.lazyPush(iHaveMsg, p)
		}
		if f.demmonView.Parent != nil {
			if !peer.PeersEqual(f.demmonView.Parent, sender) {
				f.lazyPush(iHaveMsg, f.demmonView.Parent)
			}
		}
		for _, p := range f.demmonView.Children {
			f.broadcastTo(m, p)
		}
	}

	if child {
		for _, p := range f.demmonView.Siblings {
			f.broadcastTo(m, p)
		}
		if f.demmonView.Parent != nil {
			f.broadcastTo(m, f.demmonView.Parent)
		}
		for _, v := range f.demmonView.Children {
			if peer.PeersEqual(v, sender) {
				continue
			}
			f.lazyPush(iHaveMsg, v)
		}
	}
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

	if _, ok := f.receivedMessages[gossipMsg.MID]; ok {
		return
	}
	delete(f.missingMessages, gossipMsg.MID)
	if _, ok := f.ongoingTimers[gossipMsg.MID]; ok {
		f.babel.CancelTimer(f.ongoingTimers[gossipMsg.MID])
		delete(f.ongoingTimers, gossipMsg.MID)
	}
	f.babel.SendNotification(shared.DeliverMessageNotification{
		Message: gossipMsg,
		From:    sender,
	})
	f.receivedMessages[gossipMsg.MID] = gossipMsg
	f.mids = append(f.mids, gossipMsg.MID)
	if len(f.mids) > f.size {
		toPop := f.mids[0]
		f.mids = f.mids[1:]
		delete(f.receivedMessages, toPop)
	}
	gossipMsg.Hop += 1
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
		if _, ok := f.ongoingTimers[iHaveMsg.MID]; !ok {
			tID := f.babel.RegisterTimer(f.ID(), IHaveTimeoutTimer{
				duration: IHaveTimeout,
				MID:      iHaveMsg.MID,
			})
			f.ongoingTimers[iHaveMsg.MID] = tID
		}
		if _, ok := f.missingMessages[iHaveMsg.MID]; !ok {
			f.missingMessages[iHaveMsg.MID] = make(map[string]messageSource)
		}
		// f.logger.Infof("Set up IHaveTimeoutTimer for message %d", iHaveMsg.MID)
		f.missingMessages[iHaveMsg.MID][sender.String()] = messageSource{
			p: sender,
			r: iHaveMsg.Round,
		}
	}
}

func (f *Flood) uponIHaveTimeout(t timer.Timer) {
	iHaveTimeoutTimer := t.(IHaveTimeoutTimer)
	if _, ok := f.receivedMessages[iHaveTimeoutTimer.MID]; !ok {
		// f.logger.Infof("IHaveTimeoutTimer trigger for missing message %d", iHaveTimeoutTimer.MID)
		messageSources, ok := f.missingMessages[iHaveTimeoutTimer.MID]
		if !ok {
			f.logger.Errorf("Source missing %d", iHaveTimeoutTimer.MID)
			return
		}

		if len(messageSources) == 0 {
			// f.logger.Error("Message source is empty")
			return
		}
		newTimerID := f.babel.RegisterTimer(f.ID(), iHaveTimeoutTimer)
		f.ongoingTimers[iHaveTimeoutTimer.MID] = newTimerID

		for k, messageSource := range messageSources {
			delete(messageSources, k)
			if sibling, child, parent := f.getPeerRelationshipType(messageSource.p); !sibling && !child && !parent {
				continue
			}
			// f.logger.Infof("Sending GraftMessage for mid %d to %s", iHaveTimeoutTimer.MID, messageSource.p)
			f.babel.SendMessage(shared.GraftMessage{
				MID:   iHaveTimeoutTimer.MID,
				Round: messageSource.r,
			}, messageSource.p, f.ID(), f.ID(), true)
			break
		}
	}
}

func (f *Flood) uponSendIHaveTimer(t timer.Timer) {
	for _, v := range f.lazyQueue {
		// f.logger.Info("Sending IHave message  to " + v.d.String())
		isSibling, isChildren, isParent := f.getPeerRelationshipType(v.d)
		if !isSibling && !isChildren && !isParent {
			continue
		}
		if f.useUDP {
			f.babel.SendMessageSideStream(v.m, v.d, v.d.ToUDPAddr(), FloodProtoID, FloodProtoID)
		} else {
			f.babel.SendMessage(v.m, v.d, f.ID(), f.ID(), true)
		}
	}
	f.lazyQueue = nil
}

func (f *Flood) uponReceiveGraftMessage(sender peer.Peer, m message.Message) {
	f.logger.Infof("Received graft message from %s", sender)
	graftMsg := m.(shared.GraftMessage)
	toSend, ok := f.receivedMessages[graftMsg.MID]
	if !ok {
		// f.logger.Errorf("Do not have message %d in order to respond to graft message", graftMsg.MID)
		return
	}
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
	f.demmonView = &notif.InView
	f.logger.Info("PEER DOWN!! - " + notif.PeerDown.String())
	f.logger.Infof("View: %+v", notif.InView)
}

func (f *Flood) uponDemmonNodeUpNotification(n notification.Notification) {
	notif := n.(demmonProto.NodeUpNotification)
	f.demmonView = &notif.InView
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

func NewFloodProtocol(babel protocolManager.ProtocolManager, useC, isDemmon bool) protocol.Protocol {
	logger := logs.NewLogger(name)
	logger.SetLevel(logrus.InfoLevel)
	return &Flood{
		mids:             []uint32{},
		size:             50_000,
		r:                rand.New(rand.NewSource(time.Now().UnixNano())),
		babel:            babel,
		logger:           logger,
		neighbors:        make(map[string]peer.Peer),
		receivedMessages: make(map[uint32]message.Message),
		demmonView:       nil,
		useUDP:           useC,
		isDemmon:         isDemmon,
		missingMessages:  map[uint32]map[string]messageSource{},
		ongoingTimers:    make(map[uint32]int),
	}
}
