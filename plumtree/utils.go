package plumtree

import (
	"flood/shared"

	"github.com/nm-morais/go-babel/pkg/peer"
)

func (f *Plumtree) eagerPush(msg shared.GossipMessage, round uint32, sender peer.Peer) {
	msg.Hop = round
	for _, p := range f.eagerPushPeers {
		if peer.PeersEqual(sender, p) {
			continue
		}
		f.sendMessage(msg, p)
	}
}

func (f *Plumtree) lazyPush(msg shared.GossipMessage, round uint32, sender peer.Peer) {
	msg.Hop = round
	toSend := shared.IHaveMessage{MID: msg.MID, Round: round}
	for _, p := range f.lazyPushPeers {
		if peer.PeersEqual(sender, p) {
			continue
		}
		// f.babel.RegisterTimer(f.ID(), SendIHaveTimer{
		// 	duration: 1 * time.Second,
		// 	msg: shared.IHaveMessage{
		// 		MID:   msg.MID,
		// 		Round: round,
		// 	},
		// 	dst: p,
		// })
		// f.lazyQueue = append(f.lazyQueue, addressedMsg{
		// 	d:  p,
		// 	m:  ,
		// 	ts: time.Now(),
		// })
		f.sendMessage(toSend, p)
	}
	// for _, msg := range f.lazyQueue {
	// 	f.babel.SendMessage(msg.m, msg.d, f.ID(), f.ID(), false)
	// }
	// f.lazyQueue = nil
}

func (f *Plumtree) removeFromEager(p peer.Peer) {
	// if _, ok := f.eagerPushPeers[p.String()]; ok {
	delete(f.eagerPushPeers, p.String())
	// f.logger.Infof("Deleted %s from eagerPushPeers; %+v", p.String(), f.eagerPushPeers)
	// }
}

func (f *Plumtree) removeFromLazy(p peer.Peer) {
	// if _, ok := f.lazyPushPeers[p.String()]; ok {
	delete(f.lazyPushPeers, p.String())
	// f.logger.Infof("Deleted %s from lazyPushPeers; %+v", p.String(), f.lazyPushPeers)
	// }
}

func (f *Plumtree) addToLazy(p peer.Peer) {
	if _, ok := f.view[p.String()]; !ok {
		return
	}
	if _, ok := f.lazyPushPeers[p.String()]; !ok {
		f.lazyPushPeers[p.String()] = p
		// f.logger.Infof("Added %s to lazyPushPeers; %+v", p.String(), f.lazyPushPeers)
	}
}

func (f *Plumtree) addToEager(p peer.Peer) {
	if _, ok := f.view[p.String()]; !ok {
		return
	}
	if _, ok := f.eagerPushPeers[p.String()]; !ok {
		f.eagerPushPeers[p.String()] = p
		// f.logger.Infof("Added %s to eagerPushPeers; %+v", p.String(), f.eagerPushPeers)
	}
}
