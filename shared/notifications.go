package shared

import (
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
)

//Messages notifs follow xx5xx

const DeliverMessageNotificationType = 20501

type DeliverMessageNotification struct {
	From    peer.Peer
	Message GossipMessage
}

func (n DeliverMessageNotification) ID() notification.ID {
	return DeliverMessageNotificationType
}

const NeighborUpNotificationType = 10501

type NeighborUpNotification struct {
	PeerUp peer.Peer
	View   map[string]peer.Peer
}

func (n NeighborUpNotification) ID() notification.ID {
	return NeighborUpNotificationType
}

const NeighborDownNotificationType = 10502

type NeighborDownNotification struct {
	PeerDown peer.Peer
	View     map[string]peer.Peer
}

func (n NeighborDownNotification) ID() notification.ID {
	return NeighborDownNotificationType
}
