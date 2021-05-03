package plumtree

import (
	"github.com/nm-morais/go-babel/pkg/message"
)

const PruneMessageType message.ID = 21021

type PruneMessage struct {
}
type pruneMessageSerializer struct{}

var defaultPruneMessageSerializer = pruneMessageSerializer{}

func (PruneMessage) Type() message.ID               { return PruneMessageType }
func (PruneMessage) Serializer() message.Serializer { return defaultPruneMessageSerializer }
func (PruneMessage) Deserializer() message.Deserializer {
	return defaultPruneMessageSerializer
}
func (pruneMessageSerializer) Serialize(msg message.Message) []byte {
	return []byte{}
}

func (pruneMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	return PruneMessage{}
}
