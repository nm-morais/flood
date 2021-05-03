package shared

import (
	"bytes"

	"github.com/nm-morais/go-babel/pkg/message"
)

//Messages types follow x1xx

const GossipMessageType = 21101

type GossipMessage struct {
	Content  []byte
	TimeSent uint64
	MID      uint32
	Hop      uint32
}
type gossipMessageSerializer struct{}

var defaultGossipMessageSerializer = gossipMessageSerializer{}

func (GossipMessage) Type() message.ID               { return GossipMessageType }
func (GossipMessage) Serializer() message.Serializer { return defaultGossipMessageSerializer }
func (GossipMessage) Deserializer() message.Deserializer {
	return defaultGossipMessageSerializer
}
func (gossipMessageSerializer) Serialize(msg message.Message) []byte {
	converted := msg.(GossipMessage)

	b := new(bytes.Buffer)
	err := EncodeNumberToBuffer(converted.MID, b)
	if err != nil {
		panic("Oh, oh! Error serializing mid.")
	}

	err = EncodeNumberToBuffer(converted.Hop, b)
	if err != nil {
		panic("Oh, oh! Error serializing mid.")
	}

	err = EncodeNumberToBuffer(converted.TimeSent, b)
	if err != nil {
		panic("Oh, oh! Error serializing mid.")
	}

	return append(b.Bytes(), converted.Content...)
}

func (gossipMessageSerializer) Deserialize(msgBytes []byte) message.Message {

	buff := bytes.NewBuffer(msgBytes)

	var m uint32
	err := DecodeNumberFromBuffer(&m, buff)
	if err != nil {
		panic("Oh, oh! Error deserializing int.")
	}

	var hop uint32
	err = DecodeNumberFromBuffer(&hop, buff)
	if err != nil {
		panic("Oh, oh! Error deserializing int.")
	}

	var timestamp uint64
	err = DecodeNumberFromBuffer(&timestamp, buff)
	if err != nil {
		panic("Oh, oh! Error deserializing int.")
	}

	return GossipMessage{
		TimeSent: timestamp,
		Content:  buff.Bytes(),
		MID:      m,
		Hop:      hop,
	}
}

const IHaveMessageType message.ID = 21022

type IHaveMessage struct {
	MID   uint32
	Round uint32
}
type ihaveMessageSerializer struct{}

var defaultIHaveMessageSerializer = ihaveMessageSerializer{}

func (IHaveMessage) Type() message.ID               { return IHaveMessageType }
func (IHaveMessage) Serializer() message.Serializer { return defaultIHaveMessageSerializer }
func (IHaveMessage) Deserializer() message.Deserializer {
	return defaultIHaveMessageSerializer
}
func (ihaveMessageSerializer) Serialize(msg message.Message) []byte {
	converted := msg.(IHaveMessage)

	b := new(bytes.Buffer)
	err := EncodeNumberToBuffer(converted.MID, b)
	if err != nil {
		panic("Oh, oh! Error serializing mid.")
	}

	err = EncodeNumberToBuffer(converted.Round, b)
	if err != nil {
		panic("Oh, oh! Error serializing int.")
	}

	return b.Bytes()
}

func (ihaveMessageSerializer) Deserialize(msgBytes []byte) message.Message {

	buff := bytes.NewBuffer(msgBytes)

	var m uint32
	err := DecodeNumberFromBuffer(&m, buff)
	if err != nil {
		panic("Oh, oh! Error deserializing int.")
	}

	var round uint32
	err = DecodeNumberFromBuffer(&round, buff)
	if err != nil {
		panic("Oh, oh! Error deserializing int.")
	}

	return IHaveMessage{
		MID:   m,
		Round: round,
	}
}

const GraftMessageType message.ID = 21023

type GraftMessage struct {
	MID   uint32
	Round uint32
}
type graftMessageSerializer struct{}

var defaultGraftMessageSerializer = graftMessageSerializer{}

func (GraftMessage) Type() message.ID               { return GraftMessageType }
func (GraftMessage) Serializer() message.Serializer { return defaultGraftMessageSerializer }
func (GraftMessage) Deserializer() message.Deserializer {
	return defaultGraftMessageSerializer
}
func (graftMessageSerializer) Serialize(msg message.Message) []byte {
	converted := msg.(GraftMessage)

	b := new(bytes.Buffer)
	err := EncodeNumberToBuffer(converted.MID, b)
	if err != nil {
		panic("Oh, oh! Error serializing mid.")
	}

	err = EncodeNumberToBuffer(converted.Round, b)
	if err != nil {
		panic("Oh, oh! Error serializing int.")
	}

	return b.Bytes()
}

func (graftMessageSerializer) Deserialize(msgBytes []byte) message.Message {

	buff := bytes.NewBuffer(msgBytes)

	var mID uint32
	err := DecodeNumberFromBuffer(&mID, buff)
	if err != nil {
		panic("Oh, oh! Error deserializing int.")
	}

	var round uint32
	err = DecodeNumberFromBuffer(&round, buff)
	if err != nil {
		panic("Oh, oh! Error deserializing int.")
	}

	return GraftMessage{
		MID:   mID,
		Round: round,
	}
}
