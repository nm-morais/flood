package shared

import (
	"github.com/nm-morais/go-babel/pkg/request"
)

const BroadcastRequestType = 20601

type BroadcastRequest struct {
	Content []byte
}

func (b BroadcastRequest) ID() request.ID {
	return BroadcastRequestType
}
