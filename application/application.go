package application

import (
	"encoding/csv"
	"flood/shared"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const (
	Name    = "Application"
	protoID = 31000
)

type Application struct {
	isPlumtreeRoot   bool
	writer           *csv.Writer
	serviceProtoID   protocol.ID
	babel            protocolManager.ProtocolManager
	logger           *logrus.Logger
	runTimer         int
	payloadSize      int
	nrMessagesToSend int
	nrSent           int
	prepareTime      time.Duration
	runTime          time.Duration
}

func (a *Application) ID() protocol.ID {
	return protoID
}

func (a *Application) Name() string {
	return "Application"
}

func (a *Application) Logger() *logrus.Logger {
	return a.logger
}

func (a *Application) Start() {
	a.logger.Info("Waiting...")
	if a.isPlumtreeRoot {
		a.logger.Infof("Starting as plumtree root, prepareTime: %+v", a.prepareTime)
	} else {
		a.logger.Infof("prepareTime: %+v", a.prepareTime)
	}
	a.babel.RegisterTimer(protoID, StartTimer{duration: a.prepareTime})
	a.logger.Infof("Using payloadsize=%v, sending %d messages over %+v", a.payloadSize, a.nrMessagesToSend, a.runTime)
}

func (a *Application) Init() {
	a.babel.RegisterNotificationHandler(protoID, shared.DeliverMessageNotification{}, a.uponDeliverMessageNotification)
	a.babel.RegisterTimerHandler(protoID, BroadcastTimerType, a.uponBroadcastTimer)
	a.babel.RegisterTimerHandler(protoID, StartTimerType, a.uponStartTimer)
}

func (a *Application) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	panic("this shouldn't run!")
}

func (a *Application) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	panic("this shouldn't run!")
}

func (a *Application) DialFailed(peer.Peer) {
	panic("this shouldn't run!")
}

func (a *Application) OutConnDown(peer peer.Peer) {
	panic("this shouldn't run!")
}

func (a *Application) MessageDelivered(message message.Message, peer peer.Peer) {
	panic("this shouldn't run!")
}

func (a *Application) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
	panic("this shouldn't run!")
}

/*---------------------- UPON NOTIFICATIONS ---------------------------------*/
func (a *Application) uponDeliverMessageNotification(notification notification.Notification) {
	n := notification.(shared.DeliverMessageNotification)
	// a.logger.Info("got broadcast message from " + n.From.String())
	writeOrPanic(a.writer, []string{
		a.babel.SelfPeer().IP().String(),
		fmt.Sprintf("%d", time.Now().UnixNano()),
		fmt.Sprintf("%d", time.Since(time.Unix(0, int64(n.Message.TimeSent))).Milliseconds()),
		fmt.Sprintf("%d", n.Message.Hop),
		fmt.Sprintf("%d", n.Message.MID),
	})
}

/*---------------------- UPON TIMERS ---------------------------------*/

func (a *Application) uponStartTimer(timer timer.Timer) {
	a.logger.Info("Starting...")
	duration := a.runTime / time.Duration(a.nrMessagesToSend)
	a.runTimer = a.babel.RegisterPeriodicTimer(protoID, BroadcastTimer{duration: time.Duration(duration)}, true)
}

func (a *Application) uponBroadcastTimer(t timer.Timer) {
	if a.nrSent < a.nrMessagesToSend {
		toSend := randomBytes(a.payloadSize)
		request := shared.BroadcastRequest{Content: toSend}
		a.logger.Info("Sending new request")
		a.babel.SendRequest(request, protoID, a.serviceProtoID)
		a.nrSent++
	} else {
		a.babel.CancelTimer(a.runTimer)
	}
}

/*-------------------------------------------------------------------*/

func NewApplicationProtocol(babel protocolManager.ProtocolManager, isPlumtreeRoot bool, serviceProtoID protocol.ID, csvFolder, csvFile string, nrMessagesToSend int, payloadSize int, scenarioDuration time.Duration) protocol.Protocol {
	logger := logs.NewLogger(Name)

	csvHeaders := []string{"ip", "timestamp", "time", "hopNr", "mid"}
	prepareTime := time.Duration(float32(scenarioDuration) * 0.40)
	runTime := time.Duration(float32(scenarioDuration-prepareTime) * 0.2)
	if isPlumtreeRoot {
		prepareTime -= time.Duration(float32(scenarioDuration) * 0.1)
	}

	return &Application{
		isPlumtreeRoot:   isPlumtreeRoot,
		writer:           setupCSVWriter(csvFolder, csvFile, csvHeaders),
		serviceProtoID:   serviceProtoID,
		babel:            babel,
		logger:           logger,
		runTimer:         0,
		payloadSize:      payloadSize,
		nrMessagesToSend: nrMessagesToSend,
		nrSent:           0,
		prepareTime:      prepareTime,
		runTime:          runTime,
	}
}

func setupCSVWriter(folder, fileName string, headers []string) *csv.Writer {
	err := os.MkdirAll(folder, 0777)
	if err != nil {
		panic(err)
	}
	allLogsFile, err := os.Create(folder + fileName)
	if err != nil {
		panic(err)
	}

	writer := csv.NewWriter(allLogsFile)
	writeOrPanic(writer, headers)
	return writer
}

func writeOrPanic(csvWriter *csv.Writer, records []string) {
	if err := csvWriter.Write(records); err != nil {
		panic(err)
	}
	csvWriter.Flush()
}

func randomBytes(l int) []byte {
	buf := make([]byte, l)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return buf
}
