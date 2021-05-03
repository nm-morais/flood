package flood

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

//Timers follow x2xx

const BroadcastTimerType = 41201

type BroadcastTimer struct {
	duration time.Duration
}

func (b BroadcastTimer) ID() timer.ID {
	return BroadcastTimerType
}

func (b BroadcastTimer) Duration() time.Duration {
	return b.duration
}

const StartTimerType = 4202

type StartTimer struct {
	duration time.Duration
}

func (b StartTimer) ID() timer.ID {
	return StartTimerType
}

func (b StartTimer) Duration() time.Duration {
	return b.duration
}

const StopTimerType = 4203

type StopTimer struct {
	duration time.Duration
}

func (b StopTimer) ID() timer.ID {
	return StopTimerType
}

func (b StopTimer) Duration() time.Duration {
	return b.duration
}

const ExitTimerType = 4204

type ExitTimer struct {
	duration time.Duration
}

func (b ExitTimer) ID() timer.ID {
	return ExitTimerType
}

func (b ExitTimer) Duration() time.Duration {
	return b.duration
}

const IHaveTimeoutTimerType = 41204

type IHaveTimeoutTimer struct {
	duration time.Duration
	MID      uint32
}

func (b IHaveTimeoutTimer) ID() timer.ID {
	return IHaveTimeoutTimerType
}

func (b IHaveTimeoutTimer) Duration() time.Duration {
	return b.duration
}

const SendIHaveTimerType = 41205

type SendIHaveTimer struct {
	duration time.Duration
}

func (b SendIHaveTimer) ID() timer.ID {
	return SendIHaveTimerType
}

func (b SendIHaveTimer) Duration() time.Duration {
	return b.duration
}
