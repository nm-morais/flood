package application

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

//Timers follow x2xx

const BroadcastTimerType = 31201

type BroadcastTimer struct {
	duration time.Duration
}

func (b BroadcastTimer) ID() timer.ID {
	return BroadcastTimerType
}

func (b BroadcastTimer) Duration() time.Duration {
	return b.duration
}

const StartTimerType = 3202

type StartTimer struct {
	duration time.Duration
}

func (b StartTimer) ID() timer.ID {
	return StartTimerType
}

func (b StartTimer) Duration() time.Duration {
	return b.duration
}

const StopTimerType = 3203

type StopTimer struct {
	duration time.Duration
}

func (b StopTimer) ID() timer.ID {
	return StopTimerType
}

func (b StopTimer) Duration() time.Duration {
	return b.duration
}

const ExitTimerType = 3204

type ExitTimer struct {
	duration time.Duration
}

func (b ExitTimer) ID() timer.ID {
	return ExitTimerType
}

func (b ExitTimer) Duration() time.Duration {
	return b.duration
}
