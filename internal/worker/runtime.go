package worker

import (
	"context"
	"os"
	"sync/atomic"

	"github.com/jaideep329/talk-go/disha"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

type TaskLaunchRequest struct {
	ConversationID string `json:"conversation_id"`
	BotType        string `json:"bot_type"`
	RoomURL        string `json:"room_url"`
	RoomName       string `json:"room_name"`
	Token          string `json:"token"`
	BotToken       string `json:"bot_token"`
}

type TaskStarter func(ctx context.Context, req TaskLaunchRequest, onCleanup func(*voicepipelinecore.PipelineTask)) (*voicepipelinecore.PipelineTask, error)

type Runtime struct {
	deps    disha.Deps
	starter TaskStarter
	state   workerState

	exitProcess func(int)

	shutdownInitiated        atomic.Bool
	gracefulShutdownComplete atomic.Bool
	abruptShutdownReported   atomic.Bool
}

func NewRuntime(deps disha.Deps, starter TaskStarter) *Runtime {
	return &Runtime{
		deps:        deps,
		starter:     starter,
		exitProcess: os.Exit,
	}
}
