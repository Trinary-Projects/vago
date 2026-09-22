package disha

import (
	"context"
	"fmt"
	"log"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

type Deps struct {
	Logger       *log.Logger
	Redis        RedisClient
	API          *APIClient
	Documents    *DocumentStore
	PhoneticDict *PhoneticDict
	S3           S3GetClient
	GKEPatcher   GKEPatcher
}

type GKEPatcher interface {
	SetSafeToEvict(ctx context.Context, podName string, safe bool) error
}

// BotTaskRequest carries everything a bot needs to assemble and join a
// call: which conversation to load and which media room to join.
type BotTaskRequest struct {
	ConversationID string
	RoomURL        string
	RoomName       string
	RoomToken      string
}

type Bot interface {
	BotType() string
	// BuildTask loads the conversation, constructs the frame processors,
	// assembles the pipeline, and joins the media room — returning a task
	// that is ready to Start.
	BuildTask(ctx context.Context, req BotTaskRequest, deps Deps) (*voicepipelinecore.PipelineTask, error)
}

func NewBot(botType string) (Bot, error) {
	switch botType {
	case SalesCallBotType:
		return SalesCallBot{}, nil
	case FollowUpBotType:
		return FollowUpBot{}, nil
	case OnboardingCallBotType:
		return OnboardingCallBot{}, nil
	default:
		return nil, fmt.Errorf("disha: unsupported bot_type %q", botType)
	}
}

func NewBotTask(ctx context.Context, bot Bot, req BotTaskRequest, deps Deps) (*voicepipelinecore.PipelineTask, error) {
	if bot == nil {
		return nil, fmt.Errorf("disha: bot is required")
	}
	return bot.BuildTask(ctx, req, deps)
}
