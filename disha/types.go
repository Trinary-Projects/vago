package disha

import "time"

// ConversationData mirrors the Redis payload Disha stores at
// conversation_data:{conversation_id}.
type ConversationData struct {
	Conversation           ConversationRow `json:"conversation"`
	Chunks                 [][]any         `json:"chunks"`
	UserProfile            UserProfileData `json:"user_profile"`
	UnprocessedChatContext *string         `json:"unprocessed_chat_context,omitempty"`
	ResumedChunk           map[string]any  `json:"resumed_chunk,omitempty"`
}

type ConversationRow struct {
	ID                    string         `json:"id"`
	UserID                string         `json:"user_id"`
	BotType               string         `json:"bot_type"`
	PatientInfo           string         `json:"patient_info"`
	Direction             string         `json:"direction"`
	Agenda                string         `json:"agenda"`
	DynamicVariables      map[string]any `json:"dynamic_variables"`
	CallFlowKey           string         `json:"call_flow_key"`
	CompiledCallFlowS3Key string         `json:"compiled_call_flow_s3_key"`
	ResumedFromChunkID    *string        `json:"resumed_from_chunk_id"`
	ResumeGracefully      *bool          `json:"resume_gracefully"`
	StartChunkID          *string        `json:"start_chunk_id"`
	EndChunkID            *string        `json:"end_chunk_id"`
}

type UserProfileData struct {
	UserID                            string         `json:"user_id"`
	Phone                             string         `json:"phone"`
	RemainingSalesCallTalktimeSeconds *float64       `json:"remaining_sales_call_talktime_seconds"`
	CampaignPricingExperimentFlag     *string        `json:"campaign_pricing_experiment_flag"`
	ShortTermMemory                   *string        `json:"short_term_memory"`
	LastDietChartXML                  string         `json:"last_diet_chart_xml"`
	IdealCallTimeSlots                map[string]any `json:"ideal_call_time_slots"`
	DevanagariName                    string         `json:"devanagari_name"`
	FirstName                         string         `json:"first_name"`
	Name                              string         `json:"name"`
	Gender                            string         `json:"gender"`
}

// ConversationChunk mirrors the dict Disha's chunk manager stores in
// Redis before the Redis-to-Postgres sync job persists it. The latency
// JSON keys keep Disha's legacy `_ms` names, but assistant-turn values are
// persisted in seconds.
type ConversationChunk struct {
	ID                               string   `json:"id"`
	Text                             string   `json:"text"`
	Role                             string   `json:"role"`
	BotType                          string   `json:"bot_type"`
	ConversationID                   string   `json:"conversation_id"`
	UserID                           string   `json:"user_id"`
	CurrentAgenda                    *string  `json:"current_agenda"`
	STTTTFBMs                        *float64 `json:"stt_ttfb_ms"`
	LLMTTFBMs                        *float64 `json:"llm_ttfb_ms"`
	TTSTTFBMs                        *float64 `json:"tts_ttfb_ms"`
	V2VLatencyMs                     *float64 `json:"v2v_latency_ms"`
	TextAggregationMs                *float64 `json:"text_aggregation_ms"`
	Created                          string   `json:"created"`
	IsDebugLog                       bool     `json:"is_debug_log"`
	AdditionalData                   any      `json:"additional_data"`
	MainAgentSystemPromptLangfuseKey *string  `json:"main_agent_system_prompt_langfuse_key"`
	SystemMessageParamsS3Key         *string  `json:"system_message_params_s3_key"`
	ConversationStateS3Key           *string  `json:"conversation_state_s3_key"`
}

type UpdateConversationRequest struct {
	ConversationID    string     `json:"conversation_id"`
	BotJoinedAt       *time.Time `json:"bot_joined_at,omitempty"`
	UserJoinedAt      *time.Time `json:"user_joined_at,omitempty"`
	UserFirstSpeechAt *time.Time `json:"user_first_speech_at,omitempty"`
	BotFirstSpeechAt  *time.Time `json:"bot_first_speech_at,omitempty"`
}

type PostCallOperationsRequest struct {
	ConversationID                 string         `json:"conversation_id"`
	EndReason                      *string        `json:"end_reason"`
	TotalUserDuration              int            `json:"total_user_duration"`
	FirstUserAudioFramesReceivedAt *time.Time     `json:"first_user_audio_frames_received_at"`
	EndedAt                        time.Time      `json:"ended_at"`
	LogDataS3Key                   string         `json:"log_data_s3_key"`
	OnboardingCallDone             bool           `json:"onboarding_call_done"`
	DietPlanIntensityLevel         *string        `json:"diet_plan_intensity_level"`
	FitnessPlanIntensityLevel      *string        `json:"fitness_plan_intensity_level"`
	LatestOnboardingCallStage      *string        `json:"latest_onboarding_call_stage"`
	ConversationVariables          map[string]any `json:"conversation_variables"`
}

type EnqueueJobRequest struct {
	ModuleName     string         `json:"module_name"`
	FuncName       string         `json:"func_name"`
	Kwargs         map[string]any `json:"kwargs"`
	SQSQueue       string         `json:"sqs_queue"`
	MessageGroupID string         `json:"message_group_id,omitempty"`
}
