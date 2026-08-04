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
	PatientExecutiveProfile           *string        `json:"patient_executive_profile"`
	OnboardingCallVariant             *string        `json:"onboarding_call_variant"`
	ActiveChatContext                 *string        `json:"active_chat_context"`
	Recent1HrTranscript               *string        `json:"recent_1hr_transcript"`
	LastDietChartXML                  string         `json:"last_diet_chart_xml"`
	DietChartAvailable                bool           `json:"diet_chart_available"`
	DietPlanToday                     *string        `json:"diet_plan_today"`
	MembershipStatus                  *string        `json:"membership_status"`
	MembershipExpiryDate              *string        `json:"membership_expiry_date"`
	SubscriptionStatus                *string        `json:"subscription_status"`
	SubscriptionAmount                *float64       `json:"subscription_amount"`
	NextPaymentDueDate                *string        `json:"next_payment_due_date"`
	PaymentOverdue                    *bool          `json:"payment_overdue"`
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

	// ChunkRetrievalMetrics carries per-turn retrieval telemetry on follow-up
	// calls, destined for disha-backend's
	// ChunkRetrievalMetrics(chunk_id) table. omitempty so every other bot's
	// chunk JSON is byte-identical to before. Safe to write ahead of the
	// backend change: redis_dict_to_model reads named keys only, so this is
	// ignored until the sync job asks for it.
	ChunkRetrievalMetrics *ChunkRetrievalMetrics `json:"chunk_retrieval_metrics,omitempty"`
}

// ChunkRetrievalMetrics is the per-chunk umbrella for every retrieval-shaped
// step on a turn, matching the one-row-per-chunk backend table.
//
// Each step gets its own namespaced sub-object rather than flat fields, so the
// planned post-LLM guardrail step (vector lookup, sometimes an LLM call, then
// interrupt-and-regenerate) can be added as a sibling `Guardrail
// *GuardrailCheckMetrics `json:"guardrail,omitempty"“ without renaming or
// re-keying anything that already ships.
type ChunkRetrievalMetrics struct {
	Protocol *ProtocolRetrievalMetrics `json:"protocol,omitempty"`
}

// ProtocolRetrievalMetrics summarizes one protocol-retrieval round. The full
// candidate list (including sub-threshold scores) lives in the S3 object at
// ProtocolsS3Key; this is the compact form for the backend table.
type ProtocolRetrievalMetrics struct {
	RetrievalLatencyMs   float64  `json:"retrieval_latency_ms"`
	VectorQueryLatencyMs float64  `json:"vector_query_latency_ms"`
	TopSimilarityScore   *float64 `json:"top_similarity_score"`
	InjectedCount        int      `json:"injected_count"`
	// QueryText is the exact text embedded for this round. Carried inline
	// rather than left in the S3 record so disha-backend can persist it and
	// seed ProtocolLiveQueryAnchor from the chunk alone — reading it from S3
	// would mean one GET per Disha turn inside the chunk-sync job. It is a few
	// hundred bytes, and it is user speech that already lives on the chunk's
	// own text field, so it adds no new exposure.
	QueryText      string `json:"query_text,omitempty"`
	ProtocolsS3Key string `json:"protocols_s3_key"`
	Status         string `json:"status"` // ok | skipped | error | timeout
	Error          string `json:"error,omitempty"`
}

type UpdateConversationRequest struct {
	ConversationID    string     `json:"conversation_id"`
	BotJoinedAt       *time.Time `json:"bot_joined_at,omitempty"`
	UserJoinedAt      *time.Time `json:"user_joined_at,omitempty"`
	UserFirstSpeechAt *time.Time `json:"user_first_speech_at,omitempty"`
	BotFirstSpeechAt  *time.Time `json:"bot_first_speech_at,omitempty"`
}

// PostCallOperationsRequest mirrors VoiceBotAPIService.run_post_call_
// operations. The onboarding-only fields (DietPlanIntensityLevel,
// FitnessPlanIntensityLevel, LatestOnboardingCallStage,
// ConversationVariables, OnboardingCallDone) are populated only when
// CallEventCallbacks has a post-call decorator wired (see
// CallEventCallbacks.SetPostCallDecorator; onboarding is the only current
// user); sales/follow-up never wire one, so those fields stay at their
// zero value and serialize as explicit JSON null/false, matching today's
// wire shape exactly (no omitempty on this struct).
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

// SetUserCareplanRequest mirrors VoiceBotAPIService.set_user_careplan's
// payload for POST /bot/set_user_careplan.
type SetUserCareplanRequest struct {
	UserID             string  `json:"user_id"`
	OnboardingCarePlan string  `json:"onboarding_care_plan"`
	DetectedCarePlan   *string `json:"detected_care_plan"`
}

// AddTagToUserRequest mirrors VoiceBotAPIService.add_tag_to_user's
// payload for POST /bot/add_tag_to_user.
type AddTagToUserRequest struct {
	UserID  string `json:"user_id"`
	TagName string `json:"tag_name"`
}

type EnqueueJobRequest struct {
	ModuleName     string         `json:"module_name"`
	FuncName       string         `json:"func_name"`
	Kwargs         map[string]any `json:"kwargs"`
	SQSQueue       string         `json:"sqs_queue"`
	MessageGroupID string         `json:"message_group_id,omitempty"`
}
