package disha

import "testing"

func TestBuildPromptTraceMetadataIncludesVersionAndCopiesVariables(t *testing.T) {
	schedule := map[string]any{"morning": "8 AM"}
	vars := DocumentVariables{"patient_info": "Riya", "patient_schedule": schedule}
	metadata := buildPromptTraceMetadata("system", "sales_call/main_sys-3day_v2", 17, vars)

	vars["patient_info"] = "Changed"
	schedule["morning"] = "9 AM"

	if metadata["system_prompt_name"] != "sales_call/main_sys-3day_v2" ||
		metadata["system_prompt_version"] != 17 {
		t.Fatalf("metadata identity = %+v", metadata)
	}
	copiedVars, ok := metadata["system_prompt_variables"].(DocumentVariables)
	if !ok {
		t.Fatalf("system_prompt_variables = %#v, want DocumentVariables", metadata["system_prompt_variables"])
	}
	if copiedVars["patient_info"] != "Riya" {
		t.Fatalf("copied patient_info = %#v, want original value", copiedVars["patient_info"])
	}
	copiedSchedule, ok := copiedVars["patient_schedule"].(map[string]any)
	if !ok {
		t.Fatalf("patient_schedule = %#v, want map", copiedVars["patient_schedule"])
	}
	if copiedSchedule["morning"] != "8 AM" {
		t.Fatalf("copied schedule = %#v, want original value", copiedSchedule)
	}
}
