package disha

import (
	"io"
	"log"
	"testing"
)

func TestResolveCartesiaModel(t *testing.T) {
	control := "control"
	test1 := "test1"
	test2 := "test2"
	empty := ""
	padded := "  test1  "
	unknown := "some_unrecognized_value"

	testCases := []struct {
		name      string
		flag      *string
		wantModel string
	}{
		{name: "nil", flag: nil, wantModel: "sonic-3"},
		{name: "empty", flag: &empty, wantModel: "sonic-3"},
		{name: "control", flag: &control, wantModel: "sonic-3"},
		{name: "test1", flag: &test1, wantModel: "sonic-3.5"},
		{name: "test2", flag: &test2, wantModel: "sonic-3.6"},
		{name: "whitespace_trimmed", flag: &padded, wantModel: "sonic-3.5"},
		{name: "unknown_defaults_to_control", flag: &unknown, wantModel: "sonic-3"},
	}

	logger := log.New(io.Discard, "", 0)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotModel := resolveCartesiaModel(tc.flag, logger)
			if gotModel != tc.wantModel {
				t.Errorf("model = %q, want %q", gotModel, tc.wantModel)
			}
		})
	}
}

func TestResolveCartesiaModelNilLoggerSafe(t *testing.T) {
	unknown := "unrecognized"
	model := resolveCartesiaModel(&unknown, nil)
	if model != "sonic-3" {
		t.Fatalf("resolveCartesiaModel with nil logger = %q, want sonic-3", model)
	}
}
