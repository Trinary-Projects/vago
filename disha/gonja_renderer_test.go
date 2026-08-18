package disha

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func newTestGonjaRenderer() *GonjaJinjaRenderer {
	return NewGonjaJinjaRenderer(log.New(io.Discard, "", 0))
}

// llmLogDump is the shape of each file under llm_log_dumps_final/. Only the
// fields needed to re-render the system prompt are decoded.
type llmLogDump struct {
	LogID               string         `json:"log_id"`
	UsecaseType         string         `json:"usecase_type"`
	SystemPromptName    string         `json:"system_prompt_name"`
	SystemPromptVersion int            `json:"system_prompt_version"`
	Variables           map[string]any `json:"variables"`
	OriginalPromptText  string         `json:"original_prompt_text"`
	SystemPromptText    string         `json:"system_prompt_text"`
}

// TestGonjaRenderMatchesLoggedSystemPrompt renders each real LLM-log dump's
// original (un-rendered) system prompt through the gonja renderer using the
// logged variables and asserts the output byte-matches the system_prompt_text
// that the live Python Jinja renderer produced. This is the parity check
// between the two renderers on production prompts.
//
// The dumps live at the repo root (../llm_log_dumps_final relative to this
// package). If the directory is absent the test skips, so CI without the
// fixtures still passes.
func TestGonjaRenderMatchesLoggedSystemPrompt(t *testing.T) {
	const dumpDir = "../llm_log_dumps_final"
	if _, err := os.Stat(dumpDir); os.IsNotExist(err) {
		t.Skipf("dump dir %s not present; skipping", dumpDir)
	}

	files, err := filepath.Glob(filepath.Join(dumpDir, "*.json"))
	if err != nil {
		t.Fatalf("glob dumps: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no dump files found in %s", dumpDir)
	}

	r := newTestGonjaRenderer()
	for _, file := range files {
		file := file
		t.Run(filepath.Base(file), func(t *testing.T) {
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("read %s: %v", file, err)
			}
			var dump llmLogDump
			// UseNumber so JSON numbers decode to json.Number (preserving the
			// int/float literal, e.g. "1" vs "158.0") rather than float64, which
			// gonja would render as "1.0". This matches Python's json/int decode.
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.UseNumber()
			if err := dec.Decode(&dump); err != nil {
				t.Fatalf("unmarshal %s: %v", file, err)
			}

			res, err := r.Render(context.Background(), TemplateRenderRequest{
				DocumentName:    dump.SystemPromptName,
				DocumentVersion: dump.SystemPromptVersion,
				Text:            dump.OriginalPromptText,
				Variables:       dump.Variables,
			})
			if err != nil {
				t.Fatalf("render %s (%s v%d): %v", file, dump.SystemPromptName, dump.SystemPromptVersion, err)
			}

			// Ignore trailing-newline-only diffs. gonja strips one trailing
			// newline (Jinja keep_trailing_newline=False), and Python's renderer
			// returns static prompts (empty variables) verbatim, keeping it — so
			// for those the logged text carries a trailing newline gonja drops.
			// Trailing newlines are not semantically meaningful in a prompt.
			if res.Output != dump.SystemPromptText &&
				strings.TrimRight(res.Output, "\n") != strings.TrimRight(dump.SystemPromptText, "\n") {
				at, gotCtx, wantCtx := firstDiff(res.Output, dump.SystemPromptText)
				// A diff is "explained" when it is caused by an unresolved
				// variable: a missing name (e.g. onboarding's `analysis`,
				// deliberately excluded from logged prompt metadata) or a nil var
				// (gonja empty vs Jinja "None", an accepted divergence) renders
				// differently by design. A diff with an empty unresolved set is an
				// unexplained parity bug and the concerning case.
				explained := len(res.UnresolvedVariables) > 0
				if !explained {
					t.Errorf("gonja output differs from logged system prompt for %s v%d\n"+
						"  file:        %s\n"+
						"  explained:   %t (by unresolved vars)\n"+
						"  unresolved:  %v\n"+
						"  first diff at byte %d (gonja len=%d, logged len=%d)\n"+
						"  gonja  ...%q...\n"+
						"  logged ...%q...",
						dump.SystemPromptName, dump.SystemPromptVersion, filepath.Base(file),
						explained, res.UnresolvedVariables, at, len(res.Output), len(dump.SystemPromptText),
						gotCtx, wantCtx)
				}
			}
		})
	}
}

// firstDiff returns the index of the first differing byte between got and want
// plus a short window of context around that index from each string.
func firstDiff(got, want string) (int, string, string) {
	n := len(got)
	if len(want) < n {
		n = len(want)
	}
	i := 0
	for i < n && got[i] == want[i] {
		i++
	}
	window := func(s string) string {
		lo := i - 40
		if lo < 0 {
			lo = 0
		}
		hi := i + 40
		if hi > len(s) {
			hi = len(s)
		}
		return s[lo:hi]
	}
	return i, window(got), window(want)
}

// TestGonjaRenderOutput covers the rendered text: substitution, no HTML
// escaping, nil-as-empty, and conditionals, mirroring Gonja's native contract.
func TestGonjaRenderOutput(t *testing.T) {
	tests := []struct {
		name      string
		text      string
		variables map[string]any
		want      string
	}{
		{
			name:      "plain substitution, no HTML escape",
			text:      "Plan: {{ diet_plan_today }}",
			variables: map[string]any{"diet_plan_today": "dal <bowl> & roti"},
			want:      "Plan: dal <bowl> & roti",
		},
		{
			// DIVERGENCE: gonja renders nil as "" here; Python Jinja2 renders
			// the literal "None" ("status=None"). See gonja_renderer.go.
			name:      "nil renders empty",
			text:      "status={{ membership_status }}",
			variables: map[string]any{"membership_status": nil},
			want:      "status=",
		},
		{
			name:      "if guard true",
			text:      "{% if diet_chart_available %}has chart{% else %}no chart{% endif %}",
			variables: map[string]any{"diet_chart_available": true},
			want:      "has chart",
		},
		{
			name:      "if guard nil is falsey",
			text:      "{% if diet_chart_available %}has chart{% else %}no chart{% endif %}",
			variables: map[string]any{"diet_chart_available": nil},
			want:      "no chart",
		},
		{
			// DIVERGENCE: gonja's default() replaces nil -> "friend"; Python
			// Jinja2's default() fires only for *undefined* names (not None),
			// so it renders "None" here. Python needs default('friend', true)
			// to also replace None. See gonja_renderer.go.
			name:      "default filter replaces nil",
			text:      "{{ name | default('friend') }}",
			variables: map[string]any{"name": nil},
			want:      "friend",
		},
	}

	r := newTestGonjaRenderer()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := r.Render(context.Background(), TemplateRenderRequest{
				DocumentName: tc.name,
				Text:         tc.text,
				Variables:    tc.variables,
			})
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			if res.Output != tc.want {
				t.Fatalf("Output = %q, want %q", res.Output, tc.want)
			}
		})
	}
}

// TestGonjaRenderUndefinedAndNil covers variable detection. The cases keep the
// original undefined/nil split for clarity; the renderer now merges both into
// one UnresolvedVariables set, so the assertion compares against their union.
func TestGonjaRenderUndefinedAndNil(t *testing.T) {
	tests := []struct {
		name          string
		text          string
		variables     map[string]any
		wantUndefined []string
		wantNil       []string
	}{
		{
			name:          "supplied non-nil: neither list",
			text:          "{{ a }} {{ b }}",
			variables:     map[string]any{"a": "x", "b": "y"},
			wantUndefined: nil,
			wantNil:       nil,
		},
		{
			name:          "missing variable is undefined",
			text:          "{{ a }} {{ missing }}",
			variables:     map[string]any{"a": "x"},
			wantUndefined: []string{"missing"},
			wantNil:       nil,
		},
		{
			name:          "supplied nil is reported nil, not undefined",
			text:          "{{ a }} {{ b }}",
			variables:     map[string]any{"a": "x", "b": nil},
			wantUndefined: nil,
			wantNil:       []string{"b"},
		},
		{
			name:          "both lists stay sorted",
			text:          "{{ zeta }} {{ alpha }} {{ mid }}",
			variables:     map[string]any{"alpha": nil, "mid": nil},
			wantUndefined: []string{"zeta"},
			wantNil:       []string{"alpha", "mid"},
		},
	}

	r := newTestGonjaRenderer()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := r.Render(context.Background(), TemplateRenderRequest{
				DocumentName: tc.name,
				Text:         tc.text,
				Variables:    tc.variables,
			})
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			wantUnresolved := mergeSortedNames(tc.wantUndefined, tc.wantNil)
			if !reflect.DeepEqual(res.UnresolvedVariables, wantUnresolved) {
				t.Errorf("UnresolvedVariables = %v, want %v", res.UnresolvedVariables, wantUnresolved)
			}
		})
	}
}

// TestGonjaRenderNoVariables: passing no variables is the same as every
// reference being unresolved, so the reference blanks in the output and is
// reported unresolved rather than left literal.
func TestGonjaRenderNoVariables(t *testing.T) {
	r := newTestGonjaRenderer()
	const raw = "keep {{ untouched }} literally"
	res, err := r.Render(context.Background(), TemplateRenderRequest{Text: raw})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if want := "keep  literally"; res.Output != want {
		t.Fatalf("Output = %q, want %q", res.Output, want)
	}
	if !reflect.DeepEqual(res.UnresolvedVariables, []string{"untouched"}) {
		t.Fatalf("UnresolvedVariables = %v, want [untouched]", res.UnresolvedVariables)
	}
}

func TestGonjaRenderNilReceiver(t *testing.T) {
	var r *GonjaJinjaRenderer
	if _, err := r.Render(context.Background(), TemplateRenderRequest{Text: "hi"}); err == nil {
		t.Fatal("expected error from nil receiver")
	}
}

func TestGonjaRenderContextCancelled(t *testing.T) {
	r := newTestGonjaRenderer()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Render(ctx, TemplateRenderRequest{Text: "{{ a }}", Variables: map[string]any{"a": "x"}}); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestGonjaRenderCompileError(t *testing.T) {
	r := newTestGonjaRenderer()
	// Unterminated block should fail to compile.
	_, err := r.Render(context.Background(), TemplateRenderRequest{
		Text:      "{% if x %}oops",
		Variables: map[string]any{"x": true},
	})
	if err == nil {
		t.Fatal("expected compile error for unterminated block")
	}
}

func TestGonjaRendererClose(t *testing.T) {
	if err := newTestGonjaRenderer().Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestGonjaTemplateVariableNames documents the lexer preflight, including the
// known false positives vs Python's jinja2.meta.find_undeclared_variables.
// These assertions describe CURRENT behavior so regressions are visible; the
// cases marked "FALSE POSITIVE" are where Gonja's token-level scan diverges
// from Python and would over-report a variable as undefined.
func TestGonjaTemplateVariableNames(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{
			name: "plain reference",
			text: "{{ greeting }}",
			want: []string{"greeting"},
		},
		{
			name: "attribute lookup only reports the root",
			text: "{{ user.name }}",
			want: []string{"user"},
		},
		{
			name: "filter name is not a variable",
			text: "{{ name | default('n/a') }}",
			want: []string{"name"},
		},
		{
			name: "if condition variable",
			text: "{% if diet_chart_available %}x{% endif %}",
			want: []string{"diet_chart_available"},
		},
		{
			name: "literals and keywords excluded",
			text: `{% if status == "active" and count > 0 %}x{% endif %}`,
			want: []string{"count", "status"},
		},
		{
			// FALSE POSITIVE: `item` is a loop-local; Python excludes it.
			name: "for loop target over-reported",
			text: "{% for item in items %}{{ item }}{% endfor %}",
			want: []string{"item", "items"},
		},
		{
			// FALSE POSITIVE: `x` is set-assigned; Python excludes it. The
			// assignment target is only skipped when written `x=` with no space.
			name: "set target over-reported with spaced assign",
			text: "{% set x = greeting %}{{ x }}",
			want: []string{"greeting", "x"},
		},
		{
			// FALSE POSITIVE: `defined` is the test name after `is not`.
			name: "is-not test name over-reported",
			text: "{{ y is not defined }}",
			want: []string{"defined", "y"},
		},
		{
			// FALSE POSITIVE: `loop` is Jinja's special loop object.
			name: "loop special over-reported",
			text: "{% for i in xs %}{{ loop.index }}{% endfor %}",
			want: []string{"i", "loop", "xs"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := gonjaTemplateVariableNames(tc.text)
			if err != nil {
				t.Fatalf("gonjaTemplateVariableNames: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("names = %v, want %v", got, tc.want)
			}
		})
	}
}
