package disha

import (
	"context"
	"io"
	"log"
	"reflect"
	"testing"
)

func newTestGonjaRenderer() *GonjaJinjaRenderer {
	return NewGonjaJinjaRenderer(log.New(io.Discard, "", 0))
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
			res, err := r.Render(context.Background(), GonjaRenderRequest{
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

// TestGonjaRenderUndefinedAndNil covers the two reporting lists that mirror the
// Python renderer's undefined-variable and rendered-None detection.
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
			res, err := r.Render(context.Background(), GonjaRenderRequest{
				DocumentName: tc.name,
				Text:         tc.text,
				Variables:    tc.variables,
			})
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			if !reflect.DeepEqual(res.Undefined, tc.wantUndefined) {
				t.Errorf("Undefined = %v, want %v", res.Undefined, tc.wantUndefined)
			}
			if !reflect.DeepEqual(res.NilVars, tc.wantNil) {
				t.Errorf("NilVars = %v, want %v", res.NilVars, tc.wantNil)
			}
		})
	}
}

// TestGonjaRenderNoVariables: passing no variables is the same as every
// reference being undefined, so the reference blanks in the output and is
// reported undefined rather than left literal.
func TestGonjaRenderNoVariables(t *testing.T) {
	r := newTestGonjaRenderer()
	const raw = "keep {{ untouched }} literally"
	res, err := r.Render(context.Background(), GonjaRenderRequest{Text: raw})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if want := "keep  literally"; res.Output != want {
		t.Fatalf("Output = %q, want %q", res.Output, want)
	}
	if !reflect.DeepEqual(res.Undefined, []string{"untouched"}) {
		t.Fatalf("Undefined = %v, want [untouched]", res.Undefined)
	}
}

func TestGonjaRenderNilReceiver(t *testing.T) {
	var r *GonjaJinjaRenderer
	if _, err := r.Render(context.Background(), GonjaRenderRequest{Text: "hi"}); err == nil {
		t.Fatal("expected error from nil receiver")
	}
}

func TestGonjaRenderContextCancelled(t *testing.T) {
	r := newTestGonjaRenderer()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Render(ctx, GonjaRenderRequest{Text: "{{ a }}", Variables: map[string]any{"a": "x"}}); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestGonjaRenderCompileError(t *testing.T) {
	r := newTestGonjaRenderer()
	// Unterminated block should fail to compile.
	_, err := r.Render(context.Background(), GonjaRenderRequest{
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
