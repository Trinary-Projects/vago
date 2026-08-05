package disha

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGonjaProtocolRendererValues(t *testing.T) {
	renderer := newGonjaProtocolRenderer()

	tests := []struct {
		name      string
		template  string
		variables DocumentVariables
		want      string
	}{
		{
			name:      "substitution does not HTML escape",
			template:  "Today's plan: {{ diet_plan_today }}",
			variables: DocumentVariables{"diet_plan_today": "dal <bowl> & roti"},
			want:      "Today's plan: dal <bowl> & roti",
		},
		{
			name:      "integral float follows JSON Python boundary",
			template:  "₹{{ subscription_amount }}",
			variables: DocumentVariables{"subscription_amount": 499.0},
			want:      "₹499",
		},
		{
			name:      "fractional number",
			template:  "₹{{ subscription_amount }}",
			variables: DocumentVariables{"subscription_amount": 499.5},
			want:      "₹499.5",
		},
		{
			name:      "explicit null bare output matches Jinja",
			template:  "status={{ membership_status }}",
			variables: DocumentVariables{"membership_status": nil},
			want:      "status=None",
		},
		{
			name:      "explicit null preserves whitespace control",
			template:  "before {{- membership_status -}} after",
			variables: DocumentVariables{"membership_status": nil},
			want:      "beforeNoneafter",
		},
		{
			name:     "pointers are normalized through JSON",
			template: "{% if enabled %}{{ amount }}{% else %}off{% endif %}",
			variables: func() DocumentVariables {
				enabled := true
				amount := 499.0
				return DocumentVariables{"enabled": &enabled, "amount": &amount}
			}(),
			want: "499",
		},
		{
			name:      "boolean expression",
			template:  `{% if status == "active" and amount > 0 %}yes{% else %}no{% endif %}`,
			variables: DocumentVariables{"status": "active", "amount": 1},
			want:      "yes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderer.RenderTemplate(context.Background(), tc.name, tc.template, tc.variables)
			if err != nil {
				t.Fatalf("RenderTemplate: %v", err)
			}
			if got != tc.want {
				t.Fatalf("output = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGonjaProtocolRendererIfTruthiness(t *testing.T) {
	renderer := newGonjaProtocolRenderer()
	template := "{% if value %}true{% else %}false{% endif %}"
	empty := ""
	active := "active"
	zero := 0
	one := 1

	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"false bool", false, "false"},
		{"true bool", true, "true"},
		{"nil", nil, "false"},
		{"nil string pointer", (*string)(nil), "false"},
		{"empty string", "", "false"},
		{"empty string pointer", &empty, "false"},
		{"nonempty string", "active", "true"},
		{"nonempty string pointer", &active, "true"},
		{"closed membership is still truthy", "closed", "true"},
		{"zero integer", 0, "false"},
		{"zero integer pointer", &zero, "false"},
		{"nonzero integer", 1, "true"},
		{"nonzero integer pointer", &one, "true"},
		{"zero float", 0.0, "false"},
		{"nonzero float", 0.5, "true"},
		{"empty slice", []string{}, "false"},
		{"nonempty slice", []string{"x"}, "true"},
		{"empty map", map[string]any{}, "false"},
		{"nonempty map", map[string]any{"x": 1}, "true"},
		{"empty struct normalizes to empty map", struct{}{}, "false"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderer.RenderTemplate(
				context.Background(), tc.name, template, DocumentVariables{"value": tc.value},
			)
			if err != nil {
				t.Fatalf("RenderTemplate: %v", err)
			}
			if got != tc.want {
				t.Fatalf("output = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGonjaProtocolRendererStrictUndefined(t *testing.T) {
	renderer := newGonjaProtocolRenderer()

	tests := []struct {
		name      string
		template  string
		variables DocumentVariables
		want      string
		wantError bool
	}{
		{
			name:      "evaluated output fails",
			template:  "{{ missing }}",
			variables: DocumentVariables{"present": true},
			wantError: true,
		},
		{
			name:      "evaluated condition fails",
			template:  "{% if missing %}yes{% else %}no{% endif %}",
			variables: DocumentVariables{"present": true},
			wantError: true,
		},
		{
			name:      "untaken branch stays lazy",
			template:  "{% if available %}{{ missing }}{% else %}fallback{% endif %}",
			variables: DocumentVariables{"available": false},
			want:      "fallback",
		},
		{
			name:      "defined test handles missing value",
			template:  "{% if missing is defined %}yes{% else %}no{% endif %}",
			variables: DocumentVariables{"present": true},
			want:      "no",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderer.RenderTemplate(context.Background(), tc.name, tc.template, tc.variables)
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "undefined") {
					t.Fatalf("error = %v, want undefined error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("RenderTemplate: %v", err)
			}
			if got != tc.want {
				t.Fatalf("output = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGonjaProtocolRendererErrors(t *testing.T) {
	renderer := newGonjaProtocolRenderer()

	t.Run("unsupported statement", func(t *testing.T) {
		_, err := renderer.RenderTemplate(
			context.Background(), "loop", "{% for item in items %}{{ item }}{% endfor %}",
			DocumentVariables{"items": []string{"x"}},
		)
		if err == nil || !strings.Contains(err.Error(), `unsupported protocol template statement "for"`) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("syntax error", func(t *testing.T) {
		_, err := renderer.RenderTemplate(
			context.Background(), "bad", "{% if enabled %}yes", DocumentVariables{"enabled": true},
		)
		if err == nil {
			t.Fatal("expected syntax error")
		}
	})

	t.Run("unencodable variable", func(t *testing.T) {
		_, err := renderer.RenderTemplate(
			context.Background(), "channel", "{{ value }}", DocumentVariables{"value": make(chan int)},
		)
		if err == nil || !strings.Contains(err.Error(), "unsupported type") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := renderer.RenderTemplate(ctx, "canceled", "plain", nil)
		if err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("nil renderer", func(t *testing.T) {
		var nilRenderer *gonjaProtocolRenderer
		_, err := nilRenderer.RenderTemplate(context.Background(), "nil", "plain", nil)
		if err == nil || !strings.Contains(err.Error(), "renderer is nil") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestProtocolTemplateVariableNames(t *testing.T) {
	template := `{{ profile.name | default(fallback) }}{% if status is defined and status %}{{ amount }}{% endif %}`
	got, err := protocolTemplateVariableNames(template)
	if err != nil {
		t.Fatalf("protocolTemplateVariableNames: %v", err)
	}
	want := []string{"amount", "fallback", "profile", "status"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("names = %#v, want %#v", got, want)
	}
}

// This test is opt-in because its input is a snapshot fetched from Weaviate.
// It compares every stored ProtocolInstruction and the Cartesian product of
// meaningful values for every variable currently used by the corpus against
// the exact Python Jinja renderer that protocol retrieval previously used.
func TestGonjaProtocolRendererLiveCorpusParity(t *testing.T) {
	corpusPath := os.Getenv("PROTOCOL_RENDER_PARITY_CORPUS")
	if corpusPath == "" {
		t.Skip("PROTOCOL_RENDER_PARITY_CORPUS is not set")
	}
	python := os.Getenv("PYTHON_JINJA_TEST_PYTHON")
	if python == "" {
		var err error
		python, err = exec.LookPath("python3")
		if err != nil {
			t.Skip("python3 not available")
		}
	}
	if err := exec.Command(python, "-c", "import jinja2").Run(); err != nil {
		t.Skipf("%s cannot import jinja2: %v", python, err)
	}

	wire, err := os.ReadFile(corpusPath)
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	var corpus protocolRenderParityCorpus
	if err := json.Unmarshal(wire, &corpus); err != nil {
		t.Fatalf("decode corpus: %v", err)
	}
	protocols := corpus.Data.Get.ProtocolInstruction
	wantCount := corpus.Data.Aggregate.ProtocolInstruction[0].Meta.Count
	if len(protocols) != wantCount {
		t.Fatalf("incomplete corpus: fetched=%d aggregate=%d", len(protocols), wantCount)
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	t.Setenv("JINJA_RENDERER_PYTHON", python)
	t.Setenv("JINJA_RENDERER_SCRIPT", filepath.Join(wd, "..", "jinja_renderer.py"))
	jinja := NewPythonJinjaRenderer(log.New(io.Discard, "", 0))
	t.Cleanup(func() { _ = jinja.Close() })
	gonjaRenderer := newGonjaProtocolRenderer()

	valueMatrix := map[string][]any{
		"diet_chart_available":   {true, false},
		"diet_plan_today":        {"Breakfast: poha; lunch: dal & roti", "", "सुबह: पोहा\nदोपहर: dal <bowl> & roti"},
		"membership_status":      {"active", "", nil},
		"membership_expiry_date": {"31 Aug 2026", "", nil},
		"subscription_status":    {"active", "", nil},
		"subscription_amount":    {499.5, 499, 0, nil},
		"next_payment_due_date":  {"01 Sep 2026", "", nil},
		"payment_overdue":        {true, false, nil},
	}

	cases := 0
	templated := 0
	for _, protocol := range protocols {
		names, err := protocolTemplateVariableNames(protocol.InstructionText)
		if err != nil {
			t.Fatalf("inspect %s (%s): %v", protocol.Title, protocol.ID.ID, err)
		}
		if len(names) == 0 {
			cases++
			got, err := gonjaRenderer.RenderTemplate(context.Background(), protocol.Title, protocol.InstructionText, nil)
			if err != nil {
				t.Fatalf("render plain %s: %v", protocol.Title, err)
			}
			if got != protocol.InstructionText {
				t.Fatalf("plain %s changed: got=%q want=%q", protocol.Title, got, protocol.InstructionText)
			}
			continue
		}
		templated++
		for _, name := range names {
			if _, ok := valueMatrix[name]; !ok {
				t.Fatalf("protocol %s uses variable %q without a parity matrix", protocol.Title, name)
			}
		}
		forEachProtocolVariableCombination(names, valueMatrix, func(variables DocumentVariables) {
			cases++
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			want, renderErr := jinja.Render(ctx, TemplateRenderRequest{
				DocumentName: protocol.Title,
				Text:         protocol.InstructionText,
				Variables:    variables,
			})
			if renderErr != nil {
				t.Fatalf("Jinja render %s vars=%#v: %v", protocol.Title, variables, renderErr)
			}
			if want.UndefinedError != "" || want.StrictValidationError != "" {
				t.Fatalf("Jinja validation %s vars=%#v: undefined=%q strict=%q", protocol.Title, variables, want.UndefinedError, want.StrictValidationError)
			}
			got, gonjaErr := gonjaRenderer.RenderTemplate(ctx, protocol.Title, protocol.InstructionText, variables)
			if gonjaErr != nil {
				t.Fatalf("Gonja render %s vars=%#v: %v", protocol.Title, variables, gonjaErr)
			}
			if got != want.Output {
				t.Fatalf("render mismatch %s vars=%#v\nJinja: %q\nGonja: %q", protocol.Title, variables, want.Output, got)
			}
		})
	}
	t.Logf("exact parity: protocols=%d templated=%d cases=%d", len(protocols), templated, cases)
}

type protocolRenderParityCorpus struct {
	Data struct {
		Aggregate struct {
			ProtocolInstruction []struct {
				Meta struct {
					Count int `json:"count"`
				} `json:"meta"`
			} `json:"ProtocolInstruction"`
		} `json:"Aggregate"`
		Get struct {
			ProtocolInstruction []struct {
				ID struct {
					ID string `json:"id"`
				} `json:"_additional"`
				InstructionText string `json:"instructionText"`
				Title           string `json:"title"`
			} `json:"ProtocolInstruction"`
		} `json:"Get"`
	} `json:"data"`
}

func forEachProtocolVariableCombination(
	names []string,
	valueMatrix map[string][]any,
	visit func(DocumentVariables),
) {
	variables := make(DocumentVariables, len(names))
	var walk func(int)
	walk = func(index int) {
		if index == len(names) {
			copy := make(DocumentVariables, len(variables))
			for name, value := range variables {
				copy[name] = value
			}
			visit(copy)
			return
		}
		name := names[index]
		for _, value := range valueMatrix[name] {
			variables[name] = value
			walk(index + 1)
		}
	}
	walk(0)
}
