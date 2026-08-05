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
	"regexp"
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
			name:      "integral float uses Gonja formatting",
			template:  "₹{{ subscription_amount }}",
			variables: DocumentVariables{"subscription_amount": 499.0},
			want:      "₹499.0",
		},
		{
			name:      "fractional number",
			template:  "₹{{ subscription_amount }}",
			variables: DocumentVariables{"subscription_amount": 499.5},
			want:      "₹499.5",
		},
		{
			name:      "explicit nil renders empty",
			template:  "status={{ membership_status }}",
			variables: DocumentVariables{"membership_status": nil},
			want:      "status=",
		},
		{
			name:      "explicit nil preserves whitespace control",
			template:  "before {{- membership_status -}} after",
			variables: DocumentVariables{"membership_status": nil},
			want:      "beforeafter",
		},
		{
			name:     "Gonja dereferences pointers",
			template: "{% if enabled %}{{ amount }}{% else %}off{% endif %}",
			variables: func() DocumentVariables {
				enabled := true
				amount := 499.0
				return DocumentVariables{"enabled": &enabled, "amount": &amount}
			}(),
			want: "499.0",
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
			if got.Text != tc.want {
				t.Fatalf("output = %q, want %q", got.Text, tc.want)
			}
			if len(got.MissingVariables) != 0 {
				t.Fatalf("missing variables = %#v, want none", got.MissingVariables)
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
		{"empty struct follows Gonja truthiness", struct{}{}, "true"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderer.RenderTemplate(
				context.Background(), tc.name, template, DocumentVariables{"value": tc.value},
			)
			if err != nil {
				t.Fatalf("RenderTemplate: %v", err)
			}
			if got.Text != tc.want {
				t.Fatalf("output = %q, want %q", got.Text, tc.want)
			}
			if len(got.MissingVariables) != 0 {
				t.Fatalf("missing variables = %#v, want none", got.MissingVariables)
			}
		})
	}
}

func TestGonjaProtocolRendererMissingVariables(t *testing.T) {
	renderer := newGonjaProtocolRenderer()

	tests := []struct {
		name        string
		template    string
		variables   DocumentVariables
		want        string
		wantMissing []string
	}{
		{
			name:        "output renders empty and reports missing",
			template:    "{{ missing }}",
			variables:   DocumentVariables{"present": true},
			want:        "",
			wantMissing: []string{"missing"},
		},
		{
			name:        "condition evaluates false and reports missing",
			template:    "{% if missing %}yes{% else %}no{% endif %}",
			variables:   DocumentVariables{"present": true},
			want:        "no",
			wantMissing: []string{"missing"},
		},
		{
			name:        "untaken branch stays lazy",
			template:    "{% if available %}{{ missing }}{% else %}fallback{% endif %}",
			variables:   DocumentVariables{"available": false},
			want:        "fallback",
			wantMissing: []string{"missing"},
		},
		{
			name:        "defined test handles missing value",
			template:    "{% if missing is defined %}yes{% else %}no{% endif %}",
			variables:   DocumentVariables{"present": true},
			want:        "no",
			wantMissing: []string{"missing"},
		},
		{
			name:      "supplied nil is not missing",
			template:  "{% if missing %}yes{% else %}no{% endif %}:{{ missing }}",
			variables: DocumentVariables{"missing": nil},
			want:      "no:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderer.RenderTemplate(context.Background(), tc.name, tc.template, tc.variables)
			if err != nil {
				t.Fatalf("RenderTemplate: %v", err)
			}
			if got.Text != tc.want {
				t.Fatalf("output = %q, want %q", got.Text, tc.want)
			}
			if !reflect.DeepEqual(got.MissingVariables, tc.wantMissing) {
				t.Fatalf("missing variables = %#v, want %#v", got.MissingVariables, tc.wantMissing)
			}
		})
	}
}

func TestGonjaProtocolRendererErrors(t *testing.T) {
	renderer := newGonjaProtocolRenderer()

	t.Run("syntax error", func(t *testing.T) {
		_, err := renderer.RenderTemplate(
			context.Background(), "bad", "{% if enabled %}yes", DocumentVariables{"enabled": true},
		)
		if err == nil {
			t.Fatal("expected syntax error")
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
// the Python Jinja renderer that protocol retrieval previously used. The
// comparison ignores only the two known native formatting differences:
// Jinja's None versus Gonja's empty nil, and 499 versus 499.0.
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
		"membership_status":      {pointer("active"), pointer(""), (*string)(nil)},
		"membership_expiry_date": {pointer("31 Aug 2026"), pointer(""), (*string)(nil)},
		"subscription_status":    {pointer("active"), pointer(""), (*string)(nil)},
		"subscription_amount":    {pointer(499.5), pointer(499.0), pointer(0.0), (*float64)(nil)},
		"next_payment_due_date":  {pointer("01 Sep 2026"), pointer(""), (*string)(nil)},
		"payment_overdue":        {pointer(true), pointer(false), (*bool)(nil)},
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
			if got.Text != protocol.InstructionText {
				t.Fatalf("plain %s changed: got=%q want=%q", protocol.Title, got.Text, protocol.InstructionText)
			}
			if len(got.MissingVariables) != 0 {
				t.Fatalf("plain %s reports missing variables: %#v", protocol.Title, got.MissingVariables)
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
			if len(got.MissingVariables) != 0 {
				t.Fatalf("Gonja missing variables %s vars=%#v: %#v", protocol.Title, variables, got.MissingVariables)
			}
			if normalizeProtocolParityOutput(got.Text) != normalizeProtocolParityOutput(want.Output) {
				t.Fatalf("logical render mismatch %s vars=%#v\nJinja: %q\nGonja: %q", protocol.Title, variables, want.Output, got.Text)
			}
		})
	}
	t.Logf("logical parity: protocols=%d templated=%d cases=%d", len(protocols), templated, cases)
}

var integralFloatOutput = regexp.MustCompile(`\b([0-9]+)\.0\b`)

func normalizeProtocolParityOutput(text string) string {
	text = strings.ReplaceAll(text, "None", "")
	return integralFloatOutput.ReplaceAllString(text, "$1")
}

func pointer[T any](value T) *T { return &value }

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
