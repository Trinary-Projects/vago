package disha

import "context"

// This file holds the shared template-rendering contract used by both renderer
// implementations: GonjaJinjaRenderer (gonja_renderer.go, production) and
// PythonJinjaRenderer (jinja_renderer.go, kept only as a parity test oracle),
// plus DocumentStore and the callers that render prompts.

// DocumentVariables is the variable map fed to a template render.
type DocumentVariables map[string]any

// TemplateRenderRequest is the input to a TemplateRenderer.
type TemplateRenderRequest struct {
	DocumentName    string
	DocumentVersion int
	Text            string
	Variables       DocumentVariables
}

// TemplateRenderResult is the output of a TemplateRenderer.
type TemplateRenderResult struct {
	Output string
	// UnresolvedVariables names referenced variables that have no usable value —
	// either missing (key absent) or supplied as nil. This repo merges Python's
	// separate "undefined" and "null-rendered" notions into one, because gonja
	// detects both the same way (static, top-level, branch-agnostic) and can
	// reproduce neither of Python's runtime/branch-aware checks faithfully.
	UnresolvedVariables []string
	// UndefinedError and StrictValidationError are populated only by the Python
	// oracle renderer (jinja_renderer.go, kept for the parity test); gonja leaves
	// them empty.
	UndefinedError        string
	StrictValidationError string
}

// TemplateRenderer renders a Langfuse-backed document template. Production uses
// GonjaJinjaRenderer; PythonJinjaRenderer (jinja_renderer.go) remains only as a
// parity test oracle.
type TemplateRenderer interface {
	Render(ctx context.Context, req TemplateRenderRequest) (TemplateRenderResult, error)
	Close() error
}
