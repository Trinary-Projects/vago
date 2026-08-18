package disha

import (
	"context"
	"errors"
)

// gonjaProtocolRenderer renders only protocol instruction text fetched from
// Weaviate. It reuses gonja_renderer.go's shared render core (gonjaRenderText)
// and variable preflight, but keeps the protocol contract: report referenced
// variables that are MISSING only (a present-but-nil key is not "missing" —
// AGENTS.md, 2026-08-05). The document-store renderer (GonjaJinjaRenderer)
// merges missing+nil instead.
type gonjaProtocolRenderer struct{}

func newGonjaProtocolRenderer() *gonjaProtocolRenderer {
	return &gonjaProtocolRenderer{}
}

func (r *gonjaProtocolRenderer) RenderTemplate(
	ctx context.Context,
	label, text string,
	variables DocumentVariables,
) (protocolTemplateRenderResult, error) {
	if r == nil {
		return protocolTemplateRenderResult{}, errors.New("disha: gonja protocol renderer is nil")
	}
	// Discard nilVars: protocols report missing-only. Rendering keeps gonja's
	// native empty/false behavior for both missing and nil.
	output, missing, _, err := gonjaRenderText(ctx, label, 0, text, variables)
	if err != nil {
		return protocolTemplateRenderResult{}, err
	}
	return protocolTemplateRenderResult{Text: output, MissingVariables: missing}, nil
}
