package disha

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultJinjaRenderTimeout = 10 * time.Second

// DocumentVariables, TemplateRenderRequest, TemplateRenderResult, and the
// TemplateRenderer interface are defined in gonja_renderer.go, the production
// renderer. PythonJinjaRenderer below implements the same interface and is kept
// only as a parity test oracle (see protocol_gonja_renderer_test.go); nothing
// in production wires it.

type PythonJinjaRenderer struct {
	logger  *log.Logger
	python  string
	script  string
	timeout time.Duration

	seq     atomic.Uint64
	mu      sync.Mutex
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	decoder *json.Decoder
}

type pythonJinjaRequest struct {
	ID              string            `json:"id"`
	DocumentName    string            `json:"document_name"`
	DocumentVersion int               `json:"document_version"`
	Template        string            `json:"template"`
	Variables       DocumentVariables `json:"variables"`
}

type pythonJinjaResponse struct {
	ID                     string   `json:"id"`
	OK                     bool     `json:"ok"`
	Output                 string   `json:"output"`
	Stage                  string   `json:"stage"`
	Error                  string   `json:"error"`
	CompileTimeMissingVars []string `json:"compile_time_missing_variables"`
	UndefinedError         string   `json:"undefined_error"`
	StrictValidationError  string   `json:"strict_error"`
}

type pythonRendererTransportError struct {
	err error
}

func (e pythonRendererTransportError) Error() string {
	return e.err.Error()
}

func (e pythonRendererTransportError) Unwrap() error {
	return e.err
}

func NewPythonJinjaRenderer(logger *log.Logger) *PythonJinjaRenderer {
	python, script := jinjaRendererConfig()
	return &PythonJinjaRenderer{
		logger:  logger,
		python:  python,
		script:  script,
		timeout: defaultJinjaRenderTimeout,
	}
}

func jinjaRendererConfig() (python, script string) {
	python = strings.TrimSpace(os.Getenv("JINJA_RENDERER_PYTHON"))
	if python == "" {
		python = strings.TrimSpace(os.Getenv("DAILY_BRIDGE_PYTHON"))
	}
	if python == "" {
		python = "python3"
	}
	script = strings.TrimSpace(os.Getenv("JINJA_RENDERER_SCRIPT"))
	if script == "" {
		script = "jinja_renderer.py"
	}
	if !filepath.IsAbs(script) {
		if wd, err := os.Getwd(); err == nil {
			script = filepath.Join(wd, script)
		}
	}
	return python, script
}

func (r *PythonJinjaRenderer) Render(ctx context.Context, req TemplateRenderRequest) (TemplateRenderResult, error) {
	if r == nil {
		return TemplateRenderResult{}, errors.New("disha: python jinja renderer is nil")
	}
	if req.Variables == nil {
		req.Variables = DocumentVariables{}
	}
	renderCtx := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok && r.timeout > 0 {
		renderCtx, cancel = context.WithTimeout(ctx, r.timeout)
	}
	defer cancel()

	r.mu.Lock()
	defer r.mu.Unlock()

	result, err := r.renderOnceLocked(renderCtx, req)
	if err == nil {
		return result, nil
	}
	var transportErr pythonRendererTransportError
	if errors.As(err, &transportErr) && renderCtx.Err() == nil {
		if r.logger != nil {
			r.logger.Printf("disha: python jinja renderer transport failed, retrying once: %v\n", err)
		}
		return r.renderOnceLocked(renderCtx, req)
	}
	return TemplateRenderResult{}, err
}

func (r *PythonJinjaRenderer) renderOnceLocked(ctx context.Context, req TemplateRenderRequest) (TemplateRenderResult, error) {
	if err := r.ensureStartedLocked(); err != nil {
		return TemplateRenderResult{}, err
	}

	stdin := r.stdin
	decoder := r.decoder
	wireReq := pythonJinjaRequest{
		ID:              fmt.Sprintf("%d", r.seq.Add(1)),
		DocumentName:    req.DocumentName,
		DocumentVersion: req.DocumentVersion,
		Template:        req.Text,
		Variables:       req.Variables,
	}

	type renderResponse struct {
		resp pythonJinjaResponse
		err  error
	}
	done := make(chan renderResponse, 1)
	go func() {
		if err := json.NewEncoder(stdin).Encode(wireReq); err != nil {
			done <- renderResponse{err: err}
			return
		}
		var resp pythonJinjaResponse
		if err := decoder.Decode(&resp); err != nil {
			done <- renderResponse{err: err}
			return
		}
		done <- renderResponse{resp: resp}
	}()

	select {
	case out := <-done:
		if out.err != nil {
			r.stopLocked()
			return TemplateRenderResult{}, pythonRendererTransportError{err: fmt.Errorf("disha: python jinja renderer transport: %w", out.err)}
		}
		if !out.resp.OK {
			return TemplateRenderResult{}, fmt.Errorf("disha: render document %q version=%d failed at %s: %s", req.DocumentName, req.DocumentVersion, out.resp.Stage, out.resp.Error)
		}
		return TemplateRenderResult{
			Output:                out.resp.Output,
			UnresolvedVariables:   out.resp.CompileTimeMissingVars,
			UndefinedError:        out.resp.UndefinedError,
			StrictValidationError: out.resp.StrictValidationError,
		}, nil
	case <-ctx.Done():
		r.stopLocked()
		return TemplateRenderResult{}, fmt.Errorf("disha: render document %q version=%d timed out: %w", req.DocumentName, req.DocumentVersion, ctx.Err())
	}
}

func (r *PythonJinjaRenderer) ensureStartedLocked() error {
	if r.cmd != nil {
		return nil
	}
	cmd := exec.Command(r.python, r.script)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("disha: python jinja stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("disha: python jinja stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("disha: python jinja stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("disha: start python jinja renderer %s %s: %w", r.python, r.script, err)
	}
	r.cmd = cmd
	r.stdin = stdin
	r.decoder = json.NewDecoder(stdout)
	go r.logStderr(stderr)
	if r.logger != nil {
		r.logger.Printf("disha: python jinja renderer started pid=%d script=%s\n", cmd.Process.Pid, r.script)
	}
	return nil
}

func (r *PythonJinjaRenderer) logStderr(stderr io.Reader) {
	if r.logger == nil {
		io.Copy(io.Discard, stderr)
		return
	}
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		r.logger.Printf("disha: python jinja renderer stderr: %s\n", scanner.Text())
	}
}

func (r *PythonJinjaRenderer) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopLocked()
}

func (r *PythonJinjaRenderer) stopLocked() error {
	cmd := r.cmd
	stdin := r.stdin
	r.cmd = nil
	r.stdin = nil
	r.decoder = nil
	if cmd == nil {
		return nil
	}
	if stdin != nil {
		_ = stdin.Close()
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return <-done
	}
}
