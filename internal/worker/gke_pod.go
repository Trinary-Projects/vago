package worker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultServiceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	defaultGKEAPITimeout     = 5 * time.Second
	safeToEvictAnnotation    = "cluster-autoscaler.kubernetes.io/safe-to-evict"
)

// GKEPodPatcher applies in-cluster pod annotations by talking to the
// Kubernetes API server directly. It mirrors Python's
// `GKEPodManager.set_safe_to_evict` — used to pin a pod while a call
// is active so the cluster autoscaler doesn't evict it mid-call.
//
// All inputs come from the in-cluster service-account projection at
// /var/run/secrets/kubernetes.io/serviceaccount (token, ca.crt,
// namespace). Outside Kubernetes those files are absent and
// NewGKEPodPatcher returns nil so callers can no-op gracefully.
type GKEPodPatcher struct {
	apiServer  string
	namespace  string
	token      string
	httpClient *http.Client
	logger     *log.Logger
}

// NewGKEPodPatcher builds a patcher from in-cluster credentials.
// Returns nil when the pod is not running inside Kubernetes (typical
// local dev) — that's the caller's signal to skip annotation work.
func NewGKEPodPatcher(logger *log.Logger) *GKEPodPatcher {
	saDir := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_ACCOUNT_DIR"))
	if saDir == "" {
		saDir = defaultServiceAccountDir
	}
	token, err := os.ReadFile(saDir + "/token")
	if err != nil {
		if logger != nil {
			logger.Printf("disha: GKE pod patcher disabled (no service-account token): %v\n", err)
		}
		return nil
	}
	caBytes, err := os.ReadFile(saDir + "/ca.crt")
	if err != nil {
		if logger != nil {
			logger.Printf("disha: GKE pod patcher disabled (no ca.crt): %v\n", err)
		}
		return nil
	}
	namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
	if namespace == "" {
		if data, err := os.ReadFile(saDir + "/namespace"); err == nil {
			namespace = strings.TrimSpace(string(data))
		}
	}
	if namespace == "" {
		if logger != nil {
			logger.Println("disha: GKE pod patcher disabled (namespace is empty)")
		}
		return nil
	}
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		if logger != nil {
			logger.Println("disha: GKE pod patcher disabled (KUBERNETES_SERVICE_HOST/PORT empty)")
		}
		return nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		if logger != nil {
			logger.Println("disha: GKE pod patcher disabled (could not parse ca.crt)")
		}
		return nil
	}
	return &GKEPodPatcher{
		apiServer: fmt.Sprintf("https://%s:%s", host, port),
		namespace: namespace,
		token:     strings.TrimSpace(string(token)),
		httpClient: &http.Client{
			Timeout: defaultGKEAPITimeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
			},
		},
		logger: logger,
	}
}

// SetSafeToEvict patches `metadata.annotations[cluster-autoscaler.kubernetes.io/safe-to-evict]`
// on the named pod. The Kubernetes "strategic merge patch" form keeps
// other annotations intact.
func (p *GKEPodPatcher) SetSafeToEvict(ctx context.Context, podName string, safe bool) error {
	if p == nil {
		return nil
	}
	if strings.TrimSpace(podName) == "" {
		return errors.New("disha: pod name is required")
	}
	value := "false"
	if safe {
		value = "true"
	}
	body := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, safeToEvictAnnotation, value)
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", p.apiServer, p.namespace, podName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewBufferString(body))
	if err != nil {
		return fmt.Errorf("disha: build pod patch request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
	req.Header.Set("Accept", "application/json")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("disha: pod patch request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("disha: pod patch returned %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if p.logger != nil {
		p.logger.Printf("disha: pod %s annotation %s=%s applied\n", podName, safeToEvictAnnotation, value)
	}
	return nil
}
