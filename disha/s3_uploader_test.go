package disha

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newRetryTestS3Uploader(transport http.RoundTripper) *S3Uploader {
	return &S3Uploader{
		accessKey:   "test-access",
		secretKey:   "test-secret",
		region:      "us-east-1",
		bucket:      "test-bucket",
		httpClient:  &http.Client{Transport: transport},
		retryDelays: []time.Duration{0, 0},
	}
}

func TestNewS3GetClientFromEnvUsesBucketRegion(t *testing.T) {
	t.Setenv("ACCESS_KEY_ID", "test-access")
	t.Setenv("SECRET_KEY_ID", "test-secret")
	t.Setenv("AWS_MAIN_REGION", "ap-south-1")
	t.Setenv("AWS_US_BUCKET_NAME", "us-bucket")
	t.Setenv("AWS_US_REGION", "us-east-1")

	client := NewS3GetClientFromEnv(nil, "AWS_US_BUCKET_NAME", "AWS_US_REGION")
	uploader, ok := client.(*S3Uploader)
	if !ok {
		t.Fatalf("client = %#v, want *S3Uploader", client)
	}
	if uploader.bucket != "us-bucket" || uploader.region != "us-east-1" {
		t.Fatalf("bucket=%q region=%q, want us-bucket/us-east-1", uploader.bucket, uploader.region)
	}
	if uploader.httpClient.Timeout != defaultS3RequestTimeout {
		t.Fatalf("request timeout=%s, want %s", uploader.httpClient.Timeout, defaultS3RequestTimeout)
	}
	if len(uploader.retryDelays) != 2 {
		t.Fatalf("retry delays=%v, want two retries", uploader.retryDelays)
	}
}

func TestNewS3GetClientFromEnvDisabledWhenRegionMissing(t *testing.T) {
	t.Setenv("ACCESS_KEY_ID", "test-access")
	t.Setenv("SECRET_KEY_ID", "test-secret")
	t.Setenv("AWS_US_BUCKET_NAME", "us-bucket")
	t.Setenv("AWS_US_REGION", "")

	if client := NewS3GetClientFromEnv(nil, "AWS_US_BUCKET_NAME", "AWS_US_REGION"); client != nil {
		t.Fatalf("client = %#v, want nil when region env is empty", client)
	}
}

func TestS3UploadRetriesTransportFailuresWithFreshBody(t *testing.T) {
	var attempts int
	var bodies []string
	uploader := newRetryTestS3Uploader(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		bodies = append(bodies, string(body))
		switch attempts {
		case 1:
			return nil, io.EOF
		case 2:
			return nil, context.DeadlineExceeded
		default:
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
			}, nil
		}
	}))

	if err := uploader.UploadJSON(context.Background(), "conversation_state/conv/chunk.json", map[string]any{"agenda": "introduction"}); err != nil {
		t.Fatalf("UploadJSON: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d, want 3", attempts)
	}
	if len(bodies) != 3 || bodies[0] != bodies[1] || bodies[1] != bodies[2] {
		t.Fatalf("request bodies differ across retries: %#v", bodies)
	}
}

func TestS3GetRetriesTransientResponse(t *testing.T) {
	var attempts int
	uploader := newRetryTestS3Uploader(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		status := http.StatusServiceUnavailable
		body := "<Error><Code>SlowDown</Code></Error>"
		if attempts == 3 {
			status = http.StatusOK
			body = `{"doctor":"dok-tor"}`
		}
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	}))

	body, err := uploader.GetObject(context.Background(), "", "phonetics.json")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d, want 3", attempts)
	}
	if string(body) != `{"doctor":"dok-tor"}` {
		t.Fatalf("body=%q", body)
	}
}

func TestS3UploadDoesNotRetryPermanentResponse(t *testing.T) {
	var attempts int
	uploader := newRetryTestS3Uploader(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Body:       io.NopCloser(strings.NewReader("<Error><Code>AccessDenied</Code></Error>")),
			Header:     make(http.Header),
		}, nil
	}))

	err := uploader.UploadJSON(context.Background(), "debug_log_data/conv/log_data.json", map[string]string{"type": "server-message"})
	if err == nil || !strings.Contains(err.Error(), "returned 403") {
		t.Fatalf("UploadJSON error=%v, want 403", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d, want 1 for permanent response", attempts)
	}
}

func TestS3RetryableResponses(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "request timeout", status: http.StatusRequestTimeout, want: true},
		{name: "throttled", status: http.StatusTooManyRequests, want: true},
		{name: "server error", status: http.StatusBadGateway, want: true},
		{name: "S3 request timeout code", status: http.StatusBadRequest, body: "<Code>RequestTimeout</Code>", want: true},
		{name: "bad request", status: http.StatusBadRequest, body: "<Code>InvalidRequest</Code>", want: false},
		{name: "not found", status: http.StatusNotFound, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableS3Response(tt.status, []byte(tt.body)); got != tt.want {
				t.Fatalf("isRetryableS3Response(%d, %q)=%t, want %t", tt.status, tt.body, got, tt.want)
			}
		})
	}
}

func TestS3UploadStopsWhenCallerContextIsCanceled(t *testing.T) {
	var attempts int
	uploader := newRetryTestS3Uploader(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		return nil, req.Context().Err()
	}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := uploader.UploadJSON(ctx, "conversation_state/conv/chunk.json", map[string]string{"agenda": "introduction"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("UploadJSON error=%v, want context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("transport attempts=%d, want 1 with no retry for canceled context", attempts)
	}
}
