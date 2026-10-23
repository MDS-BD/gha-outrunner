package outrunner

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-retryablehttp"
)

func TestGitHubHTTPOptionsLogSDKRootAndSessionRequests(t *testing.T) {
	var registrationRequests atomic.Int32
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var apiRequests atomic.Int32
	var rootGets atomic.Int32
	var queueRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/runners/registration-token"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"registration"}`))
		case strings.HasSuffix(r.URL.Path, "/actions/runner-registration"):
			if registrationRequests.Add(1) == 1 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusForbidden)
				return
			}
			_, _ = fmt.Fprintf(w, `{"url":%q,"token":%q}`, serverURL(r)+"/tenant/123/", testActionsToken())
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/runnerscalesets/1"):
			apiRequests.Add(1)
			_, _ = w.Write([]byte(`{"id":1,"name":"runner"}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/runnerscalesets/1/sessions"):
			_, _ = fmt.Fprintf(w, `{"sessionId":"00000000-0000-0000-0000-000000000001","messageQueueUrl":%q,"messageQueueAccessToken":"queue-token"}`, serverURL(r)+"/queue")
		case r.URL.Path == "/queue":
			if queueRequests.Add(1) == 1 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/runnerscalesets"):
			apiRequests.Add(1)
			if rootGets.Add(1) == 1 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write([]byte(`{"count":0,"value":[]}`))
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	retryClient := newGitHubRetryableHTTPClient(logger)
	retryClient.RetryWaitMin = time.Millisecond
	retryClient.RetryWaitMax = time.Millisecond
	client, err := scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     server.URL + "/org",
		PersonalAccessToken: "pat",
	}, scaleset.WithLogger(logger), scaleset.WithRetryableHTTPClint(retryClient))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := client.GetRunnerScaleSet(ctx, 1, "runner"); err != nil {
		t.Fatalf("root GET: %v", err)
	}
	if _, err := client.UpdateRunnerScaleSet(ctx, 1, &scaleset.RunnerScaleSet{ID: 1, Name: "runner"}); err != nil {
		t.Fatalf("root PATCH: %v", err)
	}
	if got := apiRequests.Load(); got != 3 {
		t.Fatalf("root API request count = %d, want 3 including the retried GET", got)
	}
	if got := registrationRequests.Load(); got != 2 {
		t.Fatalf("admin registration requests = %d, want 2 after 403 retry", got)
	}
	listenerLogger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	session, err := client.MessageSessionClient(ctx, 1, "runner", GitHubHTTPOptions(listenerLogger.WithGroup("listener.http"))...)
	if err != nil {
		t.Fatalf("create message session: %v", err)
	}
	defer func() { _ = session.Close(context.Background()) }()
	if _, err := session.GetMessage(ctx, 0, 1); err != nil {
		t.Fatalf("session GET: %v", err)
	}
	if got := logs.String(); !strings.Contains(got, `method=GET`) || !strings.Contains(got, `listener.http.request`) {
		t.Fatalf("session retry logs = %q, want listener.http GET routed through slog", got)
	}
	if _, err := client.MessageSessionClient(ctx, 1, "quiet-runner", GitHubHTTPOptions(slog.New(slog.DiscardHandler))...); err != nil {
		t.Fatalf("create isolated session: %v", err)
	}
	if got := logs.String(); !strings.Contains(got, `method=GET`) {
		t.Fatalf("session logger did not receive request GET log: %s", got)
	}
}
func TestGitHubHTTPOptionsKeepOrdinaryRootStatusFromAdminRetryPolicy(t *testing.T) {
	var scaleSetRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/runners/registration-token"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"registration"}`))
		case strings.HasSuffix(r.URL.Path, "/actions/runner-registration"):
			_, _ = fmt.Fprintf(w, `{"url":%q,"token":%q}`, serverURL(r)+"/tenant/123/", testActionsToken())
		case strings.HasSuffix(r.URL.Path, "/runnerscalesets"):
			scaleSetRequests.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     server.URL + "/org",
		PersonalAccessToken: "pat",
	}, GitHubHTTPOptions(slog.New(slog.DiscardHandler))...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetRunnerScaleSet(context.Background(), 1, "runner"); err == nil {
		t.Fatal("GetRunnerScaleSet returned nil error for 401")
	}
	if got := scaleSetRequests.Load(); got != 1 {
		t.Fatalf("ordinary root 401 attempts = %d, want 1 (SDK admin retry policy leaked)", got)
	}
}

func TestGitHubHTTPOptionsExposeGenuineSessionErrors(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/runners/registration-token"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"registration"}`))
		case strings.HasSuffix(r.URL.Path, "/actions/runner-registration"):
			_, _ = fmt.Fprintf(w, `{"url":%q,"token":%q}`, serverURL(r)+"/tenant/123/", testActionsToken())
		case strings.HasSuffix(r.URL.Path, "/runnerscalesets/1/sessions") && r.Method == http.MethodPost:
			_, _ = fmt.Fprintf(w, `{"sessionId":"00000000-0000-0000-0000-000000000001","messageQueueUrl":%q,"messageQueueAccessToken":"queue-token"}`, serverURL(r)+"/queue")
		case r.URL.Path == "/queue":
			close(started)
			<-r.Context().Done()
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     server.URL + "/org",
		PersonalAccessToken: "pat",
	}, GitHubHTTPOptions(logger)...)
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.MessageSessionClient(context.Background(), 1, "runner", GitHubHTTPOptions(logger)...)
	if err != nil {
		t.Fatalf("create message session: %v", err)
	}
	defer func() { _ = session.Close(context.Background()) }()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := session.GetMessage(ctx, 0, 1)
		result <- err
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("local queue endpoint did not receive session GET")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("GetMessage returned nil error after request cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetMessage did not return after request cancellation")
	}
	if got := logs.String(); !strings.Contains(got, "level=ERROR msg=\"request failed\"") {
		t.Fatalf("genuine session request error log = %q, want ERROR request failed", got)
	}
}

func TestGitHubHTTPOptionsKeepSessionRetriesSeparateFromAdminRetries(t *testing.T) {
	var queueRequests atomic.Int32
	var refreshRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/runners/registration-token"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"registration"}`))
		case strings.HasSuffix(r.URL.Path, "/actions/runner-registration"):
			_, _ = fmt.Fprintf(w, `{"url":%q,"token":%q}`, serverURL(r)+"/tenant/123/", testActionsToken())
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/runnerscalesets/1/sessions"):
			_, _ = fmt.Fprintf(w, `{"sessionId":"00000000-0000-0000-0000-000000000001","messageQueueUrl":%q,"messageQueueAccessToken":"queue-token"}`, serverURL(r)+"/queue")
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/runnerscalesets/1/sessions/00000000-0000-0000-0000-000000000001"):
			refreshRequests.Add(1)
			_, _ = fmt.Fprintf(w, `{"sessionId":"00000000-0000-0000-0000-000000000001","messageQueueUrl":%q,"messageQueueAccessToken":"queue-token"}`, serverURL(r)+"/queue")
		case r.URL.Path == "/queue":
			if queueRequests.Add(1) == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     server.URL + "/org",
		PersonalAccessToken: "pat",
	}, GitHubHTTPOptions(slog.New(slog.DiscardHandler))...)
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.MessageSessionClient(context.Background(), 1, "runner", GitHubHTTPOptions(slog.New(slog.DiscardHandler))...)
	if err != nil {
		t.Fatalf("create message session: %v", err)
	}
	defer func() { _ = session.Close(context.Background()) }()
	if _, err := session.GetMessage(context.Background(), 0, 1); err != nil {
		t.Fatalf("GetMessage after token refresh: %v", err)
	}
	if got := queueRequests.Load(); got != 2 {
		t.Fatalf("message queue requests = %d, want 2 (one 401, then one after session refresh)", got)
	}
	if got := refreshRequests.Load(); got != 1 {
		t.Fatalf("message session refresh requests = %d, want 1", got)
	}
}

func TestGitHubHTTPOptionsRespectConfiguredLoggerLevel(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	client := newGitHubRetryableHTTPClient(logger)
	client.RetryMax = 0
	client.HTTPClient.Transport = pollRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Status:     "202 Accepted",
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	})
	req, err := retryablehttp.NewRequest(http.MethodGet, "http://example.test/queue", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Do(req); err != nil {
		t.Fatalf("retryable GET: %v", err)
	}
	if got := logs.String(); got != "" {
		t.Fatalf("retry logs at WARN threshold = %q, want no DEBUG or INFO entries", got)
	}
}
func serverURL(r *http.Request) string {
	return "http://" + r.Host
}

func testActionsToken() string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	claims := fmt.Sprintf(`{"exp":%d}`, time.Now().Add(10*time.Minute).Unix())
	payload := base64.RawURLEncoding.EncodeToString([]byte(claims))
	return header + "." + payload + ".eA"
}
