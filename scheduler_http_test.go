package outrunner

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

func TestGroupedPollRetryLoggerClassifiesStaleHTTPRequests(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	client := newGroupedPollRetryClient(logger)
	client.RetryMax = 0
	client.HTTPClient.Timeout = time.Second

	staleCtx, staleCancel := context.WithCancelCause(context.Background())
	staleReq, err := retryablehttp.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	staleReq = staleReq.WithContext(staleCtx)
	staleResult := make(chan error, 1)
	go func() {
		_, err := client.Do(staleReq)
		staleResult <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("local HTTP server did not receive stale poll")
	}
	staleCancel(errStaleGroupPoll)
	select {
	case err := <-staleResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stale request error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stale HTTP request did not return after cancellation")
	}
	if got := logs.String(); !strings.Contains(got, "DEBUG") || strings.Contains(got, "ERROR") {
		t.Fatalf("stale cancellation log level = %q, want DEBUG and no ERROR", got)
	}

	logs.Reset()
	server.Close()
	networkClient := newGroupedPollRetryClient(logger)
	networkClient.RetryMax = 0
	networkClient.HTTPClient.Transport = pollRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("ordinary transport failure")
	})
	networkReq, err := retryablehttp.NewRequest(http.MethodGet, "http://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, networkErr := networkClient.Do(networkReq)
	if networkErr == nil {
		t.Fatal("ordinary transport failure returned nil error")
	}
	if got := logs.String(); !strings.Contains(got, "level=ERROR msg=\"request failed\"") {
		t.Fatalf("ordinary network-error log = %q, want request failure at ERROR", got)
	}
}

type pollRoundTripFunc func(*http.Request) (*http.Response, error)

func (f pollRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
