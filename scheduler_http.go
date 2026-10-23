package outrunner

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-retryablehttp"
)

// newGitHubRetryableHTTPClient routes retry logs through slog without letting
// the SDK's one-off admin-connection retry policy leak into later requests.
func newGitHubRetryableHTTPClient(logger *slog.Logger) *retryablehttp.Client {
	client := retryablehttp.NewClient()
	client.Logger = logger
	client.RequestLogHook = func(_ retryablehttp.Logger, request *http.Request, _ int) {
		if !strings.HasSuffix(request.URL.Path, "/actions/runner-registration") {
			client.CheckRetry = retryablehttp.DefaultRetryPolicy
			client.ErrorHandler = nil
		}
	}
	return client
}

// GitHubHTTPOptions configures SDK and retryable HTTP logs with a fresh client.
func GitHubHTTPOptions(logger *slog.Logger) []scaleset.HTTPOption {
	return []scaleset.HTTPOption{
		scaleset.WithLogger(logger),
		scaleset.WithRetryableHTTPClint(newGitHubRetryableHTTPClient(logger)),
	}
}

type stalePollLog struct {
	mu     sync.Mutex
	logger *slog.Logger
	stale  map[*url.Error]struct{}
}

func (l *stalePollLog) mark(err error) {
	urlErr, ok := err.(*url.Error)
	if !ok {
		return
	}
	l.mu.Lock()
	l.stale[urlErr] = struct{}{}
	l.mu.Unlock()
}

func (l *stalePollLog) Error(message string, args ...any) {
	if len(args) >= 2 && args[0] == "error" {
		if requestErr, ok := args[1].(*url.Error); ok {
			l.mu.Lock()
			_, intentional := l.stale[requestErr]
			delete(l.stale, requestErr)
			l.mu.Unlock()
			if intentional {
				l.logger.Debug(message, args...)
				return
			}
		}
	}
	l.logger.Error(message, args...)
}

func (l *stalePollLog) Info(message string, args ...any)  { l.logger.Info(message, args...) }
func (l *stalePollLog) Debug(message string, args ...any) { l.logger.Debug(message, args...) }
func (l *stalePollLog) Warn(message string, args ...any)  { l.logger.Warn(message, args...) }

func newGroupedPollRetryClient(logger *slog.Logger) *retryablehttp.Client {
	logBridge := &stalePollLog{logger: logger, stale: make(map[*url.Error]struct{})}
	retryClient := retryablehttp.NewClient()
	retryClient.Logger = logBridge
	defaultCheckRetry := retryClient.CheckRetry
	retryClient.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		if isStaleGroupPoll(ctx) && (errors.Is(err, context.Canceled) || errors.Is(err, errStaleGroupPoll)) {
			logBridge.mark(err)
		}
		return defaultCheckRetry(ctx, resp, err)
	}
	return retryClient
}

// GroupedPollHTTPOption classifies only stale offer cancellations as debug HTTP logs.
func GroupedPollHTTPOption(logger *slog.Logger) scaleset.HTTPOption {
	return scaleset.WithRetryableHTTPClint(newGroupedPollRetryClient(logger))
}
