package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

func TestLabelsMatch(t *testing.T) {
	label := func(names ...string) []scaleset.Label {
		var labels []scaleset.Label
		for _, n := range names {
			labels = append(labels, scaleset.Label{Name: n, Type: "User"})
		}
		return labels
	}

	tests := []struct {
		name     string
		existing []scaleset.Label
		desired  []scaleset.Label
		want     bool
	}{
		{"identical", label("linux", "x64"), label("linux", "x64"), true},
		{"same set different order", label("x64", "linux"), label("linux", "x64"), true},
		{"different labels", label("linux"), label("windows"), false},
		{"extra label", label("linux", "x64"), label("linux"), false},
		{"missing label", label("linux"), label("linux", "x64"), false},
		{"both empty", nil, nil, true},
		{"existing empty", nil, label("linux"), false},
		{"desired empty", label("linux"), nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := labelsMatch(tt.existing, tt.desired)
			if got != tt.want {
				t.Errorf("labelsMatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSIGTERMShutsDownListener(t *testing.T) {
	if os.Getenv("OUTRUNNER_SIGTERM_HELPER") == "1" {
		rootCmd.SetArgs([]string{"--url", os.Getenv("OUTRUNNER_SIGTERM_URL"), "--token", "token", "--config", os.Getenv("OUTRUNNER_SIGTERM_CONFIG")})
		if err := rootCmd.Execute(); err != nil {
			os.Exit(3)
		}
		os.Exit(0)
	}

	config := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(config, []byte("runners:\n  test:\n    labels: [linux]\n    docker:\n      image: test:latest\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requests := make(chan string, 16)
	var serverURL string
	adminJWT := "eyJhbGciOiJub25lIn0.eyJleHAiOjQ3MjQ0NjA4MDB9."
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method + " " + r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/orgs/org/actions/runners/registration-token"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"local-registration-token"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/actions/runner-registration":
			_, _ = w.Write([]byte(`{"url":"` + serverURL + `/api/v3","token":"` + adminJWT + `"}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/_apis/runtime/runnerscalesets"):
			_, _ = w.Write([]byte(`{"count":0,"value":[]}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_apis/runtime/runnerscalesets") && !strings.Contains(r.URL.Path, "/sessions"):
			_, _ = w.Write([]byte(`{"id":17,"name":"test","runnerGroupId":1,"labels":[{"type":"User","name":"linux"}]}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_apis/runtime/runnerscalesets/17/sessions"):
			_, _ = w.Write([]byte(`{"sessionId":"00000000-0000-4000-8000-000000000001","ownerName":"test","runnerScaleSet":{"id":17,"name":"test"},"messageQueueUrl":"` + serverURL + `/queue","messageQueueAccessToken":"queue-token","statistics":{"totalAssignedJobs":0,"totalAvailableJobs":0,"totalRunningJobs":0,"totalRegisteredRunners":0,"totalBusyRunners":0,"totalIdleRunners":0}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/queue":
			<-r.Context().Done()
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/sessions/"):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()
	serverURL = server.URL
	cmd := exec.Command(os.Args[0], "-test.run=^TestSIGTERMShutsDownListener$")
	cmd.Env = append(os.Environ(), "OUTRUNNER_SIGTERM_HELPER=1", "OUTRUNNER_SIGTERM_URL="+server.URL+"/org", "OUTRUNNER_SIGTERM_CONFIG="+config, "GITHUB_TOKEN=token", "DOCKER_HOST=unix:///nonexistent")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	queueStarted := false
	deadline := time.After(10 * time.Second)
	for !queueStarted {
		select {
		case request := <-requests:
			queueStarted = request == "GET /queue"
		case <-deadline:
			_ = cmd.Process.Kill()
			t.Fatal("outrunner did not start polling the message queue")
		}
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("outrunner did not shut down cleanly after SIGTERM: %v", err)
	}
}
