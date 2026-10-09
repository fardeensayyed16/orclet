package worker

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/fardeensayyed16/orca/pkg/types"
)

func TestNormalizedProbeAppliesDefaultsAndRejectsInvalidPeriod(t *testing.T) {
	probe, err := normalizedProbe(&types.Probe{Type: "http", Port: 8080}, "liveness")
	if err != nil {
		t.Fatal(err)
	}
	if probe.Kind != "liveness" || probe.Path != "/" || probe.PeriodSeconds != 10 ||
		probe.TimeoutSeconds != 1 || probe.FailureThreshold != 3 || probe.SuccessThreshold != 1 {
		t.Fatalf("probe defaults = %+v", probe)
	}
	if _, err := normalizedProbe(&types.Probe{Type: "tcp", Port: 80, PeriodSeconds: -1}, "readiness"); err == nil {
		t.Fatal("negative period unexpectedly accepted")
	}
}

func TestHTTPProbeAccepts200To399Only(t *testing.T) {
	for _, test := range []struct {
		status int
		want   bool
	}{
		{http.StatusOK, true},
		{http.StatusMultipleChoices, true},
		{http.StatusBadRequest, false},
		{http.StatusInternalServerError, false},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(test.status)
		}))
		host, portString, err := net.SplitHostPort(server.Listener.Addr().String())
		if err != nil {
			server.Close()
			t.Fatal(err)
		}
		port, err := strconv.Atoi(portString)
		if err != nil {
			server.Close()
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		got, probeErr := runHTTPProbe(ctx, host, port, "/health")
		cancel()
		server.Close()
		if probeErr != nil {
			t.Fatalf("HTTP status %d probe error: %v", test.status, probeErr)
		}
		if got != test.want {
			t.Errorf("HTTP status %d success = %v, want %v", test.status, got, test.want)
		}
	}
}

func TestTCPProbeUsesPublishedPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			_ = connection.Close()
		}
	}()
	host, portString, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := runTCPProbe(ctx, host, port)
	if err != nil || !success {
		t.Fatalf("TCP probe success=%v, err=%v", success, err)
	}
}

func TestProbeDueHonorsInitialDelayAndPeriod(t *testing.T) {
	now := time.Now()
	probe := &types.Probe{InitialDelaySeconds: 5, PeriodSeconds: 10}
	if probeDue(now, now, time.Time{}, probe) {
		t.Fatal("probe ran during initial delay")
	}
	startedAt := now.Add(-6 * time.Second)
	if !probeDue(now, startedAt, time.Time{}, probe) {
		t.Fatal("first probe should be due after initial delay")
	}
	if probeDue(now, startedAt, now.Add(-9*time.Second), probe) {
		t.Fatal("probe ran before period elapsed")
	}
	if !probeDue(now, startedAt, now.Add(-10*time.Second), probe) {
		t.Fatal("probe should be due after period elapsed")
	}
}
